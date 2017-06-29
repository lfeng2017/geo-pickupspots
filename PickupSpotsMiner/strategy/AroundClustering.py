# -*- coding: utf-8 -*-

'''
上车点推荐策略封装类

'''

import itertools
import json
import logging
import re
import sys
from collections import deque
import time
import arrow
import geopy
import numpy as np
import pandas as pd
from geopy.distance import vincenty
from pyspark.sql import Row
from scipy.spatial.distance import squareform, pdist
from sklearn.cluster import DBSCAN


sys.path.append("../")
sys.path.append("../common/")
from common.AmapUtil import Regeo, Around
import common.GeoUtil as GeoUtil


class AroundClusteringBatch():
    '''
    基于历史周边点的上车点推荐策略

    '''

    def __init__(self):
        # 初始化dbscan算法，半径30米（使用自定义距离度量），最小支持频次1（因为出现点已按geohash聚过类，20米之内不可能有重复点）
        self.model = DBSCAN(eps=30, min_samples=1, metric='precomputed')

    def selectKBest(self, row):
        '''
        入口一：基于密度距离和高德周边搜索命名，选出最合适的推荐上车点

        :param row: spark Row对象（为兼容pyspark并行），格式如下：
                Row(exp_geohash=期望上车点geohash,
                      exp_lat=期望上车点纬度,
                      exp_lng=期望上车点经度,
                      arounds=周边点字符串，格式："geohash_1:cnt_1:lat_1:lng_1\tgeohash_2:cnt_2:lat_2:lng_2",
                      cooccurs=共现点字符串，格式："geohash_1:cnt_1:lat_1:lng_1\tgeohash_2:cnt_2:lat_2:lng_2")


        :return: 筛选后的spark Row对象，格式如下：
                Row(startPos=期望上车点geohash,
                   lat=期望上车点纬度,
                   lng=期望上车点经度,
                   radius=推荐点半径,
                   num=推荐点个数,
                   update=生成的时间戳,
                   recomms=[
                        {
                            "geohash": XXXX,    # 推荐点的geohash
                            "lat": XX,          # 推荐点纬度
                            "lng": XX,          # 推荐点经度
                            "score": 0~1.0，     # 推荐点分数
                            "count": 10,        # 出现次数（跟权重相关）
                            "status": 0,        # 推荐点状态（内部标识使用）
                            "ext": "cooccur",   # 点类型标示，标识共现点用于提权
                            "tag": XXX,         # 策略版本标识，用于a/b test效果对比
                            "show": 0,          # 推荐点展示次数，内部使用
                            "pick": 0,          # 推荐点被选中次数，内部使用
                        }
                   ]
        '''

        t = time.time()

        # 字符串类型转换
        exp_geohash = str(row.exp_geohash)
        lat = float(row.exp_lat)
        lng = float(row.exp_lng)

        # '\t' 分割的字符串（ n个 "geohash:lat:lng:count" ）周边上车点，解析为list of dict
        arounds = self.__extract(str(row.arounds), tag="around")

        # geohash lv=6 关联的周边点，查不到周边数据, 则返回Null
        if not arounds or len(arounds) == 0:
            return Row(startPos="", lat=None, lng=None, radius=None, num=None, update=None, recomms=None)

        # '\t' 分割的字符串（ n个 "geohash:lat:lng:count" ）共现上车点，解析为list of dict
        cooccurs = self.__extract(str(row.cooccurs), tag="cooccur")

        # 识别出周边点中属于共现的，单独标记 （周边点中，可能包括共现点，通过点的geohash判断）
        for pos in arounds:
            if len(filter(lambda x: x['geohash'] == pos['geohash'], cooccurs)) > 0:
                pos['tag'] = "cooccur"

        # 按半径逐步扩大，筛选候选集（近距离能取到则不再往外扩）
        radius, recomms = self.__getCandidatesByRadius(lat, lng, arounds)

        # 如果周边均无点，则返回空值的spark Row对象
        if len(recomms) == 0:
            return Row(startPos="", lat=None, lng=None, radius=None, num=None, update=None, recomms=None)
            return self.__dumpFormat(exp_geohash, lat, lng)

        # 根据推荐点的出现频次的统计量，判断是否需要根据统计量再过滤推荐点（低频次的当做噪声）
        recomms = self.__filterByStat(recomms)
        filterSize = len(recomms)

        # 密度聚类, 根据聚类标签合并同一个区域的推荐点
        recomms = self.__clustering(recomms, lat, lng)

        # 计算推荐点score （目前主要是根据频次来折算）
        recomms = self.__weighting(recomms)

        logging.debug("selectKBest arounds={a_sz} filtered={f_sz} outPos={o_sz} elasped={t}" \
                      .format(a_sz=len(arounds), f_sz=filterSize, o_sz=len(recomms), t=(time.time() - t)))

        # 输出的推荐点格式化, Spark Row类型
        return self.__dumpFormat(exp_geohash, lat, lng, radius, recomms)

    def selectBestName(self, priorities, listOfPoi,filtSet):
        '''
        入口二：根据poi类型和距离，选择最优的poi名称
        根据poi类型的优先级, 取附近poi的名称

        :param priorities: 优先选取的poi类型list
        :param listOfPoi: 高德api请求返回的周边搜索poi信息
        :return: 决策的名称字符串
        '''

        '  '
        if not listOfPoi or len(listOfPoi) == 0:
            return ""

        # 遍历期望的poi类型list，如果有类型match的poi，优先选取此类poi的名称
        # 如果同一个poi类型下匹配到多个名称, 优先选取距离最近的
        for pri in priorities:
            # 类型吻合, 距离在50米内
            pattern = re.compile(r'^' + pri)
            candidates = filter(lambda poi: pattern.match(poi['type']) and float(poi['distance']) <= 50 and poi['name'] not in filtSet, listOfPoi)
            # 统一类型找到多个，优先取距离最近的
            if len(candidates) > 0:
                candidates.sort(key=lambda x: float(x['distance']))
                nearest = candidates[0]
                # 距离大于20米，名称后缀加附近2个字
                name = nearest['name'] if float(nearest['distance']) <= 20 else (nearest['name'] + u"附近")
                return name
        else:
            # 都不满足,直接取最近的poi名称
            listOfPoi.sort(key=lambda x: float(x['distance']))
            nearest = listOfPoi[0]
            name = nearest['name'] if float(nearest['distance']) <= 20 else (nearest['name'] + u"附近")
            return name

    def amapRequestBatch(self, listOfRow):
        '''
        入口三：批量调用阿里api, 进行周边搜索，获取推荐点名称
        '''

        # 提取上车点，及名称
        t = time.time()

        # 输入的Row转换为dict，过滤出有推荐点的数据
        listOfRow = filter(lambda x: x.get("recomms", None) is not None, map(lambda x: x.asDict(), listOfRow))
        for row in listOfRow:
            # 为了兼顾spark, recomms字段存档是json字符串
            row['recomms'] = json.loads(row['recomms'])

        # 将上车点展开为list
        startPoints = map(lambda x: geopy.Point(x['lat'], x['lng']), listOfRow)
        # 调用高德接口，批量获取上车点的逆地址名称
        startNames = Regeo.getNameBatch(startPoints)
        # 如果返回结果的个数和输入结果的格式不一致，说明有请求挂掉
        if len(startPoints) != len(startNames):
            raise RuntimeError("amapRequestBatch startPts:{sp} != startNames:{rn}"
                               .format(sp=len(startPoints), rn=len(startNames)))

        # 将上车点下挂的推荐点,展开为list（假设3个上车点，每个点推荐10个点，则展开为30个点）
        # 备注：因为高德api每次传入20个效率是最高的，因此展开后再传入，减少接口请求次数
        recommPts = map(lambda x: geopy.Point(round(x['lat'], 6), round(x['lng'], 6)),
                        itertools.chain(*map(lambda x: x['recomms'], listOfRow)))
        # 取名称时，poi类型的优先顺序
        poiType = ["10", "99", "1507", "1505", "1508", "1506", "1504", "1502", "120302", "1903"]
        # 使用封装后的高德批量搜索接口，获取推荐点名称list（每1个推荐点可能会出多个poi）
        listOfPois = Around.batch([p for p in recommPts], poiType='|'.join(poiType))

        recommNames = deque()
        filtSet = set()
        for i, pois in enumerate(listOfPois):
            # 针对高德返回的每一个推荐点的poi列表，选取最优的名称
            name = self.selectBestName(poiType, pois,filtSet)
            filtSet.add(name)
            # 周边搜索找不到, 调用regeo补齐
            if name == "":
                name = Regeo.getName(recommPts[i]) + u"-推荐上车点"
            recommNames.append(name)

        # 如果最后的名称list和推荐点list个数对不上，命名时有请求异常
        # 备注：因为是批量处理，在长度一致的情况下，需要用数组的offset进行点和名称的组合匹配
        if len(recommPts) != len(recommNames):
            raise RuntimeError("amapRequestBatch recommPts:{rp} != recommPtsName:{rn}"
                               .format(rp=len(recommPts), rn=len(recommNames)))

        # 按照list的展开顺序, 组合最终结果
        out = []
        for i, row in enumerate(listOfRow):
            # 补齐该上车点对应各推荐点的名称，推荐点list 和 名称list，按下标匹配 （2个list正常情况下长度是一致的）
            for recomm in row['recomms']:
                recomm['name'] = recommNames.popleft()
                recomm['lat'] = round(recomm['lat'], 6)
                recomm['lng'] = round(recomm['lng'], 6)
            # 补齐上车点的regeo名称
            row['regeo'] = startNames[i]
            row['recomms'] = json.dumps(row['recomms'])
            # dict to Row
            out.append(Row(**row))

        logging.debug("amapRequestBatch -> listOfRows={r_sz} listOfPois={p_sz} elapsed={t}" \
                      .format(r_sz=len(listOfRow), p_sz=len(listOfPois), t=(time.time() - t)))

        return out

    def selectKBestBatch(self, partition, skipAmap=False):
        '''
        入口一（目前未使用）: 供spark mapPatition使用
        '''
        tmpResults = []
        for row in partition:
            obj = self.selectKBest(row)
            if obj.startPos == "":
                continue
            else:
                tmpResults.append(self.selectKBest(row))
            # 每20条输出一次
            if len(tmpResults) >= 20:
                if not skipAmap:
                    tmpResults = self.amapRequestBatch(tmpResults)
                for record in tmpResults:
                    yield record
                tmpResults = []
        else:
            # 输出剩余的部分
            if len(tmpResults) > 0:
                if not skipAmap:
                    tmpResults = self.amapRequestBatch(tmpResults)
                for record in tmpResults:
                    yield record

    def __extract(self, strOfData, tag=None):
        '''
        将hdfs导出的周边点数据数据，转换为list of dict

        :param strOfData: '\t'分割的推荐点集合字符串，格式："geohash_1:cnt_1:lat_1:lng_1\tgeohash_2:cnt_2:lat_2:lng_2"
        :param tag: 此类推荐点的标示字符串，作为dict对象的1个属性加进去
        :return: list of dict， [ {"geohash":XXX, "cnt": 1, "lat": xx.xx, "lng": xx.xx, "tag": XX} ]
        '''

        ' hive 中间数据转换为 object list'
        if not strOfData or len(strOfData) == 0:
            return []
        out = list()
        try:
            for pos in strOfData.split("\t"):
                startPos = dict()
                geohash, cnt, lat, lng = pos.split(":")
                # out.append((geohash, cnt, lat, lng))
                # continue
                startPos['geohash'] = geohash
                startPos['cnt'] = int(cnt)
                startPos['lat'] = float(lat)
                startPos['lng'] = float(lng)
                startPos['tag'] = tag
                out.append(startPos)

        except Exception, e:
            return []
        return out

    def __getCandidatesByRadius(self, lat, lng, listOfPoints):
        '''
        按球面半径逐步过大半径，优先选取距离进的点集合

        :param lat: 期望上车点纬度
        :param lng: 期望上车点经度
        :param listOfPoints: list of dict， dict存的推荐点信息
        格式：[ {"geohash":XXX, "cnt": 1, "lat": xx.xx, "lng": xx.xx, "tag": XX} ]

        :return: 经过筛选的 listOfPoints
        '''

        t = time.time()

        out = []
        # 逐步试探是否有结果
        for radius in (100, 150, 200, 300, 500, 1000):
            for pos in listOfPoints:
                pos['dist'] = vincenty((lat, lng), (pos['lat'], pos['lng'])).meters
                # 距离判断
                if pos['dist'] <= radius:
                    out.append(pos)
            # 最近的半径有点，则直接返回
            if len(out) > 0:
                # out.sort(key=lambda x: x['count'], reverse=True)
                logging.debug("getCandidatesByRadius elasped={}".format(time.time() - t))
                return radius, out
        else:
            logging.debug("getCandidatesByRadius elasped={}".format(time.time() - t))
            return radius, out

    def __filterByStat(self, listOfPoints):
        '''
        根据推荐点list中的点出现频次（点按geohash做过聚合），判断拉链中的点频次差异是否过大
        若过大则频次的中位数进行过滤

        :param listOfPoints: list of dict， dict存的推荐点信息
        格式：[ {"geohash":XXX, "cnt": 1, "lat": xx.xx, "lng": xx.xx, "tag": XX} ]

        :return:  经过筛选的 listOfPoints
        '''

        t = time.time()

        df = pd.DataFrame(listOfPoints)

        # 对于出现频次不均衡的情况，按出现频次的中位数进行过滤
        std = df['cnt'].std()
        mean = df['cnt'].mean()
        if mean < std:
            median = df['cnt'].median()
            setOfGeohash = df[df['cnt'] >= median]['geohash'].values
        else:
            setOfGeohash = df['geohash'].values

        out = [pt for pt in listOfPoints if pt['geohash'] in setOfGeohash]

        logging.debug("filterByStat 2nd Filter={b} elasped={t}".format(b=(mean < std), t=(time.time() - t)))

        return out

    def __clustering(self, listOfPoints, lat, lng):
        '''
        使用sklearn的dbscan进行密度聚类，根据聚类label合并统一区域的推荐点
        用区域内的出现频次加权均值坐标，作为实际的推荐点

        :param listOfPoints: list of dict， dict存的推荐点信息
        格式：[ {"geohash":XXX, "cnt": 1, "lat": xx.xx, "lng": xx.xx, "tag": XX} ]

        :param lat: 期望上车点的纬度
        :param lng: 期望上车点的经度
        :return: 经过聚合的推荐点 list of dict, 新增id、tag、dist字段
        格式：[ {"id":XXX, "cnt": 1, "lat": xx.xx, "lng": xx.xx, "tag": XX, "tag": "cooccur"/"around", "dist":XX 米} ]

        '''

        t = time.time()

        # 为了降低运算量，经过geohash聚合的点，每个点仅当做1个sample参与dbscan运算
        X = [(pos['lat'], pos['lng']) for pos in listOfPoints]

        # 安装geohash内的出现频次展开点数据，用于求聚类区域内的加权平均坐标，作为1个区域的代表推荐点
        data = [(pos['lat'], pos['lng'], pos['cnt'], pos['tag']) for pos in listOfPoints]

        # 自定义距离函数, 供dbscan作为距离度量
        distance_matrix = squareform(
            pdist(X, lambda x, y: vincenty((x[0], x[1]), (y[0], y[1])).meters))
        dbscan = self.model.fit(distance_matrix)

        # 将聚类的标签融合至原对象
        data = pd.DataFrame(data, columns=['lat', 'lng', 'cnt', 'tag'])
        data['cluster'] = dbscan.labels_

        # 为了计算聚类后的点类型，转换为0，1格式，按sum是否>0来判断 （即：区域内只要有1个共现点，认为此区域都属于共现的类型）
        data['tag'] = np.where(data['tag'] == "cooccur", 1, 0)

        # 区域内的推荐坐标,按点实际出现次数扩展点个数后，再求均值, 确保落在最热点的位置
        data = data.to_dict(orient='records')
        extData = []
        for pos in data:
            # 按出现频次扩展点数量
            extData.extend([x for x in itertools.repeat(
                [pos['lat'], pos['lng'], pos['tag'], pos['cluster']],
                int(pos['cnt'])
            )])

        data = pd.DataFrame(extData, columns=['lat', 'lng', 'tag', 'cluster'])

        # 按聚类标签，聚合统一区域内的点坐标
        grouped = data.groupby("cluster")
        data = grouped['lat', 'lng'].mean()

        # 如果1个cluster中包含了coocurr的点，则认为是coocurr类型，后续计算权重会提升权重
        data['tag'] = grouped['tag'].sum()
        data['tag'] = np.where(data['tag'] > 0, "cooccur", "around")

        data['cnt'] = grouped.size()

        # 聚类点与起始坐标的距离， 供排序使用（目前未使用）
        data['dist'] = data.apply(lambda x: vincenty((lat, lng), (x['lat'], x['lng'])).meters, axis=1)

        # 推荐点编码9位geohash作为id（正负4.7米)
        data['id'] = data.apply(lambda x: GeoUtil.geohash_encoding(x['lat'], x['lng'])[0:9], axis=1)

        # 转换为对象list
        out = data.to_dict(orient='records')

        # for idx, row in data.iterrows():
        # 	obj = dict()
        # 	obj['lat'] = row.lat
        # 	obj['lng'] = row.lng
        # 	obj['cnt'] = row.cnt
        # 	obj['dist'] = row.dist
        # 	obj['tag'] = row.tag
        # 	out.append(obj)

        logging.debug("clustering size={s} elasped={t}".format(s=len(X), t=(time.time() - t)))
        return out

    def __weighting(self, listOfPoints):
        '''
        计算推荐点的权重，目前是按累计区域内累计出现频次的log计算
        对于属于共现类型的点（聚类区域内任意点属于共现，则判定该区域属于共现），给2倍权重

        :param listOfPoints: list of dict， dict存的推荐点信息
        格式：[ {"geohash":XXX, "cnt": 1, "lat": xx.xx, "lng": xx.xx, "tag": XX, "tag": "cooccur"/"around", "dist":XX米} ]


        :return: list of dict，增加score字段
        格式：[ {"geohash":XXX, "cnt": 1, "lat": xx.xx, "lng": xx.xx, "tag": XX, "score":, "tag": "cooccur"/"around", "dist":XX米} ]

        '''

        ' 最终推荐结果权重计算 '

        t = time.time()

        # 拉普拉斯平滑参数
        alpha = 1
        beta = 3.9  # 经验值，约为count=50

        df = pd.DataFrame(listOfPoints)
        df['score'] = np.log(df['cnt'] + alpha)
        # 提升共现点的权重
        df['score'] = np.where(df['tag'] == "cooccur", df['score'] * 2, df['score'])

        # 最终权重进行平滑
        df['score'] = df['cnt'] / (df['cnt'].max() + beta)
        df['score'] = df['score'].round(6)

        # df['score'] = MinMaxScaler().fit_transform(df['score'])
        out = list()
        for idx, row in df.iterrows():
            point = dict()
            for label in row.index.values:
                point[label] = row[label]
            out.append(point)
        out.sort(key=lambda x: x['score'], reverse=True)

        logging.debug("weighting  elasped={t}".format(t=(time.time() - t)))
        return out

    def __dumpFormat(self, startPos, lat, lng, radius=0, recomms=None):
        '''
        聚类完成后的输出点格式约定
        '''

        if not recomms or len(recomms) == 0:
            num = 0
            result = "NULL"
        else:
            num = len(recomms)
            for pos in recomms:
                pos['count'] = pos['cnt']
                del pos['cnt']
                pos["status"] = 0
                pos['ext'] = "cooccur" if pos['tag'] == "cooccur" else ""
                pos['tag'] = "AC01"
                pos['show'] = 0
                pos['pick'] = 0
                del pos['dist']
            result = json.dumps(recomms)

        return Row(startPos=startPos,
                   lat=lat,
                   lng=lng,
                   radius=radius,
                   num=num,
                   update=arrow.now().timestamp,
                   recomms=result)


if __name__ == '__main__':
    import time

    df = pd.read_csv("AroundClustering_sample.csv", index_col=None, nrows=100)
    t = time.time()

    out = []

    chunk = []
    batch = AroundClusteringBatch()
    for idx, row in df.iterrows():
        chunk.append(row)
        if len(chunk) > 25:
            out = batch.selectKBestBatch(chunk)
            tmp = []
            break

    for row in out:
        if row.num > 5:
            print row.startPos
            recomms = json.loads(row.recomms)
            for pos in recomms:
                pass
                # print pos['name']
    print "elasped:", time.time() - t
