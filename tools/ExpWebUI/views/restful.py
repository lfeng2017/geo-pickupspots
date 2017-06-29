# -*- coding: utf=8 -*-
import json
from flask import jsonify
from flask_restful import reqparse, Resource, inputs
from ext import api, rc
from common.GeoUtil import *
from common.AmapUtil import *
from config import EXPECT_PRECISION
from core import logger
import pandas as pd
import geopy
from geopy.distance import vincenty
from pymongo import MongoClient
from bson.json_util import dumps
from scipy.spatial.distance import squareform, pdist
import time

import sys

sys.path.append("..")
import common.AmapUtil as amap
import common.GeoUtil as geo
import config as cfg

import numpy as np
from flask.blueprints import Blueprint
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
from sklearn.cluster import AffinityPropagation
from sklearn.cluster import MeanShift, estimate_bandwidth
from collections import defaultdict
import itertools
from odo import odo
import re

VERBOSE = True

bp = Blueprint('api', __name__, url_prefix='/api')
api.init_app(bp)


class GeoRecomm(Resource):
    parser = reqparse.RequestParser(bundle_errors=True)
    parser.add_argument('lat', type=float, help=u'must be float', required=True)
    parser.add_argument('lng', type=float, help=u'must be float', required=True)

    def get(self):
        args = self.parser.parse_args()
        self.lat = args.get('lat')
        self.lng = args.get('lng')

        # get result from cache by spacing
        result = self.__get_native_results_mongo()
        # result = self.__get_native_results_mysql()
        import pprint
        pprint.pprint(result)
        return jsonify(result)

    # 推荐数据预处理
    def __get_native_results_mongo(self):
        key = geohash_encoding(self.lat, self.lng)[0:EXPECT_PRECISION]
        # with MongoClient('mongodb://10.0.11.192:27012,10.0.11.193:27012,10.0.11.198:27012') as conn:
        with MongoClient(cfg.MONGO_URI) as conn:
            db = conn['geo-pickups']
            cursor = db.base_results_bj.find_one({"_id": key})
        if not cursor:
            return {"startPos": key, "num": 0}
        else:
            result = dict(cursor)
            del result['_id']
            return result

            # 推荐数据预处理

    def __get_native_results_mysql(self):
        from sqlalchemy import create_engine
        from sqlalchemy.sql import text
        from collections import namedtuple

        engine = create_engine(cfg.MYSQL_URI)

        with engine.connect() as conn:
            key = geohash_encoding(self.lat, self.lng)[0:EXPECT_PRECISION]
            sql = text("select a.geohash, a.city, a.regeo, a.num, b.lat, b.lng, b.name, b.score, b.count"
                       " from base_pickup_info a join base_pickup_detail b"
                       " on a.geohash=b.exp_geohash"
                       " where geohash=:key")
            rs = conn.execute(sql, key=key)
            # 构造结果对象
            Record = namedtuple('Record', rs.keys())
            records = [Record(*r) for r in rs.fetchall()]
            out = dict()
            regeo = ""
            num = 0
            recomms = list()
            for row in records:
                regeo = row.regeo
                num = row.num
                recomms.append({
                    "lat": row.lat,
                    "lng": row.lng,
                    "name": row.name,
                    "score": row.score
                })
            out['regeo'] = regeo
            out['num'] = num
            out['recomms'] = recomms

        return out

    # 距离过滤 （暂未使用)）
    def __filter_by_distance(self, recomms):
        out = []
        for recomm in recomms:
            distance = haversine(self.lat, self.lng, recomm['lat'], recomm['lng'])
            recomm['distance'] = distance
            if distance > self.distance:
                recomm['bound'] = "out"
            else:
                recomm['bound'] = "in"
            out.append(recomm)
        return out

    # 绑路（暂未使用)）s
    def __bind_road(self, recomms):
        out = []
        latlng_tuples = [geopy.Point(recomm["lat"], recomm["lng"]) for recomm in recomms]
        road_latlng_tuples = amap.bandRoadBatch(latlng_tuples)
        if len(latlng_tuples) != len(road_latlng_tuples):
            logger.warn(
                "some data lost in banding road, diff={suze}".format(size=len(latlng_tuples) - len(road_latlng_tuples)))
            return recomms
        for i, recomm in enumerate(recomms):
            recomm['roadLat'] = road_latlng_tuples[i][0]
            recomm['roadLng'] = road_latlng_tuples[i][1]
            out.append(recomm)
        return out


class StartPosCount(Resource):
    parser = reqparse.RequestParser(bundle_errors=True)
    parser.add_argument('lat', type=float, help=u'must be float', required=True)
    parser.add_argument('lng', type=float, help=u'must be float', required=True)
    parser.add_argument('lv', type=int, default=6, help=u'must be int', required=False)
    parser.add_argument('radius', type=int, default=200, help=u'must be int', required=False)
    parser.add_argument('isVarFailter', type=str, default="false", help=u'must be bool', required=False)
    parser.add_argument('isClustering', type=str, default="false", help=u'must be bool', required=False)

    def __getCandidatesByStep(self, lst):
        out = []
        # 逐步试探是否有结果
        for step in (100, 150, 200, 300, 500, 1000):
            for data in lst:
                del data['_id']
                data['count'] = data['cnt']
                del data['cnt']
                # 距离判断
                data['distance'] = vincenty((self.lat, self.lng), (data['lat'], data['lng'])).meters
                if data['distance'] <= self.radius:
                    out.append(data)
            if len(out) > 0:
                out.sort(key=lambda x: x['count'], reverse=True)
                return step, out
        else:
            return step, out

    def __bestName(self, priorities, listOfPoi):
        ' 根据类型的优先级, 取附近poi的名称 '
        if not listOfPoi or len(listOfPoi) == 0:
            return ""

        for pri in priorities:
            # 类型吻合, 距离在50米内
            pattern = re.compile(r'^' + pri)
            candidates = [poi for poi in listOfPoi if (pattern.match(poi['type'])) and (float(poi['distance']) <= 50)]
            if len(candidates) > 0:
                candidates.sort(key=lambda x: float(x['distance']))
                nearest = candidates[0]
                name = nearest['name'] if float(nearest['distance']) <= 20 else (nearest['name'] + u"附近")
                return name
        else:
            # 都不满足,直接取最近的poi名称
            listOfPoi.sort(key=lambda x: float(x['distance']))
            nearest = listOfPoi[0]
            name = nearest['name'] if float(nearest['distance']) <= 20 else (nearest['name'] + u"附近")
            return name

    def __getPoiNameBySteps(self, points):
        poiType = ["10", "99", "1507", "1505", "1508", "1506", "1504", "1502", "120302", "1903"]
        listOfPois = Around.batch([geopy.Point(p['lat'], p['lng']) for p in points], poiType='|'.join(poiType))
        if len(points) != len(listOfPois):
            raise RuntimeError("len(points) != len(listOfPois)")
        for i, pos in enumerate(points):
            neighbours = listOfPois[i]
            pos['name'] = self.__bestName(poiType, neighbours)
            # 周边搜索找不到, 调用regeo补齐
            if pos['name'] == "":
                print "Regeo -> 补齐推荐点名称"
                pos['name'] = Regeo.getName(geopy.Point(pos['lat'], pos['lng'])) + u"-推荐上车点"
        return points

    def __DBSCAN(self, listOfPoints, minCount):

        t = time.time()

        X = []
        # 按密度扩展数据集, 为了减少计算量,用最小的count进行缩放,之后再复原
        # for pos in listOfPoints:
        # 	X.extend([x for x in itertools.repeat([pos['lat'], pos['lng']], int(pos['count'] / minCount) + 1)])

        # 因为最小支持频次为1, 不做缩放
        X = [[pos['lat'], pos['lng'], pos['count']] for pos in listOfPoints]
        data = X
        inputSize = len(data)

        # @Deprecated
        # default距离函数, 需要归一化
        # X = StandardScaler().fit_transform(X)
        # dbscan = DBSCAN(eps=0.35, min_samples=1).fit(X)

        # 自定义距离函数
        distance_matrix = squareform(pdist(X, lambda x, y: vincenty((x[0], x[1]), (y[0], y[1])).meters))
        dbscan = DBSCAN(eps=30, min_samples=1, metric='precomputed').fit(distance_matrix)

        # 求中心点时,按实际count进行扩展,确保中心点在热点区域
        data = pd.DataFrame(data, columns=['lat', 'lng', 'count'])
        data['cluster'] = dbscan.labels_
        data = data.to_dict(orient='records')
        extData = []
        for pos in data:
            extData.extend([x for x in itertools.repeat([pos['lat'], pos['lng'], pos['cluster']], int(pos['count']))])
        print "origin: ", len(listOfPoints), "cluster=", len(set(dbscan.labels_)), " extend: ", len(extData)

        data = pd.DataFrame(extData, columns=['lat', 'lng', 'cluster'])
        grouped = data.groupby("cluster")['lat', 'lng']
        data = grouped.mean()
        data['cnt'] = grouped.size()

        # Deprecated 加权dbscan, 在支持度1的时候无意义
        # data = pd.DataFrame(data, columns=['lat', 'lng'])
        # data['cluster'] = dbscan.labels_
        # # 坐标求均值
        # grouped = data.groupby("cluster")['lat', 'lng']
        # data = grouped.mean()
        # data['cnt'] = grouped.size() * minCount


        data['distance'] = data.apply(lambda x: vincenty((self.lat, self.lng), (x['lat'], x['lng'])).meters, axis=1)
        # 转换为对象list
        out = list()
        for idx, row in data.iterrows():
            obj = dict()
            # obj['lat'] = np.round(row.lat, 6)
            # obj['lng'] = np.round(row.lng, 6)
            obj['lat'] = row.lat
            obj['lng'] = row.lng
            obj['count'] = row.cnt
            obj['distance'] = row.distance
            out.append(obj)

        print "DBSCAN size: ", inputSize, "elasped: ", time.time() - t
        return out

    def __AffinityPropagation(self, listOfPoints):

        t = time.time()

        # 自定义距离函数
        X = [[pos['lat'], pos['lng']] for pos in listOfPoints]
        distance_matrix = squareform(
            pdist(X, lambda x, y: vincenty((x[0], x[1]), (y[1], y[0])).meters))

        # 用出现的次数作为preference
        # minCount = float(sorted(listOfPoints, key=lambda x : int(x['count']))[0]['count'])
        # weights = [float(pos['count']) / minCount for pos in listOfPoints]
        # af = AffinityPropagation(preference=weights, affinity="precomputed").fit(distance_matrix)
        X = StandardScaler().fit_transform(X)
        af = AffinityPropagation().fit(X)
        print "origin: ", len(listOfPoints), " extend: ", "cluster=", len(af.cluster_centers_indices_)

        # 转换为对象list
        out = list()
        for idx in af.cluster_centers_indices_:
            obj = dict()
            obj['lat'] = listOfPoints[idx]['lat']
            obj['lng'] = listOfPoints[idx]['lng']
            obj['count'] = listOfPoints[idx]['count']
            obj['distance'] = vincenty((self.lat, self.lng), (obj['lat'], obj['lng'])).meters
            out.append(obj)

        print "AffinityPropagation: ", time.time() - t
        return out

    def __MeanShift(self, listOfPoints, minCount):

        t = time.time()

        # 按密度扩展数据集, 为了减少计算量,用最小的count进行缩放,之后再复原
        X = []
        for pos in listOfPoints:
            X.extend([x for x in itertools.repeat([pos['lat'], pos['lng']], int(pos['count'] / minCount) + 1)])
        data = X

        X = StandardScaler().fit_transform(X)
        ms = MeanShift(bin_seeding=True).fit(X)
        print "origin: ", len(listOfPoints), " extend: ", "cluster=", len(ms.labels_)

        # 坐标求均值
        data = pd.DataFrame(listOfPoints, columns=['lat', 'lng']).join(pd.Series(ms.labels_, name="cluster"))
        grouped = data.groupby("cluster")['lat', 'lng']
        data = grouped.mean()
        data['cnt'] = grouped.size() * minCount
        data['count'] = data['cnt']
        data['distance'] = data.apply(lambda x: vincenty((self.lat, self.lng), (x['lat'], x['lng'])).meters, axis=1)
        df = pd.DataFrame(data)[['count', 'distance']].describe()
        # 转换为对象list
        out = list()
        for idx, row in data.iterrows():
            obj = dict()
            # obj['lat'] = np.round(row.lat, 6)
            # obj['lng'] = np.round(row.lng, 6)
            obj['lat'] = row.lat
            obj['lng'] = row.lng
            obj['count'] = row.cnt
            obj['distance'] = row.distance
            out.append(obj)

        print "MeanShift: ", time.time() - t
        return out

    def get(self):
        s = time.time()

        args = self.parser.parse_args()
        self.lat = args.get('lat')
        self.lng = args.get('lng')
        self.lv = args.get('lv')
        self.radius = args.get('radius')
        self.isVarFilter = inputs.boolean(args.get('isVarFailter'))
        self.isClustering = inputs.boolean(args.get('isClustering'))

        hash = geo.geohash_encoding(self.lat, self.lng)[0:self.lv]

        t = time.time()
        with MongoClient('mongodb://localhost:30000') as mongo:
            results = mongo.yongche.exp_startPos_count_bj.find({"geohash": {"$regex": hash}})
        if VERBOSE:
            print "[DEBUG] fetch mongo: ", time.time() - t

        # 半径实验code
        # out = []
        # for data in results:
        # 	del data['_id']
        # 	data['count'] = data['cnt']
        # 	del data['cnt']
        # 	# 距离判断
        # 	data['distance'] = vincenty((self.lat, self.lng), (data['lat'], data['lng'])).meters
        # 	if data['distance'] <= self.radius:
        # 		out.append(data)
        # out.sort(key=lambda x: x['count'], reverse=True)
        # radius = self.radius
        radius, out = self.__getCandidatesByStep(results)

        # 是否有候选结果
        if len(out) == 0:
            # 封装统计变量
            result = dict()
            result['status'] = 1
            result['info'] = "EMPTY"
            result['radius'] = radius
            return jsonify(result)

        # 计算统计变量
        df = pd.DataFrame(out)[['count', 'distance']].describe()

        # 方差截断
        if self.isVarFilter:
            preSize = len(out)
            # # 75% 过滤
            # threshold = df['count']['75%']
            # out = [x for x in out if x['count'] >= threshold]
            # df = pd.DataFrame(out)[['count', 'distance']].describe()
            # # 热点中还有热点
            # if df['count']['mean'] < df['count']['std']:
            # 	threshold = df['count']['50%']
            # 	out = [x for x in out if x['count'] >= threshold]
            # 	df = pd.DataFrame(out)[['count', 'distance']].describe()

            df = pd.DataFrame(out)
            threshold = df['count'].quantile(0.75)
            # 上四分位数进行第一道过滤
            df['isRetain'] = np.where(df['count'] >= threshold, True, False)
            setOfGeohash = df[df['isRetain'] == True]['geohash'].values
            # 第一道过滤结束后, 热点中还有热点， 用中位数再次过滤
            # std = df[df.isRetain == True]['count'].std()
            # mean = df[df.isRetain == True]['count'].mean()
            # if mean < std:
            # 	median = df[df.isRetain == True]['count'].median()
            # 	setOfGeohash = df[(df['isRetain'] == True) & (df['count']>=median)]['geohash'].values
            # else:
            # 	setOfGeohash = df[df['isRetain'] == True]['geohash'].values

            preSize = len(out)
            out = [pt for pt in out if pt['geohash'] in setOfGeohash]
            print "[DEBUG] statFilter from: ", preSize, "to: ", len(out)

        df = pd.DataFrame(out)[['count', 'distance']].describe()

        # 密度聚类
        if self.isClustering:
            out = self.__DBSCAN(out, df['count']['min'])
        # out = self.__AffinityPropagation(out)
        # out = self.__MeanShift(out, df['count']['min'])

        # 加poi name, 简单版本
        # poiType = '|'.join(["10", "99", "1507", "1505", "1508", "1506", "1504", "1502", "120302", "1903"])
        # pois = Around.batch([geopy.Point(p['lat'], p['lng']) for p in out], poiType=poiType)
        # if len(pois) == len(out):
        # 	for i, poi in enumerate(pois):
        # 		# 未查到结果
        # 		if poi[0] == "":
        # 			continue
        # 		elif 0 < float(poi[1]) <= 50:
        # 			out[i]['name'] = poi[0]
        # 		elif 20 < float(poi[1]) <= 50:
        # 			out[i]['name'] = poi[0] + u"附近"
        # else:
        # 	for i, name in enumerate(pois):
        # 		out[i]['name'] = ""

        t = time.time()
        out = self.__getPoiNameBySteps(out)
        print "naming: ", time.time() - t

        # 封装统计变量
        result = dict()
        result['status'] = 0
        result['info'] = "OK"
        result['radius'] = radius
        result['result'] = out
        result['count'] = df['count'].to_dict()
        result['distance'] = df['distance'].to_dict()

        print "total elapsed: ", time.time() - s

        return jsonify(result)


class BindRoadSingle(Resource):
    parser = reqparse.RequestParser(bundle_errors=True)
    parser.add_argument('lat', type=float, help=u'must be float', required=True)
    parser.add_argument('lng', type=float, help=u'must be float', required=True)

    def get(self):
        args = self.parser.parse_args()
        self.lat = args.get('lat')
        self.lng = args.get('lng')

        adjustPoint = amap.BindRoad.one(geopy.Point(self.lat, self.lng))

        out = {}
        out['lat'] = adjustPoint.latitude
        out['lng'] = adjustPoint.longitude

        return jsonify(result=out)


class BindRoadMultiple(Resource):
    parser = reqparse.RequestParser(bundle_errors=True)
    parser.add_argument('lat', type=float, help=u'must be float', required=True)
    parser.add_argument('lng', type=float, help=u'must be float', required=True)

    def get(self):
        args = self.parser.parse_args()
        self.lat = args.get('lat')
        self.lng = args.get('lng')

        roads = amap.Regeo.getRoads(geopy.Point(self.lat, self.lng))
        bindPoints = []
        for road in roads:
            newPos = move2road(road, geopy.Point(self.lat, self.lng))
            bindPoints.append({"lat": newPos.latitude, "lng": newPos.longitude})
        out = {}
        out['roads'] = roads
        out['binds'] = bindPoints
        return jsonify(out)


class RegeoName(Resource):
    parser = reqparse.RequestParser(bundle_errors=True)
    parser.add_argument('lat', type=float, help=u'must be float', required=True)
    parser.add_argument('lng', type=float, help=u'must be float', required=True)

    def get(self):
        args = self.parser.parse_args()
        self.lat = args.get('lat')
        self.lng = args.get('lng')

        return jsonify(result=amap.Regeo.getName(geopy.Point(self.lat, self.lng)))


## Actually setup the Api resource routing here
api.add_resource(GeoRecomm, '/GeoRecomm', endpoint="GeoRecomm")
api.add_resource(BindRoadSingle, '/BindRoadSingle', endpoint="BindRoadSingle")
api.add_resource(BindRoadMultiple, '/BindRoadMultiple', endpoint="BindRoadMultiple")
api.add_resource(StartPosCount, '/StartPosCount', endpoint="StartPosCount")
api.add_resource(RegeoName, '/RegeoName', endpoint="RegeoName")
