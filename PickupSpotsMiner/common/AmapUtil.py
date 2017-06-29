# -*- coding: utf-8 -*-

'''
高德API的封装：
1、通过Decorator为restful请求增加了超时重试
2、不支持批量请求的接口，封装为批量请求方式

'''

import os
import random
import json
import requests
import urllib
import geopy
from geopy.distance import VincentyDistance
import time
from functools import wraps
from common.CollectionUtil import chunks

# 高德api的key
AMAP_KEY = "fc872cbed2926354351064069cbdf15e"

NAME = (os.path.splitext(os.path.basename(__file__))[0])

from . import logger

log = logger.init(NAME)

direction_map = {
    u'东': 90,
    u'南': 180,
    u'西': 270,
    u'北': 0,
    u'东北': 45,
    u'东南': 135,
    u'西南': 225,
    u'西北': 315,
}


def retry(ExceptionToCheck, tries=5, delay=4, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warn(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            else:
                log.error("invoke func failed after Retry {t} times, return None as default".format(t=mtries))
                raise RuntimeError("all times http request failed")

        return f_retry  # true decorator

    return deco_retry


@retry(Exception, logger=log)
def getWithRetry(url, param):
    '''
    http get的封装，请求失败抛出异常，配合retry的decorator使用，达到重试的作用

    :param url: 请求url
    :param param: get的请求参数
    :return: 请求结果的dict对象
    '''

    result = requests.get(url, param)

    # 获取请求结果
    info = result.json().get("info", "")

    # 若请求失败，抛出异常，触发重试
    if info == "OK":
        return result
    elif info == "QPS_HAS_EXCEEDED_THE_LIMIT" or info == "CONNECT_TIMEOUT":
        wait_time = random.uniform(0.01, 0.1)
        log.warn("{info}, sleep={t}, url={url} param={param}".format(info=info, url=url, param=param, t=wait_time))
        time.sleep(wait_time)
        raise RuntimeError
    else:
        log.warn("{info}, url={url} param={param}".format(info=info, url=url, param=param))
        raise RuntimeError


@retry(Exception, logger=log)
def postWithRetry(url, param, headers):
    '''
    http post的封装，请求失败抛出异常，配合retry的decorator使用，达到重试的作用

    :param url: 请求url
    :param param: post的请求参数
    :param headers: post的请求头部
    :return: 请求结果的dict对象
    '''

    result = requests.post(url, data=json.dumps(param), headers=headers)

    # 获取请求结果
    info = result.json()[0].get("body", None).get("info", "")

    # 若请求失败，抛出异常，触发重试
    if info == u"OK":
        return result
    elif info == u"QPS_HAS_EXCEEDED_THE_LIMIT" or info == u"CONNECT_TIMEOUT":
        wait_time = random.uniform(0.01, 0.1)
        # log.warn("{info}, sleep={t}, url={url} param={param}".format(info=info, url=url, param=param, t=wait_time))
        log.warn("{info}, sleep={t}, url={url} ".format(info=info, url=url, t=wait_time))
        time.sleep(wait_time)
        raise RuntimeError
    else:
        # log.warn("{info}, url={url} param={param}".format(info=info, url=url, param=param))
        log.warn("{info}, url={url}".format(info=info, url=url))
        raise RuntimeError


def move2road(road, point):
    '''
    向最近的路方向移动

    :param road: 高德api周边搜索的road数据
    :param point: 待矫正的坐标
    :return: 矫正后的坐标
    '''

    '  '
    distance = float(road['distance'])
    direction = road['direction']
    bear_degree = direction_map[direction]
    bear_degree = (bear_degree + 180) % 360  # 路的方向是相反的
    origin = geopy.Point(point.latitude, point.longitude)
    destination = VincentyDistance(meters=distance).destination(origin, bear_degree)
    return destination


class BindRoad():
    '''
    通过高德api的周边搜索进行绑路

    '''

    def __mk_args(self, points):
        '''
        构造API请求参数的dict对象

        :param points: 待调整的点，1个或多个
        :return:
        '''

        param = dict()
        if len(points) == 1:
            param['location'] = str(points[0].longitude) + ',' + str(points[0].latitude)
        else:
            param['location'] = '|'.join(map(lambda x: str(x.longitude) + ',' + str(x.latitude), points))
            param['batch'] = 'true'
        param['key'] = AMAP_KEY
        param['poitype'] = 'all'
        param['radius'] = 300
        param['extensions'] = 'all'
        param['output'] = 'json'
        return param

    def __move2road(self, road, latlng):
        '''
        向最近的路方向移动
        根据周边搜索道路的方向和距离，将点向最近的道路方向移动，移动后的坐标作为绑路点

        :param road: 高德api道路信息dict
        :param latlng: 待修正的坐标点tuple
        :return: 修正坐标后的geopy Point对象
        '''

        distance = float(road['distance'])
        direction = road['direction']
        bear_degree = direction_map[direction]
        bear_degree = (bear_degree + 180) % 360  # 路的方向是相反的
        origin = geopy.Point(latlng[0], latlng[1])
        destination = VincentyDistance(meters=distance).destination(origin, bear_degree)
        return destination

    def __match_road_by_amap(self, points):
        '''
        以批量模式调用高德周边搜索api，批量绑路, 一次最多20个点

        :param points: 待绑路的点
        :return: 修正坐标后的点
        '''

        if not points or len(points) == 0:
            raise ValueError("input points is null or empty")

        if len(points) > 20:
            raise RuntimeError("at most 20 points")

        param = self.__mk_args(points)
        resp = requests.get("http://restapi.amap.com/v3/geocode/regeo", param).json()
        ret = []
        if resp['info'] == "OK":
            rgeos = resp['regeocodes']
            for i, item in enumerate(rgeos):
                roads = item['roads']
                point = points[i]
                if len(roads) > 0:
                    roads = sorted(roads, key=lambda x: x['distance'])
                    point = move2road(roads[0], point)
                ret.append(point)
        return ret

    @classmethod
    def one(cls, point):
        '''
        单个点绑路

        :param point: 待绑路的点
        :return: 修正坐标后的点
        '''

        if not point:
            raise ValueError("input points is null")

        if not isinstance(point, geopy.Point):
            raise ValueError("must be geopy.Point")

        obj = cls()
        param = obj.__mk_args([point])
        resp = requests.get("http://restapi.amap.com/v3/geocode/regeo", param).json()
        if resp['info'] == "OK":
            rgeo = resp['regeocode']
            roads = rgeo['roads']
            if len(roads) > 0:
                roads = sorted(roads, key=lambda x: x['distance'])
                return obj.__move2road(roads[0], point)
            else:
                print "no road"
        else:
            print "amap is failed"
        return point

    @classmethod
    def batch(cls, points):
        '''
        将输入的点list，拆分为20个一组，调用批量绑路方法进行绑路，绑路结果合并为1个list后返回
        备注：此方法为绑路类的主要入口

        :param points: 待绑路的点，个数不做限制
        :return: 修正坐标后的点
        '''

        out_list = []
        obj = cls()
        for chunck in chunks(points, 20):
            out_list.extend(obj.__match_road_by_amap(chunck))
        return out_list


class Around():
    '''
    周边搜索返回附近的poi信息
    备注：周边搜索的高德api本身不支持匹配请求，通过高德的批量请求api + 周边搜索api，来实现批量请求效果

    '''

    def __init__(self):
        self.URL = "http://restapi.amap.com/v3/place/around"

    def __mk_args(self, point, poiType=None, radius=300):
        '''
        构造API请求参数的dict对象

        :param point: 待搜索的坐标点
        :return:
        '''

        param = dict()
        param['location'] = str(point.longitude) + ',' + str(point.latitude)
        param['key'] = AMAP_KEY
        param['radius'] = radius
        param['output'] = 'json'
        param['sortrule'] = "distance"
        if not poiType:
            pass
        else:
            param['types'] = poiType
        return param

    @classmethod
    def one(cls, point, radius=300, poiType=None):
        '''
        单个点进行周边搜索

        :param point: 搜索点
        :param radius: 搜索半径
        :param poiType: 搜索的poi类型
        :return: 搜索结果的list
        '''

        # 构造请求参数的dict
        param = cls().__mk_args(point, radius, poiType)
        try:
            # 待自动重试的http get
            data = getWithRetry("http://restapi.amap.com/v3/place/around", param).json()

            # 请求结果ok
            if data['info'] == "OK":
                pois = data['pois']
                # 无搜索结果，返回 empty list
                if not pois or len(pois) == 0:
                    log.debug("Around by amap no result find, return [] as default, param={param}".format(param=param))
                    return []
                else:
                    return [{'name': poi['name'], 'type': poi['typecode'], 'distance': poi['distance']} for poi in pois]
            else:
                # 超过重试次数之后还是失败，则返回 empty list
                log.error("Around by amap response is not OK, info={info} param={param}" \
                          .sformat(info=data.get("info", "NULL"), param=param))
                return []
        except Exception, e:
            log.error("Around by amap request failed, return [], point={pt}".format(pt=point))
            return []

    @classmethod
    def batch(cls, points, poiType=None):
        '''
        批量周边搜索，输入点个数不限制，自动拆分为20个坐标一轮,进行批量周边搜索

        :param points: 搜索点集合
        :param poiType: 搜索的poi类型
        :return: 搜索结果的list
        '''

        out_list = []
        obj = cls()
        try:
            # 使用生成器将搜索点分为20个1组，以满足批量请求接口的要求
            for chunck in chunks(points, 20):
                # 使用周边搜索api，构造出高德批量请求api需要的参数（uri list）
                urls = [obj.__makeUrl4Batch(p, poiType) for p in chunck]
                # 调用高德api批量请求接口，并对返回结果进行批量解析
                results = [obj.__parseBatchResult(x) for x in AmapBatch.request(urls)]
                # 返回结果的长度不匹配，说明调用过程中有错误发生，全部设为 ""
                if len(results) != len(chunck):
                    results = map(lambda x: [], chunck)
                out_list.extend(results)
        except Exception, e:
            log.exception("amap Around batch request failed, return [[]] as default, points={pts}".format(pts=points))
            return map(lambda x: [], points)
        return out_list

    def __makeUrl4Batch(self, point, poiType=None):
        '''
        构造单个周边搜索的url，作为高德批量api的请求参数

        :param point: 搜索点
        :param poiType: 搜索的poi类型
        :return: 周边搜的请求url（供批量请求接口作为参数使用）
        '''

        url = "/v3/place/around"
        param = self.__mk_args(point, poiType)
        return url + "?" + urllib.urlencode(param)

    def __parseBatchResult(self, resp):
        '''
        解析高德批量请求接口返回结果中的1条数据

        :param resp: 高德批量接口中，对应到1条请求的返回结果的数据
        :return: 批量结果中1条周边搜索的poi对象list
        '''

        # 解析失败抛出异常
        if not resp:
            raise ValueError("input response dict is null")

        info = resp['body'].get("info", "NULL")
        pois = resp['body'].get("pois", None)
        if info == "OK":
            # 无搜索结果，返回空
            if not pois or len(pois) == 0:
                return []
            else:
                return [{'name': poi['name'], 'type': poi['typecode'], 'distance': poi['distance']} for poi in pois]
        else:
            # 正常情况不应该进入此环境
            log.error("__parseBatchResult from amap response is not OK, return [] as default, info={info} resp={resp}" \
                      .format(info=info, resp=resp))
            return []


class Regeo():
    '''
    高德api逆地址封装

    '''

    def __init__(self):
        self.url = "http://restapi.amap.com/v3/geocode/regeo"

    def __mk_args(self, lnglat_str):
        '''
        构造请求参数

        :param data: 坐标对字符串，"lng,lat"
        :return:
        '''
        param = dict()
        param['location'] = lnglat_str
        param['key'] = AMAP_KEY
        param['radius'] = 1000
        param['output'] = 'json'
        return param

    def __cut_name(self, name, addressComponent):
        '''
        调整地址名称为短格式
        有街道信息用街道信息切割，没有的话用自定义的关键词切割

        :param name: 待缩减的地名字符串
        :param addressComponent: 街道字符串（高德api和地名一起返回）
        :return: 缩短后的名称
        '''

        oriName = name
        try:
            # 取不到街道默认信息, 按预订规则切分
            if not addressComponent or len(addressComponent) == 0:
                if name.find(u"街道") > 0:
                    name = name.split(u"街道")[1]
                elif name.find(u"镇") > 0:
                    name = name.split(u"镇")[1]
                elif name.find(u"乡") > 0:
                    name = name.split(u"乡")[1]
            else:
                # 用街道附加信息切分
                province = addressComponent.get('province', "")
                if not isinstance(province, unicode):
                    province = u""
                city = addressComponent.get('city', "")
                if not isinstance(city, unicode):
                    city = u""
                district = addressComponent.get('district', "")
                if not isinstance(district, unicode):
                    district = u""
                township = addressComponent.get('township', "")
                if not isinstance(township, unicode):
                    township = u""
                name = name.replace(province, "", 1)
                name = name.replace(city, "", 1)
                name = name.replace(district, "", 1)
                name = name.replace(township, "", 1)

            return name
        except Exception, e:
            # log.error("cut regeo name failed", exec_info=True)
            log.exception("cut regeo name failed")
            return oriName

    @classmethod
    def getRoads(cls, point):
        '''
        逆地址，获取周边道路信息

        :param point: 请求的点
        :return: 周边道路的对象信息
        '''

        pos = str(point.longitude) + ',' + str(point.latitude)
        obj = cls()
        param = obj.__mk_args(pos)
        param['extensions'] = "all"
        try:
            # 待自动重试的http get
            data = getWithRetry(obj.url, param).json()
            if data['info'] == "OK":
                regeo = data.get("regeocode", {})
                out = []
                # 道路
                for pos in regeo.get("roads", []):
                    road = {}
                    lng, lat = pos['location'].split(",")
                    road['lat'] = lat
                    road['lng'] = lng
                    road['name'] = pos['name']
                    road['distance'] = pos['distance']
                    road['direction'] = pos['direction']
                    road['type'] = "road"
                    out.append(road)
                # 十字路口
                for pos in regeo.get("roadinters", []):
                    road = {}
                    lng, lat = pos['location'].split(",")
                    road['lat'] = lat
                    road['lng'] = lng
                    road['name'] = pos['first_name']
                    road['distance'] = pos['distance']
                    road['direction'] = pos['direction']
                    road['type'] = "roadinter"
                # out.append(road)
                return out
            else:
                log.error("getRoads by amap response is not OK, info={info} param={param}" \
                          .format(info=data.get("info", "NULL"), param=param))
                return []
        except Exception, e:
            log.error("getRoads by amap request failed, return [] as default, point={pt}".format(pt=point))
            return []

    @classmethod
    def getName(cls, point):
        '''
        逆地址获取点的名称

        :param point: 坐标点
        :return: 精简后的逆地址返回名称
        '''

        obj = cls()
        pos = str(point.longitude) + ',' + str(point.latitude)
        param = obj.__mk_args(pos)
        try:
            # 带重试的http get
            data = getWithRetry(obj.url, param).json()
            if data.get('info', "") == "OK":
                regeo = data.get("regeocode", {})
                formatted_address = regeo.get('formatted_address', "")
                addressComponent = regeo.get("addressComponent", None)
                # 用街道信息分割逆地址，获取1个短的地名
                return obj.__cut_name(formatted_address, addressComponent)
            else:
                log.error("getName by amap response is not OK, info={info} param={param}" \
                          .format(info=data.get("info", "NULL"), param=param))
                return ""
        except Exception, e:
            log.exception("getName by amap request failed, return empty as default, point={pt}".format(pt=point))
            return ""

    @classmethod
    def getNameBatch(cls, points):
        '''
        批量获取名称，输入点个数不做限制，自动分割为20个一批，调用高德api获取名称

        :param points:
        :return:
        '''

        obj = cls()
        out = []
        # 分割为20个1批次
        for chunk in chunks(points, 20):
            # 组装逆地理的批量请求参数
            pos = '|'.join([str(x.longitude) + ',' + str(x.latitude)
                            for x in chunk])
            param = obj.__mk_args(pos)
            param['batch'] = "true"
            try:
                # 带重试的http get
                data = getWithRetry(obj.url, param).json()
                if data['info'] == "OK":
                    regeos = data.get("regeocodes", {})
                    for regeo in regeos:
                        formatted_address = regeo.get('formatted_address', "")
                        addressComponent = regeo.get("addressComponent", None)
                        # 用街道信息分割逆地址，获取1个短的地名
                        out.append(obj.__cut_name(formatted_address, addressComponent))
                else:
                    log.error("getNameBatch by amap response is not OK, info={info} param={param}" \
                              .format(info=data.get("info", "NULL"), param=param))
                    return map(lambda x: "", points)
            except Exception, e:
                # 出错的情况，此批次命名全部设置为空
                log.error("getNameBatch by amap request failed, return [[]] as default, point={pts}".format(pts=points))
                return map(lambda x: "", points)
        return out


class AmapBatch():
    '''
    高德api批量请求接口的封装

    '''

    def __batch_by_amap(self, urls):
        '''
        对传入的url列表，调用批量请求接口获取结果，一次最大不能超过20个url

        :param urls: 具体需要执行查询的高德api url 列表 （需要提前构造好）
        :return: 批量请求返回的json结果
        '''

        if not isinstance(urls, list):
            raise ValueError("input urls must be list")

        if len(urls) > 20:
            raise ValueError("at most 20 url one time")

        param = dict()
        param['ops'] = map(lambda url: dict(url=url), urls)
        url = "http://restapi.amap.com/v3/batch?key=" + AMAP_KEY
        headers = {'Content-type': 'application/json'}

        # 带重试的http post
        return postWithRetry(url, param, headers=headers).json()
        # return requests.post(url, data=json.dumps(param), headers=headers).json()

    @classmethod
    def request(cls, urls):
        '''
        将url按20个一批次拆分，调用高德批量请求接口

        :param urls: 需要执行的高德api url
        :return: 汇总后的返回结果
        '''

        out_list = []
        obj = cls()
        for chunck in chunks(urls, 20):
            out_list.extend(obj.__batch_by_amap(chunck))
        return out_list


if __name__ == "__main__":
    # 单点绑路
    # print "单点绑路: ", BindRoad.one(geopy.Point(39.9849500000, 116.3077580000))
    #
    # # 批量绑路服务测试
    # SIZE = 30
    # origins = [x for x in repeat(geopy.Point(39.9849500000, 116.3077580000), SIZE)]
    # result = [x for x in BindRoad.batch(origins)]
    # print "批量绑路: ", SIZE
    # assert len(result) == SIZE
    #
    # # 附近搜索命名
    # result = Around.one(geopy.Point(39.9849500000, 116.3077580000))
    # if len(result) > 0:
    #     print "周边搜索命名（单点）: ", result[0]['name'], result[0]['type'], result[0]['distance']
    # else:
    #     print "周边搜索命名（单点）: 半径内无结果"

    # 批量调用高德接口
    SIZE = 30
    result = Around.batch([geopy.Point(39.9849500000, 116.3077580000) for x in range(SIZE)])
    assert len(result) == SIZE
    result = result[0]
    print "周边搜索命名（批量）: ", SIZE, result[0]['name'], result[0]['type'], result[0]['distance']

    # # 提取道路
    # print "周边道路: ", Regeo.getRoads(geopy.Point(39.9849500000, 116.3077580000))
    #
    # # 逆地址名称
    # print "逆地址名称: ", Regeo.getName(geopy.Point(39.9849500000, 116.3077580000))
    #
    # print "逆地址名称(batch): ", Regeo.getNameBatch([geopy.Point(39.9849500000, 116.3077580000)
    #                                                , geopy.Point(39.9849500000, 116.3077580000)])[0]
