# -*- coding: utf-8 -*-
import json
import requests
import pprint
import urllib
import geopy
from geopy.distance import VincentyDistance
from itertools import repeat, chain

AMAP_KEY = "fc872cbed2926354351064069cbdf15e"

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


def chunks(l, n):
	"""Yield successive n-sized chunks from l."""
	for i in range(0, len(l), n):
		yield l[i:i + n]


def move2road(road, point):
	' 向最近的路方向移动 '
	distance = float(road['distance'])
	direction = road['direction']
	bear_degree = direction_map[direction]
	bear_degree = (bear_degree + 180) % 360  # 路的方向是相反的
	origin = geopy.Point(point.latitude, point.longitude)
	destination = VincentyDistance(meters=distance).destination(origin, bear_degree)
	return destination


class BindRoad():
	'''
	调用高德api绑路
	'''

	def __mk_args(self, points):
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
		' 向最近的路方向移动 '
		distance = float(road['distance'])
		direction = road['direction']
		bear_degree = direction_map[direction]
		bear_degree = (bear_degree + 180) % 360  # 路的方向是相反的
		origin = geopy.Point(latlng[0], latlng[1])
		destination = VincentyDistance(meters=distance).destination(origin, bear_degree)
		return destination

	def __match_road_by_amap(self, points):
		' 批量绑路, 一次最多20个点 '

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
		' 单个点绑路 '
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
		' 以20个坐标一轮,进行批量绑路处理 '
		out_list = []
		obj = cls()
		for chunck in chunks(points, 20):
			out_list.extend(obj.__match_road_by_amap(chunck))
		return out_list


class Around():
	'''
	周边搜索返回附近的poi信息
	'''

	def __mk_args(self, point, poiType=None, radius=100):
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
	def one(cls, point, radius=100, poiType=None):
		' 周边搜索 '
		param = cls().__mk_args(point, radius, poiType)
		try:
			data = requests.get("http://restapi.amap.com/v3/place/around", param).json()
			if data['info'] == "OK":
				pois = data['pois']
				# 无搜索结果
				if not pois or len(pois) == 0:
					return []
				else:
					return [{'name': poi['name'], 'type': poi['typecode'], 'distance': poi['distance']} for poi in pois]
			else:
				return []
		except Exception, e:
			return []

	@classmethod
	def batch(cls, points, poiType=None):
		' 以20个坐标一轮,进行批量周边搜索 '
		out_list = []
		obj = cls()
		for chunck in chunks(points, 20):
			urls = [obj.__makeBatchUrl(p, poiType) for p in chunck]
			out_list.extend([obj.__parseBatchResult(x) for x in AmapBatch.request(urls)])
		return out_list

	def __makeBatchUrl(self, point, poiType=None):
		url = "/v3/place/around"
		param = self.__mk_args(point, poiType)
		return url + "?" + urllib.urlencode(param)

	def __parseBatchResult(self, resp):
		if not resp:
			raise ValueError("input response dict is null")
		try:
			resp = resp['body']
			if resp['info'] == "OK":
				pois = resp['pois']
				# 无搜索结果
				if not pois or len(pois) == 0:
					return []
				else:
					return [{'name': poi['name'], 'type': poi['typecode'], 'distance': poi['distance']} for poi in pois]
			else:
				return []
		except Exception, e:
			return []


class Regeo():
	def __mk_args(self, data):
		param = dict()
		param['location'] = data
		param['key'] = AMAP_KEY
		param['radius'] = 1000
		param['output'] = 'json'
		return param

	def __cut_name(self, name):
		' 调整地址名称为短格式 '
		if name.find(u"街道") > 0:
			name = name.split(u"街道")[1]
		elif name.find(u"镇") > 0:
			name = name.split(u"镇")[1]
		elif name.find(u"乡") > 0:
			name = name.split(u"乡")[1]
		elif name.find(u"区") > 0:
			name = name.split(u"区")[1]
		return name

	@classmethod
	def getRoads(cls, point):
		pos = str(point.longitude) + ',' + str(point.latitude)
		param = cls().__mk_args(pos)
		param['extensions'] = "all"
		try:
			data = requests.get("http://restapi.amap.com/v3/geocode/regeo", param).json()
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
				return []
		except Exception, e:
			print e
			return []

	@classmethod
	def getName(cls, point):
		obj = cls()
		pos = str(point.longitude) + ',' + str(point.latitude)
		param = obj.__mk_args(pos)
		try:
			data = requests.get("http://restapi.amap.com/v3/geocode/regeo", param).json()
			if data['info'] == "OK":
				regeo = data.get("regeocode", {})
				return obj.__cut_name(regeo.get('formatted_address', ""))
			else:
				return ""
		except Exception, e:
			print e
			return ""

	@classmethod
	def getNameBatch(cls, points):
		' 20个一批次调用高德regeo接口,返回地名 '
		obj = cls()
		out = []
		for chunk in chunks(points, 20):
			pos = '|'.join([str(x.longitude) + ',' + str(x.latitude) for x in chunk])
			param = obj.__mk_args(pos)
			param['batch'] = "true"
			try:
				data = requests.get("http://restapi.amap.com/v3/geocode/regeo", param).json()
				if data['info'] == "OK":
					regeos = data.get("regeocodes", {})
					for pos in regeos:
						out.append(obj.__cut_name(pos.get('formatted_address', "")))
			except Exception, e:
				print e
		return out


class AmapBatch():
	def __batch_by_amap(self, urls):
		if not isinstance(urls, list):
			raise ValueError("input urls must be list")

		if len(urls) > 20:
			raise ValueError("at most 20 url one time")

		param = dict()
		param['ops'] = map(lambda url: dict(url=url), urls)
		url = "http://restapi.amap.com/v3/batch?key=" + AMAP_KEY
		headers = {'Content-type': 'application/json'}
		return requests.post(url, data=json.dumps(param), headers=headers).json()

	@classmethod
	def request(cls, urls):
		' 以20个一轮,进行批量处理 '
		out_list = []
		obj = cls()
		for chunck in chunks(urls, 20):
			out_list.extend(obj.__batch_by_amap(chunck))
		return out_list


if __name__ == "__main__":
	# 单点绑路
	print "单点绑路: ", BindRoad.one(geopy.Point(39.9849500000, 116.3077580000))
	# 批量绑路服务测试
	SIZE = 30
	origins = [x for x in repeat(geopy.Point(39.9849500000, 116.3077580000), SIZE)]
	result = [x for x in BindRoad.batch(origins)]
	print "批量绑路: ", SIZE
	assert len(result) == SIZE

	# 附近搜索命名
	result = Around.one(geopy.Point(39.9849500000, 116.3077580000))
	print "单点命名: ", result[0]['name'], result[0]['type'], result[0]['distance']

	# 批量调用高德接口
	SIZE = 30
	result = Around.batch([geopy.Point(39.9849500000, 116.3077580000) for x in range(SIZE)])
	assert len(result) == SIZE
	result = result[0]
	print "批量请求（命名）: ", SIZE, result[0]['name'], result[0]['type'], result[0]['distance']

	# 提取道路
	print "周边道路: ", Regeo.getRoads(geopy.Point(39.9849500000, 116.3077580000))

	# 逆地址名称
	print "逆地址名称: ", Regeo.getName(geopy.Point(39.9849500000, 116.3077580000))

	print "逆地址名称(batch): ", Regeo.getNameBatch([geopy.Point(39.9849500000, 116.3077580000)
												   , geopy.Point(39.9849500000, 116.3077580000)])[0]
