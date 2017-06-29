#!/usr/bin/env python
# coding=utf-8

import urllib2
import urllib
import random
import sys
import time
import json
import os
import types
import traceback
import geopy
from geopy.distance import VincentyDistance

def send_get_request(url, attributes):
    if isinstance(attributes,dict):
        data = urllib.urlencode(attributes)
    else:
        data = attributes
    req = urllib2.Request(url + "?" + data)
    response = urllib2.urlopen(req)
    return response

def send_post_request(url, attributes):
    if isinstance(attributes, dict):
        data = json.dumps(attributes)
    else:
        data = attributes

    req = urllib2.Request(url,data,{'Content-Type': 'application/json'})
    response = urllib2.urlopen(req)
    return response


def request(url, data={}, method='GET'):
    try:
        if method == 'POST':
            response = send_post_request(url, data)
        else:
            response = send_get_request(url, data)

    except urllib2.HTTPError as response:
        print response.code, response.reason
        traceback.print_exc()
        raise
    except:
        traceback.print_exc()
        raise

    # process result
    try:
        data = response.read()
        d = json.loads(data)
        return d
    except:
        traceback.print_exc()
        raise
    finally:
        response.close()

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

def __match(road, latlng):
    distance = float(road['distance'])
    direction = road['direction']
    bear_degree = direction_map[direction]
    bear_degree = (bear_degree + 180) % 360 # 路的方向是相反的
    origin = geopy.Point(latlng[0], latlng[1])
    destination = VincentyDistance(meters=distance).destination(origin, bear_degree)
    return destination.latitude, destination.longitude

def road_match_batch_with_amap(points):
    '''
    最多 20 个点 list of points
    [(lat, lng,), (lat, lng,) , ...]
    lat, lng 高德坐标系

    '''
    if len(points) > 20:
        raise RuntimeError("at most 20 points")

    param = dict()
    param['location'] = '|'.join(map(lambda latlng: str(latlng[1]) + ',' + str(latlng[0]), points))
    param['key'] = "fc872cbed2926354351064069cbdf15e"
    param['poitype'] = 'all'
    param['radius'] = 300
    param['extensions'] = 'all'
    param['output'] = 'json'
    param['batch'] = 'true'
    d = request("http://restapi.amap.com/v3/geocode/regeo", param)
    ret = list()
    if d['info'] == "OK":
        rgeo = d['regeocodes']
        i = 0
        for item in rgeo:
            roads = item['roads']
            point = points[i]
            if len(roads) > 0:
                roads = sorted(roads, key=lambda x: x['distance'])
                point = __match(roads[0], point)
            ret.append(point)
            i += 1
    return ret


def road_match(points):
    return road_match_batch_with_amap(points)



if __name__ == '__main__':
    #origins = [(39.9849500000,116.3077580000, ), (39.9848950000,116.3064240000, )]
    # origins = [(39.9849500000,116.3077580000, )]
    # points = road_match(origins)

    # for point in points:
        # print point
    f = sys.stdin
    if len(sys.argv) > 1:
        f = open(sys.argv[1])

    origins = list()
    for line in f:
        if line:
            lat, lng = line.split(',')
            origins.append((float(lat), float(lng), ))
            if len(origins) == 20:
                points = road_match(origins)
                for point in points:
                    print str(point[0]) + ',' + str(point[1])
                origins = list()

    if len(origins) > 0:
        points = road_match(origins)
        for point in points:
            print str(point[0]) + ',' + str(point[1])
        origins = list()

    if len(sys.argv) > 1:
        f.close()









