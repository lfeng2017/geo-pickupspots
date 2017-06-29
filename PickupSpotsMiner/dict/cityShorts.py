# -*- coding: utf-8 -*-

'''
从QA接口取城市简码，暂未用到

'''

import requests
import json
from collections import defaultdict

url = "http://testing.base.yongche.org/api/region/list"

city2short = defaultdict(str)
shot2city = defaultdict(str)

result = requests.get(url).json()
if result.get('msg') == u"SUCC":
    for tag in result.get('result', []):
        if tag.get('short', "") != "":
            city2short[tag['cn']] = tag['short']
            shot2city[tag['short']] = tag['cn']
else:
    print "[ERROR] get city tag from restful api failed"

if __name__ == "__main__":
    print shot2city['gy']
    print city2short[u'昆明']