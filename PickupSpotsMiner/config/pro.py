# -*- coding: utf-8 -*-
'''
线上环境的环境变量，主要是mysql和mongo接口

'''

import logging
from default import Config


class ProdConfig(Config):
    LOG_TO_FILE = True
    LOG_LEVEL = logging.INFO

    # 存放推荐点信息的mysql，使用的是sqlalchemy连接字符串
    DB_URL = "mysql+pymysql://geo_pickups:123qwe,./@gut1.epcs.bj2.yongche.com:3306/geo_pickups?charset=utf8"

    # LBS的mongo写入接口
    MONGO_API = "http://172.17.99.10/mongo/geo-pickups/base_results_{city}"

    # 从hdfs导出推荐候选点拉链数据的
    GEO_LOCAL_DIR = "/home/lujin/tmp/geo"

    # 邮件设置
    MAIL_TO = ["lujin@yongche.com"]