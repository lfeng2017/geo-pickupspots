# -*- coding: utf-8 -*-
'''
开发环境的环境变量，主要是mysql和mongo接口

'''

import os
import logging
from default import Config


class DevConfig(Config):
    LOG_TO_FILE = False
    LOG_LEVEL = logging.DEBUG

    #DB_URL = "sqlite:///" + os.path.join(Config.BASE_DIR, "data/geo_pickups.db")
    DB_URL = "mysql+pymysql://root:123qwe,./@10.0.11.91:18806/geo_pickups?charset=utf8"

    # LBS的mongo写入接口
    #MONGO_API = "http://10.0.11.239:9975/mongo/geo-pickups/base_results_{city}"
    MONGO_API = "http://pickup.geo.yongche.org/mongo/geo-pickups/base_results_{city}"

    # 导出hdfs数据到本地的临时存放目录
    GEO_LOCAL_DIR = "/User/fengli/tmp/geo"

    # 邮件设置
    MAIL_TO = ["lujin@yongche.com"]