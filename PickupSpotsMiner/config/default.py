# -*- coding: utf-8 -*-

'''
配置文件基类，存放公共信息
具体的配置文件，通过环境变量ENV=pro/dev来加载

'''

import os
import logging


class Config(object):
    # path setting
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # spark java opts common
    JAVA_OPTS = " -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"

    # log setting
    LOG_PATH = os.path.join(BASE_DIR, "log")

    # 邮件
    MAIL_HOST = "smtp.263.net"
    MAIL_PORT = "25"
    MAIL_FROM = "fengli@yongche.com"
    MAIL_PSW = "lfeng*2016"
