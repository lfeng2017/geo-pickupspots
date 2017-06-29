# -*- coding: utf-8 -*-

'''
配置文件加载方式 （在执行python脚本前临时赋值环境变量，若为空会提示错误）：
ENV=pro/dev python YOUR_SCRIPT.py

'''

import os

env = os.getenv('ENV')
try:
    if not env or len(env) == 0:
        from dev import DevConfig as setting

        # print '[WARN] no ENV setting, using Development config as default.'
        print '[ERROR] no ENV setting, please input: dev test pro'
        exit(-1)
    elif env == 'pro':
        from pro import ProdConfig as setting

        print '[INFO] Production config loaded'
    elif env == 'dev':
        from dev import DevConfig as setting

        print '[INFO] Development config loaded'
    else:
        print '[ERROR] setting is wrong ENV=%s, please input: dev test pro', env or 'unspecified'
        exit(-1)
except ImportError:
    print '[ERROR] Loading config for %s environment failed', env or 'unspecified'
    exit(-1)
