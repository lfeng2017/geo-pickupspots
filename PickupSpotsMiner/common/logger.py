# -*- coding: utf-8 -*-

'''
python logging的简单封装：控制是否输出至文件（info和error分开2个文件）

'''

import os
import logging
from logging import Logger
from logging.handlers import TimedRotatingFileHandler
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config as cfg

FORMATTER = logging.Formatter('%(name)-12s: %(levelname)-8s %(asctime)s - %(message)s')

# disable ext libs logger setting
logging.getLogger("requests").setLevel(logging.WARN)


def init(name, isToFile=cfg.setting.LOG_TO_FILE, level=cfg.setting.LOG_LEVEL):
    if name not in Logger.manager.loggerDict:
        logger = logging.getLogger(name)
        logger.setLevel(level)

        if isToFile:
            # info log
            path = os.path.join(cfg.setting.LOG_PATH, "{name}.log".format(name=name))
            ifh = TimedRotatingFileHandler(path, when='midnight', backupCount=7)
            ifh.setFormatter(FORMATTER)
            # ifh.setLevel(level)
            logger.addHandler(ifh)
            # error log
            path = os.path.join(cfg.setting.LOG_PATH, "{name}_err.log".format(name=name))
            efh = TimedRotatingFileHandler(path, when='midnight', backupCount=7)
            efh.setFormatter(FORMATTER)
            efh.setLevel(logging.ERROR)
            logger.addHandler(efh)

        console = logging.StreamHandler()
        console.setFormatter(FORMATTER)
        # console.setLevel(level)
        logger.addHandler(console)

    logger = logging.getLogger(name)
    return logger


if __name__ == '__main__':
    log = init('test')
    log.error('test error')
    log.info('test info')
    log.warn('test warn')

    print "ok"
