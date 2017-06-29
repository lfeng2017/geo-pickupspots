# -*- coding: utf-8 -*-

'''
计算步骤4：将命名后的推荐点写入持久化存储

执行方式：ENV=dev/pro python PersistRecommPos.py --city=bj [ --clear=True ]
备注：ENV=XXX，用于指定加载的配置文件环境版本

从csv文件读取数据，转换为pandas dataframe后，使用pandas的to_sql()方法写入
'''

import os
import click
import json
import subprocess
import sys
import time
from tqdm import *
import traceback
import arrow
import pandas as pd

sys.path.append("../")
sys.path.append("../common")
import config as cfg

NAME = (os.path.splitext(os.path.basename(__file__))[0])

import common.logger as logger

log = logger.init(NAME)


def loadToMysql(inFile, city, debug=False):
    '''
    按city分表，读取推荐结果的csv文件，写入数据库

    :param inFile: 输入文件路径
    :param city: 城市简码，拼接数据库表名
    :param debug: debug开关，打开的情况只写入少量行
    :return:
    '''

    from sqlalchemy import create_engine
    from pymysql.converters import escape_string

    try:
        engine = create_engine(cfg.setting.DB_URL)
        log.info("connect to mysql: " + cfg.setting.DB_URL)

        # 每个城市，分为info和detail两张表存储
        # info以期望上车点为基准
        # detail作为关联表，存某个期望上车点对应的推荐上车点
        table_info = "base_pickup_info_{}".format(city)
        table_detail = "base_pickup_detail_{}".format(city)

        baseInfo = 0
        # 每次加载，若表已存在，会先清空数据
        if engine.dialect.has_table(engine, table_info):
            baseInfo = engine.execute("DELETE FROM {tbl} where city='{city}'" \
                                      .format(tbl=table_info, city=city)).rowcount
        else:  # 表不存在则先建表
            engine.execute("""
CREATE TABLE `{}` (
  `geohash` varchar(12) NOT NULL COMMENT '期望上车点',
  `city` varchar(10) DEFAULT NULL,
  `regeo` varchar(50) DEFAULT NULL,
  `radius` smallint(6) DEFAULT NULL COMMENT '推荐半径',
  `num` tinyint(4) DEFAULT NULL COMMENT '推荐点个数',
  `create_time` int(11) DEFAULT NULL,
  `update_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`geohash`),
  UNIQUE KEY `geohash` (`geohash`) USING HASH
) ENGINE=InnoDB DEFAULT CHARSET=utf8
            """.format(table_info))

        log.info("create table={}".format(table_info))

        baseDetail = 0
        # 每次加载，若表已存在，会先清空数据
        if engine.dialect.has_table(engine, table_detail):
            baseDetail = engine.execute("DELETE FROM {tbl} where city='{city}'" \
                                        .format(tbl=table_detail, city=city)).rowcount
        else:  # 表不存在则先建表
            engine.execute("""
CREATE TABLE `{}` (
  `no` bigint(20) NOT NULL AUTO_INCREMENT,
  `exp_geohash` varchar(12) NOT NULL COMMENT '外键, 期望上车点',
  `city` varchar(10) DEFAULT NULL,
  `id` varchar(12) NOT NULL COMMENT '推荐点geohash',
  `lat` double DEFAULT NULL,
  `lng` double DEFAULT NULL,
  `name` varchar(50) DEFAULT NULL COMMENT '推荐点regeo',
  `score` float DEFAULT NULL COMMENT '权重',
  `tag` varchar(10) DEFAULT NULL COMMENT '标志位',
  `status` tinyint(4) DEFAULT NULL COMMENT '0: 基准',
  `count` int(11) DEFAULT NULL,
  `show` int(11) DEFAULT NULL,
  `pick` int(11) DEFAULT NULL,
  `ext` varchar(10) DEFAULT NULL,
  `create_time` int(11) DEFAULT NULL,
  `update_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`no`),
  KEY `exp_geohash` (`exp_geohash`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1373559 DEFAULT CHARSET=utf8
            """.format(table_detail))
            log.info("create table={}".format(table_detail))

        log.info("delete records info={info} detail={detail}".format(info=baseInfo, detail=baseDetail))

        # 打开输入文件
        with open(inFile, 'r') as fpIn:
            line_cnt = 0  # 上车点个数
            pos_cnt = 0
            t = time.time()
            info_buff = []
            recomm_buff = []
            for line in tqdm(fpIn):

                if debug and line_cnt > 100:
                    log.debug("debug mode only process 100 lines")
                    break

                # 构造info，并缓存
                record = json.loads(line.strip())
                record['geohash'] = record['startPos']
                del record['startPos']
                record['city'] = city
                record['regeo'] = unicode(escape_string(record['regeo'].encode("utf-8")), "utf-8")
                record['create_time'] = arrow.now().timestamp
                record['update_time'] = arrow.now().timestamp
                info_buff.append(record)

                # 构造recomm points， 并缓存
                for pos in record['recomms']:
                    pos['no'] = pos_cnt
                    pos_cnt += 1
                    pos['exp_geohash'] = record['geohash']
                    pos['city'] = city
                    pos['create_time'] = arrow.now().timestamp
                    pos['update_time'] = arrow.now().timestamp
                    pos['name'] = unicode(escape_string(pos['name'].encode("utf-8")), "utf-8")
                    recomm_buff.append(pos)

                # 每500行，批量写入1次
                if len(info_buff) > 1000:
                    # 写入info数据
                    cols = ['geohash', 'city', 'regeo', 'radius', 'num', 'create_time', 'update_time']
                    df = pd.DataFrame.from_records(info_buff)[cols]
                    df.to_sql(table_info, engine, if_exists="append", index=False, chunksize=200)
                    info_buff = []
                    # 写入recomms数据
                    cols = ['exp_geohash', 'city', 'id', 'lat', 'lng', 'name', 'score', 'tag', 'status',
                            'count', 'show', 'pick', 'ext', 'create_time', 'update_time']
                    df = pd.DataFrame.from_records(recomm_buff)[cols]
                    df.to_sql(table_detail, engine, if_exists="append", index=False, chunksize=200)
                    recomm_buff = []

                # 行计数器
                line_cnt += 1
                if line_cnt % 50000 == 0:
                    log.debug("complete={sz} elapsed={t}".format(sz=line_cnt, t=(time.time() - t)))

            else:  # 批量写入
                if len(info_buff) > 0:
                    # 写入info
                    cols = ['geohash', 'city', 'regeo', 'radius', 'num', 'create_time', 'update_time']
                    df = pd.DataFrame.from_records(info_buff)[cols]
                    df.to_sql(table_info, engine, if_exists="append", index=False)
                    del info_buff
                    # 写入recomms
                    cols = ['exp_geohash', 'city', 'id', 'lat', 'lng', 'name', 'score', 'tag', 'status',
                            'count', 'show', 'pick', 'ext', 'create_time', 'update_time']
                    df = pd.DataFrame.from_records(recomm_buff)[cols]
                    df.to_sql(table_detail, engine, if_exists="append", index=False, chunksize=200)
                    del recomm_buff

    except Exception, e:
        log.exception("write recomms to mysql failed")
        exit(-1)

    log.info("load data to mysql complete")


def main(city, debug=False, clear=False):
    '''
    入口函数

    :param city:  处理的城市简码，拼接数据库表名
    :param debug: debug模式开关，True的话只写入少量的数据
    :return: 无
    '''

    log.info("city={c} debug={d} local={l}".format(c=city, d=debug, l=local))

    t = time.time()

    inJson = os.path.join(cfg.setting.GEO_LOCAL_DIR, "pickups_base_recomms_withName_{city}.json".format(city=city))
    log.info("begin to persist data inFile={inPath}".format(inPath=inJson))

    loadToMysql(inJson, city, debug=debug)

    # 删除的文件
    shell = "rm -fr {path}".format(path=inJson)
    log.debug(shell)
    if clear:
        subprocess.call(shell, shell=True)
        log.info("clear input file=" + inJson)
    else:
        log.debug("local mode not rm source files")

    log.info("all is done, elasped={t}mins".format(t=(time.time() - t) / 60))


@click.command()
@click.option('--city', type=str, required=True, help='训练的城市')
@click.option('--debug', type=bool, default=False, help='调试开关,减少数据量')
@click.option('--clear', type=bool, default=False, help='加载完成后清除源文件')
def mainWrapper(city, debug, local):
    main(city, debug, local)
    exit(0)


if __name__ == '__main__':
    mainWrapper()
