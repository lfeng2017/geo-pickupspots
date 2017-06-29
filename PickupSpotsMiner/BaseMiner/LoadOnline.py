# -*- coding: utf-8 -*-

'''
计算步骤5：推荐点数据写入LBS的缓存

执行方式：ENV=dev/pro python LoadOnline.py --city=bj [ --file=/home/lujin/tmp/data.csv ]
备注：ENV=XXX，用于指定加载的配置文件环境版本

从mysql或是文件，将推荐上车点的数据，通过restful接口写入LBS使用的mongo
'''

import requests
import os
import click
import json
import sys
import time
from collections import namedtuple
from tqdm import *

sys.path.append("../")
sys.path.append("../common")
import config as cfg

NAME = (os.path.splitext(os.path.basename(__file__))[0])

API_BUFF_SIZE = 10  # 每次写入restful接口的条数
PAGE_SIZE = 2000  # mysql分页长度

import common.logger as logger

log = logger.init(NAME)


def formatDoc(doc):
    '''
    调整写入mongo的数据格式, 修正字段名称, 移除多余的字段

    :param doc: 待调整的json dict对象
    :return: 调整后的json dict对象
    '''

    # mongo的id使用期望上车点的geohash
    doc['_id'] = doc['startPos']
    del doc['startPos']

    # 从文件加载的方式才需用转换
    if 'update_time' not in doc.keys():
        doc['update_time'] = doc['update']
        del doc['update']

    # 从文件加载的方式才需要删除, mysql版本直接不取这些字段
    # 从文件加载方式，删除多余字段
    if 'lat' in doc.keys():
        del doc['lat']
    if 'lng' in doc.keys():
        del doc['lng']

    return doc


def loadByFile(city, file, debug=False):
    '''
    从文件加载推荐结果，通过restful接口写入LBS的mongo缓存

    :param city: 城市简码
    :param file: 输入文件路径
    :param debug: 调试开关，True则只试写少量数据
    :return: 无
    '''

    try:

        url = cfg.setting.MONGO_API.format(city=city)

        ' 从文件加载 '
        with open(file, 'r') as fp:
            acc = 0
            buff = []
            for line in tqdm(fp):
                acc += 1

                if debug and acc > 100:
                    log.debug("debug mode only process 100 lines")
                    break

                # json to dict
                doc = json.loads(line.strip())
                doc = formatDoc(doc)

                # 结果批量写入接口
                buff.append(doc)
                if len(buff) > API_BUFF_SIZE:
                    requests.put(url, json.dumps(buff))
                    buff = []
            else:
                if len(buff) > 0:
                    requests.put(url, json.dumps(buff))
                    buff = []
    except Exception, e:
        log.exception("loadByFile failed")
        exit(-1)


def loadByMysql(city, debug=False):
    '''
    从mysql加载推荐点数据，分页遍历table，通过restful接口写入LBS的mongo缓存

    :param city: 城市简码
    :param debug: 调试开关，True则只试写少量数据
    :return: 无
    '''

    from sqlalchemy import create_engine

    engine = create_engine(cfg.setting.DB_URL)

    with engine.connect() as conn:

        # 按城市分表
        tbl = "base_pickup_info_{}".format(city)

        # 获取总记录数, 设置分页参数
        cnt = conn.execute("SELECT count(1) from {tbl} where city='{city}'".format(tbl=tbl, city=city)).fetchone()[0]
        offset, batch, pages = 0, PAGE_SIZE, cnt / PAGE_SIZE
        log.debug("total={t} offset={o} batch={s} pages={p}".format(t=cnt, o=offset, s=batch, p=pages))

        # 按页长遍历mysql的表，提取数据集，通过接口写入LBS的mongo
        for i in range(0, pages):

            if debug and offset > 100:
                log.debug("debug mode only process 100 lines")
                break

            # 按页处理
            processPaging(conn, city, offset, batch)

            # 累加页偏移
            offset += batch
        else:
            batch = cnt - offset
            log.debug("last page total={t} offset={o} batch={s}".format(t=cnt, o=offset, s=batch))
            # 按页处理
            processPaging(conn, city, offset, batch)


def processPaging(conn, city, offset, batch):
    '''
    遍历mysql的分页，组合推荐结果数据，通过restful接口写入LBS的mongo缓存

    :param conn: sqlalchemy的连接
    :param city: 城市简码
    :param offset: 页偏移量
    :param batch: 页长
    :return: 无
    '''

    url = cfg.setting.MONGO_API.format(city=city)

    # 根据城市简码，拼接info表名
    tbl = "base_pickup_info_{}".format(city)
    sql = "select geohash as startPos, regeo, radius, num, update_time " \
          "from {tbl} where city='{city}' limit {o}, {s} " \
        .format(tbl=tbl, city=city, o=offset, s=batch)

    # 取一页数据集
    rs = conn.execute(sql)
    Record = namedtuple('Record', rs.keys())
    records = [Record(*r) for r in rs.fetchall()]
    buff = []  # rest api batch buff

    # 逐条遍历记录, 取对应的上车点
    for doc in tqdm(records):
        doc = dict(doc.__dict__)
        recomms = []

        # 获取每一个上车点的推荐点数据
        tbl = "base_pickup_detail_{}".format(city)
        sql = "select id, lat, lng, name, score, tag " \
              "from {tbl} where exp_geohash='{id}'" \
            .format(tbl=tbl, id=doc['startPos'])
        rs = conn.execute(sql)
        Point = namedtuple('Point', rs.keys())
        Points = [Point(*r) for r in rs.fetchall()]
        for pt in Points:
            recomms.append(dict(pt.__dict__))
        doc['recomms'] = recomms

        # 最终写入数据格式转换
        doc = formatDoc(doc)

        # import pprint
        # pprint.pprint(doc)

        # 接口方式
        buff.append(doc)
        if len(buff) >= API_BUFF_SIZE:
            requests.put(url, json.dumps(buff))
            buff = []
    else:
        if len(buff) > 0:
            requests.put(url, json.dumps(buff))
            buff = []


def main(city, file, debug):
    '''
    入口函数

    :param city: 城市简码
    :param file: 从文件直接加载数据（若不指定文件则从mysql加载）
    :param debug: 调试开关，True则只试写少量数据
    :return: 无
    '''

    log.info("city={c} file={f} debug={d}".format(c=city, f=file, d=debug))

    t = time.time()

    if not file:
        log.info("no input file, load from Mysql ...")
        loadByMysql(city, debug)
    else:
        log.info("load data from file={f} ...".format(f=file))
        loadByFile(city, file, debug)

    log.info("all is done, elasped={t}mins".format(t=(time.time() - t) / 60))


@click.command()
@click.option('--city', type=str, required=True, help='训练的城市')
@click.option('--file', type=str, default=None, help='本地文件路径（不指定则默认从mysql加载）')
@click.option('--debug', type=bool, default=False, help='调试开关,减少数据量')
def mainWrapper(city, file, debug):
    main(city, file, debug)
    exit(0)


if __name__ == '__main__':
    mainWrapper()
