# -*- coding: utf-8 -*-

'''
计算步骤2：对拉链数据进行密度聚类，行程后续推荐点

执行方式：ENV=dev/pro python RecommPosLocally.py --city=bj [ --file=/home/lujin/tmp/data.csv ] [ --clear=True ]
备注：ENV=XXX，用于指定加载的配置文件环境版本

单机使用joblib多进程执行sklearn聚类

等价：RecommPosSpark.py（spark并行版）
'''

import os
import click
import json
from pyspark.sql import Row
import subprocess
import time
from tqdm import *
from joblib import Parallel, delayed

import sys

sys.path.append("../")
sys.path.append("../strategy/")
sys.path.append("../common/")
import strategy.AroundClustering as strategy
import config as cfg

# 本地sklearn的process数量
WORKER = 64
BUFF_PER_WORKER = 50

NAME = (os.path.splitext(os.path.basename(__file__))[0])

import common.logger as logger

log = logger.init(NAME)


def invokeProcess(shell):
    '''
    python调用shell的封装

    :param shell: shell命令
    :return: 无
    '''

    log.debug(shell)
    ret = subprocess.call(shell, shell=True)
    if ret != 0:
        log.error("some error in bash command, program will exit with -1")
        exit(-1)


def process(line, tact):
    '''
    对一行数据进行处理，供joblib的多进程调用

    :param line:  输入的一行数据
    :param tact:  执行具体处理的策略对象
    :return:  json字符串
    '''

    header = ['exp_geohash', 'arounds', 'exp_lat', 'exp_lng', 'cooccurs']
    # 组合结果
    segs = line.strip().split(",")
    # 跳过首行
    if segs[0] == "exp_geohash":
        return None
    row = dict(zip(header, segs))

    # 策略模块最早是基于spark实现的，所以需要转换为spark Row对象进行处理
    row = Row(exp_geohash=row['exp_geohash'],
              exp_lat=float(row['exp_lat']),
              exp_lng=float(row['exp_lng']),
              arounds=row['arounds'],
              cooccurs=row['cooccurs'])

    # 调用策略模块计算推荐点
    result = tact.selectKBest(row)

    # 输出数据为json格式
    return None if result.recomms == "NULL" else json.dumps(result.asDict())


def calcRecomms(inFile, outFile, parallel, chunk=1000):
    '''
    使用密度距离计算推荐点，此函数仅是多进程的封装，实际的逻辑处理在AroundClustering类中实现

    :param inFile:  输入文件路径
    :param outFile:  输出文件路径
    :param parallel:  joblib并行粒度，控制多进程的数量
    :param chunk:  joblib分发给多进程的内存缓冲文件行数
    :return:  无
    '''

    try:
        # 处理策略对象初始化
        tact = strategy.AroundClusteringBatch()
        log.debug("calcRecomms from={f} to={t}".format(f=inFile, t=outFile))

        # 待处理的中间结果
        fpIn = open(inFile, 'r')
        fpOut = open(outFile, 'w')

        # fix bug of joblib
        import threading
        threading.current_thread().name = 'MainThread'

        # 使用joblib多进程框架对文件遍历进行并行化
        log.info("begin to parallel process by joblib, prcocess=" + str(parallel))
        with Parallel(n_jobs=parallel) as parallel:
            buff = []
            acc = 0
            t = time.time()
            for line in tqdm(fpIn):

                # 空行排除
                if len(line.strip()) == 0:
                    continue

                # 累计处理函数标记
                acc += 1

                # 从文件中读取chunk行数据，分发给各个进程处理
                buff.append(line)
                if len(buff) >= chunk:
                    results = parallel(delayed(process)(line, tact) for line in buff)
                    # results = [process(line) for line in bufLines]
                    # 处理结果写入输出文件中
                    fpOut.writelines("%s\n" % line for line in results if line is not None)
                    fpOut.flush()
                    # 清空文件读取的buff
                    buff = []
                if acc % 10000 == 0:
                    log.debug("complete={sz} elasped={t}".format(sz=acc, t=(time.time() - t)))
            else:
                # buff剩余部分处理
                if len(buff) > 0:
                    results = parallel(delayed(process)(line, tact) for line in buff)
                    # results = [process(line) for line in lines]
                    fpOut.writelines("%s\n" % line for line in results if line is not None)
    except Exception, e:
        log.exception("using AroundClusteringBatch to clustering recomms failed")
        exit(-1)
    finally:
        fpIn.close()
        fpOut.close()


def copyToLocal(city, hdfs, mv=False):
    '''
    将hdfs的文件拷贝到本地，通过临时目录合并各个part，最后行程1个本地文件

    :param city:  需要拷贝的城市（自动拼接输出文件的名称）
    :param hdfs:  需要拷贝的hdfs目录地址
    :param mv:  是否是mv，如果为True则会删除hdfs上的原始文件
    :return: 无
    '''

    # 本地临时目录
    localDir = os.path.join(cfg.setting.GEO_LOCAL_DIR, "tmp")
    # 合并后的本地文件
    outFile = os.path.join(cfg.setting.GEO_LOCAL_DIR, "pickups_base_chain_{city}.csv".format(city=city))

    # 删除本地缓存目录
    shell = "rm -fr {tmp}/*".format(tmp=localDir)
    invokeProcess(shell)

    # 从hdfs导出json数据
    shell = "hadoop fs -copyToLocal {hdfs}/*  {tmp}/".format(hdfs=hdfs, tmp=localDir)
    invokeProcess(shell)

    # 清空hdfs的json缓存目录
    if mv:
        shell = "hadoop fs -rm -f -r {hdfs}".format(hdfs=hdfs)
        invokeProcess(shell)

    # 本地缓存目录转换为单个文件
    shell = "cat {tmp}/* > {out}".format(tmp=localDir, out=outFile)
    invokeProcess(shell)

    # 清空本地缓存目录
    shell = "rm -fr {tmp}/*".format(tmp=localDir)
    invokeProcess(shell)

    log.info("copyToLocal complete, out=" + outFile)


def main(city, parallel=WORKER, chunk=(WORKER * BUFF_PER_WORKER), file=None, clear=False):
    '''
    入口函数

    :param city:  处理的城市简码，拼接输入输出文件名使用
    :param parallel:  并行度，joblib启动的python进程数量
    :param chunk:  从文件读取的record缓冲条数，缓冲集合用于给joblib多进程分发子数据集使用
    :param file:   输入的文件路径
    :param clear:  处理完成后，是否删除原始输入文件
    :return:  无
    '''

    log.info("city={c} parallel={p} chunk={ck} file={f} clear={d}" \
             .format(c=city, p=parallel, ck=chunk, f=file, d=clear))

    t = time.time()

    if not file or len(file) == 0:

        # 导出到本地（mv控制是否删除hdfs源文件）
        hdfsFile = "/user/geo/interResult/pickups_base_{city}.csv".format(city=city)
        copyToLocal(city, hdfsFile, mv=False)

        inCsv = os.path.join(cfg.setting.GEO_LOCAL_DIR, "pickups_base_chain_{}.csv".format(city))
        outJson = os.path.join(cfg.setting.GEO_LOCAL_DIR, "pickups_base_recomms_{}.json".format(city))
        log.info("read file from default path, inFile={}".format(inCsv))
    else:
        inCsv = os.path.join(file)
        outJson = os.path.join(cfg.setting.BASE_DIR, "data/pickups_base_recomms_{}.json".format(city))
        log.info("read file from input path, inFile={}".format(inCsv))

    log.info("begin to caculate recomms, wait for a moment...")
    calcRecomms(inCsv, outJson, parallel, chunk=chunk)
    log.info("sklearn clustering complete!")

    # 删除导出的文件
    if clear:
        shell = "rm -fr {path}".format(path=inCsv)
        log.debug(shell)
        subprocess.call(shell, shell=True)
        log.info("clear tmp dumped file=" + inCsv)
    else:
        log.debug("do not delete input file")

    log.info("all is done, elasped={t}mins".format(t=(time.time() - t) / 60))


@click.command()
@click.option('--city', type=str, required=True, help='训练的城市')
@click.option('--parallel', type=int, default=WORKER, help='并行度')
@click.option('--chunk', type=int, default=WORKER * BUFF_PER_WORKER, help='每批次处理的文件行数')
@click.option('--file', type=str, default=None, help='直接指定文件路径')
@click.option('--clear', type=bool, default=False, help='处理完成后是否删除输入文件（默认不删除）')
def mainWrapper(city, parallel, chunk, file, clear):
    main(city, parallel, chunk, file, clear)
    exit(0)


if __name__ == '__main__':
    mainWrapper()
