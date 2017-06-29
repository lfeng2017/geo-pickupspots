# -*- coding: utf-8 -*-

'''
计算步骤3：调用高德api的周边搜索，对推荐点进行命名

执行方式：ENV=dev/pro python NamingLocally.py --city=bj [ --file=/home/lujin/tmp/data.csv ] [ --clear=True ]
备注：ENV=XXX，用于指定加载的配置文件环境版本

TODO：可以使用本地缓存来减少对高德api的重复请求，比如对坐标进行geohash映射会使用ssdb或者本地dict做缓存

单机使用joblib多进程执行请求

'''

import os
import click
import json
import subprocess
import sys
import time
from tqdm import *
from joblib import Parallel, delayed
import traceback
from itertools import chain
from pyspark.sql import Row     # 为了兼容spark并行的扩展，中间数据还是使用spark Row存储

sys.path.append("../")
sys.path.append("../common/")
sys.path.append("../strategy/")
from strategy import AroundClustering
from common.CollectionUtil import chunks
import config as cfg

# 并发process数量
WORKER = 2  # 进程过多，高德api的并行上限容易被触发
PARTITION_SIZE = 20 * 4
BUFF_PER_WORKER = PARTITION_SIZE * 10

NAME = (os.path.splitext(os.path.basename(__file__))[0])

import common.logger as logger

log = logger.init(NAME)


def process(chunk, tact):
    '''
    对一行数据进行处理，供joblib的多进程调用

    :param line:  输入的一行数据
    :param tact:  执行具体处理的策略对象
    :return: list of json字符串
    '''

    # 策略是基于spark Row对象实现的，为了兼容需要将dict转换为spark Row对象
    listOfRow = [Row(**row) for row in chunk]

    results = tact.amapRequestBatch(listOfRow)

    out = []
    for row in results:
        row = row.asDict()
        # 为了支持spark, 该字段是存储为str类型的
        row['recomms'] = json.loads(row['recomms']) if isinstance(row['recomms'], str) else row['recomms']
        out.append(json.dumps(row))
    return out


def namingByAmap(inFile, outFile, parallel, chunk=1000):
    '''
    调用高德api对推荐点命名，此函数仅是多进程的封装，实际的逻辑处理在AroundClustering类中实现

    :param inFile:  输入文件路径
    :param outFile:  输出文件路径
    :param parallel:  joblib并行粒度，控制多进程的数量
    :param chunk:  josblib分发给多进程的内存缓冲文件行数
    :return:  无
    '''


    try:
        # 处理策略初始化
        tact = AroundClustering.AroundClusteringBatch()

        # 待处理的中间结果
        fpIn = open(inFile, 'r')
        fpOut = open(outFile, 'w')

        # fix bug of joblib
        import threading
        threading.current_thread().name = 'MainThread'

        log.info("begin to parallel process by joblib, prcocess=" + str(parallel))
        # with Parallel(n_jobs=parallel, backend="threading") as parallel:
        with Parallel(n_jobs=parallel) as parallel:
            buff = []
            acc = 0
            t = time.time()
            for line in tqdm(fpIn):

                # 空行排除
                if len(line.strip()) == 0:
                    continue

                acc += 1
                # json to dict， 加入文件读取的缓冲区，缓存满后分发给多进程处理
                buff.append(json.loads(line.strip()))
                if len(buff) >= chunk:
                    results = parallel(delayed(process)(chunk, tact) for chunk in chunks(buff, PARTITION_SIZE))
                    # results = [process(chunk, tact) for chunk in chunks(buff, 20)]
                    fpOut.writelines("%s\n" % line for line in chain(*results) if line is not None)
                    fpOut.flush()
                    buff = []
                if acc % 10000 == 0:
                    log.debug("complete={sz} elasped={t}".format(sz=acc, t=(time.time() - t)))
                    t = time.time()
            else:
                if len(buff) > 0:
                    results = parallel(delayed(process)(chunk, tact) for chunk in chunks(buff, PARTITION_SIZE))
                    # results = [process(chunk, tact) for chunk in chunks(buff, 20)]
                    fpOut.writelines("%s\n" % line for line in chain(*results) if line is not None)
    except Exception, e:
        log.exception("naming by amap api failed")
        exit(-1)
    finally:
        fpIn.close()
        fpOut.close()


def main(city, parallel=WORKER, chunk=(WORKER * 20 * 10), file=None, clear=False):
    '''
    使用joblib的多进程封装，调用高德api进行推荐点命名

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

    # 未指定输入文件名，则采用默认文件夹+city简码拼接
    if not file or len(file) == 0:
        inJson = os.path.join(cfg.setting.GEO_LOCAL_DIR, "pickups_base_recomms_{}.json".format(city))
        outJson = os.path.join(cfg.setting.GEO_LOCAL_DIR, "pickups_base_recomms_withName_{}.json".format(city))
    else:
        inJson = os.path.join(file)
        outJson = os.path.join(cfg.setting.BASE_DIR, "data/pickups_base_recomms_withName_{}.json".format(city))
    log.info("naming pos by amap api, from=" + inJson + " to=" + outJson)

    log.info("begin to caculate recomms, wait for a moment...")
    namingByAmap(inJson, outJson, parallel, chunk=chunk)
    log.info("naming by amap restful api complete!")

    # 删除导出的文件
    if clear:
        shell = "rm -fr {path}".format(path=inJson)
        log.debug(shell)
        subprocess.call(shell, shell=True)
        log.info("clear input file=" + inJson)
    else:
        log.debug("local mode not rm source files")

    log.info("all is done, elasped={t}mins".format(t=(time.time() - t) / 60))


@click.command()
@click.option('--city', type=str, required=True, help='训练的城市')
@click.option('--parallel', type=int, default=WORKER, help='并行度')
@click.option('--chunk', type=int, default=WORKER * BUFF_PER_WORKER, help='本次读取的文件大小')
@click.option('--file', type=str, default=None, help='直接指定文件路径')
@click.option('--clear', type=bool, default=False, help='处理完成后是否删除输入文件（默认不删除）')
def mainWrapper(city, parallel, chunk, file, clear):
    main(city, parallel, chunk, file, clear)
    exit(0)


if __name__ == '__main__':
    mainWrapper()
