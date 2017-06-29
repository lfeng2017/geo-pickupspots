# -*- coding: utf-8 -*-

'''
计算步骤2 （备用）：对拉链数据进行密度聚类，行程后续推荐点

在spark上使用sklearn进行密度聚类 （通过map的udf函数调用）
目前主要使用单机版本执行，此版本备用

等价：RecommPosLocally.py（多进程单机版）
'''

import os
import click
import json
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import subprocess
import sys
import time
from tqdm import *
import traceback

sys.path.append("../")
sys.path.append("../strategy/")
sys.path.append("../common/")
import AroundClustering as strategy
import config as cfg

is_debug = False


@click.command()
@click.option('--city', type=str, required=True, help='训练的城市')
@click.option('--debug', type=bool, default=False, help='调试开关,减少数据量')
def main(city, debug):
    global is_debug

    print "[INFO] city=", city, "debug=", debug

    try:

        t = time.time()

        spark = SparkSession.builder \
            .master("local[10]") \
            .appName("GetPickupsChain") \
            .enableHiveSupport() \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.default.parallelism", "10") \
            .config("spark.sql.shuffle.partitions", "100") \
            .getOrCreate()

        spark.sparkContext.addPyFile(os.path.join(cfg.setting.BASE_DIR, "strategy/AroundClustering.py"))
        spark.sparkContext.addPyFile(os.path.join(cfg.setting.BASE_DIR, "common/AmapUtil.py"))

        # 加载上车点拉链的中间结果
        dataPath = "/user/geo/interResult/pickups_base_{city}.parquet".format(city=city)
        df = spark.read.parquet(dataPath)

        if is_debug:
            df = df.limit(100)

        print "[DEBUG] size: ", df.count()

        # 进行密度距离, 筛选出候选推荐点
        tact = strategy.AroundClusteringBatch()
        df = df.rdd.map(tact.selectKBest).toDF().filter(col("startPos") != "")
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)

        # 结果写入hdfs
        outPath = "/user/geo/interResult/pickups_base_clustered_{city}.parquet".format(city=city)
        df.write.parquet(outPath, mode="overwrite")
        df.unpersist()

        # 删除上车点拉链中间结果
        shell = "hadoop fs -rm -r {path}".format(path=dataPath)
        subprocess.call(shell, shell=True)

        print "[NOTICE] all is done! out=", outPath, " elasped:", time.time() - t

    except Exception, e:
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
