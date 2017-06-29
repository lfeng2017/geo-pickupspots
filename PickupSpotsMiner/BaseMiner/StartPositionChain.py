# -*- coding: utf-8 -*-

'''
计算步骤1：推荐点数据拉链提取

使用方式：ENV=dev/pro python StartPositionChain.py --city=bj --start_date=20161201 --end_date=20161230
备注：ENV=XXX，用于指定加载的配置文件环境版本

通过spark sql，提取出期望上车点周边的候选推荐点集合，包括：
1、期望上车latlng与实际上车latlng的共现
2、期望上车点一定半径内的实际上车点latlng（geohash周边关联）

'''

import os
import click
import arrow
import subprocess
from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import Row
import numpy as np
from geopy.distance import vincenty
import sys
import traceback
import time

NAME = (os.path.splitext(os.path.basename(__file__))[0])

sys.path.append("../")
sys.path.append("../common/")
import GeoUtil  # addPyfile是添加到根目录，需要直接import
import config as cfg

from common import logger

log = logger.init(NAME)

# 范围缩小至正负19米
EXPECT_PRECISION = 8
START_PRECISION = 8
# 推荐点的最小支持度（出现频次，频次越高置信度越高）
support_count = 1

# 日期参数
yesterday = arrow.now().replace(days=-1)
start = yesterday.replace(months=-3).span("month")[0]
end = yesterday.replace(months=-1).span("month")[1]


def getHiveData(spark, city, start_date, end_date):
    '''
    从hive取出坐标数据：期望上车latlng, 时间上车点latlng

    :param spark:  spark session 对象
    :param city:  需要提取的城市简写， e.g. bj
    :param start_date: 提取的开始日期，对应hive的partition e.g. 20161201
    :param end_date:  提取的结束日期，对应hive的partition e.g. 20161231
    :return: spark dataframe
    '''

    table = "yc_bit.geo_order_position"
    cols = "expect_start_latitude, expect_start_longitude, start_latitude, start_longitude"
    date_range = "dt between {start} and {end}".format(start=start_date, end=end_date)

    sql = "SELECT {cols} FROM {tbl} where city='{city}' and {date_range}" \
        .format(cols=cols, tbl=table, city=city, date_range=date_range)

    log.debug(sql)
    df = spark.sql(sql)
    return df


def encodeGeoHash(row):
    '''
    为坐标数据计算geohash, 此函数供spark map使用

    :param row: spark Row RDD
    :return: spark Row 对象
    '''

    # 期望上车点，实际上车点，映射为12位的geohash
    expect_geohash = GeoUtil.geohash_encoding(row.expect_start_latitude, row.expect_start_longitude)
    start_geohash = GeoUtil.geohash_encoding(row.start_latitude, row.start_longitude)

    # 取6位精度geohash（正负610米）, 作为请求点和上车点的桥接, 取周边上车点
    exp_rough = expect_geohash[0:6]
    start_rough = start_geohash[0:6]

    # RDD map 返回Row对象，便于转换为dataframe
    return Row(exp_lat=row.expect_start_latitude, \
               exp_lng=row.expect_start_longitude, \
               exp_geohash=expect_geohash[0:EXPECT_PRECISION], \
               start_lat=row.start_latitude, \
               start_lng=row.start_longitude,
               start_geohash=start_geohash[0:START_PRECISION], \
               exp_rough=exp_rough, \
               start_rough=start_rough)


def filterNearestPts(row):
    '''
    按照半径步长递增，过滤后续点的拉链中的点，优先保留半径小范围内的点，目的是减小拉链的长度
    备注：此方法供spark的map方式使用

    :param row:  spark Row RDD
    :return:  spark Row 对象
    '''

    geohash = row.exp_geohash
    exp_lat = row.exp_lat
    exp_lng = row.exp_lng
    pts = row.list

    if not pts or len(pts) == 0:
        return Row(exp_geohash=geohash, exp_lat=exp_lat, exp_lng=exp_lng, list=pts)

    freqs = list()  # 计算出现次数的四分位数缓存
    for pt in pts:
        start_geohash, cnt, lat, lng = pt.strip().split(":")
        cnt = int(cnt)
        freqs.append(cnt)

    # 按出现频次的上四分位数过滤
    size = len(pts)
    if size > 0:
        # 如果点数量较多, 为防止Spark OOM, 则用上四分位数过滤部分支持度低的点（噪声）
        filter_candidates = set()
        threshold = np.percentile(freqs, 75)
        for pt in pts:
            if int(pt.strip().split(":")[1]) >= threshold:
                filter_candidates.add(pt)
        return Row(exp_geohash=geohash, exp_lat=exp_lat, exp_lng=exp_lng, list=list(filter_candidates))
    else:
        # 候选集返回空
        return Row(exp_geohash=geohash, exp_lat=exp_lat, exp_lng=exp_lng, list=list())


def invokeProcess(shell):
    '''
    python 调用shell的封装，判断返回code

    :param shell:  shell命令字符串
    :return: 无
    '''

    log.debug(shell)
    ret = subprocess.call(shell, shell=True)
    if ret != 0:
        log.error("some error in bash command, program will exit with -1, cmd={cmd}".format(cmd=shell))
        exit(-1)


def main(city, start_date=start.format("YYYYMMDD"), end_date=end.format("YYYYMMDD"), debug=False):
    '''
    入口函数

    :param city:  处理的城市简码，供hive提取数据、hdfs存储结果拼接文件名使用
    :param start_date:  日期字符串，对应hive的partition，e.g. 20170310
    :param end_date:  日期字符串，对应hive的partition，e.g. 20170330
    :param debug:  调试开关，控制spark sql的limit数量
    :return: 无
    '''

    log.info("city={c} start={s} end={e}  debug={d}".format(c=city, s=start_date, e=end_date, d=debug))

    try:

        t = time.time()

        spark = SparkSession.builder \
            .master("local[8]") \
            .appName("GetPickupsChain_{city}".format(city=city)) \
            .enableHiveSupport() \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()

        ext_lib = os.path.join(cfg.setting.BASE_DIR, "common/GeoUtil.py")
        log.info("addPyFile={file}".format(file=ext_lib))
        spark.sparkContext.addPyFile(ext_lib)

        # 从hive取数（期望上车点、实际上车点坐标）
        srcDF = getHiveData(spark, city, start_date, end_date)
        if debug:
            srcDF = srcDF.limit(100)
        log.info("load geo data from hive complete, size={sz}".format(sz=srcDF.count()))

        # 给期望上车点、实际上车点计算geohash，用于点合并、group by求拉链
        srcDF.rdd.map(encodeGeoHash).toDF() \
            .persist(StorageLevel.MEMORY_AND_DISK_SER) \
            .createOrReplaceTempView("baseDF")

        # spark.catalog.cacheTable("baseDF")
        log.info("mapping LatLng to geohash, EXPECT_PRECISION={ep} START_PRECISION={sp}" \
                 .format(ep=EXPECT_PRECISION, sp=START_PRECISION))

        # 输出点的个数
        exp_pos_num = spark.sql("select exp_geohash from baseDF group by exp_geohash").count()
        log.info("expPos={sz}".format(sz=exp_pos_num))
        start_pos_num = spark.sql("select start_geohash from baseDF group by start_geohash").count()
        log.info("startPos={sz}".format(sz=start_pos_num))

        # 以期望上车点为基准(left)，聚合实际上车点的坐标， 组合成startPos字段（包括：geohash，频次，坐标均值的list）, 供后续拉链使用
        # 被聚合的实际上车点，先自己按geohash进行group by进行合并，减少点数量
        #
        # 备注：合并后的上车点，不可使用hive的map对象存储相关信息, 否则后续使用collect_set做拉链时会出错
        sql = """
            select exp_geohash, exp_lat, exp_lng, exp_rough, start_rough, concat_ws(':', a.start_geohash, b.cnt, b.lat, b.lng) as startPos, cnt as startCnt
            from baseDF a
            left join (
                select start_geohash, count(*) as cnt, round(mean(start_lat), 6) as lat, round(mean(start_lng), 6) as lng
                from baseDF
                group by start_geohash
            ) b
            on a.start_geohash=b.start_geohash
		"""
        log.debug(sql)

        baseDF = spark.sql(sql)

        srcDF.unpersist()

        # 缓存 -> 期望上车点 left join 实际上车点聚合数据，供后续拉链使用 （拉链使用group by + collect_set的方式提取）
        # geohash1,  startPos1  -->  geohash1, list(startPos1, startPos2, startPos3, startPos4)
        # geohash1,  startPos2
        # geohash1,  startPos3
        # geohash1,  startPos4
        baseDF.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("tempDF")
        # spark.catalog.uncacheTable("baseDF")
        log.info("groupby start pos, get mean LatLng and count")

        # 共现的上车点, 即同一个上车点对应的实际开始点集合
        sql = """
            select exp_geohash, mean(exp_lat) as exp_lat, mean(exp_lng) as exp_lng, collect_set(startPos) as list
            from tempDF
            group by exp_geohash
		"""
        log.debug(sql)
        # 按半径步长过滤，优先取小半径的点，取不到再往外扩，防止拉链过长出现OOM
        coccurDF = spark.sql(sql).rdd.map(filterNearestPts).toDF()
        coccurDF.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("coccurDF")
        log.info("get coocurrPos cached as coccurDF ")

        # 周边上车点（geohash lv=6), 上车点周边正负500米的所有上车点集合
        # [ 期望点geohash_8位 + 期望点geohash_6位 ] left join [ 实际点geohash_6位 + list(实际点Pos)
        sql = """
            select a.exp_geohash, a.exp_lat, a.exp_lng, b.list
            from
            (
                select exp_geohash, exp_rough, mean(exp_lat) as exp_lat, mean(exp_lng) as exp_lng
                from tempDF
                group by exp_geohash, exp_rough
            ) a
            left join
            (
                select start_rough, collect_set(startPos) as list
                from tempDF
                group by start_rough
            ) b
            on a.exp_rough=b.start_rough
		"""
        log.debug(sql)
        # 按半径步长过滤，优先取小半径的点，取不到再往外扩，防止拉链过长出现OOM
        aroundDF = spark.sql(sql).dropDuplicates(["exp_geohash"]).rdd.map(filterNearestPts).toDF()
        aroundDF.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("aroundDF")
        log.info("get around startPos cached as aroundDF")

        baseDF.unpersist()

        # 以期望上车点为基准，合并 周边上车点list + 共现上车点list，写入hdfs
        '''
        数据格式如下：

        期望上车点gehash、lat、lng、周边点list（制表符分割，每个点是一个字符串组合 -> "geohash:频次:lat:lng"）、共现点list（同上）
        '''
        sql = """
        select a.exp_geohash,
            concat_ws('\t', a.list) as arounds,
            round(a.exp_lat, 6) as exp_lat,
            round(a.exp_lng, 6) as exp_lng,
            concat_ws('\t', b.list) as cooccurs
        from aroundDF a left join coccurDF b
        on a.exp_geohash=b.exp_geohash
        """
        mergeDF = spark.sql(sql).persist(StorageLevel.MEMORY_AND_DISK_SER)

        aroundDF.unpersist()
        coccurDF.unpersist()

        log.info("merge coocurrPos and aroundDF to generate recomms list, then cached the result")

        # 若输出目录不存在, 则创建
        if subprocess.call("hadoop fs -test -d /user/geo/interResult", shell=True) != 0:
            log.info("interResult dir no exists, create it fristly...")
            subprocess.call("hadoop fs -mkdir -p /user/geo/interResult", shell=True)

        # 导出parquet格式
        # outFile = "/user/geo/interResult/pickups_base_{city}.parquet".format(city=city)
        # mergeDF.write.parquet(outFile, mode="overwrite")
        # mergeDF.unpersist()
        # log.info("write result to hdfs by parquet complete, out=" + outFile)

        # 导出csv格式至hdfs
        outFile = "/user/geo/interResult/pickups_base_{city}.csv".format(city=city)
        mergeDF.write.csv(outFile, mode="overwrite")
        mergeDF.unpersist()

        log.info("all is done! out={out} elasped={t}mins".format(out=outFile, t=(time.time() - t) / 60))

    except Exception, e:
        log.exception("using spark to generate recomms list failed")
        exit(-1)
    finally:
        spark.stop()


@click.command()
@click.option('--city', type=str, required=True, help='训练的城市')
@click.option('--start_date', type=str, default=start.format("YYYYMMDD"), help='训练开始日期')
@click.option('--end_date', type=str, default=end.format("YYYYMMDD"), help='训练结束日期')
@click.option('--debug', type=bool, default=False, help='调试开关,减少数据量')
def mainWrapper(city, start_date, end_date, debug):
    main(city, start_date, end_date, debug)
    exit(0)


if __name__ == '__main__':
    mainWrapper()
