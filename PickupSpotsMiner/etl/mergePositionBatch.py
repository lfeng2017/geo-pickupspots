# -*- coding: utf-8 -*-
"""
以月为单位，通过fo_service_order, fo_service_order_ext的tsv文件
合并出订单坐标数据，以parquet格式，写入hive表 (partition = dt)
"""

import os
import sys
import time
import pandas as pd
from pyspark.sql import Row
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import arrow
import click
import logbook
import traceback

NAME = (os.path.splitext(os.path.basename(__file__))[0])

logbook.StreamHandler(sys.stdout).push_application()
log = logbook.Logger(NAME)
log.level = logbook.INFO


def loadTsv2Hive(start, end, spark, tsvPath):
    for m in pd.date_range(start, end, freq="M"):
        month = m.strftime("%Y%m")
        log.info("load tsv file, month=" + month)

        name = "fo_service_order_geo_{m}.tsv".format(m=month)
        orderDF = spark.createDataFrame(
            spark.sparkContext.textFile(os.path.join(tsvPath, name)) \
                .map(lambda l: l.split("\t")) \
                .map(lambda p: Row(dt=int(p[0]), city=p[1], service_order_id=int(p[2]),
                                   user_id=int(p[3]), driver_id=int(p[4]),
                                   start_time=int(p[5]), end_time=int(p[6]),
                                   start_position=p[7], start_address=p[8],
                                   end_position=p[9], end_address=p[10],
                                   expect_start_latitude=float(p[11]), expect_start_longitude=float(p[12]),
                                   expect_end_latitude=float(p[13]), expect_end_longitude=float(p[14]),
                                   start_latitude=float(p[15]), start_longitude=float(p[16]),
                                   end_latitude=float(p[17]), end_longitude=float(p[18])))) \
            .dropDuplicates(["service_order_id"])
        log.debug("load tsv={} to dataframe complete".format(name))

        name = "fo_service_order_ext_geo_{m}.tsv".format(m=month)
        extDF = spark.createDataFrame(
            spark.sparkContext.textFile(os.path.join(tsvPath, name)) \
                .map(lambda l: l.split("\t")) \
                .map(lambda p: Row(dt=int(p[0]), dest_city=p[1], service_order_id=int(p[2]),
                                   create_order_longitude=float(p[3]), create_order_latitude=float(p[4]),
                                   confirm_latitude=float(p[5]), confirm_longitude=float(p[6]),
                                   arrive_latitude=float(p[7]), arrive_longitude=float(p[8])))) \
            .dropDuplicates(["service_order_id"])
        log.debug("load tsv={} to dataframe complete".format(name))

        # merge latlng by order and order_ext
        orderDF.join(extDF, orderDF.service_order_id == extDF.service_order_id, 'left') \
            .drop(extDF.service_order_id) \
            .drop(extDF.dt) \
            .createOrReplaceTempView("temp")
        spark.catalog.cacheTable("temp")
        log.debug("join 2 table and cache in memory, month={}".format(m))

        # write into hive, day by day
        start, end = arrow.get(m).span('month')
        for dt in pd.date_range(start.format("YYYYMMDD"), end.format("YYYYMMDD"), freq="D"):
            dt = dt.strftime("%Y%m%d")
            table = "geo_order_position"
            cols = "city,service_order_id,user_id,driver_id,start_time,end_time,start_position,start_address,end_position,end_address,expect_start_latitude,expect_start_longitude,expect_end_latitude,expect_end_longitude,start_latitude,start_longitude,end_latitude,end_longitude,create_order_longitude,create_order_latitude,confirm_latitude,confirm_longitude,arrive_latitude,arrive_longitude"
            sql = "insert overwrite table yc_bit.{tbl} partition (dt={dt}) select {cols} from temp where dt={dt}" \
                .format(tbl=table, cols=cols, dt=dt)
            spark.sql(sql)
            log.info("dt={} load complete!".format(dt))

        spark.catalog.uncacheTable("temp")


@click.command()
@click.option('--month_offset', type=int, default=-3, help='使用前N个月的数据（默认为最近3个月）')
@click.option('--tsv_path', type=str, default="file:///home/lujin/tmp", help='待导入tsv文件的存放目录')
@click.option('--verbose', type=bool, default=False, help='输出详细信息')
def main(month_offset, tsv_path, verbose):
    start = arrow.now().replace(months=month_offset).format("YYYYMMDD")
    end = arrow.now().format("YYYYMMDD")

    log.info("month_offset={o} start_date={s} end_date={e} tsv={t} verbose={v}" \
             .format(o=month_offset, s=start, e=end, t=tsv_path, v=verbose))

    if verbose:
        log.level = logbook.DEBUG

    t = time.time()
    spark = None
    try:
        spark = SparkSession.builder \
            .master("local[4]") \
            .appName("MergePositionDaily") \
            .enableHiveSupport() \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "100") \
            .getOrCreate()

        # 将tsv文件按月加载，并按天导入hive
        loadTsv2Hive(start, end, spark, tsv_path)

        log.notice("all is done, elasped:{}".format(time.time() - t))
        exit(0)

    except Exception, e:
        traceback.print_exc()
        exit(-1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
