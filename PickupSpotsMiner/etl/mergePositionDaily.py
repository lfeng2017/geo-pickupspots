# -*- coding: utf-8 -*-
"""
此

指定1天日期，从本地hive：fo_service_order, fo_service_order_ext
合并出订单坐标数据，以parquet格式，写入geo_order_position

执行方式：python mergePositionDaily.py [ --date=20170310 ]

"""

import os
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import arrow
import click
import logging
import sys
import traceback

sys.path.append("../")
sys.path.append("../common")

NAME = (os.path.splitext(os.path.basename(__file__))[0])

import common.logger as logger

log = logger.init(NAME)

yesterday = arrow.now('Asia/Shanghai').replace(days=-1)


def mergeGeoData(date, spark, debug=False):
    '''
    执行日期

    :param date: 需要进行数据合并的partition dt， e.g. 20170410
    :param spark: spark session
    :param debug: debug开关，True的情况不实际执行spark sql
    :return: 合并后的spark dataframe
    '''

    sql = """
select
o.dt, city,o.service_order_id,user_id,driver_id,start_time, end_time,start_position,start_address,end_position,end_address,expect_start_latitude,expect_start_longitude,expect_end_latitude,expect_end_longitude,start_latitude,start_longitude,end_latitude,end_longitude,create_order_longitude,create_order_latitude,confirm_latitude,confirm_longitude,arrive_latitude,arrive_longitude
from
(
    --订单表
    select * from (
        select dt,city,service_order_id,user_id,driver_id,start_time, end_time,start_position,start_address,end_position,end_address,expect_start_latitude,expect_start_longitude,expect_end_latitude,expect_end_longitude,start_latitude,start_longitude,end_latitude,end_longitude,
        row_number() over (partition by service_order_id order by update_time desc ) as rank
        from yc_bit.fo_service_order
        where dt={dt} and status=7 and start_latitude>0 and start_longitude>0 and expect_start_latitude>0 and expect_start_longitude>0
    ) t1
    where rank=1
) o
left join
(
    --订单表扩展
    select * from (
        select dt,dest_city,service_order_id,create_order_longitude,create_order_latitude,confirm_latitude,confirm_longitude,arrive_latitude,arrive_longitude,
        row_number() over (partition by service_order_id order by update_time desc ) as rank
        from yc_bit.fo_service_order_ext
        where dt={dt} and create_order_longitude>0 and create_order_latitude>0 and arrive_latitude>0 and arrive_longitude>0
    ) t2
    where rank=1
) ext
on o.service_order_id=ext.service_order_id
    """.format(dt=date)
    log.debug(sql)

    df = None
    if not debug:
        df = spark.sql(sql)
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    return df


@click.command()
@click.option('--date', type=str, default=yesterday.format("YYYYMMDD"), help='日期')
@click.option('--verbose', type=bool, default=False, help='输出详情')
@click.option('--debug', type=bool, default=False, help='调试, 不执行')
def main(date, verbose, debug):
    '''
    入口函数

    :param date: 执行日期，默认为当前日期的前一天，e.g. 20170410
    :param verbose: 控制日志级别，True的话为debug基本
    :param debug: debug开关，True的话不执行spark sql
    :return: 无
    '''

    log.info("date={dt} verbose={v} debug={d}".format(dt=date, v=verbose, d=debug))

    if debug:
        log.info("debug mode will no exec spark code")

    if verbose:
        log.level = logging.DEBUG

    try:
        spark = None
        if not debug:
            spark = SparkSession.builder \
                .master("local[4]") \
                .appName("MergePositionDaily") \
                .enableHiveSupport() \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.default.parallelism", "8") \
                .config("spark.sql.shuffle.partitions", "100") \
                .getOrCreate()

        # 从service_order和service_order_ext合并出订单的所有坐标信息
        geoDF = mergeGeoData(date, spark, debug=debug)

        if geoDF is None or geoDF.count() == 0:
            log.error("no records with dt={dt}, please check your datasource".format(dt=date))
            exit(-1)

        # 合并后的中间结果存为临时表
        geoDF.createOrReplaceTempView("temp")

        # write to hive
        table = "yc_bit.geo_order_position"
        cols = "city,service_order_id,user_id,driver_id,start_time,end_time,start_position,start_address,end_position,end_address,expect_start_latitude,expect_start_longitude,expect_end_latitude,expect_end_longitude,start_latitude,start_longitude,end_latitude,end_longitude,create_order_longitude,create_order_latitude,confirm_latitude,confirm_longitude,arrive_latitude,arrive_longitude"
        sql = "insert overwrite table {tbl} partition (dt={dt}) select {cols} from temp where dt={dt}" \
            .format(tbl=table, cols=cols, dt=date)
        log.debug(sql)
        if not debug:
            spark.sql(sql)

        log.info("merge geo data of order complete! dt={dt} size={sz}".format(dt=date, sz=geoDF.count()))
        exit(0)

    except Exception, e:
        traceback.print_exc()
        exit(-1)
    finally:
        if not debug:
            geoDF.unpersist()
            spark.stop()


if __name__ == "__main__":
    main()
