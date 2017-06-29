-- 订单坐标表（该表的数据是 ods_service_order + ods_service_order_ext 拼接而成）
CREATE EXTERNAL TABLE `yc_bit`.`geo_order_position`(
    `city` string,
    `service_order_id` bigint,
    `user_id` bigint,
    `driver_id` bigint,
    `start_time` int,
    `end_time` int,
    `start_position` string,
    `start_address` string,
    `end_position` string,
    `end_address` string,
    `expect_start_latitude` double,
    `expect_start_longitude` double,
    `expect_end_latitude` double,
    `expect_end_longitude` double,
    `start_latitude` double,
    `start_longitude` double,
    `end_latitude` double,
    `end_longitude` double,
    `create_order_longitude` double,
    `create_order_latitude` double,
    `confirm_latitude` double,
    `confirm_longitude` double, 
    `arrive_latitude` double,
    `arrive_longitude` double
)
PARTITIONED BY (`dt` int)
STORED AS PARQUET
LOCATION
  'hdfs://hdp25/user/hive/warehouse/yc_bit.db/geo_order_position'
