--上车点推荐, 基准数据, 基本信息表 （关联关系 -> info 1:n detail）
DROP TABLE IF EXISTS `base_pickup_info`;
CREATE TABLE `base_pickup_info` (
  `geohash` varchar(12) NOT NULL COMMENT '期望上车点',
  `city` varchar(10) DEFAULT NULL ,
  `regeo` varchar(50) DEFAULT NULL,
  `radius` smallint(6) DEFAULT NULL COMMENT '推荐半径',
  `num` tinyint(4) DEFAULT NULL COMMENT '推荐点个数',
  `create_time` int(11) DEFAULT NULL,
  `update_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`geohash`),
  UNIQUE KEY `geohash` (`geohash`) USING HASH
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--上车点推荐, 基准数据, 上车点明细
DROP TABLE IF EXISTS `base_pickup_detail`;
CREATE TABLE `base_pickup_detail` (
  `no` bigint(20) NOT NULL AUTO_INCREMENT,
  `exp_geohash` varchar(12) NOT NULL COMMENT '外键, 期望上车点',
  `city` varchar(10) DEFAULT NULL ,
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


--上车点推荐, 增量数据, 基本信息表
DROP TABLE IF EXISTS `incr_pickup_info`;
CREATE TABLE `incr_pickup_info` (
  `geohash` varchar(12) NOT NULL,
  `city` varchar(10) DEFAULT NULL,
  `regeo` varchar(50) DEFAULT NULL,
  `radius` smallint(6) DEFAULT NULL,
  `num` tinyint(4) DEFAULT NULL,
  `create_time` int(11) DEFAULT NULL,
  `update_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`geohash`),
  UNIQUE KEY `geohash` (`geohash`) USING HASH
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--上车点推荐, 增量数据, 上车点明细
DROP TABLE IF EXISTS `incr_pickup_detail`;
CREATE TABLE `incr_pickup_detail` (
  `no` bigint(20) NOT NULL AUTO_INCREMENT,
  `exp_geohash` varchar(12) NOT NULL,
  `city` varchar(10) DEFAULT NULL ,
  `id` varchar(12) NOT NULL COMMENT '推荐点geohash',
  `lat` double DEFAULT NULL,
  `lng` double DEFAULT NULL,
  `name` varchar(50) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `tag` varchar(10) DEFAULT NULL,
  `status` tinyint(4) DEFAULT NULL COMMENT '0: 基准',
  `count` int(11) DEFAULT NULL,
  `show` int(11) DEFAULT NULL,
  `pick` int(11) DEFAULT NULL,
  `ext` varchar(10) DEFAULT NULL,
  `create_time` int(11) DEFAULT NULL,
  `update_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`no`),
  KEY `exp_geohash` (`exp_geohash`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
