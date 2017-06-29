# -*- coding: utf-8 -*-

from flask_restful import reqparse, abort, Api, Resource
from flask_redis import FlaskRedis
from flask_pymongo import PyMongo
from werkzeug.contrib.cache import SimpleCache
import config as cfg

api = Api()
cache = SimpleCache()
redis = FlaskRedis()

from rediscluster import RedisCluster

redis_nodes = [
    {"host": "10.0.11.174", "port": "6381"},
    {"host": "10.0.11.174", "port": "6382"},
    {"host": "10.0.11.174", "port": "6383"}
]
rc = RedisCluster(startup_nodes=redis_nodes, max_connections=10, decode_responses=True)

mongo = PyMongo()
