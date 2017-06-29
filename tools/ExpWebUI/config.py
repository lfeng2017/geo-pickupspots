import os

APP = "GeoUI"

BASE_PATH = os.path.abspath(os.path.dirname(__file__))

JSON_AS_ASCII=False

DATABASE_URI = 'sqlite://:memory:'
RECOMM_RESULT = os.path.join(BASE_PATH, "data/geo_recomm_result.txt")
REDIS_URL = "redis://10.0.11.91:7000/0"
MONGO_URI = "mongodb://10.0.11.192:27012,10.0.11.193:27012,10.0.11.198:27012"
#MONGO_URI = "mongodb://localhost:30000"
MYSQL_URI = "mysql+pymysql://root:123qwe,./@localhost:8806/geo_pickups?charset=utf8"

DEBUG = False

EXPECT_PRECISION=8

try:
	from local_settings import *  # noqa
except ImportError:
	pass

