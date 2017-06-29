# coding: utf-8
import os

from .default import Config


class TestingConfig(Config):
    # Flask app config
    DEBUG = False
    TESTING = True
    SECRET_KEY = "sample_key"

    # Db config
    SQLALCHEMY_BINDS = {
        'geo_pickups': 'mysql+pymysql://root:123qwe,./@localhost:8806/geo_pickups?charset=utf8'
    }

