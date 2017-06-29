# coding: utf-8
import os

from .default import Config


class DevelopmentConfig(Config):
    """Base config class."""
    # Flask app config
    DEBUG = True
    TESTING = False
    SECRET_KEY = "sample_key"

    # Db config
    SQLALCHEMY_BINDS = {
        'geo_pickups': 'mysql+pymysql://root:123qwe,./@10.0.11.91:18806/geo_pickups?charset=utf8'
    }
