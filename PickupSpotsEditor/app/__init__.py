# -*- coding: utf-8 -*-

import os

from controller.api import api_bp
from flask import Flask


def create_app(mode):
    from config import load_config
    from extensions import db, appbuilder

    config = load_config(mode)

    app = Flask(__name__)
    app.config.from_object(config)

    if not hasattr(app, 'production'):
        app.production = not app.debug and not app.testing

    # Register components
    configure_logging(app)

    # init plugin
    db.init_app(app)
    with app.app_context():
        db.create_all()

    with app.test_request_context("/"):
        appbuilder.init_app(app, db.session)
        # 需要显示赋值app context， 否则绑定@appbuilder.app.errorhandler(404) 报错
        appbuilder.app = app
        import controller
        from app.model import models
        from app.controller import views
        from app.controller import custom
        # 动态构造 models 和 views 里面的类 （因为数据是按city分表的，需要逐个做ORM
        area_cities_dict = app.config.get("CITIES", {})
        models.init_orm(area_cities_dict)
        views.init_views(area_cities_dict)

    # init blueprint
    app.register_blueprint(api_bp)

    app.logger.info("F.A.B create complete!")

    return app


def configure_logging(app):
    import logging
    from logging import Formatter
    from logging.handlers import RotatingFileHandler

    FORMAT = "'%(name)-12s: %(levelname)-8s %(asctime)s - %(message)s'"
    FORMAT_ERR = "'%(name)-12s: %(levelname)-8s %(asctime)s - %(message)s' [in %(pathname)s:%(lineno)d - %(funcName)s]"

    logging.basicConfig(format=FORMAT)
    if app.config.get('TESTING'):
        app.logger.setLevel(logging.CRITICAL)
        return
    elif app.config.get('DEBUG'):
        app.logger.setLevel(logging.DEBUG)
        return

    app.logger.setLevel(logging.INFO)

    # info log
    path = os.path.join(app.config['PROJECT_PATH'], "log/{}_info.log".format(app.config['SITE_TITLE']))
    info_file_handler = RotatingFileHandler(path, maxBytes=104857600, backupCount=10)
    info_file_handler.setLevel(logging.INFO)
    info_file_handler.setFormatter(Formatter(FORMAT))
    app.logger.addHandler(info_file_handler)
    # error log
    path = os.path.join(app.config['PROJECT_PATH'], "log/{}_err.log".format(app.config['SITE_TITLE']))
    err_file_handler = RotatingFileHandler(path, maxBytes=104857600, backupCount=10)
    err_file_handler.setLevel(logging.ERROR)
    err_file_handler.setFormatter(Formatter(FORMAT_ERR))
    app.logger.addHandler(err_file_handler)


'''
from sqlalchemy.engine import Engine
from sqlalchemy import event


# Only include this for SQLLite constraints
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    # Will force sqllite contraint foreign keys
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
'''

'''
import logging

from flask import Flask

from flask_appbuilder import SQLA, AppBuilder

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(logging.DEBUG)

app = Flask(__name__)
app.config.from_object('config')
db = SQLA(app)
appbuilder = AppBuilder(app, db.session)


from app import models, views
'''

