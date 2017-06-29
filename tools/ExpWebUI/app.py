# -*- coding: utf-8 -*-

import json
import logging

from flask import Flask

import config
from ext import api, cache, redis, mongo
import views


def init_cache(app):
    with open(app.config["RECOMM_RESULT"], "r") as fp:
        lines = 0
        for line in fp:
            recomm = line.strip().split('\t')
            if len(recomm) != 2:
                continue
            else:
                cache.set(recomm[0], json.loads(recomm[1]), timeout=0)
                lines += 1
    print "load recomms={size}".format(size=lines)


def init_logger(app):
    # logging.basicConfig(level=logging.INFO,
    # 					format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    # 					datefmt='%m-%d %H:%M',
    # 					filename='myapp.log',
    # 					filemode='w')
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    app.logger.addHandler(console)


def create_app():
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.config.from_object(config)
    redis.init_app(app)
    # mongo.init_app(app)
    # db.init_app(app)

    # init_cache(app)
    init_logger(app)

    for bp in views.all_bp:
        app.register_blueprint(bp)

    return app


if __name__ == '__main__':
    app = create_app()
    for rule in app.url_map.iter_rules():
       print rule
    app.run(host='0.0.0.0', port=5000, debug=app.debug)
