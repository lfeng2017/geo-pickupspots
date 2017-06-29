#!/usr/bin/env python
# encoding: utf-8
import arrow
from flask import Blueprint, request, jsonify, g
from flask_restful import reqparse, Resource, inputs
from app.extensions import api, db
from app.core import log
# from app.model.models import BasePickupInfo, BasePickupDetail
from app.model.models import DYNAMIC_ORMS
from common import GeoUtil

api_bp = Blueprint('api', __name__, url_prefix='/api')
api.init_app(api_bp)


class recomms(Resource):
    parser = reqparse.RequestParser(bundle_errors=True)
    parser.add_argument('city', type=str, help=u'city shortcut is needed', required=True)
    parser.add_argument('lat', type=float, help=u'start latitude', required=False)
    parser.add_argument('lng', type=float, help=u'start lngtitude', required=False)
    parser.add_argument('geohash', type=str, help=u'start position geohash', required=False)

    def get(self):
        args = self.parser.parse_args()
        city = args.get('city')
        lat = args.get('lat', None)
        lng = args.get('lng', None)
        geohash = args.get('geohash', None)

        id = None
        if not geohash and (not lat or not lng):
            log.warn("both geohash and latlng are null")
            return jsonify({"ret": -1, "info": "both geohash and latlng are null"})
        elif geohash:
            id = geohash[0:8]
        elif lat and lng:
            id = GeoUtil.geohash_encoding(lat, lng, 8)
        else:
            log.warn("args composition is invalid lat={lat} lng={lng} geohash={geohash}" \
                     .format(lat=lat, lng=lng, geohash=geohash))
            return jsonify({"ret": 1, "info": "args composition is invalid, see log for detail"})

        log.info("city={city} latlng={lat},{lng} geohsh={geo}".format(city=city, lat=lat, lng=lng, geo=id))

        # 从DB获取上车点信息

        # 获取动态创建的model
        Model = DYNAMIC_ORMS.get(city, None)
        if not Model:
            log.warn("can not get model of city={}".format(city))
            return jsonify({"ret": -2, "info": "can not get model of city"})
        InfoModel, DetailFkModel, DetailModel = Model

        info = db.session.query(InfoModel).filter_by(geohash=id).first()
        # info = db.session.query(BasePickupInfo).filter_by(geohash=id).first()
        if not info:
            log.info("no start pos find")
            return jsonify({
                "geohash": id,
                "city": city,
                "regeo": "no recomms",
                "radius": 0,
                "num": 0,
                "recomms": [],
                "create_time": arrow.get().timestamp,
                "update_time": arrow.get().timestamp
            })
        log.debug("startPos -> {}".format(info.to_json()))

        # 获取推荐点信息
        recomms = []
        for pos in db.session.query(DetailFkModel).filter_by(exp_geohash=id).all():
            # for pos in db.session.query(BasePickupDetail).filter_by(exp_geohash=id).all():
            recomms.append({
                "id": pos.id,
                "lat": float(pos.lat),
                "lng": float(pos.lng),
                "name": pos.name,
                "score": pos.score,
                "count": pos.count,
                "tag": pos.tag,
                "ext": pos.ext
            })
        log.debug("recomm size={s} -> {d}".format(s=len(recomms), d=recomms))

        out = {
            "geohash": id,
            "city": city,
            "regeo": info.regeo,
            "radius": info.radius,
            "num": info.num,
            "recomms": recomms,
            "create_time": int(info.create_time),
            "update_time": int(info.update_time)
        }

        return jsonify(out)


api.add_resource(recomms, '/recomms', endpoint="recomms")
