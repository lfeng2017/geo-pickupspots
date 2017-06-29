# -*- coding: utf-8 -*-

from flask import render_template, request
from flask_appbuilder import BaseView, expose, has_access
from app.extensions import appbuilder
from app.core import log


class PositionView(BaseView):
    route_base = "/custom"

    default_view = 'showRecomms'

    @expose('/index')
    def index(self):
        return "hello world"

    @expose('/showRecomms')
    def showRecomms(self):
        lat = request.args.get("lat", 39.983658)
        lng = request.args.get("lng", 116.310294)
        geohash = request.args.get("geohash", "")
        return self.render_template('showRecomms.html', lat=lat, lng=lng, geohash=geohash)


appbuilder.add_view(PositionView, "show recomms", icon="fa-search-plus", category='MapView',
                    category_icon="fa-map-marker")
appbuilder.add_link("LatLng Search", href="http://www.gpsspg.com/maps.htm",
                    icon="fa-google-plus", category="MapView", category_icon="fa-map-marker")

# appbuilder.add_view_no_menu(PositionView())
