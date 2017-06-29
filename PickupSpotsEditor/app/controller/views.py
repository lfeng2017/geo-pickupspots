# -*- coding: utf-8 -*-
import inflection
from flask import render_template, redirect, url_for
from flask_appbuilder import ModelView
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.actions import action
from app.extensions import appbuilder
from app.core import log

# from app.model.models import BasePickupDetail, BasePickupInfo
from app.model.models import DYNAMIC_ORMS
from common import GeoUtil

DYNAMIC_VIEWS = dict()

DETAIL_LIST_COLUMNS = ['id', 'city', 'name', 'lat', 'lng', 'score']

DETAIL_SHOW_FIELDS = [
    ('RecommPos',
     {
         'fields': ['no', 'id', 'name', 'lat', 'lng', 'score']
     }),
    ('Additional Info',
     {
         'fields': ['status', 'tag', 'count', 'show', 'pick', 'ext', 'create_time', 'update_time'],
         'expanded': True
     })
]

INFO_LIST_COLUMNS = ['geohash', 'city', 'regeo', 'radius', 'num', 'create_time_', 'update_time_']

INFO_SHOW_FIELDS = [
    ('StartPos', {'fields': ['geohash', 'regeo']}),
    ('Summary', {'fields': ['city', 'radius', 'num', 'create_time_', 'update_time_'], 'expanded': True}),
]

INFO_SEARCH_COLUMNS = ['geohash', 'city']


class AbstractGeoBaseInfoView():
    @action("showPosition", u"显示位置", icon="fa-search-plus", confirmation=None, multiple=False, single=True)
    def showPosition(self, item):
        geohash = item.geohash
        city = item.city
        lat, lng = GeoUtil.geohash_decoding(geohash)
        log.info("{city} {geohash} {lat},{lng}".format(city=city, geohash=geohash, lat=lat, lng=lng))
        return redirect(url_for("PositionView.showRecomms", lat=lat, lng=lng, geohash=geohash))

    @action("updateOnline", u"更新线上", "Do you really want to?", "fa-rocket")
    def updateOnline(self, items):
        """
            do something with the item record
        """
        if isinstance(items, list):
            pass
        else:
            pass

        return redirect(self.get_redirect())


def init_views(area_cites_dict):
    global DYNAMIC_VIEWS

    for area, cites in area_cites_dict.items():
        for city in cites:
            # 获取动态创建的model类
            InfoModel, DetailFkModel, DetailModel = DYNAMIC_ORMS[city]

            # 构造详情页（外键）的视图
            detail_fk_cls = inflection.camelize("fk_base_detail_view_{}".format(city))
            DetailFkView = type(detail_fk_cls, (ModelView,),
                                {
                                    "datamodel": SQLAInterface(DetailFkModel),
                                    "list_columns": DETAIL_LIST_COLUMNS,
                                    "show_fieldsets": DETAIL_SHOW_FIELDS
                                })

            # 构造detail视图
            info_cls = inflection.camelize("base_info_view_{}".format(city))
            InfoView = type(info_cls, (AbstractGeoBaseInfoView, ModelView),
                            {
                                "datamodel": SQLAInterface(InfoModel),
                                "related_views": [DetailFkView],
                                "list_columns": INFO_LIST_COLUMNS,
                                "show_fieldsets": INFO_SHOW_FIELDS
                            })

            # 保持该城市的视图
            DYNAMIC_VIEWS[city] = (InfoView, DetailFkView)

            # 注册到flask_appbuilder
            appbuilder.add_view(InfoView, city, category=area,
                                icon="fa-info-circle", category_icon="fa-bars")
            appbuilder.add_view_no_menu(DetailFkView)


'''
class GeoBaseDetailView(ModelView):
    datamodel = SQLAInterface(BasePickupDetail)

    # label_columns = {'contact_group': 'Contacts Group'}

    list_columns = ['id', 'city', 'name', 'lat', 'lng', 'score']

    search_columns = ['id', 'status', 'tag', 'pick']

    show_fieldsets = [
        ('RecommPos', {'fields': ['no', 'id', 'name', 'lat', 'lng', 'score']}),
        ('Additional Info',
         {'fields': ['status', 'tag', 'count', 'show', 'pick', 'ext', 'create_time', 'update_time'], 'expanded': True}),
    ]


class GeoBaseInfoView(ModelView):
    datamodel = SQLAInterface(BasePickupInfo)
    # datamodel = SQLAInterface(BasePickupInfo)
    # related_views = [GeoBaseDetailView]
    related_views = [GeoBaseDetailView]

    list_columns = ['geohash', 'city', 'regeo', 'radius', 'num', 'create_time_', 'update_time_']

    search_columns = ['geohash', 'city']

    show_fieldsets = [
        ('StartPos', {'fields': ['geohash', 'regeo']}),
        ('Summary', {'fields': ['city', 'radius', 'num', 'create_time_', 'update_time_'], 'expanded': True}),
    ]

    @action("showPosition", u"显示位置", icon="fa-search-plus", confirmation=None, multiple=False, single=True)
    def showPosition(self, item):
        geohash = item.geohash
        city = item.city
        lat, lng = GeoUtil.geohash_decoding(geohash)
        log.info("{city} {geohash} {lat},{lng}".format(city=city, geohash=geohash, lat=lat, lng=lng))
        return redirect(url_for("PositionView.showRecomms", lat=lat, lng=lng, geohash=geohash))

    @action("updateOnline", u"更新线上", "Do you really want to?", "fa-rocket")
    def updateOnline(self, items):
        """
            do something with the item record
        """
        if isinstance(items, list):
            pass
        else:
            pass

        return redirect(self.get_redirect())


appbuilder.add_view(GeoBaseInfoView, "GeoBaseInfo", icon="fa-info-circle", category="GeoSample", category_icon="fa-bars")
appbuilder.add_view_no_menu(GeoBaseDetailView)
'''


@appbuilder.app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html', base_template=appbuilder.base_template, appbuilder=appbuilder), 404
