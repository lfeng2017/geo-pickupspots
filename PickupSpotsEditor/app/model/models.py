# -*- coding: utf-8 -*-
import arrow
import inflection
from sqlalchemy import BigInteger, Column, Float, Integer, SmallInteger, String, ForeignKey
from sqlalchemy.orm import relationship, backref
from flask_appbuilder import Model
from flask_appbuilder.models.decorators import renders


class AbstractPickupInfo():
    ' 推荐点摘要信息基类 '

    # 放到子类才能生效
    # __bind_key__ = 'geo_pickups'

    geohash = Column(String(12), primary_key=True, unique=True)
    city = Column(String(10))
    regeo = Column(String(50))
    radius = Column(SmallInteger)
    num = Column(Integer)
    create_time = Column(Integer)
    update_time = Column(Integer)

    @renders('create_time')
    def create_time_(self):
        return arrow.get(self.create_time).format("YYYY-MM-DD HH:mm:ss")

    @renders('update_time')
    def update_time_(self):
        return arrow.get(self.update_time).format("YYYY-MM-DD HH:mm:ss")


class AbstractPickupDetail():
    ' 推荐点明细信息基类 '

    # 放到子类才能生效
    # __bind_key__ = 'geo_pickups'

    no = Column(BigInteger, primary_key=True)

    # 映射只能放到子类实现
    # exp_geohash = Column(String(12), ForeignKey('base_pickup_info.geohash'), nullable=False, index=True)
    # basePickupInfo = relationship("BasePickupInfo")

    city = Column(String(10))
    id = Column(String(12), nullable=False)
    lat = Column(Float(asdecimal=True))
    lng = Column(Float(asdecimal=True))
    name = Column(String(50))
    score = Column(Float)
    tag = Column(String(10))
    status = Column(Integer)
    count = Column(Integer)
    show = Column(Integer)
    pick = Column(Integer)
    ext = Column(String(10))
    create_time = Column(Integer)
    update_time = Column(Integer)

    @renders('create_time')
    def my_create_time(self):
        return arrow.get(create_time).format("YYYY-MM-DD HH:mm:ss")

    @renders('update_time')
    def my_update_time(self):
        return arrow.get(update_time).format("YYYY-MM-DD HH:mm:ss")


# 保持动态创建的对象
DYNAMIC_ORMS = dict()


def init_orm(area_cites_dict):
    '''按片区构造各城市的ORM对象

    :param area_cites_dict: 字典，记录格式 -> "area" : ['city1', 'city2' ... 'city N']
    :return: ORM字段，格式 -> "city" : [(info_class, detail_fk_class, detail_class), ...]
    '''

    global DYNAMIC_ORMS

    for area, cites in area_cites_dict.items():
        for city in cites:
            # 定义表名
            info_tablename = 'base_pickup_info_{}'.format(city)
            detail_tablename = 'base_pickup_detail_{}'.format(city)

            # 定义动态类名
            info_cls = inflection.camelize(info_tablename)
            detail_fk_cls = inflection.camelize("Fk" + detail_tablename)
            detail_cls = inflection.camelize(detail_tablename)

            # 定义概要表
            InfoModel = type(info_cls,
                             (AbstractPickupInfo, Model),
                             {
                                 "__tablename__": info_tablename,
                                 "__bind_key__": 'geo_pickups'
                             })

            # 定义外键
            foreign_key = '{}.geohash'.format(info_tablename)
            fk_column = Column(String(12), ForeignKey(foreign_key), nullable=False, index=True)
            backref = inflection.camelize(info_tablename, False)

            # 定义详情关联表
            DetailFkModel = type(detail_fk_cls,
                                 (AbstractPickupDetail, Model),
                                 {
                                     "__tablename__": detail_tablename,
                                     "__bind_key__": 'geo_pickups',
                                     "exp_geohash": fk_column,
                                     backref: relationship(info_cls)
                                 })

            # 定义详情显示表（不含外键）
            DetailModel = type(detail_cls,
                               (AbstractPickupDetail, Model),
                               {
                                   "__tablename__": detail_tablename,
                                   "__bind_key__": 'geo_pickups',
                                   "fk": Column(String(12), nullable=False)
                               })

            # 逐个城市加入模型字典
            DYNAMIC_ORMS[city] = (InfoModel, DetailFkModel, DetailModel)

    return DYNAMIC_ORMS


'''
class BasePickupInfo(Model):
    ' 推荐点摘要信息基类 '

    # 放到子类才能生效
    __bind_key__ = 'geo_pickups'
    __tablename__ = 'base_pickup_info'

    geohash = Column(String(12), primary_key=True, unique=True)
    city = Column(String(10))
    regeo = Column(String(50))
    radius = Column(SmallInteger)
    num = Column(Integer)
    create_time = Column(Integer)
    update_time = Column(Integer)

    @renders('create_time')
    def create_time_(self):
        return arrow.get(self.create_time).format("YYYY-MM-DD HH:mm:ss")

    @renders('update_time')
    def update_time_(self):
        return arrow.get(self.update_time).format("YYYY-MM-DD HH:mm:ss")


class BasePickupDetail(Model):
    ' 推荐点明细信息基类 '

    # 放到子类才能生效
    __bind_key__ = 'geo_pickups'
    __tablename__ = 'base_pickup_detail'

    no = Column(BigInteger, primary_key=True)

    # 映射只能放到子类实现
    exp_geohash = Column(String(12), ForeignKey('base_pickup_info.geohash'), nullable=False, index=True)
    PickupInfo = relationship("BasePickupInfo")

    city = Column(String(10))
    id = Column(String(12), nullable=False)
    lat = Column(Float(asdecimal=True))
    lng = Column(Float(asdecimal=True))
    name = Column(String(50))
    score = Column(Float)
    tag = Column(String(10))
    status = Column(Integer)
    count = Column(Integer)
    show = Column(Integer)
    pick = Column(Integer)
    ext = Column(String(10))
    create_time = Column(Integer)
    update_time = Column(Integer)

    @renders('create_time')
    def my_create_time(self):
        return arrow.get(create_time).format("YYYY-MM-DD HH:mm:ss")

    @renders('update_time')
    def my_update_time(self):
        return arrow.get(update_time).format("YYYY-MM-DD HH:mm:ss")
'''