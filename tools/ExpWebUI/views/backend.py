# coding=utf-8
from flask.blueprints import Blueprint
from flask import render_template

bp = Blueprint('index', __name__)

@bp.route('/')
def home():
    return render_template('geo_marker.html')

@bp.route('/show_recomms')
def showRecomms():
    return render_template('showRecomm.html')

@bp.route('/show_bind_one')
def bindRoad_simple():
    return render_template('bindRoadSingle.html')

@bp.route('/show_bind')
def bindRoad():
    return render_template('bindRoadMultiple.html')

@bp.route('/show_start')
def startPos():
    return render_template('startPosCount.html')

@bp.route('/show_start_heat')
def startPosHeat():
    return render_template('startPosHeatmap.html')

@bp.route('/regeo_name')
def regeoName():
    return render_template('regeo.html')

@bp.route('/driving')
def driving():
    return render_template('driving.html')

