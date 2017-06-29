from flask_appbuilder import SQLA, AppBuilder
from flask_restful import Api


api = Api()
db = SQLA()
appbuilder = AppBuilder()