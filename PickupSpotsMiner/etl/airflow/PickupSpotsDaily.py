# -*- coding: utf-8 -*-

'''
airflow 定时任务调度脚本，每日
备注: 现在2.48的机器任务使用azkaban调度，此脚本暂时无用

'''

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import HivePartitionSensor
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable

# 文件是在airflow/dags下面, 必须手动指定待执行文件的绝对路径
BASE_PATH = Variable.get("PickupSpotsMiner")

args = {
    'owner': 'lujin',
    'start_date': datetime(2017, 02, 27, 15, 00, 0),
    'email': ['lujin@yongche.com'],
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG(
    dag_id='UpdateSpotsDaily',
    default_args=args,
    schedule_interval='0 15 * * *'
    # schedule_interval=timedelta(days=1)
)

# step 1: 检测hive数据是否load到位
depHive_OK = DummyOperator(task_id='depHive_OK', dag=dag)

odsOrderOK = HivePartitionSensor(
    table="yc_bit.ods_service_order",
    partition="dt={{ ds_nodash }}",
    task_id='odsOrderOK',
    poke_interval=60 * 30,
    timeout=60 * 30 * 2,
    dag=dag)

foOrderExtOK = HivePartitionSensor(
    table="yc_bit.fo_service_order_ext",
    partition="dt={{ ds_nodash }}",
    task_id='foOrderExtOK',
    poke_interval=60 * 30,
    timeout=60 * 30 * 2,
    dag=dag)

odsOrderOK >> depHive_OK
foOrderExtOK >> depHive_OK

EnvShell = "source /home/lujin/.bashrc && PYTHONIOENCODING=utf-8 && cd {dir}".format(dir=BASE_PATH)

# step 2: 执行统计脚本
templated_command = """
{{ params.baseShell }} && cd etl && python mergePositionDaily.py --date={{ ds_nodash }}
"""
mergePosDaily = BashOperator(
    task_id='mergePos2Hive',
    bash_command=templated_command,
    params={'baseShell': EnvShell},
    dag=dag)

depHive_OK >> mergePosDaily