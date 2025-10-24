from __future__ import annotations
from airflow import DAG
from datetime import timedelta, datetime
from utilidades_qlikflow import qlikflow_utils
from pathlib import Path

LOCAL_TZ = pendulum.timezone("America/Mexico_City")
START_DATE_LOCAL = (
    pendulum.now(LOCAL_TZ)
    .replace(hour=0, minute=0, second=0, microsecond=0)
    .subtract(days=1)
)

tasksDict = {
    u'qliksense. Test task': {
        'Soft' : 'qs1',
        'TaskId' : 'c022842b-4934-4ae4-b5f0-5e6c915fdb70',
        'RandomStartDelay' : 10, 
        }
    }

default_args  = {
    'owner': 'DWH',
    'depends_on_past': False,
}
DAG_NAME = Path(__file__).stem

with DAG(
    dag_id = DAG_NAME,
    default_args = default_args ,
    start_date = START_DATE_LOCAL,
    schedule = None,
    tags = ['QLIKSENSE', 'INGEST', 'MCC', 'STAGING'],
    catchup = False,
    description = 'Default test dag',
    doc_md=__doc__,
) as dag:
    
    airflowTasksDict = {}  
    qlikflow_utils.create_tasks(tasksDict, airflowTasksDict, dag)