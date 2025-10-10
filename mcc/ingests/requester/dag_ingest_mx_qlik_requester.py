from __future__ import annotations
from airflow import DAG
from datetime import timedelta, datetime
from utilidades_qlikflow import qlikflow_utils
from pathlib import Path


tasksDict = {
    u'qliksense. Test task': {
        'Soft' : 'qs1',
        'TaskId' : 'eeff6e26-11ad-4bf9-aa9f-e4c9724faefb',
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
    start_date = datetime.now() - timedelta(days=1),
    schedule = None,
    description = 'Default test dag',
    tags = ['QLIKSENSE', 'INGEST', 'MCC', 'STAGING'],
    catchup = False,
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
) as dag:
    
    airflowTasksDict = {}  
    qlikflow_utils.create_tasks(tasksDict, airflowTasksDict, dag)