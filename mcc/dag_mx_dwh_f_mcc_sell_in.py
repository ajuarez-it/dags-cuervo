import os
import pendulum
from pathlib import Path
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.task_group import TaskGroup
from sources import get_freshness_sources

# ---
# 1. Environment variables and constants
# ---
# It's best practice to store sensitive IDs and configurations in Airflow Variables or a secret backend

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cc-data-analytics-prd")
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
GCP_CONN_ID = "google_cloud_default" # Your Airflow connection ID for Google Cloud
JOB_NAME = "dbt-cuervo"
DAG_NAME = Path(__file__).stem
LOCAL_TZ = pendulum.timezone("America/Mexico_City")
START_DATE_LOCAL = (
    pendulum.now(LOCAL_TZ)
    .replace(hour=0, minute=0, second=0, microsecond=0)
    .subtract(days=1)
)
# ---
# 2. DAG Definition
# ---
default_args = {
    'owner': 'Miguel Dieguillo', 
    'retries': 0, 
    'maintainer': 'Aaron Juarez',
    "start_date": START_DATE_LOCAL,
    }

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="15 4 * * *",
    default_args=default_args,
    tags=["MCC", "SELL_IN", "SILVER", "GOLD"],
    catchup=False,
    description="A DAG to transform data from silver to gold",
) as dag:
    
    default_cloudrun_args = {
        "project_id": GCP_PROJECT_ID,
        "region": GCP_REGION,
        "job_name": JOB_NAME,
        "gcp_conn_id": GCP_CONN_ID,
        "retries": 0,
    }   
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    # ---------------- SELL_IN ----------------
    bronze_sources = [
    "BRZ_MX_ONP_SAP_BW.raw_zcppa001_q0011"
    ]
    
    with TaskGroup("Bronze", default_args={'pool': 'emetrix'}) as TG_bronze:
        # 2. Create a single operator instance instead of looping
        trigger_cloud_run_job_freshness_bronze_sell_in = CloudRunExecuteJobOperator(
            task_id="trigger_cloud_run_job_freshness_bronze_sources",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "source",
                            "freshness",
                            "--select",
                            get_freshness_sources(bronze_sources)
                        ],
                    }
                ],
            },
            doc_md="Triggers a single Cloud Run job for all bronze source freshness checks.",
            **default_cloudrun_args,
        )
        
    with TaskGroup("Silver", default_args={'pool': 'emetrix'}) as TG_silver:
        trigger_cloud_run_job_build_silver_sell_in = CloudRunExecuteJobOperator(
            task_id="sellin_silver",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "staging.commercial.mcc.sell_in"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the silver layer",
            **default_cloudrun_args,
        )

    with TaskGroup("Gold", default_args={'pool': 'emetrix'}) as TG_gold:
        trigger_cloud_run_job_build_gold_sell_in = CloudRunExecuteJobOperator(
            task_id="sellin_gold",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "marts.commercial.mcc.f_mcc_sell_in"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the gold layer",
            **default_cloudrun_args,
        )
start >> TG_bronze >> TG_silver >> TG_gold >> end 