import os
import pendulum
from pathlib import Path
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.task_group import TaskGroup
from sources import get_freshness_sources

# ---
# 1. Environment variables and constants
# ---
# It's best practice to store sensitive IDs and configurations in Airflow Variables or a secret backend

DAG_NAME = Path(__file__).stem
INGEST_DAG_ID = "dag_ingest_mx_qlik_material"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cc-data-analytics-prd")
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
GCP_CONN_ID = "google_cloud_default" # Your Airflow connection ID for Google Cloud
JOB_NAME = "dbt-cuervo"
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
    schedule_interval="30 11,23 * * *",
    default_args=default_args,
    tags=["MCC", "MATERIAL", "SILVER", "GOLD"],
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
    bronze_sources = [
        "BRZ_MX_ONP_SAP_BW.raw_bi0tdivision",
        "BRZ_MX_ONP_SAP_BW.raw_bi0textmatlgrp",
        "BRZ_MX_ONP_SAP_BW.raw_bi0tmaterial",
        "BRZ_MX_ONP_SAP_BW.raw_bi0tmat_kondm",
        "BRZ_MX_ONP_SAP_BW.raw_bi0tmatl_group",
        "BRZ_MX_ONP_SAP_BW.raw_bi0tmatl_grp_3",
        "BRZ_MX_ONC_SPO_INT.d_material_dummies",
        "SAP_CDC_AECORSOFT.mvke",
        "BRZ_MX_ONP_SAP_BW.raw_bictzmm_zcate",
        "BRZ_MX_ONP_SAP_BW.raw_tzmm_zcatg",
        "BRZ_MX_ONP_SAP_BW.raw_bi0pmaterial"
    ]

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    with TaskGroup("Ingest", default_args={'pool': 'emetrix'}) as TG_ingest:

        trigger_and_wait_for_staging_dag = TriggerDagRunOperator(
            task_id="trigger_and_wait_for_staging_dag",
            trigger_dag_id=INGEST_DAG_ID, # The ID of the DAG to call
            conf={
                "execution_mode": "triggered_by_parent",
                "source_data_date": "{{ ds }}", # Passes the parent's execution date
            },
            execution_date="{{ ds }}", # Pass the parent's execution date to the child
            reset_dag_run=True, # Recommended to allow the task to retry triggering the child
            wait_for_completion=True, # This is the key change! The operator will now wait.
            poke_interval=20, # How often to check the status of the triggered DAG.
            failed_states=["failed"], # The states in the child DAG that will cause this task to fail.
        )

    with TaskGroup("Bronze", default_args={'pool': 'emetrix'}) as TG_bronze:
        # 2. Create a single operator instance instead of looping
        trigger_cloud_run_job_freshness_bronze_material = CloudRunExecuteJobOperator(
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
        trigger_cloud_run_job_build_silver_material = CloudRunExecuteJobOperator(
            task_id="material_silver",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "staging.material"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the silver layer",
            **default_cloudrun_args,
        )
    with TaskGroup("Gold", default_args={'pool': 'emetrix'}) as TG_gold:
        trigger_cloud_run_job_build_gold_material = CloudRunExecuteJobOperator(
            task_id="material_gold",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "marts.commercial.d_mcc_material"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the gold layer",
            **default_cloudrun_args,
        )

start >> TG_ingest >> TG_bronze >> TG_silver >> TG_gold >> end