import os
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.task_group import TaskGroup
from pathlib import Path

from airflow.utils.dates import days_ago

# ---
# 1. Environment variables and constants
# ---
# It's best practice to store sensitive IDs and configurations in Airflow Variables or a secret backend
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cc-data-analytics-prd")
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
GCP_CONN_ID = "google_cloud_default" # Your Airflow connection ID for Google Cloud
JOB_NAME = "dbt-cuervo"
DAG_NAME = Path(__file__).stem
# ---
# 2. DAG Definition
# ---
default_args = {
    'owner': 'Miguel Dieguillo', 
    'retries': 0, 
    'maintainer': 'Aaron Juarez'
    }

with DAG(
    dag_id=DAG_NAME,
    start_date=days_ago(1),
    schedule_interval="10 0,13 * * *",
    catchup=False,
    tags=["MCC", "MATERIAL", "SILVER", "GOLD"],
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
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tdivision",
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0textmatlgrp",
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tmaterial",
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tmat_kondm",
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tmatl_group",
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tmatl_grp_3",
        "dbt_cuervo.BRZ_MX_ONC_SPO_INT.d_material_dummies",
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_mvke",
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bictzmm_zcate",
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_tzmm_zcatg",
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bi0pmaterial"
    ]
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    with TaskGroup("Bronze", default_args={'pool': 'emetrix'}) as TG_bronze:
            prev_task = None
            for idx, source in enumerate(bronze_sources, start=1):
                trigger_cloud_run_job_freshness_bronze_material= CloudRunExecuteJobOperator(
                    task_id=f"trigger_cloud_run_job_test_bronze_material_{idx:02d}",
                    overrides={
                        "container_overrides": [
                            {
                                "args": [
                                    "source",
                                    "freshness",
                                    "--select",
                                    f"source:{source}"
                                ],
                            }
                        ],
                    },
                    doc_md="Triggers the Cloud Run job with overrides for freshness.",
                    **default_cloudrun_args,
                )        
                if prev_task:
                    prev_task >> trigger_cloud_run_job_freshness_bronze_material
                prev_task = trigger_cloud_run_job_freshness_bronze_material

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

start >> TG_bronze >> TG_silver >> TG_gold >> end