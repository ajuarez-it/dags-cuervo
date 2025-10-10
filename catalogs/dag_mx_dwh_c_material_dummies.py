import os
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from pathlib import Path
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
with DAG(
    dag_id=DAG_NAME,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["MCC", "MATERIAL_DUMMIES", "SILVER", "GOLD"],
    description="A DAG to trigger MATERIAL_DUMMIES Workflow with BigQuery, and Cloud Run jobs.",
) as dag:
    default_cloudrun_args = {
        "project_id": GCP_PROJECT_ID,
        "region": GCP_REGION,
        "job_name": JOB_NAME,
        "gcp_conn_id": GCP_CONN_ID,
    }
    # ---
    # 3. Task Definitions
    # ---
    bronze_sources = [
    "dbt_cuervo.BRZ_MX_ONC_SPO_INT.d_material_dummies",
    ]
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("Bronze", default_args={'pool': 'emetrix'}) as TG_bronze:
        prev_task = None
        for idx, source in enumerate(bronze_sources, start=1):
            trigger_cloud_run_job_freshness_bronze_material_dummies= CloudRunExecuteJobOperator(
                task_id=f"trigger_cloud_run_job_freshness_bronze_material_dummies_{idx:02d}",
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
                prev_task >> trigger_cloud_run_job_freshness_bronze_material_dummies
            prev_task = trigger_cloud_run_job_freshness_bronze_material_dummies

    with TaskGroup("Silver", default_args={'pool': 'emetrix'}) as TG_silver:
        trigger_cloud_run_job_build_silver_material_dummies = CloudRunExecuteJobOperator(
            task_id="trigger_cloud_run_job_build_silver_material_dummies",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "staging.material.stg_material_dummies"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the silver layer",
            **default_cloudrun_args,
        )

    with TaskGroup("Gold", default_args={'pool': 'emetrix'}) as TG_gold:
        trigger_cloud_run_job_build_gold_material_dummies = CloudRunExecuteJobOperator(
            task_id="material_dummies_gold",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.c_material_dummies"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the gold layer",
            **default_cloudrun_args,
        )
start >> TG_bronze >> TG_silver >> TG_gold >> end