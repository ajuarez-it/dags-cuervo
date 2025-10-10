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
    tags=["MCC", "CALENDAR", "SILVER", "GOLD"],
    description="A DAG to trigger calendar Workflow with BigQuery, and Cloud Run jobs.",
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
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("Silver", default_args={'pool': 'emetrix'}) as TG_silver:
        trigger_cloud_run_job_build_silver_calendar = CloudRunExecuteJobOperator(
            task_id="trigger_cloud_run_job_build_silver_calendar",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "staging.staging.calendar"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the silver layer",
            **default_cloudrun_args,
        )

    with TaskGroup("Gold", default_args={'pool': 'emetrix'}) as TG_gold:
        trigger_cloud_run_job_build_gold_calendar = CloudRunExecuteJobOperator(
            task_id="calendar_gold",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_calendar"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the gold layer",
            **default_cloudrun_args,
        )
start >> TG_silver >> TG_gold >> end 