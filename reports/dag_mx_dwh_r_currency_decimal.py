import os
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from sources import get_freshness_sources

# ---
LOCAL_TZ = pendulum.timezone("America/Mexico_City")
START_DATE_LOCAL = (
    pendulum.now(LOCAL_TZ)
    .replace(hour=0, minute=0, second=0, microsecond=0)
    .subtract(days=1)
)
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
    start_date=START_DATE_LOCAL,
    schedule_interval=None,
    tags=["MCC", "CURRENCY_DECIMAL", "SILVER", "GOLD"],
    catchup=False,
    description="A DAG to trigger currency_decimal Workflow with BigQuery, and Cloud Run jobs.",
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
    "BRZ_MX_ONP_SAP_BW.raw_tcurx",
    ]
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
                
    with TaskGroup("Bronze", default_args={'pool': 'emetrix'}) as TG_bronze:
        trigger_cloud_run_job_freshness_bronze_currency_decimal = CloudRunExecuteJobOperator(
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
        trigger_cloud_run_job_build_silver_currency_decimal = CloudRunExecuteJobOperator(
            task_id="trigger_cloud_run_job_build_silver_currency_decimal",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "staging.currency_decimal"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the silver layer",
            **default_cloudrun_args,
        )

    with TaskGroup("Gold", default_args={'pool': 'emetrix'}) as TG_gold:
        trigger_cloud_run_job_build_gold_currency_decimal = CloudRunExecuteJobOperator(
            task_id="currency_decimal_gold",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "marts.reports.r_currency_decimal"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the gold layer",
            **default_cloudrun_args,
        )

start >> TG_bronze >> TG_silver >> TG_gold >> end 