import os
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.task_group import TaskGroup 
from airflow.utils.dates import days_ago

# ---
# 1. Environment variables and constants
# ---
# It's best practice to store sensitive IDs and configurations in Airflow Variables or a secret backend
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cc-data-analytics-prd")
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
GCP_CONN_ID = "google_cloud_default"  # Your Airflow connection ID for Google Cloud
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
    schedule_interval="15 4 * * *",
    catchup=False,
    default_args=default_args,
    tags=["MCC", "SELL_OUT", "SILVER", "GOLD"],
    description="A DAG to trigger sell_out Workflow with BigQuery, and Cloud Run jobs.",
) as dag:
    # ---
    # 3. Default Arguments for Cloud Run Operators
    # ---
    default_cloudrun_args = {
        "project_id": GCP_PROJECT_ID,
        "region": GCP_REGION,
        "job_name": JOB_NAME,
        "gcp_conn_id": GCP_CONN_ID,
        "retries": 0,
    }

    # ---
    # 4. Task Definitions
    # ---
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # This task now passes the Airflow run_id to the Cloud Run job.
    # The container can use this ID for logging, tracking, or creating unique outputs.
    with TaskGroup("Bronze", default_args={'pool': 'emetrix'}) as TG_bronze:
        trigger_cloud_run_job_freshness_bronze_sell_out= CloudRunExecuteJobOperator(
            task_id="sellout_freshness_bronze",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "source",
                            "freshness",
                            "--select",
                            "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_zcprt001_q0006"
                        ],
                    }
                ],
            },
            doc_md="Triggers the Cloud Run job with overrides for freshness.",
            **default_cloudrun_args,
        )

    with TaskGroup("Silver", default_args={'pool': 'emetrix'}) as TG_silver:
        trigger_cloud_run_job_build_silver_sell_out = CloudRunExecuteJobOperator(
            task_id="sellout_silver",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "staging.sell_out"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the silver layer",
            **default_cloudrun_args,
        )

    with TaskGroup("Gold", default_args={'pool': 'emetrix'}) as TG_gold:
        trigger_cloud_run_job_build_gold_sell_out = CloudRunExecuteJobOperator(
            task_id="sellout_gold",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "marts.commercial.f_mcc_sell_out"
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the gold layer",
            **default_cloudrun_args,
        )

    # ---
    # 5. Task Orchestration
    # ---
    start >> TG_bronze >> TG_silver >> TG_gold >> end 