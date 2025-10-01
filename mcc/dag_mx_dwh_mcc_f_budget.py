import os
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.task_group import TaskGroup
from utils import get_current_filename_base
# ---
# 1. Environment variables and constants
# ---
# It's best practice to store sensitive IDs and configurations in Airflow Variables or a secret backend
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cc-data-analytics-prd")
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
GCP_CONN_ID = "google_cloud_default" # Your Airflow connection ID for Google Cloud
JOB_NAME = "dbt-cuervo"
DAG_NAME = get_current_filename_base() # get the filename
# ---
# 2. DAG Definition
# ---
with DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["MCC", "BUDGET", "SILVER", "GOLD"],
    description="A DAG to trigger budget Workflow with BigQuery, and Cloud Run jobs.",
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
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_zcppa001_q0001",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_zcppa001_q0022"
    ]
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    with TaskGroup("TG_bronze") as TG_bronze:
        with TaskGroup("tests") as bronze_tests:
            prev_task = None
            for idx, source in enumerate(bronze_sources, start=1):
                task = CloudRunExecuteJobOperator(
                    task_id=f"trigger_cloud_run_job_test_bronze_requester_{idx:02d}",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME,
                                "args": ["test", "--select", f"source:{source}"],
                            }
                        ]
                    },
                    doc_md=f"Triggers the Cloud Run job for testing bronze layer",
                    **default_cloudrun_args,
                )

                
                if prev_task:
                    prev_task >> task
                prev_task = task
    bronze_tests
    with TaskGroup("TG_silver") as TG_silver:
        with TaskGroup("test") as silver_test:
                trigger_cloud_run_job_test_silver_budget = CloudRunExecuteJobOperator(
                    task_id="trigger_cloud_run_job_test_silver_budget",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME, # Must match the job name
                                "args": ["test", "--select", "dbt_cuervo.staging.budget"]
                            }
                        ]
                    },
                    doc_md="Triggers the Cloud Run job for testing silver layer",
                    **default_cloudrun_args,
                )
        with TaskGroup("run") as silver_run:
                trigger_cloud_run_job_for_silver_budget = CloudRunExecuteJobOperator(
                    task_id="trigger_cloud_run_job_for_silver_budget",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME,
                                "args": ["run", "--select", "dbt_cuervo.staging.budget"],
                            }
                        ]
                    },
                    doc_md="Triggers the Cloud Run job for running silver layer",
                    **default_cloudrun_args,
                )
        trigger_cloud_run_job_test_silver_budget >> trigger_cloud_run_job_for_silver_budget
    silver_test >> silver_run
    with TaskGroup("TG_gold") as TG_gold:
        with TaskGroup("test") as gold_test:
                trigger_cloud_run_job_test_gold_budget = CloudRunExecuteJobOperator(
                    task_id="trigger_cloud_run_job_test_gold_budget",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME, # Must match the job name
                                "args": ["test", "--select", "dbt_cuervo.marts.commercial.f_mcc_budget"]
                            }
                        ]
                    },
                    doc_md="Triggers the Cloud Run job for testing gold layer",
                    **default_cloudrun_args,
                )
        with TaskGroup("run") as gold_run:
                trigger_cloud_run_job_for_gold_budget = CloudRunExecuteJobOperator(
                    task_id="trigger_cloud_run_job_for_gold_budget",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME,
                                "args": ["run", "--select", "dbt_cuervo.marts.commercial.f_mcc_budget"],
                            }
                        ]
                    },
                    doc_md="Triggers the Cloud Run job for running gold layer",
                    **default_cloudrun_args,
                )
        trigger_cloud_run_job_test_gold_budget >> trigger_cloud_run_job_for_gold_budget
    gold_test >> gold_run
start >> TG_bronze >> TG_silver >> TG_gold >> end
