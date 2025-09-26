import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteJobOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
# from airflow.providers.http.operators.http import SimpleHttpOperator

# ---
# 1. Environment variables and constants
# ---
# It's best practice to store sensitive IDs and configurations in Airflow Variables or a secret backend
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cc-data-analytics-prd")
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
GCP_CONN_ID = "google_cloud_default" # Your Airflow connection ID for Google Cloud
JOB_NAME = "dbt-cuervo"
# ---
# 2. DAG Definition
# ---
with DAG(
    dag_id="dag_portfolio_mcc",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["BATCH", "MCC", "portfolio", "SILVER", "GOLD"],
    description="A DAG to trigger portfolio Workflow with Qlik, BigQuery, and Cloud Run jobs.",
) as dag:

    # ---
    # 3. Task Definitions
    # ---
    start = EmptyOperator(task_id="start")
    # Job 1: 
    trigger_cloud_run_job_for_silver_portfolio = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_silver_portfolio",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME, # Name of the Cloud Run Job
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME, # Must match the job name
                    "args": ["run", "--select", "dbt_cuervo.staging.portfolio"]
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for the NA region."
    )

    # Job 2: 
    trigger_cloud_run_job_for_gold_portfolio = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_gold_portfolio",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["run", "--select", "marts.commercial.f_mcc_portfolio"]
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for gold stage in portfolio"
    )

    # Job 3: 
    trigger_cloud_run_job_test_bronze_portfolio = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_portfolio",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_zcpfi002_q0001"]
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for testing.",
    )
    end = EmptyOperator(task_id="end")
     # Job 4: 
    trigger_cloud_run_job_test_silver_portfolio = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_silver_portfolio",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "dbt_cuervo.staging.portfolio"]
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for testing."
    )
    end = EmptyOperator(task_id="end")
     # Job 5: 
    trigger_cloud_run_job_test_gold_portfolio = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_gold_portfolio",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "dbt_cuervo.marts.commercial.f_mcc_portfolio"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing gold layer",
    )
    # ---
    # 4. Task Dependencies
    # ---
    # Defines the execution order of the tasks in the DAG.
    # trigger_qlik_reload >> execute_external_table_query >> 
(
    start
    >> trigger_cloud_run_job_test_bronze_portfolio
    >> trigger_cloud_run_job_test_silver_portfolio
    >> trigger_cloud_run_job_for_silver_portfolio
    >> trigger_cloud_run_job_test_gold_portfolio
    >> trigger_cloud_run_job_for_gold_portfolio
    >> end
)