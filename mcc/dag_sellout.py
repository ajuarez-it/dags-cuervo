import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteJobOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
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
    dag_id="dag_sell_out",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mcc", "sell_out", "silver", "gold"],
    description="A DAG to transform data from silver to gold",
) as dag:

    start = EmptyOperator(task_id="start")
    # Step 1: execute all the transformations from gold to silver 
    trigger_cloud_run_job_for_silver_sell_out = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_silver_sell_out",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME, # Name of the Cloud Run Job
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME, # Must match the job name
                    "args": ["run", "--select", "dbt_cuervo.staging.sell_out"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for the NA region.",
    )
    # Step 2: execute the fact table in gold 
    trigger_cloud_run_job_for_gold_sell_out = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_gold_sell_out",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["run", "--select", "dbt_cuervo.marts.commercial.f_mcc_sell_out"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for gold stage in sellout",
    )

    trigger_cloud_run_job_test_bronze_sell_out = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_sell_out",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_zcprt001_q0006"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )
    
    trigger_cloud_run_job_test_silver_sell_out = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_silver_sell_out",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "dbt_cuervo.staging.sell_out"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing silver layer",
    )

    trigger_cloud_run_job_test_gold_sell_out = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_gold_sell_out",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "dbt_cuervo.marts.commercial.f_mcc_sell_out"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing gold layer",
    )
    end = EmptyOperator(task_id="end")

    # ---
    # 4. Task Dependencies
    # ---
    # Defines the execution order of the tasks in the DAG.
(
    start
    >> trigger_cloud_run_job_test_bronze_sell_out
    >> trigger_cloud_run_job_test_silver_sell_out
    >> trigger_cloud_run_job_for_silver_sell_out
    >> trigger_cloud_run_job_test_gold_sell_out
    >> trigger_cloud_run_job_for_gold_sell_out
    >> end
)