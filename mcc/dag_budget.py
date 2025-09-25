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
# QLIK_CONN_ID = "qlik_sense_api"      # Your Airflow HTTP connection ID for Qlik Sense
# QLIK_APP_ID = "your-qlik-app-id"         # The ID of the Qlik app (.qvf) to reload
JOB_NAME = "dbt-cuervo"
# ---
# 2. DAG Definition
# ---
with DAG(
    dag_id="dag_budget_mcc",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["BATCH", "MCC", "BUDGET", "SILVER", "GOLD"],
    description="A DAG to trigger Budget Workflow with Qlik, BigQuery, and Cloud Run jobs.",
) as dag:

    # ---
    # 3. Task Definitions
    # ---

    # Task 1: Trigger a Qlik Sense App Reload using the REST API
    # This task sends a POST request to the Qlik Sense API to start a reload task.
    # You must configure an HTTP Connection in the Airflow UI named 'qlik_sense_api'.
    # The connection should contain the host (e.g., your-tenant.us.qlikcloud.com)
    # and authentication headers (e.g., 'Authorization: Bearer <API_KEY>').
    # trigger_qlik_reload = SimpleHttpOperator(
    #     task_id="trigger_qlik_app_reload",
    #     http_conn_id=QLIK_CONN_ID,
    #     endpoint="/api/v1/reloads",  # Qlik Cloud API endpoint for reloads
    #     method="POST",
    #     data=f'{{"appId": "{QLIK_APP_ID}"}}',  # Body of the request specifying the app
    #     headers={"Content-Type": "application/json"},
    #     response_check=lambda response: response.status_code == 201,
    #     doc_md="Triggers a reload task for a specific Qlik Sense application.",
    # )

    # # Task 2: Execute a query on a BigQuery External Table
    # # This task runs after the Qlik reload is successfully triggered.
    # # It executes a simple query against an external table.
    # execute_external_table_query = BigQueryExecuteQueryOperator(
    #     task_id="execute_bigquery_external_table",
    #     gcp_conn_id=GCP_CONN_ID,
    #     sql="""
    #         -- This query references an external table configured in BigQuery
    #         SELECT customer_id, region
    #         FROM `your-gcp-project-id.your_dataset.your_external_table_name`
    #         WHERE event_date = CURRENT_DATE();
    #     """,
    #     use_legacy_sql=False,
    #     doc_md="Executes a SQL query on a BigQuery external table.",
    # )

    # Task 3 Group: Trigger three parallel Cloud Run Jobs with overrides
    # These tasks run after the BigQuery query completes.
    # Each task overrides the default command/args for the Cloud Run job's container.

    start = EmptyOperator(task_id="start")
    # Job 1: Process data for North America
    trigger_cloud_run_job_for_silver_sellout = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_silver_sellout",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME, # Name of the Cloud Run Job
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME, # Must match the job name
                    "args": ["run", "--select", "staging.budget.*"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for the NA region.",
    )

    # Job 2: Process data for Europe
    trigger_cloud_run_job_for_gold_sellout = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_gold_sellout",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["run", "--select", "marts.commercial.f_mcc_budget"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for gold stage in budget",
    )

    # Job 3: Process data for Asia-Pacific
    trigger_cloud_run_job_tests = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_tests",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test"]
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for testing.",
    )
    end = EmptyOperator(task_id="end")
 
    # ---
    # 4. Task Dependencies
    # ---
    # Defines the execution order of the tasks in the DAG.
    # trigger_qlik_reload >> execute_external_table_query >> 
    start >> trigger_cloud_run_job_for_silver_sellout >> trigger_cloud_run_job_for_gold_sellout >> trigger_cloud_run_job_tests >> end