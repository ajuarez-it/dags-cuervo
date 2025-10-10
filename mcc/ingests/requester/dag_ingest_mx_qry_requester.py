from __future__ import annotations

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from utils import get_current_filename_base

DAG_NAME = get_current_filename_base() # get the filename

MEXICO_TZ = pendulum.timezone("America/Mexico_City")

# --- Configuration Variables ---
CHILD_DAG_ID = "dag_ingest_mx_bi0pcustomer_staging"
GCP_CONN_ID = "google_cloud_default"
BQ_DATASET_ID = "BRZ_MX_ONP_SAP_BW"
BQ_TABLE_ID = "raw_bi0pcustomer"
BASE_GCS_PATH = "gs://gcs-dwh-mx/staging/raw/sap/bw"
XCOM_KEY = "create_external_table_sql" # Key for the SQL query in XCom


# 1. Define the Python function to build the SQL
def create_dynamic_sql(ti=None, **context):
    """
    Calculates the date for the day before the execution date and
    constructs the full CREATE EXTERNAL TABLE query using that date.
    """
    
    # Get the execution date (ds) as a datetime object
    # Airflow passes execution_date (or logical_date) in the context
    execution_date = context["dag_run"].execution_date
    
    # Calculate the date for the day before
    target_date = execution_date - timedelta(days=1, hours=6) # To change Mexico time and the day before.
    
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    
    # Construct the dynamic GCS URI
    gcs_uri = (
        f"'{BASE_GCS_PATH}/{year}/bi0pcustomer/{month}/{day}/bi0pcustomer.parquet'",
        f"'{BASE_GCS_PATH}/{year}/bi0pcustomer/{month}/{day}/bi0pcustomer.parquet'",
    )

    # Constructing the final SQL query
    sql_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS
          `{BQ_DATASET_ID}.{BQ_TABLE_ID}`
        OPTIONS (
          format = 'parquet',
          uris = [{gcs_uri}]
        );
    """
    
    # Pushing the generated SQL query to XCom
    ti.xcom_push(key=XCOM_KEY, value=sql_query)
    print(f"Pushed SQL to XCom:\n{sql_query}")


with DAG(
    dag_id=DAG_NAME,
    start_date=pendulum.datetime(2025, 10, 1, tz=pendulum.timezone("America/Mexico_City")),
    schedule=None,
    catchup=False,
    tags=["MCC", "INGEST", "BRONZE", "BI0PCUSTOMER", "MX", "CATALOG"],
) as dag:
    
    # This task now triggers the child DAG AND waits for it to complete.
    trigger_and_wait_for_staging_dag = TriggerDagRunOperator(
        task_id="trigger_and_wait_for_staging_dag",
        trigger_dag_id=CHILD_DAG_ID, # The ID of the DAG to call
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

    # The ExternalTaskSensor is no longer needed.

    # Get the date and construct query.
    generate_sql_task = PythonOperator(
        task_id="generate_sql_with_python",
        python_callable=create_dynamic_sql,
    )

    # Execute the query retrieved from XCom
    execute_external_table_task = BigQueryInsertJobOperator(
        task_id="execute_create_external_table",
        configuration={
            "query": {
                # Pull the SQL query from XCom
                "query": f"{{{{ task_instance.xcom_pull(task_ids='generate_sql_with_python', key='{XCOM_KEY}') }}}}",
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # --- Orchestration ---
    # The dependency chain is now simpler without the sensor.
    trigger_and_wait_for_staging_dag >> generate_sql_task >> execute_external_table_task
 