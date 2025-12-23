from airflow import DAG
from pathlib import Path
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
import pendulum

LOCAL_TZ = pendulum.timezone("America/Mexico_City")
START_DATE_LOCAL = (
    pendulum.now(LOCAL_TZ)
    .replace(hour=0, minute=0, second=0, microsecond=0)
    .subtract(days=1)
)
DAG_NAME = Path(__file__).stem

# Default arguments for the DAG
default_args = {
    'owner': 'Samuel Torres',
    'maintainer': 'Aaron Juarez',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    "start_date": START_DATE_LOCAL,
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="45 7 * * *",
    default_args=default_args,
    tags=["ASPROVA", "SUPPLY_CHAIN", "SILVER", "GOLD"],
    catchup=False,
    description="A DAG to trigger a BigQuery Stored Procedure for ASPROVA",
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    # Task: Trigger the Stored Procedure
    trigger_asprova_sp = BigQueryInsertJobOperator(
        task_id='trigger_sp_asprova',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                # Uses Standard SQL to call your specific routine
                "query": "CALL `cc-data-analytics-prd.GOLD_MEX_FACT.Sp_Asprova`();",
                "useLegacySql": False,
            }
        },
        # IMPORTANT: Change 'US' to 'northamerica-northeast1' or wherever 
        # your dataset GOLD_MEX_FACT is actually located.
        location='US', 
    )

    start >> trigger_asprova_sp >> end