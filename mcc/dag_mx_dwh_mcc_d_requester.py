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
    tags=["MCC", "REQUESTER", "SILVER", "GOLD"],
    description="A DAG to transform data from silver to gold",
) as dag:

    default_cloudrun_args = {
        "project_id": GCP_PROJECT_ID,
        "region": GCP_REGION,
        "job_name": JOB_NAME,
        "gcp_conn_id": GCP_CONN_ID,
    }
    bronze_sources = [
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bi0pcustomer",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tcust_group",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tdistr_chan",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tcustomer",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tregion",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tsales_dist",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tsales_grp",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tsales_off",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bictzctetds",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bictzdircom",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bictzsgmntcts",
    "dbt_cuervo.BRZ_MX_ONC_SPO_INT.layoutmcc",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bi0tcust_grp2",
    "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bi0tcust_grp5",
    "dbt_cuervo.BRZ_MX_ONC_SPO_INT.d01_2_solicitante_dummie",
    "dbt_cuervo.BRZ_MX_ONC_SPO_INT.d_areasnielsen",
    ]

    start = EmptyOperator(task_id="start")

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
                    doc_md=f"Triggers the Cloud Run job for testing bronze",
                    **default_cloudrun_args,
                )

                
                if prev_task:
                    prev_task >> task
                prev_task = task
    bronze_tests
 
    with TaskGroup("TG_silver") as TG_silver:
        with TaskGroup("tests") as silver_tests:
                trigger_cloud_run_job_test_silver_requester = CloudRunExecuteJobOperator(
                    task_id="trigger_cloud_run_job_test_silver_requester",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME,
                                "args": ["test", "--select", "dbt_cuervo.staging.requester"],
                            }
                        ]
                    },
                    doc_md="Triggers the Cloud Run job for testing silver layer",
                    **default_cloudrun_args,
                )

        with TaskGroup("runs") as silver_run:
                trigger_cloud_run_job_for_silver_requester = CloudRunExecuteJobOperator(
                    task_id="trigger_cloud_run_job_for_silver_requester",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME,
                                "args": ["run", "--select", "dbt_cuervo.staging.requester"],
                            }
                        ]
                    },
                    doc_md="Triggers the Cloud Run job with overrides for the NA region.",
                    **default_cloudrun_args,
                )
        trigger_cloud_run_job_test_silver_requester >> trigger_cloud_run_job_for_silver_requester
    silver_tests >> silver_run

    with TaskGroup("TG_gold") as TG_gold:
        with TaskGroup("tests") as gold_tests:
                trigger_cloud_run_job_test_gold_requester = CloudRunExecuteJobOperator(
                    task_id="trigger_cloud_run_job_test_gold_requester",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME,
                                "args": ["test", "--select", "dbt_cuervo.marts.commercial.d_mcc_requester"],
                            }
                        ]
                    },
                    doc_md="Triggers the Cloud Run job for testing gold layer",
                    **default_cloudrun_args,
                )

        with TaskGroup("runs") as gold_run:
                trigger_cloud_run_job_for_gold_requester = CloudRunExecuteJobOperator(
                    task_id="trigger_cloud_run_job_for_gold_requester",
                    overrides={
                        "container_overrides": [
                            {
                                "name": JOB_NAME,
                                "args": ["run", "--select", "dbt_cuervo.marts.commercial.d_mcc_requester"],
                            }
                        ]
                    },
                    doc_md="Triggers the Cloud Run job with overrides for gold stage in requester",
                    **default_cloudrun_args,
                )
        trigger_cloud_run_job_test_gold_requester >> trigger_cloud_run_job_for_gold_requester
    gold_tests >> gold_run

    end = EmptyOperator(task_id="end")


start >> TG_bronze >> TG_silver >> TG_gold >> end