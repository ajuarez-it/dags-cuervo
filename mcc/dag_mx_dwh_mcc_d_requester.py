import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
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

    start = EmptyOperator(task_id="start")
    # Step 1: execute all the transformations from gold to silver 
    trigger_cloud_run_job_for_silver_requester = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_silver_requester",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME, # Name of the Cloud Run Job
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME, # Must match the job name
                    "args": ["run", "--select", "dbt_cuervo.staging.requester"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for the NA region.",
    )
    # Step 2: execute the fact table in gold 
    trigger_cloud_run_job_for_gold_requester = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_gold_requester",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["run", "--select", "dbt_cuervo.marts.commercial.d_mcc_requester"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for gold stage in sellout",
    )

    trigger_cloud_run_job_test_bronze_requester_01 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_01",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bi0pcustomer"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_02 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_02",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tcust_group"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_03 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_03",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tcustomer"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_04 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_04",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tdistr_chan"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_05 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_05",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tregion"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_06 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_06",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tsales_dist"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_07 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_07",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tsales_grp"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_08 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_08",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tsales_off"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_09 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_09",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bictzctetds"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_10 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_10",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bictzdircom"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_11 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_11",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bictzsgmntcts"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_12 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_12",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONC_SPO_INT.layoutmcc"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )
   
    trigger_cloud_run_job_test_bronze_requester_13 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_13",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bi0tcust_grp2"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_14 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_14",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bi0tcust_grp5"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_15 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_15",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONC_SPO_INT.d01_2_solicitante_dummie"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_requester_16 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_requester_16",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONC_SPO_INT.d_areasnielsen"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_silver_requester = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_silver_requester",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "dbt_cuervo.staging.requester"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing silver layer",
    )

    trigger_cloud_run_job_test_gold_requester = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_gold_requester",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "dbt_cuervo.marts.commercial.d_mcc_requester"],
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
    >> trigger_cloud_run_job_test_bronze_requester_01
    >> trigger_cloud_run_job_test_bronze_requester_02
    >> trigger_cloud_run_job_test_bronze_requester_03
    >> trigger_cloud_run_job_test_bronze_requester_04
    >> trigger_cloud_run_job_test_bronze_requester_05
    >> trigger_cloud_run_job_test_bronze_requester_06
    >> trigger_cloud_run_job_test_bronze_requester_07
    >> trigger_cloud_run_job_test_bronze_requester_08
    >> trigger_cloud_run_job_test_bronze_requester_09
    >> trigger_cloud_run_job_test_bronze_requester_10
    >> trigger_cloud_run_job_test_bronze_requester_11
    >> trigger_cloud_run_job_test_bronze_requester_12
    >> trigger_cloud_run_job_test_bronze_requester_13
    >> trigger_cloud_run_job_test_bronze_requester_14
    >> trigger_cloud_run_job_test_bronze_requester_15
    >> trigger_cloud_run_job_test_bronze_requester_16
    >> trigger_cloud_run_job_test_silver_requester
    >> trigger_cloud_run_job_for_silver_requester
    >> trigger_cloud_run_job_test_gold_requester
    >> trigger_cloud_run_job_for_gold_requester
    >> end
)