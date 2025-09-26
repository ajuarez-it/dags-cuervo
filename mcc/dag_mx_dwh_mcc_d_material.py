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
    tags=["MCC", "MATERIAL", "SILVER", "GOLD"],
    description="A DAG to transform data from silver to gold",
) as dag:

    start = EmptyOperator(task_id="start")
    # Step 1: execute all the transformations from gold to silver 
    trigger_cloud_run_job_for_silver_material = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_silver_material",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME, # Name of the Cloud Run Job
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME, # Must match the job name
                    "args": ["run", "--select", "dbt_cuervo.staging.material"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for the NA region.",
    )
    # Step 2: execute the fact table in gold 
    trigger_cloud_run_job_for_gold_material = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_for_gold_material",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["run", "--select", "dbt_cuervo.marts.commercial.d_mcc_material"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job with overrides for gold stage in sellout",
    )

    trigger_cloud_run_job_test_bronze_material_01 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_01",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tdivision"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_02 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_02",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0textmatlgrp"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_03 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_03",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tmaterial"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_04 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_04",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tmat_kondm"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_05 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_05",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tmatl_group"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_06 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_06",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_bi0tmatl_grp_3"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_07 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_07",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONC_SPO_INT.d_material_dummies"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_08 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_08",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_AECORS.raw_mvke"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_09 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_09",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bictzmm_zcate"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_10 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_10",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_tzmm_zcatg"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )

    trigger_cloud_run_job_test_bronze_material_11 = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_bronze_material_11",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_qs_bi0pmaterial"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing bronze layer",
    )
   
    trigger_cloud_run_job_test_silver_material = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_silver_material",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "dbt_cuervo.staging.material"],
                }
            ]
        },
        doc_md="Triggers the Cloud Run job for testing silver layer",
    )

    trigger_cloud_run_job_test_gold_material = CloudRunExecuteJobOperator(
        task_id="trigger_cloud_run_job_test_gold_material",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job_name=JOB_NAME,
        gcp_conn_id=GCP_CONN_ID,
        overrides={
            "container_overrides": [
                {
                    "name": JOB_NAME,
                    "args": ["test", "--select", "dbt_cuervo.marts.commercial.d_mcc_material"],
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
    >> trigger_cloud_run_job_test_bronze_material_01
    >> trigger_cloud_run_job_test_bronze_material_02
    >> trigger_cloud_run_job_test_bronze_material_03
    >> trigger_cloud_run_job_test_bronze_material_04
    >> trigger_cloud_run_job_test_bronze_material_05
    >> trigger_cloud_run_job_test_bronze_material_06
    >> trigger_cloud_run_job_test_bronze_material_07
    >> trigger_cloud_run_job_test_bronze_material_08
    >> trigger_cloud_run_job_test_bronze_material_09
    >> trigger_cloud_run_job_test_bronze_material_10
    >> trigger_cloud_run_job_test_bronze_material_11
    >> trigger_cloud_run_job_test_silver_material
    >> trigger_cloud_run_job_for_silver_material
    >> trigger_cloud_run_job_test_gold_material
    >> trigger_cloud_run_job_for_gold_material
    >> end
)