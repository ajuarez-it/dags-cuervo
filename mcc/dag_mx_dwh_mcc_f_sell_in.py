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
    tags=["MCC", "SELL_IN", "SILVER", "GOLD"],
    description="A DAG to transform data from silver to gold",
) as dag:
    
    default_cloudrun_args = {
        "project_id": GCP_PROJECT_ID,
        "region": GCP_REGION,
        "job_name": JOB_NAME,
        "gcp_conn_id": GCP_CONN_ID,
    }
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ---------------- SELL_IN ----------------
    with TaskGroup("TG_sell_in") as TG_sell_in:
        with TaskGroup("tests") as sell_in_tests:
            t1 = EmptyOperator(task_id="test_bronze_sell_in")
            t2 = EmptyOperator(task_id="test_silver_sell_in")
            t3 = EmptyOperator(task_id="test_gold_sell_in")
            t1 >> t2 >> t3

        with TaskGroup("run") as sell_in_run:
            r1 = EmptyOperator(task_id="for_silver_sell_in")
            r2 = EmptyOperator(task_id="for_gold_sell_in")
            r1 >> r2

        sell_in_tests >> sell_in_run

    # ---------------- CURRENCY_DECIMAL ----------------
    with TaskGroup("TG_currency_decimal") as TG_currency_decimal:
        with TaskGroup("tests") as currency_tests:
            t1 = EmptyOperator(task_id="test_bronze_currency_decimal")
            t2 = EmptyOperator(task_id="test_silver_currency_decimal")
            t3 = EmptyOperator(task_id="test_gold_currency_decimal")
            t1 >> t2 >> t3

        with TaskGroup("run") as currency_run:
            r1 = EmptyOperator(task_id="for_silver_currency_decimal")
            r2 = EmptyOperator(task_id="for_gold_currency_decimal")
            r1 >> r2

        currency_tests >> currency_run

    # ---------------- CALENDAR_DIM ----------------
    with TaskGroup("TG_calendar") as TG_calendar:
        with TaskGroup("tests") as calendar_tests:
            trigger_cloud_run_job_test_silver_calendar = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_silver_calendar",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "dbt_cuervo.staging.reports.stg_calendar"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing silver layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_gold_calendar = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_gold_calendar",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "dbt_cuervo.reports.r_calendar"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing gold layer",
                **default_cloudrun_args,
            )

        trigger_cloud_run_job_test_silver_calendar >> trigger_cloud_run_job_test_gold_calendar

        with TaskGroup("runs") as calendar_run:
            trigger_cloud_run_job_for_silver_calendar = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_for_silver_calendar",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME, # Must match the job name
                            "args": ["run", "--select", "dbt_cuervo.staging.reports.stg_calendar"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job with overrides for silver stage in calendar",
                **default_cloudrun_args,
            )
            # Step 2: execute the fact table in gold 
            trigger_cloud_run_job_for_gold_calendar = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_for_gold_calendar",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["run", "--select", "dbt_cuervo.reports.r_calendar"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job with overrides for gold stage in calendar",
                 **default_cloudrun_args,               
            )

            trigger_cloud_run_job_for_silver_calendar >> trigger_cloud_run_job_for_gold_calendar

        calendar_tests >> calendar_run

    # ---------------- BILLING ----------------
    with TaskGroup("TG_billing") as TG_billing:
        with TaskGroup("tests") as billing_tests:
            trigger_cloud_run_job_test_bronze_billing_01 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_billing_01",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbrk"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_bronze_billing_02 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_billing_02",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbrp"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_bronze_billing_03 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_billing_03",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_AECORS.raw_konv"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )


            trigger_cloud_run_job_test_silver_billing = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_silver_billing",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "dbt_cuervo.staging.stg_billing"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing silver layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_gold_billing = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_gold_billing",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "dbt_cuervo.reports.r_billing"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing gold layer",
                **default_cloudrun_args,
            )
            (
                   trigger_cloud_run_job_test_bronze_billing_01
                >> trigger_cloud_run_job_test_bronze_billing_02
                >> trigger_cloud_run_job_test_bronze_billing_03
                >> trigger_cloud_run_job_test_silver_billing
                >> trigger_cloud_run_job_test_gold_billing
            )
        with TaskGroup("runs") as billing_run:
            trigger_cloud_run_job_for_silver_billing = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_for_silver_billing",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME, # Must match the job name
                            "args": ["run", "--select", "dbt_cuervo.staging.stg_billing"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job with overrides for the NA region.",
                **default_cloudrun_args,
            )
            # Step 2: execute the fact table in gold 
            trigger_cloud_run_job_for_gold_billing = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_for_gold_billing",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["run", "--select", "dbt_cuervo.reports.r_billing"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job with overrides for gold stage in billing",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_for_silver_billing >> trigger_cloud_run_job_for_gold_billing

        billing_tests >> billing_run

    # ---------------- SALES_ORDERS ----------------
    with TaskGroup("TG_sales_orders") as TG_sales_orders:
        with TaskGroup("tests") as sales_orders_tests:
            t1 = EmptyOperator(task_id="test_bronze_sales_orders_01")
            t2 = EmptyOperator(task_id="test_bronze_sales_orders_02")
            t3 = EmptyOperator(task_id="test_bronze_sales_orders_03")
            t4 = EmptyOperator(task_id="test_bronze_sales_orders_04")
            t5 = EmptyOperator(task_id="test_bronze_sales_orders_05")
            t6 = EmptyOperator(task_id="test_bronze_sales_orders_06")
            t7 = EmptyOperator(task_id="test_bronze_sales_orders_07")
            t8 = EmptyOperator(task_id="test_gold_sales_orders")
            trigger_cloud_run_job_test_bronze_sales_orders_01 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_sales_orders_01",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_vbak"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )
            
            trigger_cloud_run_job_test_bronze_sales_orders_02 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_sales_orders_02",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_vbuk"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_bronze_sales_orders_03 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_sales_orders_03",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_vbap"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_bronze_sales_orders_04 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_sales_orders_04",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_vbpa"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_bronze_sales_orders_05 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_sales_orders_05",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_kna1"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_bronze_sales_orders_06 = CloudRunExecuteJobOperator(
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_vbep"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_bronze_sales_orders_07 = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_bronze_sales_orders_07",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_konv"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing bronze layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_silver_sales_orders = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_silver_sales_orders",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "dbt_cuervo.staging.stg_sales_orders"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing silver layer",
                **default_cloudrun_args,
            )

            trigger_cloud_run_job_test_gold_sales_orders = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_test_gold_sales_orders",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["test", "--select", "dbt_cuervo.reports.r_sales_orders"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job for testing gold layer",
                **default_cloudrun_args,
            )
            (
                   trigger_cloud_run_job_test_bronze_sales_orders_01
                >> trigger_cloud_run_job_test_bronze_sales_orders_02
                >> trigger_cloud_run_job_test_bronze_sales_orders_03
                >> trigger_cloud_run_job_test_bronze_sales_orders_04
                >> trigger_cloud_run_job_test_bronze_sales_orders_05
                >> trigger_cloud_run_job_test_bronze_sales_orders_06
                >> trigger_cloud_run_job_test_bronze_sales_orders_07
                >> trigger_cloud_run_job_test_gold_sales_orders
            )

        with TaskGroup("run") as sales_orders_run:
            trigger_cloud_run_job_for_silver_sales_orders = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_for_silver_sales_orders",
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME, # Must match the job name
                            "args": ["run", "--select", "dbt_cuervo.staging.stg_sales_orders"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job with overrides for the NA region.",
                **default_cloudrun_args,
            )
            # Step 2: execute the fact table in gold 
            trigger_cloud_run_job_for_gold_sales_orders = CloudRunExecuteJobOperator(
                task_id="trigger_cloud_run_job_for_gold_sales_orders",
                project_id=GCP_PROJECT_ID,
                region=GCP_REGION,
                job_name=JOB_NAME,
                gcp_conn_id=GCP_CONN_ID,
                overrides={
                    "container_overrides": [
                        {
                            "name": JOB_NAME,
                            "args": ["run", "--select", "dbt_cuervo.reports.r_sales_orders"],
                        }
                    ]
                },
                doc_md="Triggers the Cloud Run job with overrides for gold stage in sales orders",
                **default_cloudrun_args,
            )
            trigger_cloud_run_job_for_silver_sales_orders >> trigger_cloud_run_job_for_gold_sales_orders

        sales_orders_tests >> sales_orders_run

    # ---------------- DEPENDENCIES BETWEEN GROUPS ----------------
    start >> [TG_calendar, TG_currency_decimal]

    [TG_calendar, TG_currency_decimal] >> TG_billing
    TG_currency_decimal >> TG_sales_orders

    [TG_billing, TG_sales_orders] >> TG_sell_in
    TG_sell_in >> end