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
        TG_bronze = CloudRunExecuteJobOperator(
            task_id="trigger_cloud_run_job_test_bronze_sell_in",
            overrides={
                "container_overrides": [
                    {
                        "name": JOB_NAME,
                        "args": ["test", "--select", "source:dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_zcppa001_q0011"]
                    }
                ]
            },
            doc_md="Triggers the Cloud Run job with overrides for testing.",
            **default_cloudrun_args,
        )
        with TaskGroup("TG_silver") as TG_silver:
            with TaskGroup("test") as silver_test:
                    trigger_cloud_run_job_test_silver_sell_in = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_test_silver_sell_in",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME, # Must match the job name
                                    "args": ["test", "--select", "dbt_cuervo.staging.sell_in"]
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for testing silver layer",
                        **default_cloudrun_args,
                    )
            with TaskGroup("run") as silver_run:
                    trigger_cloud_run_job_for_silver_sell_in = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_for_silver_sell_in",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME,
                                    "args": ["run", "--select", "dbt_cuervo.staging.sell_in"],
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for running silver layer",
                        **default_cloudrun_args,
                    )
            trigger_cloud_run_job_test_silver_sell_in >> trigger_cloud_run_job_for_silver_sell_in
        silver_test >> silver_run
        with TaskGroup("TG_gold") as TG_gold:
            with TaskGroup("test") as gold_test:
                    trigger_cloud_run_job_test_gold_sell_in = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_test_gold_sell_in",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME, # Must match the job name
                                    "args": ["test", "--select", "dbt_cuervo.marts.commercial.f_mcc_sell_in"]
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for testing gold layer",
                        **default_cloudrun_args,
                    )
            with TaskGroup("run") as gold_run:
                    trigger_cloud_run_job_for_gold_sell_in = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_for_gold_sell_in",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME,
                                    "args": ["run", "--select", "dbt_cuervo.marts.commercial.f_mcc_sell_in"],
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for running gold layer",
                        **default_cloudrun_args,
                    )
            trigger_cloud_run_job_test_gold_sell_in >> trigger_cloud_run_job_for_gold_sell_in
        gold_test >> gold_run
    TG_bronze >> TG_silver >> TG_gold
    # ---------------- CURRENCY_DECIMAL ----------------
    with TaskGroup("TG_currency_decimal") as TG_currency_decimal:
        bronze_sources = [
        "dbt_cuervo.BRZ_MX_ONP_SAP_BW.raw_tcurx",
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
                    trigger_cloud_run_job_test_silver_currency_decimal = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_test_silver_currency_decimal",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME, # Must match the job name
                                    "args": ["test", "--select", "dbt_cuervo.staging.currency_decimal"]
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for testing silver layer",
                        **default_cloudrun_args,
                    )
            with TaskGroup("run") as silver_run:
                    trigger_cloud_run_job_for_silver_currency_decimal = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_for_silver_currency_decimal",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME,
                                    "args": ["run", "--select", "dbt_cuervo.staging.currency_decimal"],
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for running silver layer",
                        **default_cloudrun_args,
                    )
            trigger_cloud_run_job_test_silver_currency_decimal >> trigger_cloud_run_job_for_silver_currency_decimal
        silver_test >> silver_run
        with TaskGroup("TG_gold") as TG_gold:
            with TaskGroup("test") as gold_test:
                    trigger_cloud_run_job_test_gold_currency_decimal = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_test_gold_currency_decimal",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME, # Must match the job name
                                    "args": ["test", "--select", "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_currency_decimal"]
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for testing gold layer",
                        **default_cloudrun_args,
                    )
            with TaskGroup("run") as gold_run:
                    trigger_cloud_run_job_for_gold_currency_decimal = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_for_gold_currency_decimal",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME,
                                    "args": ["run", "--select", "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_currency_decimal"],
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for running gold layer",
                        **default_cloudrun_args,
                    )
            trigger_cloud_run_job_test_gold_currency_decimal >> trigger_cloud_run_job_for_gold_currency_decimal
        gold_test >> gold_run
    TG_bronze >> TG_silver >> TG_gold
    # ---------------- CALENDAR_DIM ----------------
    with TaskGroup("TG_calendar") as TG_calendar:
        with TaskGroup("TG_silver") as TG_silver:
            with TaskGroup("test") as silver_test:
                    trigger_cloud_run_job_test_silver_calendar = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_test_silver_calendar",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME, # Must match the job name
                                    "args": ["test", "--select", "dbt_cuervo.staging.calendar"]
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for testing silver layer",
                        **default_cloudrun_args,
                    )
            with TaskGroup("run") as silver_run:
                    trigger_cloud_run_job_for_silver_calendar = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_for_silver_calendar",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME,
                                    "args": ["run", "--select", "dbt_cuervo.staging.calendar"],
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for running silver layer",
                        **default_cloudrun_args,
                    )
            trigger_cloud_run_job_test_silver_calendar >> trigger_cloud_run_job_for_silver_calendar
        silver_test >> silver_run
        with TaskGroup("TG_gold") as TG_gold:
            with TaskGroup("test") as gold_test:
                    trigger_cloud_run_job_test_gold_calendar = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_test_gold_calendar",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME, # Must match the job name
                                    "args": ["test", "--select", "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_calendar"]
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for testing gold layer",
                        **default_cloudrun_args,
                    )
            with TaskGroup("run") as gold_run:
                    trigger_cloud_run_job_for_gold_calendar = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_for_gold_calendar",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME,
                                    "args": ["run", "--select", "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_calendar"],
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for running gold layer",
                        **default_cloudrun_args,
                    )
            trigger_cloud_run_job_test_gold_calendar >> trigger_cloud_run_job_for_gold_calendar
        gold_test >> gold_run
    TG_silver >> TG_gold
    # ---------------- BILLING ----------------
    with TaskGroup("TG_billing") as TG_billing:
        bronze_sources = [
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbrk",
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbrp"
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_konv"
        ]
        with TaskGroup("TG_billing") as TG_billing:
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
                        trigger_cloud_run_job_test_silver_billing = CloudRunExecuteJobOperator(
                            task_id="trigger_cloud_run_job_test_silver_billing",
                            overrides={
                                "container_overrides": [
                                    {
                                        "name": JOB_NAME, # Must match the job name
                                        "args": ["test", "--select", "dbt_cuervo.staging.billing"]
                                    }
                                ]
                            },
                            doc_md="Triggers the Cloud Run job for testing silver layer",
                            **default_cloudrun_args,
                        )
                with TaskGroup("run") as silver_run:
                        trigger_cloud_run_job_for_silver_billing = CloudRunExecuteJobOperator(
                            task_id="trigger_cloud_run_job_for_silver_billing",
                            overrides={
                                "container_overrides": [
                                    {
                                        "name": JOB_NAME,
                                        "args": ["run", "--select", "dbt_cuervo.staging.billing"],
                                    }
                                ]
                            },
                            doc_md="Triggers the Cloud Run job for running silver layer",
                            **default_cloudrun_args,
                        )
                trigger_cloud_run_job_test_silver_billing >> trigger_cloud_run_job_for_silver_billing
            silver_test >> silver_run
            with TaskGroup("TG_gold") as TG_gold:
                with TaskGroup("test") as gold_test:
                        trigger_cloud_run_job_test_gold_billing = CloudRunExecuteJobOperator(
                            task_id="trigger_cloud_run_job_test_gold_billing",
                            overrides={
                                "container_overrides": [
                                    {
                                        "name": JOB_NAME, # Must match the job name
                                        "args": ["test", "--select", "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_billing"]
                                    }
                                ]
                            },
                            doc_md="Triggers the Cloud Run job for testing gold layer",
                            **default_cloudrun_args,
                        )
                with TaskGroup("run") as gold_run:
                        trigger_cloud_run_job_for_gold_billing = CloudRunExecuteJobOperator(
                            task_id="trigger_cloud_run_job_for_gold_billing",
                            overrides={
                                "container_overrides": [
                                    {
                                        "name": JOB_NAME,
                                        "args": ["run", "--select", "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_billing"],
                                    }
                                ]
                            },
                            doc_md="Triggers the Cloud Run job for running gold layer",
                            **default_cloudrun_args,
                        )
                trigger_cloud_run_job_test_gold_billing >> trigger_cloud_run_job_for_gold_billing
            gold_test >> gold_run
        TG_bronze >> TG_silver >> TG_gold
    # ---------------- SALES_ORDERS ----------------
    with TaskGroup("TG_sales_orders") as TG_sales_orders:
        bronze_sources = [
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbak",
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbuk",
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbap",
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbpa",
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_kna1",
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_vbep",
        "dbt_cuervo.BRZ_MX_ONP_AECORS.raw_konv"
        ]
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
                    trigger_cloud_run_job_test_silver_sales_orders = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_test_silver_sales_orders",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME, # Must match the job name
                                    "args": ["test", "--select", "dbt_cuervo.staging.sales_orders"]
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for testing silver layer",
                        **default_cloudrun_args,
                    )
            with TaskGroup("run") as silver_run:
                    trigger_cloud_run_job_for_silver_sales_orders = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_for_silver_sales_orders",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME,
                                    "args": ["run", "--select", "dbt_cuervo.staging.sales_orders"],
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for running silver layer",
                        **default_cloudrun_args,
                    )
            trigger_cloud_run_job_test_silver_sales_orders >> trigger_cloud_run_job_for_silver_sales_orders
        silver_test >> silver_run
        with TaskGroup("TG_gold") as TG_gold:
            with TaskGroup("test") as gold_test:
                    trigger_cloud_run_job_test_gold_sales_orders = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_test_gold_sales_orders",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME, # Must match the job name
                                    "args": ["test", "--select", "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_sales_orders"]
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for testing gold layer",
                        **default_cloudrun_args,
                    )
            with TaskGroup("run") as gold_run:
                    trigger_cloud_run_job_for_gold_sales_orders = CloudRunExecuteJobOperator(
                        task_id="trigger_cloud_run_job_for_gold_sales_orders",
                        overrides={
                            "container_overrides": [
                                {
                                    "name": JOB_NAME,
                                    "args": ["run", "--select", "dbt_cuervo.GLD_GLOBAL_MASTER_REPORT.r_sales_orders"],
                                }
                            ]
                        },
                        doc_md="Triggers the Cloud Run job for running gold layer",
                        **default_cloudrun_args,
                    )
            trigger_cloud_run_job_test_gold_sales_orders >> trigger_cloud_run_job_for_gold_sales_orders
        gold_test >> gold_run
    TG_bronze >> TG_silver >> TG_gold
    # ---------------- DEPENDENCIES BETWEEN GROUPS ----------------
    start >> [TG_calendar, TG_currency_decimal]
    [TG_calendar, TG_currency_decimal] >> TG_billing
    TG_currency_decimal >> TG_sales_orders
    [TG_billing, TG_sales_orders] >> TG_sell_in
    TG_sell_in >> end