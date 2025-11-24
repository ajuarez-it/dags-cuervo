import os
import pendulum
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator # Import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.task_group import TaskGroup
from sources import get_freshness_sources, warn_error

# ---
# 1. Environment variables and constants
# ---
# It's best practice to store sensitive IDs and configurations in Airflow Variables or a secret backend

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cc-data-analytics-prd")
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
GCP_CONN_ID = "google_cloud_default" # Your Airflow connection ID for Google Cloud
JOB_NAME = "dbt-cuervo"
DAG_NAME = Path(__file__).stem
LOCAL_TZ = pendulum.timezone("America/Mexico_City")
START_DATE_LOCAL = (
    pendulum.now(LOCAL_TZ)
    .replace(hour=0, minute=0, second=0, microsecond=0)
    .subtract(days=1)
)
# ---
# 2. DAG Definition
# ---

default_args = {
    'owner': 'Samuel Torres', 
    'retries': 0, 
    'maintainer': 'Aaron Juarez',
    "start_date": START_DATE_LOCAL,
    }

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="30 1 * * *",
    default_args=default_args,
    tags=["SELLOUT", "MAIN"],
    catchup=False,
    description="A DAG to transform data from silver to gold",
) as dag:
    default_cloudrun_args = {
        "project_id": GCP_PROJECT_ID,
        "region": GCP_REGION,
        "job_name": JOB_NAME,
        "gcp_conn_id": GCP_CONN_ID,
        "retries": 0,
    }
    bronze_sources = [
    "DS_Data_RM_Sellout.F_Rotacion_RealMetrics_00001",
    "DS_Data_RM_Sellout.F_Inventarios_RealMetrics_00001",
    "GLD_MX_INVENTORY.f_sell_out_inventory_detail_history",
    "STG_SQLSERVER_MEX_INV.stg_VIEW_MATERIALS_HIERARCHY",
    "BRONZE_MEX_FLATFILE.d06_1_SubCanales",
    "BRONZE_MEX_FLATFILE.d05_1_Relacion_Chain_ID",
    "SAP_BW.ZCPRT001_Q0041",
    "SAP_BW.ZCPRT001_Q0042",
    "GOLD_MEX.F_SO_Budget"
    ]

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("Bronze", default_args={'pool': 'emetrix'}) as TG_bronze:
        # 2. Create a single operator instance instead of looping
        trigger_cloud_run_job_freshness_bronze_requester = CloudRunExecuteJobOperator(
            task_id="trigger_cloud_run_job_freshness_bronze_sources",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "source",
                            "freshness",
                            "--select",
                            get_freshness_sources(bronze_sources)
                        ],
                    }
                ],
            },
            doc_md="Triggers a single Cloud Run job for all bronze source freshness checks.",
            **default_cloudrun_args,
        )
        
    with TaskGroup("Real_Metrics_Sources", default_args={'pool': 'emetrix'}) as TG_real_metrics:
        f_sell_out_rotation_real_metrics_source = CloudRunExecuteJobOperator(
        task_id="f_sell_out_rotation_real_metrics_source",
        overrides={
            "container_overrides": [
                {
                    "args": [
                        "build",
                        "--select",
                        "+f_sell_out_rotation_real_metrics_source",
                        "--warn-error-options",
                        warn_error
                    ],
                }
            ],
        },
        doc_md="Triggers a Cloud Run job to run and then test the silver layer",
        **default_cloudrun_args,
        )
        f_sell_out_inventory_real_metrics_source = CloudRunExecuteJobOperator(
        task_id="f_sell_out_inventory_real_metrics_source",
        overrides={
            "container_overrides": [
                {
                    "args": [
                        "build",
                        "--select",
                        "+f_sell_out_inventory_real_metrics_source",
                        "--warn-error-options",
                        warn_error
                    ],
                }
            ],
        },
        doc_md="Triggers a Cloud Run job to run and then test the silver layer",
        **default_cloudrun_args,
        )
        [f_sell_out_rotation_real_metrics_source, f_sell_out_inventory_real_metrics_source]
                
    with TaskGroup("Budget", default_args={'pool': 'emetrix'}) as TG_budget:
        d_sell_out_materials_hierarchy = CloudRunExecuteJobOperator(
                task_id="d_sell_out_materials_hierarchy",
                overrides={
                    "container_overrides": [
                        {
                            "args": [
                                "build",
                                "--select",
                                "+d_sell_out_materials_hierarchy",
                                "--warn-error-options",
                                warn_error
                            ],
                        }
                    ],
                },
                doc_md="Triggers a Cloud Run job to run and then test the silver layer",
                **default_cloudrun_args,
            )

        d_sell_out_all_customers_chain = CloudRunExecuteJobOperator(
                task_id="d_sell_out_all_customers_chain",
                overrides={
                    "container_overrides": [
                        {
                            "args": [
                                "build",
                                "--select",
                                "+d_sell_out_all_customers_chain",
                                "--exclude",
                                "+d_mcc_requester",
                                "+f_sell_out_rotation_real_metrics_source"
                                "--warn-error-options",
                                warn_error
                            ],
                        }
                    ],
                },
                doc_md="Triggers a Cloud Run job to run and then test the silver layer",
                **default_cloudrun_args,
            )

        f_sell_out_budget_source = CloudRunExecuteJobOperator(
                task_id="f_sell_out_budget_source",
                overrides={
                    "container_overrides": [
                        {
                            "args": [
                                "build",
                                "--select",
                                "+f_sell_out_budget_source"
                                "--warn-error-options",
                                warn_error
                            ],
                        }
                    ],
                },
                doc_md="Triggers a Cloud Run job to run and then test the silver layer",
                **default_cloudrun_args,
            )

        f_sell_out_budget = CloudRunExecuteJobOperator(
                task_id="f_sell_out_budget",
                overrides={
                    "container_overrides": [
                        {
                            "args": [
                                "build",
                                "--select",
                                "+f_sell_out_budget",
                                "--exclude",
                                "+d_mcc_requester",
                                "+d_sell_out_materials_hierarchy",
                                "+f_sell_out_budget_source",
                                "+d_sell_out_all_customers_chain",
                                "--warn-error-options",
                                warn_error
                            ],
                        }
                    ],
                },
                doc_md="Triggers a Cloud Run job to run and then test the silver layer",
                **default_cloudrun_args,
            )
        [d_sell_out_materials_hierarchy, d_sell_out_all_customers_chain, f_sell_out_budget_source] >> f_sell_out_budget

    with TaskGroup("Dates", default_args={'pool': 'emetrix'}) as TG_dates:
        d_sell_out_update_date_ym = CloudRunExecuteJobOperator(
                task_id="d_sell_out_update_date_ym",
                overrides={
                    "container_overrides": [
                        {
                            "args": [
                                "build",
                                "--select",
                                "+d_sell_out_update_date_ym",
                                "--exclude",
                                "+d_mcc_requester",
                                "+f_sell_out_rotation_real_metrics_source",
                                "--warn-error-options",
                                warn_error
                            ],
                        }
                    ],
                },
                doc_md="Triggers a Cloud Run job to run and then test the silver layer",
                **default_cloudrun_args,
            )

        d_sell_out_sub_channels = CloudRunExecuteJobOperator(
                task_id="d_sell_out_sub_channels",
                overrides={
                    "container_overrides": [
                        {
                            "args": [
                                "build",
                                "--select",
                                "+d_sell_out_sub_channels",
                                "--warn-error-options",
                                warn_error
                            ],
                        }
                    ],
                },
                doc_md="Triggers a Cloud Run job to run and then test the silver layer",
                **default_cloudrun_args,
            )

        d_sell_out_update_date = CloudRunExecuteJobOperator(
                task_id="d_sell_out_update_date",
                overrides={
                    "container_overrides": [
                        {
                            "args": [
                                "build",
                                "--select",
                                "+d_sell_out_update_date",
                                "--exclude",
                                "+d_mcc_requester", 
                                "+f_sell_out_rotation_real_metrics_source",
                                "--warn-error-options",
                                warn_error
                            ],
                        }
                    ],
                },
                doc_md="Triggers a Cloud Run job to run and then test the silver layer",
                **default_cloudrun_args,
            )

        d_sell_out_relationship_chain = CloudRunExecuteJobOperator(
                task_id="d_sell_out_relationship_chain",
                overrides={
                    "container_overrides": [
                        {
                            "args": [
                                "build",
                                "--select",
                                "+d_sell_out_relationship_chain",
                                "--warn-error-options",
                                warn_error
                            ],
                        }
                    ],
                },
                doc_md="Triggers a Cloud Run job to run and then test the silver layer",
                **default_cloudrun_args,
            )
        [d_sell_out_update_date_ym, d_sell_out_sub_channels, d_sell_out_update_date, d_sell_out_relationship_chain]
    
    with TaskGroup("Sellout", default_args={'pool': 'emetrix'}) as TG_sellout:

        f_sell_out_inventory_real_metrics = CloudRunExecuteJobOperator(
            task_id="f_sell_out_inventory_real_metrics",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "+f_sell_out_inventory_real_metrics",
                            "--exclude",
                            "+d_sell_out_relationship_chain",
                            "+d_sell_out_update_date", 
                            "+d_sell_out_update_date_ym",
                            "+f_sell_out_inventory_real_metrics_source",
                            "+d_sell_out_sub_channels",
                            "--warn-error-options",
                            warn_error
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the silver layer",
            **default_cloudrun_args,
        )

        f_sell_out_rotation_real_metrics = CloudRunExecuteJobOperator(
            task_id="f_sell_out_rotation_real_metrics",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "+f_sell_out_rotation_real_metrics",
                            "--exclude",
                            "+f_sell_out_budget", 
                            "+d_sell_out_update_date_ym", 
                            "+d_sell_out_update_date",
                            "+d_sell_out_relationship_chain", 
                            "+d_sell_out_sub_channels",
                            "--warn-error-options",
                            warn_error
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the silver layer",
            **default_cloudrun_args,
        )
        [f_sell_out_inventory_real_metrics, f_sell_out_rotation_real_metrics]

    with TaskGroup("Validation", default_args={'pool': 'emetrix'}) as TG_validation:
        f_sell_out_real_metrics_validation = CloudRunExecuteJobOperator(
            task_id="f_sell_out_real_metrics_validation",
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            "f_sell_out_real_metrics_validation",
                            "--warn-error-options",
                            warn_error
                        ],
                    }
                ],
            },
            doc_md="Triggers a Cloud Run job to run and then test the gold layer",
            **default_cloudrun_args,
        )
start >> TG_bronze >> TG_real_metrics >> TG_budget >> TG_dates >> TG_sellout >> TG_validation >> end