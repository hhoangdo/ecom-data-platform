from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from vina_bim_shop.orchestration.mini_coursework_pipeline import (
    build_coursework_run_root,
    compute_offline_features,
    ingest_raw_to_bronze,
    settings_from_airflow,
    transform_bronze_to_silver_gold,
    validate_bronze,
    validate_offline_features,
    validate_silver_gold,
)


def _run(*, stage_function, **context):
    settings = settings_from_airflow(
        get_connection=BaseHook.get_connection,
        get_variable=Variable.get,
    )
    return stage_function(
        run_id=context["run_id"],
        start_ts=context["data_interval_start"].isoformat(),
        end_ts=context["data_interval_end"].isoformat(),
        settings=settings,
        run_root=build_coursework_run_root(run_id=context["run_id"], settings=settings),
    )


with DAG(
    dag_id="mini_coursework_pipeline",
    description="Run DP1 Bronze landing, DP2 core lakehouse, and DP3 offline feature stages.",
    start_date=datetime(2026, 4, 21),
    schedule="@hourly",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["coursework", "orchestration", "dp1", "dp2", "dp3", "gx"],
) as dag:
    with TaskGroup(group_id="dp1_raw_to_bronze"):
        ingest_raw_to_bronze = PythonOperator(
            task_id="ingest_raw_to_bronze",
            python_callable=_run,
            op_kwargs={"stage_function": ingest_raw_to_bronze},
        )
        validate_bronze = PythonOperator(
            task_id="validate_bronze",
            python_callable=_run,
            op_kwargs={"stage_function": validate_bronze},
        )

    with TaskGroup(group_id="dp2_bronze_to_silver_gold"):
        transform_bronze_to_silver_gold = PythonOperator(
            task_id="transform_bronze_to_silver_gold",
            python_callable=_run,
            op_kwargs={"stage_function": transform_bronze_to_silver_gold},
        )
        validate_silver_gold = PythonOperator(
            task_id="validate_silver_gold",
            python_callable=_run,
            op_kwargs={"stage_function": validate_silver_gold},
        )

    with TaskGroup(group_id="dp3_offline_features"):
        compute_offline_features = PythonOperator(
            task_id="compute_offline_features",
            python_callable=_run,
            op_kwargs={"stage_function": compute_offline_features},
        )
        validate_offline_features = PythonOperator(
            task_id="validate_offline_features",
            python_callable=_run,
            op_kwargs={"stage_function": validate_offline_features},
        )

    ingest_raw_to_bronze >> validate_bronze >> transform_bronze_to_silver_gold >> validate_silver_gold >> compute_offline_features >> validate_offline_features
