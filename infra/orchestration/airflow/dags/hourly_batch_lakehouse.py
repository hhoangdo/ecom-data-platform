from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from vina_bim_shop.orchestration.hourly_batch import run_hourly_batch_lakehouse


def _run(**context):
    return run_hourly_batch_lakehouse(
        run_id=context["run_id"],
        start_ts=context["data_interval_start"].isoformat(),
        end_ts=context["data_interval_end"].isoformat(),
    )


with DAG(
    dag_id="hourly_batch_lakehouse",
    description="Run one logical hourly batch window with Bronze warnings and Gold blocking checks.",
    start_date=datetime(2026, 4, 21),
    schedule="@hourly",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adr06", "orchestration", "batch", "gx"],
) as dag:
    PythonOperator(
        task_id="run_hourly_batch_window",
        python_callable=_run,
    )
