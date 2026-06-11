from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from vina_bim_shop.orchestration.reconciliation import run_reconciliation_report


def _run(**context):
    return run_reconciliation_report(
        run_id=context["run_id"],
        start_ts=context["data_interval_start"].isoformat(),
        end_ts=context["data_interval_end"].isoformat(),
    )


with DAG(
    dag_id="reconciliation_report",
    description="Compare Pinot provisional metrics with canonical Trino hourly windows.",
    start_date=datetime(2026, 4, 21),
    schedule="@hourly",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adr06", "orchestration", "reconciliation", "pinot"],
) as dag:
    PythonOperator(
        task_id="run_reconciliation_window",
        python_callable=_run,
    )
