from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from vina_bim_shop.orchestration.pinot_bootstrap import run_pinot_bootstrap


def _run(**context):
    return run_pinot_bootstrap(run_id=context["run_id"])


with DAG(
    dag_id="pinot_bootstrap",
    description="Apply Pinot assets and run sample queries without touching Flink control-plane ownership.",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adr06", "orchestration", "pinot"],
) as dag:
    PythonOperator(
        task_id="bootstrap_pinot_assets",
        python_callable=_run,
    )

