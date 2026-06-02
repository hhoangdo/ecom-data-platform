from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from vina_bim_shop.orchestration.runtime import run_datahub_ingestion


def _run(**context):
    return run_datahub_ingestion(run_id=context["run_id"])


with DAG(
    dag_id="datahub_ingestion",
    description="ADR 07 placeholder ingestion entrypoint with warn-first semantics until governance assets exist.",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adr06", "orchestration", "datahub"],
) as dag:
    PythonOperator(
        task_id="run_datahub_ingestion",
        python_callable=_run,
    )

