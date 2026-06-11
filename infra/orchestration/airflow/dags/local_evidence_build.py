from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from vina_bim_shop.orchestration.local_evidence import run_local_evidence_build


def _run(**context):
    return run_local_evidence_build(run_id=context["run_id"])


with DAG(
    dag_id="local_evidence_build",
    description="Collect ADR 06 health, manifests, and GX docs pointers into the local evidence package.",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adr06", "orchestration", "evidence"],
) as dag:
    PythonOperator(
        task_id="collect_local_evidence",
        python_callable=_run,
    )

