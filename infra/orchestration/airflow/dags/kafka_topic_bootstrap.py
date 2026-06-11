from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from vina_bim_shop.orchestration.kafka_bootstrap import run_kafka_topic_bootstrap


def _run(**context):
    return run_kafka_topic_bootstrap(run_id=context["run_id"])


with DAG(
    dag_id="kafka_topic_bootstrap",
    description="Bootstrap Kafka topics, schemas, and Bronze sink wiring.",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adr06", "orchestration", "kafka"],
) as dag:
    PythonOperator(
        task_id="bootstrap_ingestion",
        python_callable=_run,
    )

