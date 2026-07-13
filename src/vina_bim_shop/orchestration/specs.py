"""Declare the supported Airflow DAG inventory and scheduling contracts."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass


@dataclass(frozen=True)
class DagSpec:
    """Describe one deployable DAG's ID, schedule, window support, and monitoring role."""

    dag_id: str
    schedule: str
    supports_hourly_logical_window: bool
    monitors_flink: bool = False


REQUIRED_DAG_IDS = (
    "hourly_batch_lakehouse",
    "kafka_topic_bootstrap",
    "pinot_bootstrap",
    "datahub_ingestion",
    "reconciliation_report",
    "local_evidence_build",
    "mini_coursework_pipeline",
)


def dag_specs_by_id() -> OrderedDict[str, DagSpec]:
    """Return the ordered deployable DAG specification mapping used by orchestration.

    The mapping includes only supported DAG IDs; callers must reject unknown IDs rather
    than constructing an undocumented runtime contract.
    """

    return OrderedDict(
        (
            ("hourly_batch_lakehouse", DagSpec("hourly_batch_lakehouse", "hourly_demo", True)),
            ("kafka_topic_bootstrap", DagSpec("kafka_topic_bootstrap", "manual", False)),
            ("pinot_bootstrap", DagSpec("pinot_bootstrap", "manual", False)),
            ("datahub_ingestion", DagSpec("datahub_ingestion", "manual", False)),
            ("reconciliation_report", DagSpec("reconciliation_report", "hourly_demo", True)),
            ("local_evidence_build", DagSpec("local_evidence_build", "manual", False)),
            ("mini_coursework_pipeline", DagSpec("mini_coursework_pipeline", "hourly_demo", True)),
        )
    )
