from __future__ import annotations

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import DataJobInfoClass, DataJobInputOutputClass

from vina_bim_shop.datahub_lineage.emitter import ice_urn, kafka_urn
from vina_bim_shop.datahub_lineage.flink_lineage import DERIVED_TOPICS, DERIVED_UPSTREAM_MAP
from vina_bim_shop.datahub_lineage.spark_lineage import GOLD_UPSTREAM_MAP, REQUIRED_GOLD_TABLES

SPARK_DATAFLOW_URN = "urn:li:dataFlow:(spark,vina-bim-shop-batch,local)"
FLINK_DATAFLOW_URN = "urn:li:dataFlow:(flink,vina-bim-shop-streaming,local)"


def _sanitize_job_id(name: str) -> str:
    return name.replace(".", "_").replace("-", "_").lower()


def _spark_job_id(gold_table: str) -> str:
    return f"iceberg_transform_{_sanitize_job_id(gold_table)}"


def _flink_job_id(derived_topic: str) -> str:
    return f"flink_derive_{_sanitize_job_id(derived_topic)}"


def spark_job_urn(gold_table: str) -> str:
    return f"urn:li:dataJob:({SPARK_DATAFLOW_URN},{_spark_job_id(gold_table)})"


def flink_job_urn(derived_topic: str) -> str:
    return f"urn:li:dataJob:({FLINK_DATAFLOW_URN},{_flink_job_id(derived_topic)})"


def _emit_spark_datajob(
    emitter: DataHubRestEmitter,
    gold_table: str,
    upstream_tables: list[str],
) -> None:
    job_urn = spark_job_urn(gold_table)
    input_urns = [ice_urn(t) for t in upstream_tables]
    output_urns = [ice_urn(gold_table)]

    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=f"Iceberg transform: {gold_table}",
                type="SPARK",
                flowUrn=SPARK_DATAFLOW_URN,
                customProperties={
                    "layer": "gold",
                    "platform": "iceberg",
                    "source": "vina-bim-shop",
                },
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=input_urns,
                outputDatasets=output_urns,
            ),
        )
    )


def _emit_flink_datajob(
    emitter: DataHubRestEmitter,
    derived_topic: str,
    upstream_topics: list[str],
) -> None:
    job_urn = flink_job_urn(derived_topic)
    input_urns = [kafka_urn(t) for t in upstream_topics]
    output_urns = [kafka_urn(derived_topic)]

    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=f"Flink derive: {derived_topic}",
                type="FLINK",
                flowUrn=FLINK_DATAFLOW_URN,
                customProperties={
                    "layer": "streaming",
                    "platform": "kafka",
                },
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=input_urns,
                outputDatasets=output_urns,
            ),
        )
    )


def emit_datajob_lineage(gms_url: str = "http://datahub-gms:8080") -> dict[str, str]:
    """Emit v2 DataJob-based lineage (DataJobInfo + DataJobInputOutput) for Spark and Flink.

    Returns a mapping of DataJob URN -> "success" or "warning: <reason>". Mirrors the
    shape of `emit_spark_batch_lineage()` so it can be merged into the lineage
    manifest under `custom_lineage.datajob`.
    """
    emitter = DataHubRestEmitter(gms_url)
    results: dict[str, str] = {}

    for gold_table in REQUIRED_GOLD_TABLES:
        upstream_tables = GOLD_UPSTREAM_MAP.get(gold_table, [])
        if not upstream_tables:
            continue
        job_urn = spark_job_urn(gold_table)
        try:
            _emit_spark_datajob(emitter, gold_table, upstream_tables)
            results[job_urn] = "success"
        except Exception as exc:
            results[job_urn] = f"warning: {exc}"

    for derived_topic in DERIVED_TOPICS:
        upstream_topics = DERIVED_UPSTREAM_MAP.get(derived_topic, [])
        if not upstream_topics:
            continue
        job_urn = flink_job_urn(derived_topic)
        try:
            _emit_flink_datajob(emitter, derived_topic, upstream_topics)
            results[job_urn] = "success"
        except Exception as exc:
            results[job_urn] = f"warning: {exc}"

    return results
