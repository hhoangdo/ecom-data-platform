"""DataHub lineage helpers with a pure contract surface for local tests."""
from __future__ import annotations


__all__ = [
    "DataHubLineageEmitter",
    "emit_spark_batch_lineage",
    "emit_flink_streaming_lineage",
]


def __getattr__(name: str):
    if name == "DataHubLineageEmitter":
        from vina_bim_shop.datahub_lineage.emitter import DataHubLineageEmitter

        return DataHubLineageEmitter
    if name == "emit_spark_batch_lineage":
        from vina_bim_shop.datahub_lineage.spark_lineage import emit_spark_batch_lineage

        return emit_spark_batch_lineage
    if name == "emit_flink_streaming_lineage":
        from vina_bim_shop.datahub_lineage.flink_lineage import emit_flink_streaming_lineage

        return emit_flink_streaming_lineage
    raise AttributeError(name)
