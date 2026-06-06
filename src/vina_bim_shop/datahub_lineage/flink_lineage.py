from __future__ import annotations

from vina_bim_shop.datahub_lineage.emitter import DataHubLineageEmitter, kafka_urn, pinot_urn

RAW_TOPICS = [
    "commerce_events",
    "catalog_events",
    "fulfillment_events",
    "ops_events",
]

DERIVED_TOPICS = [
    "realtime_commerce_metrics_1m",
    "realtime_ops_alerts",
    "realtime_metric_corrections",
]

DERIVED_UPSTREAM_MAP: dict[str, list[str]] = {
    "realtime_commerce_metrics_1m": ["commerce_events"],
    "realtime_ops_alerts": ["ops_events"],
    "realtime_metric_corrections": ["realtime_commerce_metrics_1m"],
}

PINOT_TABLES = [
    "realtime_commerce_metrics_1m",
    "realtime_ops_alerts",
    "realtime_metric_corrections",
]


def emit_flink_streaming_lineage(gms_url: str = "http://datahub-gms:8080") -> dict[str, str]:
    emitter = DataHubLineageEmitter(gms_url)
    results: dict[str, str] = {}

    for raw_topic in RAW_TOPICS:
        try:
            emitter.emit_tag(kafka_urn(raw_topic), "bronze")
        except Exception:
            pass

    for derived_topic in DERIVED_TOPICS:
        derived_urn = kafka_urn(derived_topic)
        upstream_topics = DERIVED_UPSTREAM_MAP.get(derived_topic, [])
        if not upstream_topics:
            continue

        parent_urns = [kafka_urn(t) for t in upstream_topics]
        try:
            emitter.emit_upstream_lineage(derived_urn, parent_urns)
            emitter.emit_tag(derived_urn, "provisional")
            results[derived_topic] = "success"
        except Exception as exc:
            results[derived_topic] = f"warning: {exc}"

    for pinot_table in PINOT_TABLES:
        pinot_topic = pinot_table
        try:
            emitter.emit_upstream_lineage(
                pinot_urn(pinot_table),
                [kafka_urn(pinot_topic)],
            )
            emitter.emit_tag(pinot_urn(pinot_table), "provisional")
        except Exception:
            pass

    return results
