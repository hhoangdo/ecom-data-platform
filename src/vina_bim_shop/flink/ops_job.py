from __future__ import annotations

import json

from vina_bim_shop.flink.alerts import normalize_source_alert_event
from vina_bim_shop.flink.config import load_streaming_config
from vina_bim_shop.flink.runtime import (
    add_required_jars,
    configure_checkpointing,
    event_timestamp_assigner,
    jsonl_file_sink,
    kafka_sink,
    kafka_source,
    load_runtime_settings,
)


def run() -> None:
    config = load_streaming_config()
    runtime = load_runtime_settings(config)

    from pyflink.common import Types
    from pyflink.datastream import StreamExecutionEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    add_required_jars(env)
    configure_checkpointing(
        env,
        checkpoint_uri=f"s3://{runtime.checkpoint_bucket}/{runtime.checkpoint_prefix}/ops_alerts",
    )

    sources = []
    for source_name in ["ops", "catalog", "fulfillment"]:
        sources.append(
            kafka_source(
                env=env,
                topic=config.source_topics[source_name],
                bootstrap_servers=runtime.kafka_bootstrap_servers,
                group_id=f"vina-bim-shop-{source_name}-alerts",
                watermark_strategy=event_timestamp_assigner(),
            ).map(lambda raw: json.loads(raw), output_type=Types.PICKLED_BYTE_ARRAY())
        )

    union_stream = sources[0].union(*sources[1:])
    alert_stream = union_stream.filter(
        lambda event: event["event_type"] in {
            "traffic_burst_detected",
            "late_arrival_observed",
            "duplicate_event_observed",
            "inventory_low_stock",
            "shipment_delayed",
            "shipment_blocked_payment_failed",
        },
    ).map(
        lambda event: json.dumps(normalize_source_alert_event(event), separators=(",", ":")),
        output_type=Types.STRING(),
    )

    alert_stream.sink_to(kafka_sink(topic=config.derived_topics["ops_alerts"], bootstrap_servers=runtime.kafka_bootstrap_servers))
    alert_stream.sink_to(
        jsonl_file_sink(
            bucket=runtime.evidence_bucket,
            prefix=runtime.curated_output_prefix,
            topic=config.derived_topics["ops_alerts"],
        )
    )
    env.execute("vina-bim-shop-ops-alerts")
