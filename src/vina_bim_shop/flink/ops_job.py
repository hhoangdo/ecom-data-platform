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


def add_ops_pipeline(*, env, config, runtime, group_ids: dict[str, str]) -> None:
    from pyflink.common import Types, WatermarkStrategy

    sources = []
    for source_name in ["ops", "catalog", "fulfillment"]:
        sources.append(
            kafka_source(
                env=env,
                topic=config.source_topics[source_name],
                bootstrap_servers=runtime.kafka_bootstrap_servers,
                group_id=group_ids[source_name],
                watermark_strategy=WatermarkStrategy.no_watermarks(),
            ).map(lambda raw: json.loads(raw), output_type=Types.PICKLED_BYTE_ARRAY()).assign_timestamps_and_watermarks(
                event_timestamp_assigner(out_of_orderness_seconds=config.out_of_orderness_seconds)
            )
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

    alert_stream.add_sink(kafka_sink(topic=config.derived_topics["ops_alerts"], bootstrap_servers=runtime.kafka_bootstrap_servers))
    alert_stream.sink_to(
        jsonl_file_sink(
            bucket=runtime.evidence_bucket,
            prefix=runtime.curated_output_prefix,
            topic=config.derived_topics["ops_alerts"],
        )
    )


def run() -> None:
    config = load_streaming_config()
    runtime = load_runtime_settings(config)

    from pyflink.datastream import StreamExecutionEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    add_required_jars(env)
    configure_checkpointing(
        env,
        checkpoint_uri=f"s3://{runtime.checkpoint_bucket}/{runtime.checkpoint_prefix}/ops_alerts",
    )
    add_ops_pipeline(
        env=env,
        config=config,
        runtime=runtime,
        group_ids={
            "catalog": "vina-bim-shop-catalog-alerts",
            "fulfillment": "vina-bim-shop-fulfillment-alerts",
            "ops": "vina-bim-shop-ops-alerts",
        },
    )
    env.execute("vina-bim-shop-ops-alerts")
