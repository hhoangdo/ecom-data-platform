from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from vina_bim_shop.flink.alerts import build_payment_failure_alert
from vina_bim_shop.flink.config import load_streaming_config
from vina_bim_shop.flink.corrections import build_correction_record, next_correction_version
from vina_bim_shop.flink.metrics import build_metric_snapshot
from vina_bim_shop.flink.runtime import (
    add_required_jars,
    configure_checkpointing,
    event_timestamp_assigner,
    jsonl_file_sink,
    kafka_sink,
    kafka_source,
    load_runtime_settings,
)


class CommerceWindowProcessor:
    def __init__(
        self,
        *,
        metrics_topic: str,
        correction_topic: str,
        ops_alerts_topic: str,
        payment_failure_threshold: dict[str, float | int],
        allowed_lateness_seconds: int,
    ):
        self._versions: dict[str, int] = {}
        self._metrics_topic = metrics_topic
        self._correction_topic = correction_topic
        self._ops_alerts_topic = ops_alerts_topic
        self._payment_failure_threshold = payment_failure_threshold
        self._allowed_lateness_seconds = allowed_lateness_seconds

    def process(self, key: str, context: Any, elements: Any):
        rows = list(elements)
        window = context.window()
        window_start = datetime.fromtimestamp(window.start / 1000, tz=timezone.utc).isoformat()
        window_end = datetime.fromtimestamp(window.end / 1000, tz=timezone.utc).isoformat()
        watermark_ts = datetime.fromtimestamp(context.current_watermark() / 1000, tz=timezone.utc).isoformat() if context.current_watermark() > 0 else window_end

        snapshot = build_metric_snapshot(
            events=rows,
            window_start_ts=window_start,
            window_end_ts=window_end,
            watermark_ts=watermark_ts,
            allowed_lateness_seconds=self._allowed_lateness_seconds,
        )
        metric_key = str(snapshot["metric_key"])
        if metric_key not in self._versions:
            self._versions[metric_key] = 0
            yield {"target_topic": self._metrics_topic, "value": snapshot}
        else:
            version = next_correction_version(previous_version=self._versions[metric_key])
            self._versions[metric_key] = version
            yield {
                "target_topic": self._correction_topic,
                "value": build_correction_record(
                    target_topic=self._metrics_topic,
                    metric_snapshot={**snapshot, "correction_version": version},
                    correction_version=version,
                    correction_reason="late_event" if snapshot["late_event_count"] else "recompute",
                    created_ts=watermark_ts,
                ),
            }

        failure_count = int(snapshot["payment_failure_count"])
        order_count = int(snapshot["order_count"])
        failure_rate = (failure_count / order_count) if order_count else 0.0
        if failure_count >= int(self._payment_failure_threshold["count"]) or failure_rate >= float(self._payment_failure_threshold["rate"]):
            yield {
                "target_topic": self._ops_alerts_topic,
                "value": build_payment_failure_alert(
                    snapshot=snapshot,
                    count_threshold=int(self._payment_failure_threshold["count"]),
                    rate_threshold=float(self._payment_failure_threshold["rate"]),
                    emitted_ts=watermark_ts,
                ),
            }


def add_commerce_pipeline(*, env: Any, config: Any, runtime: Any, group_id: str) -> None:
    from pyflink.common import Types, WatermarkStrategy
    from pyflink.datastream.functions import ProcessWindowFunction
    from pyflink.datastream.window import Time, TumblingEventTimeWindows

    source_stream = kafka_source(
        env=env,
        topic=config.source_topics["commerce"],
        bootstrap_servers=runtime.kafka_bootstrap_servers,
        group_id=group_id,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
    ).map(lambda raw: json.loads(raw), output_type=Types.PICKLED_BYTE_ARRAY()).assign_timestamps_and_watermarks(
        event_timestamp_assigner(out_of_orderness_seconds=config.out_of_orderness_seconds)
    )

    processor = CommerceWindowProcessor(
        metrics_topic=config.derived_topics["commerce_metrics"],
        correction_topic=config.derived_topics["metric_corrections"],
        ops_alerts_topic=config.derived_topics["ops_alerts"],
        payment_failure_threshold=config.payment_failure_alert_threshold,
        allowed_lateness_seconds=config.allowed_lateness_seconds[config.source_topics["commerce"]],
    )

    class _WindowBridge(ProcessWindowFunction):
        def process(self, key: str, context: Any, elements: Any):
            yield from processor.process(key, context, elements)

    outputs = (
        source_stream
        .key_by(
            lambda event: "|".join(
                str(event.get("payload", {}).get(field))
                for field in ["primary_category", "source", "device_type", "payment_method", "order_status"]
            )
            + f"|{event.get('schema_version')}"
        )
        .window(TumblingEventTimeWindows.of(Time.minutes(config.window_minutes)))
        .allowed_lateness(config.allowed_lateness_seconds[config.source_topics["commerce"]] * 1000)
        .process(_WindowBridge(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    metrics_stream = outputs.filter(
        lambda row: row["target_topic"] == config.derived_topics["commerce_metrics"],
    ).map(lambda row: json.dumps(row["value"], separators=(",", ":")), output_type=Types.STRING())
    corrections_stream = outputs.filter(
        lambda row: row["target_topic"] == config.derived_topics["metric_corrections"],
    ).map(lambda row: json.dumps(row["value"], separators=(",", ":")), output_type=Types.STRING())
    alerts_stream = outputs.filter(
        lambda row: row["target_topic"] == config.derived_topics["ops_alerts"],
    ).map(lambda row: json.dumps(row["value"], separators=(",", ":")), output_type=Types.STRING())

    metrics_stream.add_sink(kafka_sink(topic=config.derived_topics["commerce_metrics"], bootstrap_servers=runtime.kafka_bootstrap_servers))
    corrections_stream.add_sink(kafka_sink(topic=config.derived_topics["metric_corrections"], bootstrap_servers=runtime.kafka_bootstrap_servers))
    alerts_stream.add_sink(kafka_sink(topic=config.derived_topics["ops_alerts"], bootstrap_servers=runtime.kafka_bootstrap_servers))
    corrections_stream.sink_to(
        jsonl_file_sink(
            bucket=runtime.evidence_bucket,
            prefix=runtime.curated_output_prefix,
            topic=config.derived_topics["metric_corrections"],
        )
    )
    alerts_stream.sink_to(
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
        checkpoint_uri=f"s3://{runtime.checkpoint_bucket}/{runtime.checkpoint_prefix}/commerce_metrics",
    )
    add_commerce_pipeline(
        env=env,
        config=config,
        runtime=runtime,
        group_id="vina-bim-shop-commerce-metrics",
    )
    env.execute("vina-bim-shop-commerce-metrics")
