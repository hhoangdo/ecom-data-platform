"""Resolve the deployable Flink runtime settings used by streaming jobs."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from vina_bim_shop.flink.config import StreamingConfig


@dataclass(frozen=True)
class RuntimeSettings:
    """Expose Kafka, MinIO, checkpoint, and output settings for a Flink job."""

    kafka_bootstrap_servers: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    checkpoint_bucket: str
    evidence_bucket: str
    checkpoint_prefix: str
    curated_output_prefix: str


@dataclass(frozen=True)
class ContainerRuntimeLimit:
    max_runtime_minutes: int
    grace_seconds: int
    disable_auto_stop: bool


def load_runtime_settings(config: StreamingConfig) -> RuntimeSettings:
    """Load environment overrides with pipeline-config defaults for a Flink run.

    Returns an immutable settings value; malformed environment values surface through
    downstream runtime configuration rather than being silently ignored.
    """

    return RuntimeSettings(
        kafka_bootstrap_servers=os.getenv("VBS_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        minio_endpoint=os.getenv("VBS_MINIO_INTERNAL_ENDPOINT", "http://minio:9000"),
        minio_access_key=os.getenv("VBS_MINIO_ROOT_USER", "vina_minio"),
        minio_secret_key=os.getenv("VBS_MINIO_ROOT_PASSWORD", "vina_minio_password"),
        checkpoint_bucket=os.getenv("VBS_CHECKPOINTS_BUCKET", config.checkpoint_bucket),
        evidence_bucket=os.getenv("VBS_EVIDENCE_BUCKET", config.curated_output_bucket),
        checkpoint_prefix=config.checkpoint_prefix,
        curated_output_prefix=config.curated_output_prefix,
    )


def load_container_runtime_limit() -> ContainerRuntimeLimit:
    return ContainerRuntimeLimit(
        max_runtime_minutes=int(os.getenv("VBS_FLINK_MAX_RUNTIME_MINUTES", "45")),
        grace_seconds=int(os.getenv("VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS", "30")),
        disable_auto_stop=os.getenv("VBS_FLINK_DISABLE_AUTO_STOP", "false").strip().lower() == "true",
    )


def add_required_jars(env: Any) -> None:
    _ = env


def configure_checkpointing(env: Any, *, checkpoint_uri: str) -> None:
    from pyflink.datastream import FileSystemCheckpointStorage
    from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
    from pyflink.datastream.checkpointing_mode import CheckpointingMode

    env.enable_checkpointing(30_000)
    config = env.get_checkpoint_config()
    config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    config.set_min_pause_between_checkpoints(15_000)
    config.set_checkpoint_timeout(120_000)
    config.set_max_concurrent_checkpoints(1)
    config.set_externalized_checkpoint_cleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    config.set_checkpoint_storage(FileSystemCheckpointStorage(checkpoint_uri))


def kafka_source(*, env: Any, topic: str, bootstrap_servers: str, group_id: str, watermark_strategy: Any) -> Any:
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return env.from_source(source, watermark_strategy, f"Kafka {topic}")


def kafka_sink(*, topic: str, bootstrap_servers: str) -> Any:
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import FlinkKafkaProducer

    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": bootstrap_servers},
    )


def event_timestamp_assigner(*, out_of_orderness_seconds: int = 5) -> Any:
    from pyflink.common import Duration, WatermarkStrategy
    from pyflink.common.watermark_strategy import TimestampAssigner

    class EventTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value: str | dict[str, Any], record_timestamp: int) -> int:
            return event_timestamp_millis(value)

    return WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(out_of_orderness_seconds)).with_timestamp_assigner(EventTimestampAssigner())


def event_timestamp_millis(value: str | dict[str, Any]) -> int:
    event = _coerce_event_record(value)
    event_ts = normalize_ts(str(event["event_timestamp"]))
    return int(datetime.fromisoformat(event_ts).timestamp() * 1000)


def jsonl_file_sink(*, bucket: str, prefix: str, topic: str) -> Any:
    from pyflink.common.serialization import Encoder
    from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy

    path = f"s3://{bucket}/{prefix.strip('/')}/{topic}"
    output_file_config = OutputFileConfig.builder().with_part_prefix("part").with_part_suffix(".jsonl").build()
    rolling_policy = RollingPolicy.default_rolling_policy(
        part_size=1024 * 1024,
        rollover_interval=60_000,
        inactivity_interval=30_000,
    )
    return (
        FileSink.for_row_format(path, Encoder.simple_string_encoder())
        .with_output_file_config(output_file_config)
        .with_rolling_policy(rolling_policy)
        .build()
    )


def normalize_ts(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _coerce_event_record(value: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise TypeError("Expected JSON object payload for Flink event timestamp extraction.")
        return parsed
    return value
