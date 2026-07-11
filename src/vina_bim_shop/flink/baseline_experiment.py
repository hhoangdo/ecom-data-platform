from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
from typing import Any

import yaml

from vina_bim_shop.flink.config import StreamingConfig, load_streaming_config
from vina_bim_shop.flink.runtime import RuntimeSettings, add_required_jars, configure_checkpointing, load_runtime_settings
from vina_bim_shop.flink.smoke import build_cleanroom_smoke_phases


DEFAULT_EXPERIMENT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "configs" / "pipelines" / "flink_baseline_experiment.yaml"

_CANONICAL_CONSUMER_GROUPS = {
    "commerce": "vina-bim-shop-commerce-metrics",
    "catalog": "vina-bim-shop-catalog-alerts",
    "fulfillment": "vina-bim-shop-fulfillment-alerts",
    "ops": "vina-bim-shop-ops-alerts",
}


@dataclass(frozen=True)
class FlinkExperimentProfile:
    variant: str
    job_name: str
    consumer_group_suffix: str
    source_topics: dict[str, str]
    derived_topics: dict[str, str]
    consumer_groups: dict[str, str]
    window_minutes: int
    checkpoint_prefix: str
    curated_output_prefix: str
    parallelism: int
    checkpointing_enabled: bool
    out_of_orderness_seconds: int
    allowed_lateness_seconds: dict[str, int]
    payment_failure_alert_threshold: dict[str, float | int]
    checkpoint_bucket: str
    curated_output_bucket: str


def load_experiment_profile(
    variant: str,
    *,
    experiment_config_path: str | Path = DEFAULT_EXPERIMENT_CONFIG_PATH,
    streaming_config: StreamingConfig | None = None,
) -> FlinkExperimentProfile:
    config = streaming_config or load_streaming_config()
    raw = yaml.safe_load(Path(experiment_config_path).read_text(encoding="utf-8"))
    variants = dict(raw.get("variants", {})) if isinstance(raw, dict) else {}
    if variant not in variants:
        raise ValueError("variant must be baseline or optimized")

    settings = dict(variants[variant])
    suffix = str(settings["derived_topic_suffix"])
    lateness_setting = settings.get("allowed_lateness_seconds")
    allowed_lateness = (
        {topic: int(lateness_setting) for topic in config.source_topics.values()}
        if lateness_setting is not None
        else dict(config.allowed_lateness_seconds)
    )
    group_suffix = str(settings["consumer_group_suffix"])
    profile = FlinkExperimentProfile(
        variant=variant,
        job_name=str(settings["job_name"]),
        consumer_group_suffix=group_suffix,
        source_topics=dict(config.source_topics),
        derived_topics={name: f"{topic}_{suffix}" for name, topic in config.derived_topics.items()},
        consumer_groups={name: f"{group}-{group_suffix}" for name, group in _CANONICAL_CONSUMER_GROUPS.items()},
        window_minutes=config.window_minutes,
        checkpoint_prefix=str(settings["checkpoint_prefix"]),
        curated_output_prefix=str(settings["curated_output_prefix"]),
        parallelism=int(settings["parallelism"]),
        checkpointing_enabled=bool(settings["checkpointing_enabled"]),
        out_of_orderness_seconds=int(settings.get("out_of_orderness_seconds", config.out_of_orderness_seconds)),
        allowed_lateness_seconds=allowed_lateness,
        payment_failure_alert_threshold=dict(config.payment_failure_alert_threshold),
        checkpoint_bucket=config.checkpoint_bucket,
        curated_output_bucket=config.curated_output_bucket,
    )
    _validate_profile(profile, config)
    return profile


def _validate_profile(profile: FlinkExperimentProfile, canonical: StreamingConfig) -> None:
    if profile.parallelism != 1:
        raise ValueError("experiment parallelism must be 1")
    if profile.job_name in {"vina-bim-shop-commerce-metrics", "vina-bim-shop-ops-alerts"}:
        raise ValueError("experiment job name collides with a canonical job")
    if set(profile.derived_topics.values()) & set(canonical.derived_topics.values()):
        raise ValueError("experiment derived topics collide with canonical topics")
    if set(profile.consumer_groups.values()) & set(_CANONICAL_CONSUMER_GROUPS.values()):
        raise ValueError("experiment consumer groups collide with canonical groups")
    if profile.checkpoint_prefix == canonical.checkpoint_prefix:
        raise ValueError("experiment checkpoint prefix collides with canonical prefix")
    if profile.curated_output_prefix == canonical.curated_output_prefix:
        raise ValueError("experiment curated output prefix collides with canonical prefix")


def profile_streaming_config(profile: FlinkExperimentProfile) -> StreamingConfig:
    return StreamingConfig(
        source_topics=dict(profile.source_topics),
        derived_topics=dict(profile.derived_topics),
        window_minutes=profile.window_minutes,
        out_of_orderness_seconds=profile.out_of_orderness_seconds,
        allowed_lateness_seconds=dict(profile.allowed_lateness_seconds),
        payment_failure_alert_threshold=dict(profile.payment_failure_alert_threshold),
        checkpoint_bucket=profile.checkpoint_bucket,
        checkpoint_prefix=profile.checkpoint_prefix,
        curated_output_bucket=profile.curated_output_bucket,
        curated_output_prefix=profile.curated_output_prefix,
    )


def compose_variant_job(
    profile: FlinkExperimentProfile,
    *,
    environment_factory: Callable[[], Any],
    add_required_jars_fn: Callable[[Any], None],
    configure_checkpointing_fn: Callable[[Any, str], None],
    add_commerce_pipeline_fn: Callable[[Any, StreamingConfig, RuntimeSettings, str], None],
    add_ops_pipeline_fn: Callable[[Any, StreamingConfig, RuntimeSettings, dict[str, str]], None],
) -> Any:
    config = profile_streaming_config(profile)
    runtime = load_runtime_settings(config)
    env = environment_factory()
    env.set_parallelism(profile.parallelism)
    add_required_jars_fn(env)
    if profile.checkpointing_enabled:
        configure_checkpointing_fn(
            env,
            checkpoint_uri=f"s3://{runtime.checkpoint_bucket}/{runtime.checkpoint_prefix}/experiment",
        )
    add_commerce_pipeline_fn(
        env=env,
        config=config,
        runtime=runtime,
        group_id=profile.consumer_groups["commerce"],
    )
    add_ops_pipeline_fn(
        env=env,
        config=config,
        runtime=runtime,
        group_ids=profile.consumer_groups,
    )
    return env.execute(profile.job_name)


def run_experiment_job(variant: str) -> Any:
    from pyflink.datastream import StreamExecutionEnvironment

    from vina_bim_shop.flink.commerce_job import add_commerce_pipeline
    from vina_bim_shop.flink.ops_job import add_ops_pipeline

    profile = load_experiment_profile(variant)
    return compose_variant_job(
        profile,
        environment_factory=StreamExecutionEnvironment.get_execution_environment,
        add_required_jars_fn=add_required_jars,
        configure_checkpointing_fn=configure_checkpointing,
        add_commerce_pipeline_fn=add_commerce_pipeline,
        add_ops_pipeline_fn=add_ops_pipeline,
    )


def compare_runs(baseline: dict[str, Any], optimized: dict[str, Any]) -> dict[str, Any]:
    baseline_hash = str(baseline.get("replay_sha256", ""))
    optimized_hash = str(optimized.get("replay_sha256", ""))
    if not baseline_hash or baseline_hash != optimized_hash:
        raise ValueError("baseline and optimized replay hashes must match")

    baseline_aggregates = dict(baseline.get("on_time_aggregates", {}))
    optimized_aggregates = dict(optimized.get("on_time_aggregates", {}))
    if baseline_aggregates != optimized_aggregates:
        raise ValueError("baseline and optimized on-time aggregates must match")

    return {
        "passed": True,
        "replay_sha256": baseline_hash,
        "on_time_aggregates_equal": True,
        "on_time_aggregate_keys": sorted(baseline_aggregates),
        "correction_counts": {
            "baseline": int(baseline.get("correction_count", 0)),
            "optimized": int(optimized.get("correction_count", 0)),
        },
    }


def write_replay_fixture(path: str | Path) -> dict[str, Any]:
    replay_path = Path(path)
    replay_path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, str]] = []
    phase_counts: dict[str, int] = {}
    phase_event_ids: dict[str, list[str]] = {}
    topic_counts: dict[str, int] = {}

    for phase in build_cleanroom_smoke_phases():
        phase_name = str(phase["name"])
        phase_counts[phase_name] = 0
        phase_event_ids[phase_name] = []
        for topic, frame in sorted(dict(phase["topic_events"]).items()):
            for record in frame.to_dict("records"):
                value = json.dumps(record, default=_json_default, separators=(",", ":")).encode("utf-8")
                event_id = str(record.get("event_id", ""))
                rows.append(
                    {
                        "event_id": event_id,
                        "key": event_id,
                        "phase": phase_name,
                        "topic": str(topic),
                        "value_b64": base64.b64encode(value).decode("ascii"),
                    }
                )
                phase_counts[phase_name] += 1
                phase_event_ids[phase_name].append(event_id)
                topic_counts[str(topic)] = topic_counts.get(str(topic), 0) + 1

    payload = b"".join(
        json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
        for row in rows
    )
    replay_path.write_bytes(payload)
    return {
        "format": "flink-experiment-replay-v1",
        "phases": list(phase_counts),
        "phase_counts": phase_counts,
        "phase_event_ids": phase_event_ids,
        "record_count": len(rows),
        "replay_sha256": hashlib.sha256(payload).hexdigest(),
        "topic_counts": topic_counts,
    }


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
