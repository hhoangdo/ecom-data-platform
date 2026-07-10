from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from vina_bim_shop.generators.config import GeneratorConfig


def write_evidence(
    config: GeneratorConfig,
    datasets: dict[str, pd.DataFrame],
    issue_records: list[dict[str, Any]],
    *,
    mode: str,
    topic_events: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Path]:
    topic_events = topic_events or {}
    config.evidence_root.mkdir(parents=True, exist_ok=True)
    sample_root = config.evidence_root / "sample_rows"
    sample_root.mkdir(parents=True, exist_ok=True)

    row_count_records = [
        {"dataset": name, "row_count": len(frame)}
        for name, frame in sorted(datasets.items())
    ]
    if topic_events:
        row_count_records.append({"dataset": "kafka_topics", "row_count": sum(len(frame) for frame in topic_events.values())})
    row_counts = pd.DataFrame(row_count_records)
    row_counts_path = config.evidence_root / "row_counts.csv"
    row_counts.to_csv(row_counts_path, index=False)

    schema_summary = _schema_summary(datasets)
    schema_summary_path = config.evidence_root / "schema_summary.csv"
    schema_summary.to_csv(schema_summary_path, index=False)

    quality_metrics = _quality_metrics(datasets, issue_records)
    quality_metrics_path = config.evidence_root / "quality_metrics.csv"
    quality_metrics.to_csv(quality_metrics_path, index=False)

    issues = pd.DataFrame(issue_records)
    if issues.empty:
        issues = pd.DataFrame(columns=["dataset", "issue_type", "affected_rows", "observed_rate"])
    issue_path = config.evidence_root / "issue_manifest.csv"
    issues.to_csv(issue_path, index=False)

    for name, frame in sorted(datasets.items()):
        frame.head(20).to_csv(sample_root / f"{name}.csv", index=False)
    for topic, frame in sorted(topic_events.items()):
        frame.head(20).to_csv(sample_root / f"kafka_topic_{topic}.csv", index=False)

    event_topic_counts = _event_topic_counts(topic_events)
    event_topic_counts_path = config.evidence_root / "event_topic_row_counts.csv"
    event_topic_counts.to_csv(event_topic_counts_path, index=False)

    schema_versions = _schema_version_summary(topic_events)
    schema_versions_path = config.evidence_root / "schema_version_summary.csv"
    schema_versions.to_csv(schema_versions_path, index=False)

    cardinality_summary = _cardinality_summary(datasets, topic_events)
    cardinality_summary_path = config.evidence_root / "cardinality_summary.csv"
    cardinality_summary.to_csv(cardinality_summary_path, index=False)

    source_config = _load_report_config(config.source_config_path)
    rubric_evidence_summary_path = config.evidence_root / "rubric_evidence_summary.md"
    rubric_evidence_summary_path.write_text(
        _rubric_evidence_report(
            config,
            source_config,
            quality_metrics,
            issues,
            cardinality_summary,
            schema_versions,
        ),
        encoding="utf-8",
    )

    manifest = {
        "platform_name": config.platform_name,
        "scale": config.scale,
        "mode": mode,
        "random_seed": config.random_seed,
        "history_days": config.history_days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "raw_root": _portable_path(config, config.raw_root),
        "evidence_root": _portable_path(config, config.evidence_root),
        "row_counts": {row["dataset"]: int(row["row_count"]) for row in row_counts.to_dict("records")},
        "kafka_topics": {
            row["event_topic"]: int(row["row_count"])
            for row in event_topic_counts.to_dict("records")
        },
        "config_path": _portable_path(config, config.source_config_path),
        "evidence_artifacts": {
            "cardinality_summary": _portable_path(config, cardinality_summary_path),
            "rubric_evidence_summary": _portable_path(config, rubric_evidence_summary_path),
        },
    }
    manifest_path = config.evidence_root / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    report_path = config.evidence_root / "quality_report.md"
    report_path.write_text(
        _quality_report(
            config,
            row_counts,
            quality_metrics,
            issues,
            event_topic_counts,
            schema_versions,
            rubric_evidence_summary_path,
        ),
        encoding="utf-8",
    )

    return {
        "row_counts": row_counts_path,
        "schema_summary": schema_summary_path,
        "quality_metrics": quality_metrics_path,
        "issue_manifest": issue_path,
        "event_topic_row_counts": event_topic_counts_path,
        "schema_version_summary": schema_versions_path,
        "cardinality_summary": cardinality_summary_path,
        "rubric_evidence_summary": rubric_evidence_summary_path,
        "run_manifest": manifest_path,
        "quality_report": report_path,
    }


def _schema_summary(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for name, frame in sorted(datasets.items()):
        for column in frame.columns:
            rows.append(
                {
                    "dataset": name,
                    "column": column,
                    "dtype": str(frame[column].dtype),
                    "non_null_count": int(frame[column].notna().sum()),
                    "null_rate": round(float(frame[column].isna().mean()), 5),
                }
            )
    return pd.DataFrame(rows)


def _quality_metrics(datasets: dict[str, pd.DataFrame], issue_records: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if "customers" in datasets:
        customers = datasets["customers"]
        rows.append({"metric": "hcmc_hanoi_customer_share", "value": round(float(customers["city"].isin(["Ho Chi Minh City", "Ha Noi"]).mean()), 5)})
    if "products" in datasets:
        products = datasets["products"]
        rows.append({"metric": "fmcg_elha_product_share", "value": round(float(products["primary_category"].isin(["FMCG", "ELHA"]).mean()), 5)})
        rows.append({"metric": "missing_brand_rate", "value": round(float(products["brand"].isna().mean()), 5)})
    if "orders" in datasets:
        orders = datasets["orders"]
        rows.append({"metric": "missing_shipping_method_rate", "value": round(float(orders["shipping_method"].isna().mean()), 5)})
        rows.append({"metric": "order_session_link_rate", "value": round(float(orders["session_id"].notna().mean()), 5)})
    if "order_items" in datasets:
        order_items = datasets["order_items"]
        duplicate_rate = order_items.duplicated(
            subset=["order_id", "product_id", "quantity", "unit_price", "discount_amount"]
        ).mean()
        rows.append({"metric": "offline_order_item_duplicate_rate", "value": round(float(duplicate_rate), 5)})
    for issue in issue_records:
        rows.append({"metric": f"issue_{issue['dataset']}_{issue['issue_type']}", "value": issue["observed_rate"]})
    return pd.DataFrame(rows)


def _quality_report(
    config: GeneratorConfig,
    row_counts: pd.DataFrame,
    quality_metrics: pd.DataFrame,
    issues: pd.DataFrame,
    event_topic_counts: pd.DataFrame,
    schema_versions: pd.DataFrame,
    rubric_evidence_summary_path: Path,
) -> str:
    row_count_lines = "\n".join(
        f"- `{row.dataset}`: {int(row.row_count):,} rows" for row in row_counts.itertuples(index=False)
    )
    metric_lines = "\n".join(
        f"- `{row.metric}`: {row.value}" for row in quality_metrics.itertuples(index=False)
    )
    issue_lines = "\n".join(
        f"- `{row.dataset}` / `{row.issue_type}`: {int(row.affected_rows):,} rows, observed rate {row.observed_rate}"
        for row in issues.itertuples(index=False)
    )
    topic_lines = "\n".join(
        f"- `{row.event_topic}`: {int(row.row_count):,} events" for row in event_topic_counts.itertuples(index=False)
    )
    version_lines = "\n".join(
        f"- `{row.event_topic}` schema `{row.schema_version}`: {int(row.row_count):,} events"
        for row in schema_versions.itertuples(index=False)
    )
    return f"""# Section 01 Data Generator Quality Report

## Run Context

- Platform: `{config.platform_name}`
- Scale: `{config.scale}`
- History days: `{config.history_days}`
- Seed: `{config.random_seed}`

## Row Counts

{row_count_lines}

## Quality Metrics

{metric_lines}

## Kafka Topic Row Counts

{topic_lines}

## Schema Version Summary

{version_lines}

## Issue Manifest Summary

{issue_lines}

## Rubric Evidence

- Consolidated rows 5-14 evidence: [{rubric_evidence_summary_path.name}]({rubric_evidence_summary_path.name})
"""


def _portable_path(config: GeneratorConfig, path: Path) -> str:
    repo_root = config.source_config_path.resolve().parents[2]
    try:
        return str(path.resolve().relative_to(repo_root)).replace("\\", "/")
    except ValueError:
        return str(path)


def _event_topic_counts(topic_events: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = [
        {"event_topic": topic, "row_count": len(frame)}
        for topic, frame in sorted(topic_events.items())
    ]
    return pd.DataFrame(rows, columns=["event_topic", "row_count"])


def _schema_version_summary(topic_events: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for topic, frame in sorted(topic_events.items()):
        if frame.empty:
            rows.append({"event_topic": topic, "schema_version": None, "row_count": 0})
            continue
        grouped = frame.groupby(["event_topic", "schema_version"]).size().reset_index(name="row_count")
        rows.extend(grouped.to_dict("records"))
    return pd.DataFrame(rows, columns=["event_topic", "schema_version", "row_count"])


def _cardinality_summary(
    datasets: dict[str, pd.DataFrame], topic_events: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    import duckdb

    relations: list[tuple[str, str, str, pd.DataFrame]] = []
    for entity, id_column in [
        ("customers", "customer_id"),
        ("products", "product_id"),
        ("orders", "order_id"),
    ]:
        frame = datasets.get(entity)
        if frame is not None and id_column in frame.columns:
            relations.append((entity, id_column, f"{entity}_evidence", frame[[id_column]]))

    event_frames = [
        frame[["event_id"]]
        for _, frame in sorted(topic_events.items())
        if "event_id" in frame.columns
    ]
    if event_frames:
        relations.append(("events", "event_id", "events_evidence", pd.concat(event_frames, ignore_index=True)))

    connection = duckdb.connect()
    rows: list[dict[str, Any]] = []
    try:
        for entity, id_column, relation_name, frame in relations:
            connection.register(relation_name, frame)
            row_count, approx_distinct_count = connection.execute(
                f"select count(*) as row_count, approx_count_distinct({id_column}) as approx_distinct_count from {relation_name}"
            ).fetchone()
            row_count = int(row_count)
            approx_distinct_count = int(approx_distinct_count or 0)
            uniqueness_ratio = round(min(approx_distinct_count, row_count) / row_count, 5) if row_count else 0.0
            rows.append(
                {
                    "entity": entity,
                    "id_column": id_column,
                    "row_count": row_count,
                    "approx_distinct_count": approx_distinct_count,
                    "uniqueness_ratio": uniqueness_ratio,
                }
            )
    finally:
        connection.close()

    return pd.DataFrame(
        rows,
        columns=["entity", "id_column", "row_count", "approx_distinct_count", "uniqueness_ratio"],
    )


def _load_report_config(source_config_path: Path) -> dict[str, Any]:
    with source_config_path.open("r", encoding="utf-8") as handle:
        source_config = yaml.safe_load(handle)
    if not isinstance(source_config, dict):
        raise ValueError(f"Expected a YAML mapping in {source_config_path}")
    return source_config


def _rubric_evidence_report(
    config: GeneratorConfig,
    source_config: dict[str, Any],
    quality_metrics: pd.DataFrame,
    issues: pd.DataFrame,
    cardinality_summary: pd.DataFrame,
    schema_versions: pd.DataFrame,
) -> str:
    quality_scenarios = source_config["quality_scenarios"]
    category_weights = source_config["category_weights"]
    city_weights = source_config["geography"]["cities"]
    outputs = source_config["outputs"]
    scale_profiles = source_config["scale_profiles"]
    burst_windows = source_config["streaming"]["burst_windows"]

    metric_values = {str(row.metric): row.value for row in quality_metrics.itertuples(index=False)}
    issue_values = {
        f"issue_{row.dataset}_{row.issue_type}": row.observed_rate for row in issues.itertuples(index=False)
    }

    def observed(name: str) -> str:
        value = metric_values.get(name, issue_values.get(name))
        return "not generated for this mode" if value is None else f"`{value}`"

    def configured(value: Any) -> str:
        return f"`{value}`"

    def table_row(name: str, configured_value: Any, observed_value: str) -> str:
        return f"| {name} | {configured(configured_value)} | {observed_value} |"

    city_weight_lines = "; ".join(f"{city['city']}={city['weight']}" for city in city_weights)
    category_weight_lines = "; ".join(f"{category}={weight}" for category, weight in category_weights.items())
    cardinality_lines = "\n".join(
        f"| {row.entity} | {row.id_column} | {int(row.row_count):,} | {int(row.approx_distinct_count):,} | {row.uniqueness_ratio:.5f} |"
        for row in cardinality_summary.itertuples(index=False)
    ) or "| not generated for this mode | - | - | - | - |"
    schema_version_lines = "\n".join(
        f"| {row.event_topic} | {row.schema_version} | {int(row.row_count):,} |"
        for row in schema_versions.itertuples(index=False)
    ) or "| not generated for this mode | - | - |"
    scale_profile_lines = "\n".join(
        f"| {profile_name} | {profile['history_days']} | "
        + "; ".join(f"{entity}={count}" for entity, count in profile["entities"].items())
        + " |"
        for profile_name, profile in scale_profiles.items()
    )
    ops_event_rows = int(
        schema_versions.loc[schema_versions["event_topic"] == "ops_events", "row_count"].sum()
    )

    return f"""# Section 01 Generator Rubric Evidence Summary

- Source configuration: `{_portable_path(config, config.source_config_path)}`
- Generated mode: `{config.scale}` scale, `{config.history_days}` history days, seed `{config.random_seed}`.
- Approximate distinct counts use DuckDB `approx_count_distinct`; uniqueness ratios are capped at `1.0` because an approximate estimate can exceed its population.

## Row 5 - Offline Skew

| Evidence | Configured value | Observed result |
| --- | --- | --- |
{table_row("HCMC and Ha Noi customer skew target", quality_scenarios["hcmc_hanoi_skew_ratio"], observed("hcmc_hanoi_customer_share"))}
{table_row("City weights", city_weight_lines, "See customer-share metric above")}
{table_row("Category weights", category_weight_lines, observed("fmcg_elha_product_share"))}

## Row 6 - Offline High Cardinality

| Entity | ID column | Row count | DuckDB approximate distinct count | Uniqueness ratio |
| --- | --- | ---: | ---: | ---: |
{cardinality_lines}

## Row 7 - Offline Schema Evolution

| Evidence | Configured value | Observed result |
| --- | --- | --- |
{table_row("Schema evolution cutoff ratio", quality_scenarios["schema_evolution_cutoff_ratio"], observed("issue_products_schema_evolution_category_attributes"))}
{table_row("Missing brand rate", quality_scenarios["missing_brand_rate"], observed("missing_brand_rate"))}
{table_row("Missing shipping method rate", quality_scenarios["missing_shipping_method_rate"], observed("missing_shipping_method_rate"))}

| Topic | Schema version | Event rows |
| --- | ---: | ---: |
{schema_version_lines}

## Row 8 - Offline Duplicates

| Evidence | Configured value | Observed result |
| --- | --- | --- |
{table_row("Offline order-item duplicate rate", quality_scenarios["offline_duplicate_rate"], observed("offline_order_item_duplicate_rate"))}

## Row 9 - Offline Generator Configuration

| Profile | History days | Entity counts |
| --- | ---: | --- |
{scale_profile_lines}

## Row 10 - Bronze Input Storage

| Evidence | Configured value | Observed result |
| --- | --- | --- |
{table_row("Raw output root", outputs["raw_root"], "Parquet snapshots and Kafka topic JSONL are generated under this root")}
{table_row("Kafka topic inputs", "catalog_events; commerce_events; fulfillment_events; ops_events; dead_letter_events", "dead_letter_events is the Bronze quarantine input")}

## Row 11 - Streaming Burst

| Evidence | Configured value | Observed result |
| --- | --- | --- |
{table_row("Burst windows", "; ".join(burst_windows), f"`{ops_event_rows}` ops event rows")}

## Row 12 - Streaming Late Arrivals

| Evidence | Configured value | Observed result |
| --- | --- | --- |
{table_row("Late-arrival rate", quality_scenarios["late_arrival_rate"], observed("issue_commerce_events_late_arrival"))}
{table_row("Late delay minutes", f"{quality_scenarios['late_delay_minutes_min']}-{quality_scenarios['late_delay_minutes_max']}", "Measured through the late-arrival issue metric")}
{table_row("Missing device type rate", quality_scenarios["missing_device_type_rate"], observed("issue_commerce_events_missing_device_type"))}

## Row 13 - Streaming Duplicates

| Evidence | Configured value | Observed result |
| --- | --- | --- |
{table_row("Streaming duplicate rate", quality_scenarios["stream_duplicate_rate"], observed("issue_commerce_events_exact_duplicate_event_payload"))}

## Row 14 - Streaming Generator Configuration

| Evidence | Configured value | Observed result |
| --- | --- | --- |
{table_row("Selected scale profile", config.scale, f"`{config.history_days}` history days")}
{table_row("Source configuration", _portable_path(config, config.source_config_path), "All reported controls above are loaded from this YAML mapping")}
"""
