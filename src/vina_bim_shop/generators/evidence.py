from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

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
    }
    manifest_path = config.evidence_root / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    report_path = config.evidence_root / "quality_report.md"
    report_path.write_text(
        _quality_report(config, row_counts, quality_metrics, issues, event_topic_counts, schema_versions),
        encoding="utf-8",
    )

    return {
        "row_counts": row_counts_path,
        "schema_summary": schema_summary_path,
        "quality_metrics": quality_metrics_path,
        "issue_manifest": issue_path,
        "event_topic_row_counts": event_topic_counts_path,
        "schema_version_summary": schema_versions_path,
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
