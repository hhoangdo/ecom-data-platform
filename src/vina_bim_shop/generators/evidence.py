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
) -> dict[str, Path]:
    config.evidence_root.mkdir(parents=True, exist_ok=True)
    sample_root = config.evidence_root / "sample_rows"
    sample_root.mkdir(parents=True, exist_ok=True)

    row_counts = pd.DataFrame(
        [{"dataset": name, "row_count": len(frame)} for name, frame in sorted(datasets.items())]
    )
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
        "config_path": _portable_path(config, config.source_config_path),
    }
    manifest_path = config.evidence_root / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    report_path = config.evidence_root / "quality_report.md"
    report_path.write_text(_quality_report(config, row_counts, quality_metrics, issues), encoding="utf-8")

    return {
        "row_counts": row_counts_path,
        "schema_summary": schema_summary_path,
        "quality_metrics": quality_metrics_path,
        "issue_manifest": issue_path,
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
    if "stream_events" in datasets:
        events = datasets["stream_events"]
        rows.append({"metric": "stream_late_arrival_rate", "value": round(float(events["is_late_arrival"].mean()), 5)})
        rows.append({"metric": "stream_event_duplicate_id_rate", "value": round(float(events["event_id"].duplicated().mean()), 5)})
        rows.append({"metric": "stream_missing_device_type_rate", "value": round(float(events["device_type"].isna().mean()), 5)})
        rows.append({"metric": "stream_burst_event_count", "value": int(events["is_burst_window"].sum())})
    for issue in issue_records:
        rows.append({"metric": f"issue_{issue['dataset']}_{issue['issue_type']}", "value": issue["observed_rate"]})
    return pd.DataFrame(rows)


def _quality_report(
    config: GeneratorConfig,
    row_counts: pd.DataFrame,
    quality_metrics: pd.DataFrame,
    issues: pd.DataFrame,
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

## Issue Manifest Summary

{issue_lines}
"""


def _portable_path(config: GeneratorConfig, path: Path) -> str:
    repo_root = config.source_config_path.resolve().parents[2]
    try:
        return str(path.resolve().relative_to(repo_root)).replace("\\", "/")
    except ValueError:
        return str(path)
