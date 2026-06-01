from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from vina_bim_shop.lakehouse.spark.trino import execute_trino_query


REPO_ROOT = Path(__file__).resolve().parents[3]
SQL_DIR = REPO_ROOT / "infra" / "pinot" / "sql"
DEFAULT_EVIDENCE_ROOT = Path("evidence/07_pinot_serving")


def execute_pinot_query(name: str, query: str, *, broker_url: str = "http://localhost:8000") -> dict[str, Any]:
    response = requests.post(
        f"{broker_url.rstrip('/')}/query/sql",
        json={"sql": query},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return {"name": name, "query": query, **payload}


def _default_window() -> tuple[str, str]:
    return (
        datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        datetime(2026, 5, 1, 11, 0, 0, tzinfo=timezone.utc).isoformat(),
    )


def _pinot_dashboard_queries(start_ts: str, end_ts: str) -> dict[str, str]:
    return {
        "live_revenue_by_category": f"""
select metric_minute, primary_category, sum(revenue_amount) as revenue_amount
from pinot_realtime_commerce_metrics_1m
where metric_minute >= '{start_ts}' and metric_minute < '{end_ts}'
group by metric_minute, primary_category
order by metric_minute, primary_category
limit 20
""".strip(),
        "live_payment_failures": f"""
select metric_minute, payment_method, sum(payment_failure_count) as payment_failure_count
from pinot_realtime_commerce_metrics_1m
where metric_minute >= '{start_ts}' and metric_minute < '{end_ts}'
group by metric_minute, payment_method
order by metric_minute, payment_method
limit 20
""".strip(),
        "live_ops_alerts": """
select alert_type, severity, count(*) as alert_count
from pinot_realtime_ops_alerts
group by alert_type, severity
order by alert_count desc, alert_type
limit 20
""".strip(),
        "correction_audit": """
select metric_key, correction_version, correction_reason, revenue_amount, gmv_proxy_amount
from pinot_realtime_metric_corrections
order by correction_version desc, metric_key
limit 20
""".strip(),
    }


def _load_trino_query(start_ts: str, end_ts: str) -> str:
    template = (SQL_DIR / "reconciliation_trino.sql").read_text(encoding="utf-8")
    return template.format(start_ts=start_ts, end_ts=end_ts).strip().rstrip(";")


def _rows_from_pinot(result: dict[str, Any]) -> list[list[Any]]:
    return result.get("resultTable", {}).get("rows", []) or []


def _row_value(row: list[Any], index: int, default: Any) -> Any:
    return row[index] if len(row) > index else default


def _pinot_hourly_rollup(base_result: dict[str, Any], correction_result: dict[str, Any]) -> dict[str, Any]:
    by_metric_key: dict[str, dict[str, Any]] = {}
    for row in _rows_from_pinot(base_result):
        metric_key = str(_row_value(row, 0, f"base-{len(by_metric_key)}"))
        by_metric_key[metric_key] = {
            "metric_key": metric_key,
            "order_count": int(_row_value(row, 2, 0) or 0),
            "order_placed_count": int(_row_value(row, 3, 0) or 0),
            "checkout_started_count": int(_row_value(row, 4, 0) or 0),
            "revenue_amount": float(_row_value(row, 5, 0.0) or 0.0),
            "gmv_proxy_amount": float(_row_value(row, 6, 0.0) or 0.0),
            "correction_version": int(_row_value(row, 7, 0) or 0),
        }

    for row in _rows_from_pinot(correction_result):
        metric_key = str(_row_value(row, 0, f"correction-{len(by_metric_key)}"))
        version = int(_row_value(row, 7, 0) or 0)
        existing = by_metric_key.get(metric_key)
        if existing is None or version >= int(existing["correction_version"]):
            by_metric_key[metric_key] = {
                "metric_key": metric_key,
                "order_count": int(_row_value(row, 2, 0) or 0),
                "order_placed_count": int(_row_value(row, 3, 0) or 0),
                "checkout_started_count": int(_row_value(row, 4, 0) or 0),
                "revenue_amount": float(_row_value(row, 5, 0.0) or 0.0),
                "gmv_proxy_amount": float(_row_value(row, 6, 0.0) or 0.0),
                "correction_version": version,
            }

    return {
        "metric_keys": len(by_metric_key),
        "order_count": sum(int(row["order_count"]) for row in by_metric_key.values()),
        "order_placed_count": sum(int(row["order_placed_count"]) for row in by_metric_key.values()),
        "checkout_started_count": sum(int(row["checkout_started_count"]) for row in by_metric_key.values()),
        "revenue_amount": round(sum(float(row["revenue_amount"]) for row in by_metric_key.values()), 2),
        "gmv_proxy_amount": round(sum(float(row["gmv_proxy_amount"]) for row in by_metric_key.values()), 2),
    }


def run_query_examples(
    *,
    evidence_root: str | Path = DEFAULT_EVIDENCE_ROOT,
    broker_url: str = "http://localhost:8000",
    trino_url: str = "http://localhost:8080",
    trino_user: str = "vina_analyst",
    start_ts: str | None = None,
    end_ts: str | None = None,
) -> dict[str, Any]:
    evidence_path = Path(evidence_root)
    query_output_path = evidence_path / "query_outputs"
    query_output_path.mkdir(parents=True, exist_ok=True)

    if start_ts is None or end_ts is None:
        start_ts, end_ts = _default_window()

    pinot_results = {
        name: execute_pinot_query(name, query, broker_url=broker_url)
        for name, query in _pinot_dashboard_queries(start_ts, end_ts).items()
    }
    (query_output_path / "pinot_dashboard_results.json").write_text(
        json.dumps(pinot_results, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    pinot_base = execute_pinot_query(
        "pinot_reconciliation_base",
        f"""
select metric_key, metric_minute, order_count, order_placed_count, checkout_started_count,
       revenue_amount, gmv_proxy_amount, correction_version
from pinot_realtime_commerce_metrics_1m
where metric_minute >= '{start_ts}' and metric_minute < '{end_ts}'
""".strip(),
        broker_url=broker_url,
    )
    pinot_corrections = execute_pinot_query(
        "pinot_reconciliation_corrections",
        f"""
select metric_key, metric_minute, order_count, order_placed_count, checkout_started_count,
       revenue_amount, gmv_proxy_amount, correction_version
from pinot_realtime_metric_corrections
where metric_minute >= '{start_ts}' and metric_minute < '{end_ts}'
""".strip(),
        broker_url=broker_url,
    )
    pinot_rollup = _pinot_hourly_rollup(pinot_base, pinot_corrections)
    (query_output_path / "pinot_reconciliation_results.json").write_text(
        json.dumps(
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "base_query": pinot_base,
                "correction_query": pinot_corrections,
                "hourly_rollup": pinot_rollup,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    trino_result = execute_trino_query(_load_trino_query(start_ts, end_ts), trino_url=trino_url, user=trino_user)
    (query_output_path / "trino_reconciliation_results.json").write_text(
        json.dumps(trino_result, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    trino_rows = trino_result.get("rows", [])
    trino_row = trino_rows[0] if trino_rows else []
    comparison = {
        "pinot_order_count": pinot_rollup["order_count"],
        "pinot_revenue_amount": pinot_rollup["revenue_amount"],
        "pinot_gmv_proxy_amount": pinot_rollup["gmv_proxy_amount"],
        "trino_order_count": trino_row[1] if len(trino_row) > 1 else None,
        "trino_official_paid_revenue": trino_row[2] if len(trino_row) > 2 else None,
        "trino_gross_merchandise_value": trino_row[3] if len(trino_row) > 3 else None,
        "trino_conversion_rate": trino_row[8] if len(trino_row) > 8 else None,
    }
    (query_output_path / "reconciliation_report.md").write_text(
        "\n".join(
            [
                "# Pinot Reconciliation Report",
                "",
                f"- Window: `{start_ts}` -> `{end_ts}`",
                "- Pinot is fresh and provisional; Spark Gold through Trino is canonical.",
                "- Correction handling uses the latest correction row per `metric_key` when correction rows exist.",
                "",
                "## Comparison",
                "",
                f"- Pinot order_count: `{comparison['pinot_order_count']}`",
                f"- Pinot revenue_amount: `{comparison['pinot_revenue_amount']}`",
                f"- Pinot gmv_proxy_amount: `{comparison['pinot_gmv_proxy_amount']}`",
                f"- Trino order_count: `{comparison['trino_order_count']}`",
                f"- Trino official_paid_revenue: `{comparison['trino_official_paid_revenue']}`",
                f"- Trino gross_merchandise_value: `{comparison['trino_gross_merchandise_value']}`",
                f"- Trino conversion_rate: `{comparison['trino_conversion_rate']}`",
            ]
        ),
        encoding="utf-8",
    )

    manifest = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "artifacts": [
            "query_outputs/pinot_dashboard_results.json",
            "query_outputs/pinot_reconciliation_results.json",
            "query_outputs/trino_reconciliation_results.json",
            "query_outputs/reconciliation_report.md",
        ],
    }
    (evidence_path / "query_examples_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest
