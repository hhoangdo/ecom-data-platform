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


def _load_sql_template(filename: str, *, start_ts: str, end_ts: str) -> str:
    template = (SQL_DIR / filename).read_text(encoding="utf-8").strip()
    return template.format(start_ts=start_ts, end_ts=end_ts).strip().rstrip(";")


def _wrap_windowed_query(base_query: str, *, start_ts: str, end_ts: str) -> str:
    return f"""
select *
from (
{base_query}
) as windowed_contract
where metric_minute >= '{start_ts}' and metric_minute < '{end_ts}'
order by metric_minute
limit 50
""".strip()


def _pinot_dashboard_queries(start_ts: str, end_ts: str) -> dict[str, str]:
    dashboard_contract = _load_sql_template("dashboard_pinot.sql", start_ts=start_ts, end_ts=end_ts)
    return {
        "dashboard_contract": _wrap_windowed_query(
            dashboard_contract,
            start_ts=start_ts,
            end_ts=end_ts,
        ),
        "live_ops_alerts": """
select alert_type, severity, count(*) as alert_count
from pinot_realtime_ops_alerts
group by alert_type, severity
order by alert_count desc, alert_type
limit 20
""".strip(),
        "correction_audit": f"""
select metric_key, metric_minute, correction_version, correction_reason, revenue_amount, gmv_proxy_amount
from pinot_realtime_metric_corrections
where metric_minute >= '{start_ts}' and metric_minute < '{end_ts}'
order by correction_version desc, metric_key
limit 20
""".strip(),
    }


def _load_trino_query(start_ts: str, end_ts: str) -> str:
    return _load_sql_template("reconciliation_trino.sql", start_ts=start_ts, end_ts=end_ts)


def _rows_from_pinot(result: dict[str, Any]) -> list[list[Any]]:
    return result.get("resultTable", {}).get("rows", []) or []


def _row_value(row: list[Any], index: int, default: Any) -> Any:
    return row[index] if len(row) > index else default


def _pinot_hourly_rollup(contract_result: dict[str, Any]) -> dict[str, Any]:
    rows = _rows_from_pinot(contract_result)
    return {
        "metric_minutes": len(rows),
        "order_count": sum(int(_row_value(row, 1, 0) or 0) for row in rows),
        "revenue_amount": round(sum(float(_row_value(row, 2, 0.0) or 0.0) for row in rows), 2),
        "gmv_proxy_amount": round(sum(float(_row_value(row, 3, 0.0) or 0.0) for row in rows), 2),
        "order_placed_count": sum(int(_row_value(row, 4, 0) or 0) for row in rows),
        "checkout_started_count": sum(int(_row_value(row, 5, 0) or 0) for row in rows),
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

    pinot_reconciliation_query = _wrap_windowed_query(
        _load_sql_template("reconciliation_pinot.sql", start_ts=start_ts, end_ts=end_ts),
        start_ts=start_ts,
        end_ts=end_ts,
    )
    pinot_contract = execute_pinot_query(
        "pinot_reconciliation_contract",
        pinot_reconciliation_query,
        broker_url=broker_url,
    )
    pinot_rollup = _pinot_hourly_rollup(pinot_contract)
    (query_output_path / "pinot_reconciliation_results.json").write_text(
        json.dumps(
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "contract_query": pinot_contract,
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
    comparison_notes = [
        "- Comparison mode: contract check by default. Do not expect numerical parity unless Pinot and Gold were intentionally rebuilt for the same closed hour."
    ]
    if pinot_rollup["metric_minutes"] == 0:
        comparison_notes.append("- Pinot returned no rows for the selected window, so this is not a like-for-like numerical comparison.")
    (query_output_path / "reconciliation_report.md").write_text(
        "\n".join(
            [
                "# Pinot Reconciliation Report",
                "",
                f"- Window: `{start_ts}` -> `{end_ts}`",
                "- Pinot is fresh and provisional; Spark Gold through Trino is canonical.",
                "- Correction handling uses the latest correction row per `metric_key` when correction rows exist.",
                *comparison_notes,
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
                f"- Pinot metric minutes returned: `{pinot_rollup['metric_minutes']}`",
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
