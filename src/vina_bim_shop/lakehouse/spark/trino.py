from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests


def execute_trino_query(
    query: str,
    *,
    trino_url: str = os.getenv("VBS_TRINO_URL", "http://localhost:8080"),
    user: str = os.getenv("VBS_TRINO_USER", "vina_analyst"),
    post=requests.post,
    get=requests.get,
) -> dict[str, Any]:
    headers = {"X-Trino-User": user}
    response = post(
        f"{trino_url.rstrip('/')}/v1/statement",
        data=query,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    column_metadata = payload.get("columns", []) or []
    columns = [column["name"] for column in column_metadata]
    rows = payload.get("data", []) or []

    while payload.get("nextUri"):
        payload = get(payload["nextUri"], headers=headers, timeout=30).json()
        if not column_metadata and payload.get("columns"):
            column_metadata = payload.get("columns", []) or []
            columns = [column["name"] for column in column_metadata]
        rows.extend(payload.get("data", []) or [])

    if payload.get("error"):
        raise RuntimeError(payload["error"]["message"])

    return {
        "query": query,
        "columns": columns,
        "column_metadata": column_metadata,
        "rows": rows,
        "stats": payload.get("stats", {}),
    }


def run_gold_smoke_queries(
    *,
    evidence_root: str | Path,
    trino_url: str = os.getenv("VBS_TRINO_URL", "http://localhost:8080"),
    user: str = os.getenv("VBS_TRINO_USER", "vina_analyst"),
) -> dict[str, Any]:
    queries = {
        "gold_inventory": "show tables from iceberg.gold",
        "fact_order_count": "select count(*) as fact_order_count from iceberg.gold.fact_order",
        "fact_order_paid_revenue": "select round(sum(official_paid_revenue), 2) as official_paid_revenue from iceberg.gold.fact_order",
        "hourly_kpi_sample": """
select metric_hour, order_count, official_paid_revenue
from iceberg.gold.agg_hourly_reconciled_kpi
order by metric_hour
limit 10
""".strip(),
    }
    results = {
        name: execute_trino_query(query, trino_url=trino_url, user=user)
        for name, query in queries.items()
    }
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "trino_gold_smoke_results.json").write_text(
        json.dumps(results, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return results
