from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
from typing import Any

from vina_bim_shop.lakehouse.spark.constants import REQUIRED_GOLD_TABLES
from vina_bim_shop.lakehouse.spark.trino import execute_trino_query


def _load_duckdb() -> Any:
    try:
        return importlib.import_module("duckdb")
    except ModuleNotFoundError as exc:
        raise RuntimeError("DuckDB parity checks require project dependencies; run via `uv run`.") from exc


def _duckdb_scalar(connection: Any, query: str) -> Any:
    return connection.execute(query).fetchone()[0]


def _trino_scalar(query: str, *, trino_url: str, user: str) -> Any:
    result = execute_trino_query(query, trino_url=trino_url, user=user)
    if not result["rows"]:
        return None
    return result["rows"][0][0]


def run_parity_checks(
    *,
    evidence_root: str | Path,
    duckdb_path: str | Path = "data/gold/vina_bim_shop.duckdb",
    trino_url: str = os.getenv("VBS_TRINO_URL", "http://localhost:8080"),
    user: str = os.getenv("VBS_TRINO_USER", "vina_analyst"),
) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    duckdb = _load_duckdb()
    duckdb_connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        for table_name in REQUIRED_GOLD_TABLES:
            duckdb_value = _duckdb_scalar(duckdb_connection, f"select count(*) from gold.{table_name}")
            trino_value = _trino_scalar(
                f"select count(*) from iceberg.gold.{table_name}",
                trino_url=trino_url,
                user=user,
            )
            comparisons.append(
                {
                    "name": f"{table_name}.row_count",
                    "duckdb": duckdb_value,
                    "spark_trino": trino_value,
                    "delta": (trino_value or 0) - (duckdb_value or 0),
                    "tolerance": 0,
                    "success": duckdb_value == trino_value,
                }
            )

        for name, duckdb_query, trino_query in [
            (
                "fact_order.official_paid_revenue",
                "select round(sum(official_paid_revenue), 2) from gold.fact_order",
                "select round(sum(official_paid_revenue), 2) from iceberg.gold.fact_order",
            ),
            (
                "fact_order.gross_merchandise_value",
                "select round(sum(gross_merchandise_value), 2) from gold.fact_order",
                "select round(sum(gross_merchandise_value), 2) from iceberg.gold.fact_order",
            ),
            (
                "fact_order_item.estimated_cost",
                "select round(sum(estimated_cost), 2) from gold.fact_order_item",
                "select round(sum(estimated_cost), 2) from iceberg.gold.fact_order_item",
            ),
            (
                "fact_order_item.estimated_margin",
                "select round(sum(estimated_margin), 2) from gold.fact_order_item",
                "select round(sum(estimated_margin), 2) from iceberg.gold.fact_order_item",
            ),
            (
                "agg_hourly_reconciled_kpi.official_paid_revenue",
                "select round(sum(official_paid_revenue), 2) from gold.agg_hourly_reconciled_kpi",
                "select round(sum(official_paid_revenue), 2) from iceberg.gold.agg_hourly_reconciled_kpi",
            ),
        ]:
            duckdb_value = _duckdb_scalar(duckdb_connection, duckdb_query)
            trino_value = _trino_scalar(trino_query, trino_url=trino_url, user=user)
            delta = round((trino_value or 0) - (duckdb_value or 0), 2)
            comparisons.append(
                {
                    "name": name,
                    "duckdb": duckdb_value,
                    "spark_trino": trino_value,
                    "delta": delta,
                    "tolerance": 0.01,
                    "success": abs(delta) <= 0.01,
                }
            )
    finally:
        duckdb_connection.close()

    report = {
        "success": all(comparison["success"] for comparison in comparisons),
        "comparisons": comparisons,
    }
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "dbt_parity_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (evidence_path / "dbt_parity_report.md").write_text(
        "\n".join(
            [
                "# dbt Parity Report",
                "",
                f"Overall success: {'PASS' if report['success'] else 'FAIL'}",
                "",
                "| Check | DuckDB | Spark/Trino | Delta | Success |",
                "| --- | ---: | ---: | ---: | --- |",
                *[
                    f"| {comparison['name']} | {comparison['duckdb']} | {comparison['spark_trino']} | {comparison['delta']} | {'PASS' if comparison['success'] else 'FAIL'} |"
                    for comparison in comparisons
                ],
            ]
        ),
        encoding="utf-8",
    )
    if not report["success"]:
        raise RuntimeError("dbt parity checks failed.")
    return report
