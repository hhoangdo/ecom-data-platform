"""``reconciliation_report`` DAG runtime.

Compares provisional realtime Pinot metrics against canonical
Trino/Iceberg Gold for the same logical hourly window. The DAG fails only
when ``should_fail_reconciliation`` returns True — this is the single Pinot
path that can escalate to a blocking failure.
"""
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from vina_bim_shop.lakehouse.spark.window import BatchWindow
from vina_bim_shop.pinot.query_examples import run_query_examples
from vina_bim_shop.quality.policies import should_fail_reconciliation

from .paths import _utc_now, _write_json, build_run_root
from .quality_helpers import _render_docs, _validate_pandas_dataframe, _window_payload


def run_reconciliation_report(*, run_id: str, start_ts: str, end_ts: str) -> dict[str, Any]:
    run_root = build_run_root("reconciliation_report", run_id)
    window = BatchWindow.from_args(start_ts=start_ts, end_ts=end_ts, mode="hourly")
    window_payload = _window_payload(window)

    query_manifest = run_query_examples(
        evidence_root=run_root,
        broker_url="http://pinot-broker:8000",
        trino_url="http://trino:8080",
        trino_user="vina_analyst",
        start_ts=window_payload["start_ts"],
        end_ts=window_payload["end_ts"],
    )
    pinot_result = json.loads((run_root / "query_outputs" / "pinot_dashboard_results.json").read_text(encoding="utf-8"))
    dashboard_rows = len(pinot_result["dashboard_contract"].get("resultTable", {}).get("rows", []) or [])
    pinot_report = _validate_pandas_dataframe(
        dataframe=pd.DataFrame([{"dashboard_rows": dashboard_rows}]),
        datasource_name="pinot_runtime",
        asset_name="pinot_dashboard_asset",
        suite_name="pinot_query_contract",
        layer="pinot_queries",
        expectations=[
            __import__("great_expectations").expectations.ExpectColumnValuesToBeBetween(
                column="dashboard_rows",
                min_value=0,
            )
        ],
        output_root=run_root / "quality",
        window=window_payload,
    )
    reconciliation_success = dashboard_rows > 0
    manifest = {
        "captured_at": _utc_now(),
        "window": window_payload,
        "quality_reports": [f"quality/{pinot_report.suite_name}.json"],
        "query_manifest": "query_examples_manifest.json",
        "artifacts": sorted({*query_manifest["artifacts"], "quality/pinot_query_contract.json"}),
    }
    _write_json(run_root / "run_manifest.json", manifest)
    _render_docs([pinot_report])
    if should_fail_reconciliation(pinot_success=pinot_report.success, reconciliation_success=reconciliation_success):
        raise RuntimeError("Reconciliation report did not find Pinot rows for the selected hourly window.")
    return manifest
