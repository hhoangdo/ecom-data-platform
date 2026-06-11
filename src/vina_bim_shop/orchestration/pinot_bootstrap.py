"""``pinot_bootstrap`` DAG runtime.

Applies Pinot schema and table assets against the local Pinot controller
and runs sample serving queries through the Pinot broker and Trino. The DAG
consumes the realtime topics and Gold tables that Flink and Spark produce;
it does not own Flink job lifecycle.
"""
from __future__ import annotations

from typing import Any

from vina_bim_shop.pinot.bootstrap import apply_assets
from vina_bim_shop.pinot.query_examples import run_query_examples

from .paths import _utc_now, _write_json, build_run_root


def run_pinot_bootstrap(*, run_id: str) -> dict[str, Any]:
    run_root = build_run_root("pinot_bootstrap", run_id)
    manifest = apply_assets(
        controller_url="http://pinot-controller:9000",
        evidence_root=run_root,
    )
    query_manifest = run_query_examples(
        evidence_root=run_root,
        broker_url="http://pinot-broker:8000",
        trino_url="http://trino:8080",
        trino_user="vina_analyst",
    )
    result = {
        "captured_at": _utc_now(),
        "tables": manifest["tables"],
        "artifacts": sorted({"pinot_bootstrap_manifest.json", *query_manifest["artifacts"]}),
    }
    _write_json(run_root / "run_manifest.json", result)
    return result
