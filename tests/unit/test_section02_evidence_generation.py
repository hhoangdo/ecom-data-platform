import importlib.util
import hashlib
import json
from pathlib import Path

import pandas as pd


def load_evidence_module():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "scripts" / "qa" / "generate_section02_evidence.py"
    spec = importlib.util.spec_from_file_location("generate_section02_evidence", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_expected_evidence_artifacts_are_declared() -> None:
    evidence = load_evidence_module()

    assert evidence.expected_artifact_paths() == [
        "dbt_build_report.md",
        "dbt_test_results.csv",
        "dbt_model_results.csv",
        "dbt_catalog_summary.csv",
        "schema_inventory.csv",
        "table_row_counts.csv",
        "run_manifest.json",
        "screenshots/schema_design.png",
        "screenshots/gold_schema_inventory.png",
        "screenshots/dbt_test_summary.png",
    ]


def test_split_dbt_run_results_separates_models_and_tests() -> None:
    evidence = load_evidence_module()
    run_results = {
        "results": [
            {"unique_id": "model.vina_bim_shop.fact_order", "status": "success", "execution_time": 1.25},
            {
                "unique_id": "test.vina_bim_shop.not_null_fact_order_order_id",
                "status": "pass",
                "execution_time": 0.05,
                "failures": 0,
            },
        ]
    }
    manifest = {
        "nodes": {
            "model.vina_bim_shop.fact_order": {
                "resource_type": "model",
                "name": "fact_order",
                "schema": "gold",
                "config": {"materialized": "table"},
            },
            "test.vina_bim_shop.not_null_fact_order_order_id": {
                "resource_type": "test",
                "name": "not_null_fact_order_order_id",
                "depends_on": {"nodes": ["model.vina_bim_shop.fact_order"]},
            },
        }
    }

    model_rows, test_rows = evidence.split_dbt_run_results(run_results, manifest)

    assert model_rows == [
        {
            "model_name": "fact_order",
            "schema": "gold",
            "materialized": "table",
            "status": "success",
            "execution_time_seconds": 1.25,
        }
    ]
    assert test_rows == [
        {
            "test_name": "not_null_fact_order_order_id",
            "status": "pass",
            "failures": 0,
            "execution_time_seconds": 0.05,
            "depends_on": "fact_order",
        }
    ]


def test_catalog_summary_rows_include_columns_and_descriptions() -> None:
    evidence = load_evidence_module()
    manifest = {
        "nodes": {
            "model.vina_bim_shop.fact_order": {
                "resource_type": "model",
                "name": "fact_order",
                "schema": "gold",
                "description": "Order fact.",
            }
        }
    }
    catalog = {
        "nodes": {
            "model.vina_bim_shop.fact_order": {
                "metadata": {"type": "BASE TABLE"},
                "columns": {
                    "order_id": {"type": "VARCHAR"},
                    "official_paid_revenue": {"type": "DOUBLE"},
                },
            }
        }
    }

    rows = evidence.catalog_summary_rows(manifest, catalog)

    assert rows == [
        {
            "schema": "gold",
            "model_name": "fact_order",
            "relation_type": "BASE TABLE",
            "column_count": 2,
            "columns": "official_paid_revenue, order_id",
            "description": "Order fact.",
        }
    ]


def test_all_zone_model_inventory_and_metadata_match_schema_design_source() -> None:
    evidence = load_evidence_module()
    repo_root = Path(__file__).resolve().parents[2]

    model_names = evidence.all_zone_model_names(repo_root)

    assert {zone: len(names) for zone, names in model_names.items()} == {
        "bronze": 16,
        "silver": 14,
        "gold": 22,
    }
    metadata = evidence.validate_schema_design_model_coverage(repo_root)
    source_path = repo_root / "architecture" / "diagrams" / "schema_design.puml"
    assert metadata == {
        "schema_design_source": "architecture/diagrams/schema_design.puml",
        "schema_design_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "schema_design_model_counts": {"bronze": 16, "silver": 14, "gold": 22},
    }


def test_schema_design_render_metadata_records_fallback(tmp_path: Path) -> None:
    evidence = load_evidence_module()

    def unavailable_server(source: str, output_path: Path) -> None:
        raise OSError("offline")

    evidence.render_plantuml_png = unavailable_server

    metadata = evidence.render_schema_design(Path(__file__).resolve().parents[2], tmp_path / "schema_design.png")

    assert metadata["schema_design_render_mode"] == "fallback_png"
    assert "offline" in metadata["schema_design_render_error"]
    assert (tmp_path / "schema_design.png").is_file()


def test_section02_deliverable_references_evidence_artifacts() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    content = (repo_root / "deliverables" / "02_schema_design.md").read_text(encoding="utf-8")

    for phrase in [
        "uv run python scripts/qa/generate_section02_evidence.py",
        "evidence/02_schema_design/dbt_build_report.md",
        "evidence/02_schema_design/screenshots/schema_design.png",
        "evidence/02_schema_design/table_row_counts.csv",
    ]:
        assert phrase in content


def test_generated_section02_evidence_records_quarantine_and_render_mode() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    manifest = json.loads((repo_root / "evidence" / "02_schema_design" / "run_manifest.json").read_text(encoding="utf-8"))
    row_counts = pd.read_csv(repo_root / "evidence" / "02_schema_design" / "table_row_counts.csv")
    indexed_counts = row_counts.set_index(["schema", "table_name"])["row_count"]

    assert manifest["schema_design_render_mode"] in {"plantuml_server", "fallback_png"}
    assert manifest["schema_design_source"] == "architecture/diagrams/schema_design.puml"
    assert len(manifest["schema_design_sha256"]) == 64
    assert manifest["schema_design_model_counts"] == {"bronze": 16, "silver": 14, "gold": 22}
    assert indexed_counts[("bronze", "raw_bad_events")] > 0
    assert indexed_counts[("bronze", "raw_bad_snapshots")] > 0

    catalog = pd.read_csv(repo_root / "evidence" / "02_schema_design" / "dbt_catalog_summary.csv")
    for feature_name in ["feat_customer_90d", "feat_stream_60m", "feat_customer_unified"]:
        columns = catalog.loc[catalog["model_name"] == feature_name, "columns"].item().split(", ")
        assert "event_timestamp" in columns
        assert "created" in columns
        assert "created_ts" not in columns
