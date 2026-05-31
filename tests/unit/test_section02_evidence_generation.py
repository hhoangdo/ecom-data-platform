import importlib.util
from pathlib import Path


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
