from pathlib import Path


def test_root_readme_documents_section01_and_section02_finalization() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    required_phrases = [
        "Sections 01 and 02 are fulfilled for the mini-coursework phase",
        "Spark, Flink, Apache Pinot, and Trino are architectural target contracts",
        "dbt-DuckDB is the runnable local implementation",
        "uv run python scripts/qa/finalize_sections_01_02.py",
        "evidence/final_dataset/vina_bim_shop_medium_raw.zip",
        "evidence/final_dataset/final_dataset_manifest.json",
        "data/gold/vina_bim_shop.duckdb",
        "data/gold/vina_bim_shop_executive.duckdb",
        "DuckDB Executive Mart",
        "Trino Gold snapshot export",
        "Runnable locally",
        "Architectural contract",
    ]
    for phrase in required_phrases:
        assert phrase in readme


def test_root_readme_links_section_evidence() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    for path in [
        "deliverables/01_data_generator.md",
        "deliverables/02_schema_design.md",
        "architecture/diagrams/erd/physical_gold_model.puml",
        "architecture/diagrams/erd/physical_gold_model.png",
        "evidence/01_data_generator/quality_report.md",
        "evidence/02_schema_design/dbt_build_report.md",
    ]:
        assert path in readme
