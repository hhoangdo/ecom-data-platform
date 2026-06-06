from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


OFFICIAL_DELIVERABLES = [
    "01_data_generator.md",
    "02_schema_design.md",
    "03_kafka_ingestion.md",
    "04_lakehouse.md",
    "05_spark_batch.md",
    "06_flink_streaming.md",
    "07_pinot_serving.md",
    "08_airflow_gx_orchestration.md",
    "09_datahub_governance.md",
    "10_duckdb_dbt_local_analytics.md",
]


def test_official_deliverables_do_not_reference_internal_adr_runbooks() -> None:
    repo_root = _repo_root()
    forbidden_phrases = [
        "architecture/decisions",
        "Acceptance tests cover:",
        "This runbook implements",
        "# ADR ",
        "ADR 01",
        "ADR 02",
        "ADR 03",
        "ADR 04",
        "ADR 05",
        "ADR 06",
        "ADR 07",
        "ADR 08",
    ]

    for file_name in OFFICIAL_DELIVERABLES:
        content = (repo_root / "deliverables" / file_name).read_text(encoding="utf-8")
        for phrase in forbidden_phrases:
            assert phrase not in content, f"{file_name} still contains {phrase!r}"


def test_root_readme_links_official_deep_dive_docs() -> None:
    repo_root = _repo_root()
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    for required_link in [
        "deliverables/02_schema_design.md",
        "deliverables/09_datahub_governance.md",
        "deliverables/10_duckdb_dbt_local_analytics.md",
    ]:
        assert required_link in readme

    assert "Data Dictionary" in readme
    assert "architecture/decisions" not in readme
    assert "DataHub governance ADR" not in readme


def test_placeholder_deliverables_are_explicitly_out_of_scope() -> None:
    repo_root = _repo_root()

    for file_name in [
        "03_data_generator_improvement.md",
        "04.1_ml_design.md",
        "04.2_llm_design.md",
    ]:
        content = (repo_root / "deliverables" / file_name).read_text(encoding="utf-8")
        assert "Out of scope for the current platform evidence" in content
