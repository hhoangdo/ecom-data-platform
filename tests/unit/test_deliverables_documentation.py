import re
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
    "12_novel_ideas.md",
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


def test_generator_deliverables_link_cardinality_and_rubric_evidence() -> None:
    repo_root = _repo_root()
    generator_deliverable = (repo_root / "deliverables" / "01_data_generator.md").read_text(encoding="utf-8")
    challenges_deliverable = (repo_root / "deliverables" / "11_solving_data_challenges.md").read_text(encoding="utf-8")

    assert "cardinality_summary.csv" in generator_deliverable
    assert "rubric_evidence_summary.md" in generator_deliverable
    assert "approx_count_distinct" in generator_deliverable
    assert "evidence-only" in generator_deliverable
    assert "rubric_evidence_summary.md" in challenges_deliverable


def test_spark_deliverables_link_standalone_optimization_evidence() -> None:
    repo_root = _repo_root()
    spark_deliverable = (repo_root / "deliverables" / "05_spark_batch.md").read_text(encoding="utf-8")
    challenges_deliverable = (repo_root / "deliverables" / "11_solving_data_challenges.md").read_text(encoding="utf-8")

    for artifact in [
        "optimization_report.md",
        "skew_equivalence.json",
        "high_cardinality_equivalence.json",
        "spark_skew_baseline_history.png",
        "spark_high_cardinality_optimized_history.png",
        "run_hourly_batch_window",
    ]:
        assert artifact in spark_deliverable
    assert "canonical Spark batch path does not use salting" in spark_deliverable
    assert "standalone optimization experiment" in challenges_deliverable.lower()


def test_orchestration_deliverable_documents_all_six_coursework_stage_artifacts() -> None:
    content = (_repo_root() / "deliverables" / "08_airflow_gx_orchestration.md").read_text(encoding="utf-8")

    for expected in [
        "mini_coursework_pipeline",
        "dp1_raw_to_bronze.ingest_raw_to_bronze",
        "dp1_raw_to_bronze.validate_bronze",
        "dp2_bronze_to_silver_gold.transform_bronze_to_silver_gold",
        "dp2_bronze_to_silver_gold.validate_silver_gold",
        "dp3_offline_features.compute_offline_features",
        "dp3_offline_features.validate_offline_features",
        "coursework_pipeline/<run-id>/dp1_ingest.json",
        "coursework_pipeline/<run-id>/dp3_validate.json",
        "mini_coursework_pipeline_graph.png",
        "mini_coursework_pipeline_grid.png",
    ]:
        assert expected in content


def test_novel_ideas_deliverable_uses_exact_ordered_headings_and_evidence_links() -> None:
    content = (_repo_root() / "deliverables" / "12_novel_ideas.md").read_text(encoding="utf-8")

    idea_1 = "## Novel Idea 1: DuckDB/dbt local analytics"
    idea_2 = "## Novel Idea 2: Pinot realtime serving"
    assert idea_1 in content
    assert idea_2 in content
    assert content.index(idea_1) < content.index(idea_2)
    for evidence_path in [
        "evidence/10_novel_ideas/idea_1_duckdb_dbt.json",
        "evidence/10_novel_ideas/idea_2_pinot_realtime.json",
        "evidence/10_novel_ideas/run_manifest.json",
        "idea_1_duckdb_dbt_lineage.png",
        "idea_2_pinot_realtime_query.png",
    ]:
        assert evidence_path in content


def test_final_rubric_deliverable_is_indexed_and_row_ordered() -> None:
    repo_root = _repo_root()
    index = (repo_root / "deliverables" / "README.md").read_text(encoding="utf-8")
    content = (repo_root / "deliverables" / "13_mini_coursework_rubric_evidence.md").read_text(encoding="utf-8")

    assert "`13_mini_coursework_rubric_evidence.md`" in index
    assert index.index("`12_novel_ideas.md`") < index.index("`13_mini_coursework_rubric_evidence.md`")
    rows = [int(value) for value in re.findall(r"^\| (\d+) \|", content, re.MULTILINE)]
    assert rows == list(range(2, 47))
    assert "mini_coursework_rubric_manifest.json" in content
