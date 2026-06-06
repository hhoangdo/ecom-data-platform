from pathlib import Path


def test_pinot_runbook_preserves_provisional_truth_policy() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    deliverable = (repo_root / "deliverables" / "07_pinot_serving.md").read_text(encoding="utf-8")

    assert "Pinot is fresh and provisional" in deliverable
    assert "Spark Gold through Trino is canonical" in deliverable
    assert "Do not ingest raw source topics directly into Pinot for v1." in deliverable


def test_pinot_session_does_not_add_airflow_assets() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dag_path = repo_root / "infra" / "orchestration" / "airflow" / "dags" / "pinot_bootstrap.py"

    assert dag_path.is_file()
    dag_source = dag_path.read_text(encoding="utf-8")
    assert "pinot_bootstrap" in dag_source
    assert "vina_bim_shop.flink" not in dag_source
    assert "scripts/flink/run_" not in dag_source
