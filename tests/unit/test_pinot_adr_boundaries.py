from pathlib import Path


def test_pinot_runbook_preserves_provisional_truth_policy() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    deliverable = (repo_root / "deliverables" / "07_pinot_serving.md").read_text(encoding="utf-8")

    assert "Pinot is fresh and provisional" in deliverable
    assert "Spark Gold through Trino is canonical" in deliverable
    assert "Do not ingest raw source topics directly into Pinot for v1." in deliverable


def test_pinot_session_does_not_add_airflow_assets() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    airflow_root = repo_root / "airflow"

    if airflow_root.exists():
        forbidden_matches = list(airflow_root.rglob("*pinot*"))
        assert not forbidden_matches
