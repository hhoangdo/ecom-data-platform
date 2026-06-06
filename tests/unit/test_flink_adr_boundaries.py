from pathlib import Path


def test_streaming_session_does_not_add_airflow_flink_monitoring_assets() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    airflow_root = repo_root / "airflow"

    if airflow_root.exists():
        forbidden_matches = list(airflow_root.rglob("*flink*"))
        assert not forbidden_matches

    deliverable = (repo_root / "deliverables" / "06_flink_streaming.md").read_text(encoding="utf-8")
    assert "Airflow must not monitor or restart Flink in v1." in deliverable
