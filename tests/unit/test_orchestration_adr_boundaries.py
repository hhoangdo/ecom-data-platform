from pathlib import Path


def test_orchestration_runbook_documents_adr06_failure_policies() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    runbook = (repo_root / "deliverables" / "08_airflow_gx_orchestration.md").read_text(encoding="utf-8")

    assert "Airflow must not monitor or restart Flink in v1." in runbook
    assert "Bronze warns and quarantines" in runbook
    assert "Silver/Gold failures block the DAG" in runbook
    assert "Pinot is fresh and provisional" in runbook
    assert "GX Data Docs" in runbook


def test_orchestration_evidence_artifacts_exist() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    assert (repo_root / "evidence" / "08_airflow_gx" / "gx_data_docs" / "index.html").is_file()
