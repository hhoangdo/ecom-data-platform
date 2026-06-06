from pathlib import Path


def test_flink_runtime_assets_exist_and_are_pinned() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    dockerfile = (repo_root / "infra" / "flink" / "Dockerfile").read_text(encoding="utf-8")
    submit_script = (repo_root / "infra" / "flink" / "bin" / "submit-jobs.sh").read_text(encoding="utf-8")
    runtime_limit_script = (repo_root / "infra" / "flink" / "bin" / "run-with-time-limit.sh").read_text(encoding="utf-8")
    deliverable = (repo_root / "deliverables" / "06_flink_streaming.md").read_text(encoding="utf-8")

    assert "FROM flink:1.19.2-scala_2.12-java17" in dockerfile
    assert "apache-flink==1.19.2" in dockerfile
    assert "flink-sql-connector-kafka-3.2.0-1.19.jar" in dockerfile
    assert "s3-fs-hadoop-1.19.2.jar" in dockerfile
    assert "run-with-time-limit.sh" in dockerfile
    assert "run_commerce_metrics_job.py" in submit_script
    assert "run_ops_alerts_job.py" in submit_script
    assert "VBS_FLINK_MAX_RUNTIME_MINUTES" in runtime_limit_script
    assert "VBS_FLINK_DISABLE_AUTO_STOP" in runtime_limit_script
    assert "Airflow does not monitor or restart Flink in v1." in deliverable


def test_streaming_scripts_exist_for_job_entrypoints() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    assert (repo_root / "scripts" / "flink" / "run_commerce_metrics_job.py").is_file()
    assert (repo_root / "scripts" / "flink" / "run_ops_alerts_job.py").is_file()
