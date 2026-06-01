from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

from vina_bim_shop.lakehouse.spark.evidence import capture_evidence
from vina_bim_shop.lakehouse.spark.parity import run_parity_checks
from vina_bim_shop.lakehouse.spark.trino import run_gold_smoke_queries
from vina_bim_shop.lakehouse.spark.window import BatchWindow


RunCommand = Callable[[list[str]], subprocess.CompletedProcess[str]]


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, capture_output=True, encoding="utf-8", errors="replace")


def build_spark_submit_command(window: BatchWindow, *, evidence_root: str | Path) -> list[str]:
    evidence_path = Path(evidence_root)
    container_evidence_root = evidence_path.as_posix()
    if not evidence_path.is_absolute():
        container_evidence_root = f"/workspace/{container_evidence_root}"
    return [
        "docker",
        "compose",
        "exec",
        "-T",
        "spark-master",
        "bash",
        "-lc",
        "cd /workspace && "
        "PYTHONPATH=/workspace/src spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "scripts/spark/job.py "
        + " ".join(window.to_cli_args())
        + f" --evidence-root {container_evidence_root}",
    ]


def build_dbt_build_command() -> list[str]:
    return ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"]


def persist_run_summary(*, evidence_root: str | Path, summary: dict[str, Any]) -> Path:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    destination = evidence_path / "run_batch_summary.json"
    destination.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return destination


def run_batch_pipeline(
    *,
    start_ts: str,
    end_ts: str,
    mode: str,
    evidence_root: str | Path = "evidence/05_spark_batch",
    run_command: RunCommand = _run_command,
) -> dict[str, Any]:
    window = BatchWindow.from_args(start_ts=start_ts, end_ts=end_ts, mode=mode)
    spark_submit = build_spark_submit_command(window, evidence_root=evidence_root)
    spark_result = run_command(spark_submit)

    dbt_command = build_dbt_build_command()
    dbt_result = run_command(dbt_command)
    parity_report = run_parity_checks(evidence_root=evidence_root)
    trino_smoke = run_gold_smoke_queries(evidence_root=evidence_root)
    evidence_manifest = capture_evidence(evidence_root=evidence_root)

    summary = {
        "window": {
            "start_ts": window.start_ts.isoformat().replace("+00:00", "Z"),
            "end_ts": window.end_ts.isoformat().replace("+00:00", "Z"),
            "mode": window.mode,
        },
        "spark_submit_command": spark_submit,
        "dbt_build_command": dbt_command,
        "spark_stdout": spark_result.stdout,
        "dbt_stdout": dbt_result.stdout,
        "parity_success": parity_report["success"],
        "trino_smoke_queries": list(trino_smoke),
        "evidence_artifact_count": len(evidence_manifest["artifacts"]),
    }
    persist_run_summary(evidence_root=evidence_root, summary=summary)
    return summary
