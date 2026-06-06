import subprocess
import sys
from pathlib import Path


def test_generator_cli_runs_smoke_full_mode(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]

    completed = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "generate" / "run_generator.py"),
            "--config",
            str(repo_root / "configs" / "generator" / "base.yaml"),
            "--scale",
            "smoke",
            "--mode",
            "full",
            "--clean",
            "--seed",
            "77",
            "--raw-root",
            str(tmp_path / "raw"),
            "--evidence-root",
            str(tmp_path / "evidence"),
        ],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Generated Section 01 data" in completed.stdout
    assert (tmp_path / "raw" / "orders").exists()
    assert not (tmp_path / "raw" / "stream_events").exists()
    assert (tmp_path / "raw" / "kafka_topics" / "commerce_events" / "events.jsonl").is_file()
    assert (tmp_path / "evidence" / "run_manifest.json").is_file()
