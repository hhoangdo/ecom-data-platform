import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_capture_module():
    script_path = _repo_root() / "scripts" / "kafka" / "capture_connect_image_optimization.py"
    assert script_path.is_file(), "Expected Kafka Connect image evidence capture script."
    spec = importlib.util.spec_from_file_location("capture_connect_image_optimization", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_write_image_optimization_evidence_enforces_reduction_and_schema(monkeypatch, tmp_path: Path) -> None:
    module = _load_capture_module()
    baseline_image = "vina-bim-shop/kafka-connect:7.8.3-s3-baseline"
    optimized_image = "vina-bim-shop/kafka-connect:7.8.3-s3"
    responses = {
        f"docker image inspect --format {{{{json .}}}} {baseline_image}": json.dumps(
            {"Id": "sha256:baseline", "Size": 209715200}
        ),
        f"docker image inspect --format {{{{json .}}}} {optimized_image}": json.dumps(
            {"Id": "sha256:optimized", "Size": 157286400}
        ),
        f"docker history --no-trunc {baseline_image}": "BASELINE HISTORY\n",
        f"docker history --no-trunc {optimized_image}": "OPTIMIZED HISTORY\n",
    }
    calls = []

    def fake_run(command, *, check, text, capture_output):
        calls.append((command, check, text, capture_output))
        return SimpleNamespace(stdout=responses[" ".join(command)])

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    result = module.write_image_optimization_evidence(
        baseline_image=baseline_image,
        optimized_image=optimized_image,
        evidence_root=tmp_path,
        captured_at="2026-07-10T00:00:00+00:00",
    )

    assert all(check and text and capture_output for _, check, text, capture_output in calls)
    assert result == {
        "baseline_size_bytes": 209715200,
        "optimized_size_bytes": 157286400,
        "reduction_bytes": 52428800,
        "reduction_mib": 50.0,
        "reduction_percent": 25.0,
    }
    assert json.loads((tmp_path / "kafka_connect_image_baseline.json").read_text(encoding="utf-8")) == {
        "captured_at": "2026-07-10T00:00:00+00:00",
        "image_id": "sha256:baseline",
        "image_tag": baseline_image,
        "size_bytes": 209715200,
        "size_mib": 200.0,
    }
    assert json.loads((tmp_path / "kafka_connect_image_optimized.json").read_text(encoding="utf-8")) == {
        "captured_at": "2026-07-10T00:00:00+00:00",
        "image_id": "sha256:optimized",
        "image_tag": optimized_image,
        "size_bytes": 157286400,
        "size_mib": 150.0,
    }
    assert json.loads((tmp_path / "kafka_connect_image_comparison.json").read_text(encoding="utf-8")) == result
    comparison_markdown = (tmp_path / "kafka_connect_image_comparison.md").read_text(encoding="utf-8")
    assert "| Baseline size | 209715200 bytes (200.0 MiB) |" in comparison_markdown
    assert "| Optimized size | 157286400 bytes (150.0 MiB) |" in comparison_markdown
    assert "| Reduction | 52428800 bytes (50.0 MiB) |" in comparison_markdown
    assert "| Reduction percentage | 25.0% |" in comparison_markdown
    assert (tmp_path / "kafka_connect_image_history_baseline.txt").read_text(encoding="utf-8") == "BASELINE HISTORY\n"
    assert (tmp_path / "kafka_connect_image_history_optimized.txt").read_text(encoding="utf-8") == "OPTIMIZED HISTORY\n"
    assert json.loads((tmp_path / "run_manifest.json").read_text(encoding="utf-8"))["artifacts"] == [
        "kafka_connect_image_baseline.json",
        "kafka_connect_image_optimized.json",
        "kafka_connect_image_history_baseline.txt",
        "kafka_connect_image_history_optimized.txt",
        "kafka_connect_image_comparison.json",
        "kafka_connect_image_comparison.md",
    ]

    responses[f"docker image inspect --format {{{{json .}}}} {optimized_image}"] = json.dumps(
        {"Id": "sha256:not-smaller", "Size": 209715200}
    )
    with pytest.raises(ValueError, match="must be smaller"):
        module.write_image_optimization_evidence(
            baseline_image=baseline_image,
            optimized_image=optimized_image,
            evidence_root=tmp_path / "not-smaller",
            captured_at="2026-07-10T00:00:00+00:00",
        )
