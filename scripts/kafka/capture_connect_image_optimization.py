from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACTS = [
    "kafka_connect_image_baseline.json",
    "kafka_connect_image_optimized.json",
    "kafka_connect_image_history_baseline.txt",
    "kafka_connect_image_history_optimized.txt",
    "kafka_connect_image_comparison.json",
    "kafka_connect_image_comparison.md",
]


def _run_command(command: list[str]) -> str:
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    return result.stdout


def _mib(size_bytes: int) -> float:
    return round(size_bytes / 1048576, 2)


def _inspect_image(image_tag: str) -> dict[str, Any]:
    output = _run_command(["docker", "image", "inspect", "--format", "{{json .}}", image_tag])
    image = json.loads(output)
    size_bytes = image["Size"]
    if not isinstance(size_bytes, int) or size_bytes <= 0:
        raise ValueError(f"Image {image_tag} did not report a positive byte size.")
    image_id = image["Id"]
    if not isinstance(image_id, str) or not image_id:
        raise ValueError(f"Image {image_tag} did not report an image ID.")
    return {
        "image_id": image_id,
        "size_bytes": size_bytes,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _comparison_markdown(comparison: dict[str, int | float]) -> str:
    return "\n".join(
        [
            "# Kafka Connect Image Optimization",
            "",
            "The baseline and optimized images were built on the same Docker engine with `--no-cache`.",
            "",
            "| Metric | Value |",
            "|---|---:|",
            f"| Baseline size | {comparison['baseline_size_bytes']} bytes ({_mib(comparison['baseline_size_bytes'])} MiB) |",
            f"| Optimized size | {comparison['optimized_size_bytes']} bytes ({_mib(comparison['optimized_size_bytes'])} MiB) |",
            f"| Reduction | {comparison['reduction_bytes']} bytes ({comparison['reduction_mib']} MiB) |",
            f"| Reduction percentage | {comparison['reduction_percent']}% |",
            "",
            "The optimized image installs the S3 connector in a builder stage and copies only the plugin directory into the pinned Kafka Connect runtime stage.",
            "",
            "See `kafka_connect_plugin_smoke.json` and `kafka_connect_bronze_sink_response.json` for runtime proof.",
            "",
        ]
    )


def write_image_optimization_evidence(
    *,
    baseline_image: str,
    optimized_image: str,
    evidence_root: Path,
    captured_at: str | None = None,
) -> dict[str, int | float]:
    baseline = _inspect_image(baseline_image)
    optimized = _inspect_image(optimized_image)
    if optimized["size_bytes"] >= baseline["size_bytes"]:
        raise ValueError("Optimized image must be smaller than the baseline image.")

    captured_at = captured_at or datetime.now(timezone.utc).isoformat()
    evidence_root.mkdir(parents=True, exist_ok=True)
    comparison: dict[str, int | float] = {
        "baseline_size_bytes": baseline["size_bytes"],
        "optimized_size_bytes": optimized["size_bytes"],
        "reduction_bytes": baseline["size_bytes"] - optimized["size_bytes"],
        "reduction_mib": _mib(baseline["size_bytes"] - optimized["size_bytes"]),
        "reduction_percent": round(
            (baseline["size_bytes"] - optimized["size_bytes"]) / baseline["size_bytes"] * 100,
            2,
        ),
    }
    baseline_record = {
        "captured_at": captured_at,
        "image_id": baseline["image_id"],
        "image_tag": baseline_image,
        "size_bytes": baseline["size_bytes"],
        "size_mib": _mib(baseline["size_bytes"]),
    }
    optimized_record = {
        "captured_at": captured_at,
        "image_id": optimized["image_id"],
        "image_tag": optimized_image,
        "size_bytes": optimized["size_bytes"],
        "size_mib": _mib(optimized["size_bytes"]),
    }

    _write_json(evidence_root / "kafka_connect_image_baseline.json", baseline_record)
    _write_json(evidence_root / "kafka_connect_image_optimized.json", optimized_record)
    (evidence_root / "kafka_connect_image_history_baseline.txt").write_text(
        _run_command(["docker", "history", "--no-trunc", baseline_image]),
        encoding="utf-8",
    )
    (evidence_root / "kafka_connect_image_history_optimized.txt").write_text(
        _run_command(["docker", "history", "--no-trunc", optimized_image]),
        encoding="utf-8",
    )
    _write_json(evidence_root / "kafka_connect_image_comparison.json", comparison)
    (evidence_root / "kafka_connect_image_comparison.md").write_text(
        _comparison_markdown(comparison),
        encoding="utf-8",
    )
    _write_json(
        evidence_root / "run_manifest.json",
        {
            "artifacts": ARTIFACTS,
            "captured_at": captured_at,
        },
    )
    return comparison


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture Kafka Connect image optimization evidence.")
    parser.add_argument("--baseline-image", required=True)
    parser.add_argument("--optimized-image", required=True)
    parser.add_argument("--evidence-root", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    comparison = write_image_optimization_evidence(
        baseline_image=args.baseline_image,
        optimized_image=args.optimized_image,
        evidence_root=Path(args.evidence_root),
    )
    print(
        "Kafka Connect image reduced by "
        f"{comparison['reduction_bytes']} bytes ({comparison['reduction_percent']}%)."
    )


if __name__ == "__main__":
    main()
