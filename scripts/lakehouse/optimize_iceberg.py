from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from vina_bim_shop.lakehouse.spark.maintenance import (
    DEFAULT_MIN_INPUT_FILES,
    DEFAULT_TARGET_FILE_SIZE_BYTES,
    TABLE_ALLOWLIST,
    benchmark_trino_queries,
    collect_file_stats,
    compare_invariants,
    rewrite_data_files,
    validate_tables,
)


def build_spark_session() -> Any:
    from vina_bim_shop.lakehouse.spark.job import build_spark_session as _build_spark_session

    return _build_spark_session()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture Iceberg compaction evidence for approved tables.")
    parser.add_argument("--tables", nargs="+", required=True, choices=TABLE_ALLOWLIST)
    parser.add_argument("--target-file-size-bytes", type=int, default=DEFAULT_TARGET_FILE_SIZE_BYTES)
    parser.add_argument("--min-input-files", type=int, default=DEFAULT_MIN_INPUT_FILES)
    parser.add_argument("--evidence-root", default="evidence/04_lakehouse/optimization")
    parser.add_argument("--trino-url", default=os.getenv("VBS_TRINO_URL", "http://trino:8080"))
    parser.add_argument("--trino-user", default=os.getenv("VBS_TRINO_USER", "vina_analyst"))
    return parser.parse_args()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_file_stats(path: Path, rows: list[dict[str, object]]) -> None:
    fields = [
        "table",
        "file_count",
        "total_bytes",
        "min_bytes",
        "max_bytes",
        "average_bytes",
        "row_count",
        "aggregate_hash",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())


def _write_report(
    path: Path,
    *,
    tables: tuple[str, ...],
    rewrites: list[dict[str, object]],
    benchmark: dict[str, object],
) -> None:
    lines = [
        "# Iceberg Storage Optimization Report",
        "",
        "## Method",
        "",
        "- Captured Iceberg data-file metadata and logical invariants before every rewrite.",
        "- Rewrote only approved tables with a 128 MiB target and at least two input files.",
        "- Used two warmups and seven measured Trino executions per table before and after compaction.",
        "",
        "## Rewrite Results",
        "",
        "| Table | Status | Input files | Rewritten files | Duration ms |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for rewrite in rewrites:
        lines.append(
            "| {table} | {status} | {input_file_count} | {rewritten_data_files_count} | {duration} |".format(
                table=rewrite["table"],
                status=rewrite["status"],
                input_file_count=rewrite["input_file_count"],
                rewritten_data_files_count=rewrite["rewritten_data_files_count"],
                duration=round(float(rewrite.get("duration_ms", 0.0)), 3),
            )
        )
    lines.extend(
        [
            "",
            "## Timing Interpretation",
            "",
            "The timing samples are observed local measurements. They do not establish a guaranteed performance improvement.",
        ]
    )
    for phase in ("before", "after"):
        for query in benchmark[phase]["queries"]:
            lines.append(
                f"- {phase} `{query['table']}` median client time: "
                f"{float(query['median_client_elapsed_ms']):.3f} ms."
            )
    lines.extend(["", f"Tables evaluated: {', '.join(tables)}."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_blocked_report(
    path: Path,
    *,
    tables: tuple[str, ...],
    rewrites: list[dict[str, object]],
) -> None:
    lines = [
        "# Iceberg Storage Optimization Report",
        "",
        "## Blocked Physical Layout",
        "",
        "No approved table had an eligible partition-level group of at least two data files.",
        "No rewrite was forced and no after-rewrite Trino timing was collected.",
        "",
        "| Table | Status | Input files | Rewritten files |",
        "| --- | --- | ---: | ---: |",
    ]
    for rewrite in rewrites:
        lines.append(
            "| {table} | {status} | {input_file_count} | {rewritten_data_files_count} |".format(
                table=rewrite["table"],
                status=rewrite["status"],
                input_file_count=rewrite["input_file_count"],
                rewritten_data_files_count=rewrite["rewritten_data_files_count"],
            )
        )
    lines.extend(["", f"Tables evaluated: {', '.join(tables)}."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    tables = validate_tables(args.tables)
    evidence_root = Path(args.evidence_root)
    evidence_root.mkdir(parents=True, exist_ok=True)
    spark = build_spark_session()
    try:
        before = [collect_file_stats(spark, table_name) for table_name in tables]
        _write_file_stats(evidence_root / "before_file_stats.csv", before)
        before_benchmark = benchmark_trino_queries(
            tables=tables,
            trino_url=args.trino_url,
            user=args.trino_user,
        )
        rewrites = [
            rewrite_data_files(
                spark,
                table_name,
                target_file_size_bytes=args.target_file_size_bytes,
                min_input_files=args.min_input_files,
                input_file_count=int(before[index]["file_count"]),
            )
            for index, table_name in enumerate(tables)
        ]
        after = [collect_file_stats(spark, table_name) for table_name in tables]
        _write_file_stats(evidence_root / "after_file_stats.csv", after)
        invariants = [
            {
                "table": table_name,
                **compare_invariants(before[index], after[index]),
            }
            for index, table_name in enumerate(tables)
        ]
        rewritten = [rewrite for rewrite in rewrites if rewrite["status"] == "rewritten"]
        if not rewritten or sum(int(rewrite["rewritten_data_files_count"]) for rewrite in rewritten) < 1:
            blocker = "No approved table had an eligible partition-level group with at least two data files."
            _write_json(
                evidence_root / "compaction_results.json",
                {
                    "success": False,
                    "status": "blocked_no_eligible_file_groups",
                    "reason": blocker,
                    "tables": list(tables),
                    "target_file_size_bytes": args.target_file_size_bytes,
                    "min_input_files": args.min_input_files,
                    "rewrites": rewrites,
                    "invariants": invariants,
                },
            )
            _write_json(
                evidence_root / "query_benchmark.json",
                {
                    "status": "blocked_no_eligible_file_groups",
                    "reason": blocker,
                    "before": before_benchmark,
                    "after": None,
                },
            )
            _write_blocked_report(evidence_root / "report.md", tables=tables, rewrites=rewrites)
            _write_json(
                evidence_root / "run_manifest.json",
                {
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                    "status": "blocked_no_eligible_file_groups",
                    "command": sys.argv,
                    "tables": list(tables),
                    "artifacts": [
                        "before_file_stats.csv",
                        "after_file_stats.csv",
                        "compaction_results.json",
                        "query_benchmark.json",
                        "report.md",
                        "run_manifest.json",
                    ],
                },
            )
            raise RuntimeError("Iceberg compaction did not rewrite any data files.")
        for index, rewrite in enumerate(rewrites):
            if rewrite["status"] == "rewritten" and int(after[index]["file_count"]) > int(before[index]["file_count"]):
                raise RuntimeError(f"Compaction increased data-file count for {rewrite['table']}.")
        after_benchmark = benchmark_trino_queries(
            tables=tables,
            trino_url=args.trino_url,
            user=args.trino_user,
        )
        benchmark = {"before": before_benchmark, "after": after_benchmark}
        results = {
            "success": True,
            "tables": list(tables),
            "target_file_size_bytes": args.target_file_size_bytes,
            "min_input_files": args.min_input_files,
            "rewrites": rewrites,
            "invariants": invariants,
        }
        _write_json(evidence_root / "compaction_results.json", results)
        _write_json(evidence_root / "query_benchmark.json", benchmark)
        _write_report(evidence_root / "report.md", tables=tables, rewrites=rewrites, benchmark=benchmark)
        _write_json(
            evidence_root / "run_manifest.json",
            {
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "command": sys.argv,
                "tables": list(tables),
                "artifacts": [
                    "before_file_stats.csv",
                    "after_file_stats.csv",
                    "compaction_results.json",
                    "query_benchmark.json",
                    "report.md",
                    "run_manifest.json",
                ],
            },
        )
    finally:
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()


if __name__ == "__main__":
    main()
