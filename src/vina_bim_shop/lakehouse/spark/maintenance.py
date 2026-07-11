from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable
from typing import Any

from vina_bim_shop.lakehouse.spark.trino import execute_trino_query


TABLE_ALLOWLIST = (
    "silver.stg_orders",
    "silver.stg_order_items",
    "gold.fact_order",
    "gold.fact_order_item",
)
DEFAULT_TARGET_FILE_SIZE_BYTES = 134217728
DEFAULT_MIN_INPUT_FILES = 2
DEFAULT_WARMUP_RUNS = 2
DEFAULT_MEASURED_RUNS = 7

TRINO_BENCHMARK_QUERIES = {
    "silver.stg_orders": """
select count(*) as row_count, round(sum(order_net_amount), 2) as value_sum
from iceberg.silver.stg_orders
""".strip(),
    "silver.stg_order_items": """
select count(*) as row_count, round(sum(net_amount), 2) as value_sum
from iceberg.silver.stg_order_items
""".strip(),
    "gold.fact_order": """
select count(*) as row_count, round(sum(official_paid_revenue), 2) as value_sum
from iceberg.gold.fact_order
""".strip(),
    "gold.fact_order_item": """
select count(*) as row_count, round(sum(estimated_margin), 2) as value_sum
from iceberg.gold.fact_order_item
""".strip(),
}


def validate_tables(table_names: Iterable[str]) -> tuple[str, ...]:
    tables = tuple(table_names)
    if not tables:
        raise ValueError("At least one approved Iceberg table is required.")
    unknown = [table_name for table_name in tables if table_name not in TABLE_ALLOWLIST]
    if unknown:
        raise ValueError(f"Table targets are not approved for compaction: {', '.join(unknown)}")
    if len(set(tables)) != len(tables):
        raise ValueError("Duplicate Iceberg table targets are not allowed.")
    return tables


def rewrite_data_files_sql(
    table_name: str,
    *,
    target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE_BYTES,
    min_input_files: int = DEFAULT_MIN_INPUT_FILES,
) -> str:
    validate_tables([table_name])
    if target_file_size_bytes <= 0:
        raise ValueError("target_file_size_bytes must be positive.")
    if min_input_files < 2:
        raise ValueError("min_input_files must be at least two.")
    return (
        "CALL iceberg.system.rewrite_data_files("
        f"table => '{table_name}', "
        "options => map("
        f"'target-file-size-bytes', '{target_file_size_bytes}', "
        f"'min-input-files', '{min_input_files}'"
        ")"
        ")"
    )


def _aggregate_hash(spark: Any, table_name: str) -> str:
    rows = spark.table(f"iceberg.{table_name}").selectExpr(
        "sha2(to_json(struct(*)), 256) as row_hash"
    ).collect()
    row_hashes = sorted(str(row["row_hash"]) for row in rows)
    return hashlib.sha256("\n".join(row_hashes).encode("utf-8")).hexdigest()


def collect_file_stats(spark: Any, table_name: str) -> dict[str, object]:
    validate_tables([table_name])
    metadata_row = spark.sql(
        f"""
select
  count(*) as file_count,
  coalesce(sum(file_size_in_bytes), 0) as total_bytes,
  coalesce(min(file_size_in_bytes), 0) as min_bytes,
  coalesce(max(file_size_in_bytes), 0) as max_bytes
from iceberg.{table_name}.files
where content = 0
""".strip()
    ).first()
    file_count = int(metadata_row["file_count"])
    total_bytes = int(metadata_row["total_bytes"])
    return {
        "table": table_name,
        "file_count": file_count,
        "total_bytes": total_bytes,
        "min_bytes": int(metadata_row["min_bytes"]),
        "max_bytes": int(metadata_row["max_bytes"]),
        "average_bytes": total_bytes / file_count if file_count else 0.0,
        "row_count": int(spark.table(f"iceberg.{table_name}").count()),
        "aggregate_hash": _aggregate_hash(spark, table_name),
    }


def rewrite_data_files(
    spark: Any,
    table_name: str,
    *,
    target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE_BYTES,
    min_input_files: int = DEFAULT_MIN_INPUT_FILES,
    input_file_count: int | None = None,
) -> dict[str, object]:
    validate_tables([table_name])
    if input_file_count is None:
        input_file_count = int(collect_file_stats(spark, table_name)["file_count"])
    if input_file_count < min_input_files:
        return {
            "table": table_name,
            "status": "skipped_insufficient_files",
            "input_file_count": input_file_count,
            "minimum_input_files": min_input_files,
            "rewritten_data_files_count": 0,
        }

    started = time.perf_counter_ns()
    procedure_rows = [row.asDict(recursive=True) for row in spark.sql(
        rewrite_data_files_sql(
            table_name,
            target_file_size_bytes=target_file_size_bytes,
            min_input_files=min_input_files,
        )
    ).collect()]
    duration_ms = (time.perf_counter_ns() - started) / 1_000_000
    rewritten_data_files_count = sum(
        int(row.get("rewritten_data_files_count", 0)) for row in procedure_rows
    )
    return {
        "table": table_name,
        "status": "rewritten" if rewritten_data_files_count else "skipped_no_eligible_file_groups",
        "input_file_count": input_file_count,
        "minimum_input_files": min_input_files,
        "target_file_size_bytes": target_file_size_bytes,
        "rewritten_data_files_count": rewritten_data_files_count,
        "procedure_rows": procedure_rows,
        "duration_ms": duration_ms,
    }


def compare_invariants(before: dict[str, object], after: dict[str, object]) -> dict[str, object]:
    failures = [
        field_name
        for field_name in ("row_count", "aggregate_hash")
        if before[field_name] != after[field_name]
    ]
    if failures:
        raise ValueError(f"Iceberg invariant mismatch: {', '.join(failures)}")
    return {"success": True, "failures": []}


def _result_hash(rows: list[list[object]]) -> str:
    payload = json.dumps(rows, default=str, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _query_sample(query: str, *, trino_url: str, user: str) -> dict[str, object]:
    started = time.perf_counter_ns()
    result = execute_trino_query(query, trino_url=trino_url, user=user)
    return {
        "client_elapsed_ms": (time.perf_counter_ns() - started) / 1_000_000,
        "trino_elapsed_ms": result.get("stats", {}).get("elapsedTimeMillis"),
        "result_hash": _result_hash(result["rows"]),
        "rows": result["rows"],
    }


def benchmark_trino_queries(
    *,
    tables: Iterable[str],
    trino_url: str,
    user: str,
    warmup_runs: int = DEFAULT_WARMUP_RUNS,
    measured_runs: int = DEFAULT_MEASURED_RUNS,
) -> dict[str, object]:
    if warmup_runs != DEFAULT_WARMUP_RUNS or measured_runs != DEFAULT_MEASURED_RUNS:
        raise ValueError("Storage benchmarks require exactly two warmups and seven measured runs.")

    queries = []
    for table_name in validate_tables(tables):
        query = TRINO_BENCHMARK_QUERIES[table_name]
        warmups = [_query_sample(query, trino_url=trino_url, user=user) for _ in range(warmup_runs)]
        measured = [_query_sample(query, trino_url=trino_url, user=user) for _ in range(measured_runs)]
        result_hashes = {str(sample["result_hash"]) for sample in [*warmups, *measured]}
        if len(result_hashes) != 1:
            raise ValueError(f"Trino query results changed while benchmarking {table_name}.")
        client_samples = sorted(float(sample["client_elapsed_ms"]) for sample in measured)
        queries.append(
            {
                "table": table_name,
                "query": query,
                "warmups": warmups,
                "measured": measured,
                "median_client_elapsed_ms": client_samples[len(client_samples) // 2],
                "result_hash": result_hashes.pop(),
            }
        )
    return {
        "warmup_runs": warmup_runs,
        "measured_runs": measured_runs,
        "queries": queries,
    }
