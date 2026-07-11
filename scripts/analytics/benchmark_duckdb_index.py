from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import time
from pathlib import Path
from typing import Any


CANONICAL_DATABASE = Path("data/gold/vina_bim_shop.duckdb")
INDEX_NAME = "idx_benchmark_fact_order_order_id"
INDEX_COLUMN = "order_id"
WARMUP_RUNS = 2
MEASURED_RUNS = 7


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_hash(rows: list[tuple[Any, ...]]) -> str:
    payload = json.dumps(rows, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _explain(connection: Any, order_id: str) -> str:
    rows = connection.execute(
        f"EXPLAIN SELECT * FROM benchmark_fact_order WHERE {INDEX_COLUMN} = {_quote_string(order_id)}"
    ).fetchall()
    return "\n".join("\t".join(str(value) for value in row) for row in rows) + "\n"


def _run_variant(connection: Any, order_id: str) -> dict[str, object]:
    query = f"SELECT * FROM benchmark_fact_order WHERE {INDEX_COLUMN} = ?"
    warmups = []
    for _ in range(WARMUP_RUNS):
        started = time.perf_counter_ns()
        rows = connection.execute(query, [order_id]).fetchall()
        warmups.append((time.perf_counter_ns() - started) / 1_000_000)

    measured = []
    result_hashes = set()
    result_rows: list[tuple[Any, ...]] = []
    for _ in range(MEASURED_RUNS):
        started = time.perf_counter_ns()
        rows = connection.execute(query, [order_id]).fetchall()
        measured.append((time.perf_counter_ns() - started) / 1_000_000)
        result_rows = rows
        result_hashes.add(_json_hash(rows))
    if len(result_hashes) != 1:
        raise ValueError("Benchmark query results changed across repeated executions.")
    return {
        "warmups_ms": warmups,
        "measured_ms": measured,
        "median_ms": statistics.median(measured),
        "rows": json.loads(json.dumps(result_rows, default=str)),
        "result_hash": result_hashes.pop(),
    }


def _index_metadata(connection: Any) -> list[dict[str, object]]:
    cursor = connection.execute("select * from duckdb_indexes() where table_name = 'benchmark_fact_order'")
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_report(path: Path, result: dict[str, object]) -> None:
    timings = result["timings"]
    path.write_text(
        "\n".join(
            [
                "# DuckDB ART Index Benchmark",
                "",
                "The benchmark database is an isolated copy of `gold.fact_order`; the canonical dbt database was read-only.",
                "",
                "| Variant | Median ms | Result hash |",
                "| --- | ---: | --- |",
                f"| Baseline | {float(timings['baseline']['median_ms']):.3f} | {result['result_hashes']['baseline']} |",
                f"| Indexed | {float(timings['indexed']['median_ms']):.3f} | {result['result_hashes']['indexed']} |",
                "",
                f"Explain plan changed: {result['explain_plan_changed']}.",
                "Local timings are observed evidence and do not promise an index speedup.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def benchmark_index(
    source_db: str | Path,
    benchmark_db: str | Path,
    evidence_root: str | Path,
) -> dict[str, object]:
    import duckdb

    source = Path(source_db).resolve()
    benchmark = Path(benchmark_db).resolve()
    canonical = CANONICAL_DATABASE.resolve()
    if source == benchmark or benchmark == canonical:
        raise ValueError("Source and benchmark databases must differ; the canonical database cannot be a benchmark target.")
    if not source.is_file():
        raise FileNotFoundError(f"Canonical DuckDB source database does not exist: {source}")

    benchmark.parent.mkdir(parents=True, exist_ok=True)
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    if benchmark.exists():
        benchmark.unlink()
    wal_path = benchmark.with_name(f"{benchmark.name}.wal")
    if wal_path.exists():
        wal_path.unlink()

    source_sha256_before = _sha256(source)
    source_sql = _quote_string(str(source))
    with duckdb.connect(str(benchmark)) as connection:
        connection.execute(f"ATTACH {source_sql} AS source_db (READ_ONLY)")
        connection.execute("CREATE TABLE benchmark_fact_order AS SELECT * FROM source_db.gold.fact_order")
        connection.execute("DETACH source_db")
        order_id = connection.execute("SELECT min(order_id) FROM benchmark_fact_order").fetchone()[0]
        if order_id is None:
            raise ValueError("gold.fact_order contains no order_id to benchmark.")

        baseline_explain = _explain(connection, str(order_id))
        baseline = _run_variant(connection, str(order_id))
        connection.execute(f"CREATE INDEX {INDEX_NAME} ON benchmark_fact_order ({INDEX_COLUMN})")
        indexed_explain = _explain(connection, str(order_id))
        indexed = _run_variant(connection, str(order_id))
        index_metadata = _index_metadata(connection)

    source_sha256_after = _sha256(source)
    if source_sha256_before != source_sha256_after:
        raise RuntimeError("The canonical DuckDB source changed during the benchmark.")
    if baseline["result_hash"] != indexed["result_hash"]:
        raise RuntimeError("Indexed DuckDB query result differs from its baseline.")

    (evidence_path / "baseline_explain.txt").write_text(baseline_explain, encoding="utf-8")
    (evidence_path / "indexed_explain.txt").write_text(indexed_explain, encoding="utf-8")
    result = {
        "source_db": str(source),
        "benchmark_db": str(benchmark),
        "source_sha256_before": source_sha256_before,
        "source_sha256_after": source_sha256_after,
        "benchmark_table": "benchmark_fact_order",
        "index_name": INDEX_NAME,
        "index_column": INDEX_COLUMN,
        "order_id": str(order_id),
        "timing_policy": {"warmup_runs": WARMUP_RUNS, "measured_runs": MEASURED_RUNS},
        "timings": {"baseline": baseline, "indexed": indexed},
        "result_hashes": {"baseline": baseline["result_hash"], "indexed": indexed["result_hash"]},
        "index_metadata": index_metadata,
        "explain_plan_changed": baseline_explain != indexed_explain,
    }
    _write_json(evidence_path / "index_benchmark.json", result)
    _write_report(evidence_path / "report.md", result)
    _write_json(
        evidence_path / "run_manifest.json",
        {
            "source_db": str(source),
            "benchmark_db": str(benchmark),
            "source_sha256": source_sha256_before,
            "index_name": INDEX_NAME,
            "index_column": INDEX_COLUMN,
            "artifacts": [
                "baseline_explain.txt",
                "indexed_explain.txt",
                "index_benchmark.json",
                "report.md",
                "run_manifest.json",
            ],
        },
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark a DuckDB ART index in an isolated database.")
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--benchmark-db", required=True)
    parser.add_argument("--evidence-root", default="evidence/10_duckdb_dbt_local_analytics/index_optimization")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = benchmark_index(args.source_db, args.benchmark_db, args.evidence_root)
    print(
        "Benchmarked "
        f"{result['index_name']} on {result['benchmark_table']} using order_id={result['order_id']}."
    )


if __name__ == "__main__":
    main()
