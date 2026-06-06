from __future__ import annotations

import json
import os
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from vina_bim_shop.lakehouse.spark.constants import REQUIRED_GOLD_TABLES
from vina_bim_shop.lakehouse.spark.trino import execute_trino_query


ExecuteQuery = Callable[..., dict[str, Any]]


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _duckdb_type(trino_type: str) -> str:
    normalized = trino_type.strip().lower()
    if normalized in {"boolean"}:
        return "BOOLEAN"
    if normalized in {"tinyint", "smallint", "integer", "int"}:
        return "INTEGER"
    if normalized in {"bigint"}:
        return "BIGINT"
    if normalized in {"real", "double"}:
        return "DOUBLE"
    if normalized.startswith("decimal("):
        return normalized.upper().replace(" ", "")
    if normalized in {"date"}:
        return "DATE"
    if normalized.startswith("timestamp") and "with time zone" in normalized:
        return "TIMESTAMPTZ"
    if normalized.startswith("timestamp"):
        return "TIMESTAMP"
    if normalized.startswith("time"):
        return "TIME"
    return "VARCHAR"


def _table_query(table_name: str) -> str:
    return f"select * from iceberg.gold.{table_name}"


def _create_gold_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    column_metadata: Sequence[dict[str, Any]],
) -> None:
    if not column_metadata:
        raise ValueError(f"Trino returned no column metadata for iceberg.gold.{table_name}.")
    columns_sql = ", ".join(
        f"{_quote_identifier(str(column['name']))} {_duckdb_type(str(column.get('type', 'varchar')))}"
        for column in column_metadata
    )
    connection.execute(f"create table gold.{_quote_identifier(table_name)} ({columns_sql})")


def _insert_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    row_count: int,
    rows: Sequence[Sequence[Any]],
) -> None:
    if row_count == 0:
        return
    placeholders = ", ".join("?" for _ in range(len(rows[0])))
    connection.executemany(f"insert into gold.{_quote_identifier(table_name)} values ({placeholders})", rows)


def _write_manifest_tables(
    *,
    connection: duckdb.DuckDBPyConnection,
    exported_at: str,
    duckdb_path: Path,
    table_manifest: list[dict[str, Any]],
) -> None:
    total_row_count = sum(int(row["row_count"]) for row in table_manifest)
    connection.execute("create schema mart_metadata")
    connection.execute(
        """
        create table mart_metadata.export_manifest (
            key varchar primary key,
            value varchar
        )
        """
    )
    connection.execute(
        """
        create table mart_metadata.table_manifest (
            table_name varchar,
            source_relation varchar,
            row_count bigint,
            column_count bigint,
            exported_at varchar
        )
        """
    )
    connection.executemany(
        "insert into mart_metadata.export_manifest values (?, ?)",
        [
            ("exported_at", exported_at),
            ("source", "trino://iceberg.gold"),
            ("duckdb_path", str(duckdb_path)),
            ("table_count", str(len(table_manifest))),
            ("total_row_count", str(total_row_count)),
        ],
    )
    connection.executemany(
        "insert into mart_metadata.table_manifest values (?, ?, ?, ?, ?)",
        [
            (
                row["table_name"],
                row["source_relation"],
                row["row_count"],
                row["column_count"],
                exported_at,
            )
            for row in table_manifest
        ],
    )


def _write_evidence(
    *,
    evidence_root: str | Path,
    manifest: dict[str, Any],
) -> None:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "executive_mart_export_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    table_lines = [
        f"| `{row['table_name']}` | `{row['source_relation']}` | {row['row_count']} | {row['column_count']} |"
        for row in manifest["tables"]
    ]
    (evidence_path / "executive_mart_export_report.md").write_text(
        "\n".join(
            [
                "# DuckDB Executive Mart Export Report",
                "",
                f"- Exported at: `{manifest['exported_at']}`",
                f"- DuckDB path: `{manifest['duckdb_path']}`",
                f"- Source: `{manifest['source']}`",
                f"- Tables: {manifest['table_count']}",
                f"- Rows: {manifest['total_row_count']}",
                "",
                "| Table | Source relation | Rows | Columns |",
                "| --- | --- | ---: | ---: |",
                *table_lines,
                "",
            ]
        ),
        encoding="utf-8",
    )


def export_executive_mart(
    *,
    duckdb_path: str | Path = "data/gold/vina_bim_shop_executive.duckdb",
    evidence_root: str | Path = "evidence/05_spark_batch",
    trino_url: str = os.getenv("VBS_TRINO_URL", "http://localhost:8080"),
    user: str = os.getenv("VBS_TRINO_USER", "vina_analyst"),
    table_names: Sequence[str] = REQUIRED_GOLD_TABLES,
    execute_query: ExecuteQuery = execute_trino_query,
    exported_at: str | None = None,
) -> dict[str, Any]:
    destination = Path(duckdb_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_destination = destination.with_name(f"{destination.stem}.tmp{destination.suffix}")
    temp_destination.unlink(missing_ok=True)

    exported_at_value = exported_at or datetime.now(timezone.utc).isoformat()
    table_manifest: list[dict[str, Any]] = []
    connection = duckdb.connect(str(temp_destination))
    try:
        connection.execute("create schema gold")
        for table_name in table_names:
            source_relation = f"iceberg.gold.{table_name}"
            result = execute_query(_table_query(table_name), trino_url=trino_url, user=user)
            column_metadata = result.get("column_metadata") or [
                {"name": column_name, "type": "varchar"}
                for column_name in result.get("columns", [])
            ]
            rows = result.get("rows", []) or []
            _create_gold_table(
                connection=connection,
                table_name=table_name,
                column_metadata=column_metadata,
            )
            _insert_rows(
                connection=connection,
                table_name=table_name,
                row_count=len(rows),
                rows=rows,
            )
            table_manifest.append(
                {
                    "table_name": table_name,
                    "source_relation": source_relation,
                    "row_count": len(rows),
                    "column_count": len(column_metadata),
                }
            )

        _write_manifest_tables(
            connection=connection,
            exported_at=exported_at_value,
            duckdb_path=destination,
            table_manifest=table_manifest,
        )
    finally:
        connection.close()

    os.replace(temp_destination, destination)
    manifest = {
        "exported_at": exported_at_value,
        "source": "trino://iceberg.gold",
        "duckdb_path": str(destination),
        "table_count": len(table_manifest),
        "total_row_count": sum(int(row["row_count"]) for row in table_manifest),
        "tables": table_manifest,
    }
    _write_evidence(evidence_root=evidence_root, manifest=manifest)
    return manifest
