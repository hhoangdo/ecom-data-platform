from __future__ import annotations

import argparse

from vina_bim_shop.lakehouse.spark.executive_mart import export_executive_mart


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Trino-served Iceberg Gold tables into a DuckDB executive mart.")
    parser.add_argument("--duckdb-path", default="data/gold/vina_bim_shop_executive.duckdb")
    parser.add_argument("--evidence-root", default="evidence/05_spark_batch")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = export_executive_mart(
        duckdb_path=args.duckdb_path,
        evidence_root=args.evidence_root,
    )
    print(
        "Exported "
        f"{manifest['table_count']} Gold tables and "
        f"{manifest['total_row_count']} rows to {manifest['duckdb_path']}."
    )


if __name__ == "__main__":
    main()
