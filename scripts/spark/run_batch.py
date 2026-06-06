from __future__ import annotations

import argparse

from vina_bim_shop.lakehouse.spark.runner import run_batch_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Spark batch Iceberg pipeline.")
    parser.add_argument("--start-ts", required=True)
    parser.add_argument("--end-ts", required=True)
    parser.add_argument("--mode", required=True, choices=["hourly", "backfill"])
    parser.add_argument("--evidence-root", default="evidence/05_spark_batch")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_batch_pipeline(
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        mode=args.mode,
        evidence_root=args.evidence_root,
    )
    window = summary["window"]
    print(
        "Spark batch completed for "
        f"{window['start_ts']} -> {window['end_ts']} "
        f"({window['mode']})."
    )


if __name__ == "__main__":
    main()
