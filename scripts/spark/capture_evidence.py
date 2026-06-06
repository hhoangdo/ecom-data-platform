from __future__ import annotations

import argparse

from vina_bim_shop.lakehouse.spark.evidence import capture_evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture ADR 03 Spark batch evidence.")
    parser.add_argument("--evidence-root", default="evidence/05_spark_batch")
    parser.add_argument("--master-url", default="http://localhost:8085")
    parser.add_argument("--history-url", default="http://localhost:18080")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = capture_evidence(
        evidence_root=args.evidence_root,
        master_url=args.master_url,
        history_url=args.history_url,
    )
    print(f"Captured Spark batch evidence with {len(manifest['artifacts'])} artifacts.")


if __name__ == "__main__":
    main()
