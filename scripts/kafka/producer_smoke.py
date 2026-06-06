from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.kafka.producer_smoke import run_producer_smoke


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish deterministic ADR 01 source events to Kafka.")
    parser.add_argument("--config", default="configs/generator/base.yaml")
    parser.add_argument("--raw-root", default="data/raw")
    parser.add_argument("--evidence-root", default="evidence/03_kafka_ingestion")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_producer_smoke(
        config_path=Path(args.config),
        raw_root=Path(args.raw_root),
        evidence_root=Path(args.evidence_root),
        bootstrap_servers=args.bootstrap_servers,
    )
    print(f"Published smoke Kafka events for {len(summary['source_topics'])} source topics.")


if __name__ == "__main__":
    main()
