from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.kafka.consumer_smoke import run_consumer_smoke


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read back ADR 01 source events from Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--evidence-root", default="evidence/03_kafka_ingestion")
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_consumer_smoke(
        bootstrap_servers=args.bootstrap_servers,
        evidence_root=Path(args.evidence_root),
        group_id=args.group_id,
        timeout_seconds=args.timeout_seconds,
    )
    print(f"Consumed smoke messages from {len(summary['topics_read'])} source topics.")


if __name__ == "__main__":
    main()
