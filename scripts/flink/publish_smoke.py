from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.flink.smoke import run_streaming_smoke_publish


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish deterministic ADR 04 Flink smoke events to Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--evidence-root", default="evidence/06_flink_streaming")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_streaming_smoke_publish(
        bootstrap_servers=args.bootstrap_servers,
        evidence_root=Path(args.evidence_root),
    )
    print(f"Published Flink smoke events for {len(summary['published_counts'])} source topics.")


if __name__ == "__main__":
    main()
