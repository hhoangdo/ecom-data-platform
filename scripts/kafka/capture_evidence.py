from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.kafka.evidence import capture_evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture ADR 01 Kafka ingestion evidence.")
    parser.add_argument("--evidence-root", default="evidence/03_kafka_ingestion")
    parser.add_argument("--schema-registry-url", default="http://localhost:8081")
    parser.add_argument("--kafka-connect-url", default="http://localhost:8083")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = capture_evidence(
        evidence_root=Path(args.evidence_root),
        schema_registry_url=args.schema_registry_url,
        kafka_connect_url=args.kafka_connect_url,
    )
    print(f"Captured ADR 01 evidence with {len(manifest['artifacts'])} artifacts.")


if __name__ == "__main__":
    main()
