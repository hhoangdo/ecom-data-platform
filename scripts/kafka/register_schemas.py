from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.kafka.schema_registry import register_schema_subjects


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register ADR 01 JSON Schemas in Schema Registry.")
    parser.add_argument("--registry-url", default="http://localhost:8081")
    parser.add_argument("--schemas-dir", default="infra/kafka/schemas")
    parser.add_argument("--evidence-root", default="evidence/03_kafka_ingestion")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    responses = register_schema_subjects(
        registry_url=args.registry_url,
        schemas_dir=Path(args.schemas_dir),
        evidence_root=Path(args.evidence_root),
    )
    print(f"Registered {len(responses)} JSON Schema subjects at {args.registry_url}.")


if __name__ == "__main__":
    main()
