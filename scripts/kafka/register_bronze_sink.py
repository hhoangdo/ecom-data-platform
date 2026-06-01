from __future__ import annotations

import argparse
import json
from pathlib import Path

from vina_bim_shop.kafka.bronze_sink import register_bronze_sink


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register the Bronze Kafka Connect sink.")
    parser.add_argument("--connect-url", default="http://localhost:8083")
    parser.add_argument("--template-path", default="infra/kafka/connect/source-events-s3-sink.template.json")
    parser.add_argument("--connector-name", required=True)
    parser.add_argument("--bronze-bucket", required=True)
    parser.add_argument("--minio-endpoint", required=True)
    parser.add_argument("--minio-region", required=True)
    parser.add_argument("--minio-access-key", required=True)
    parser.add_argument("--minio-secret-key", required=True)
    parser.add_argument("--evidence-root", default="evidence/03_kafka_ingestion")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    response = register_bronze_sink(
        connect_url=args.connect_url,
        template_path=Path(args.template_path),
        connector_name=args.connector_name,
        bronze_bucket=args.bronze_bucket,
        minio_endpoint=args.minio_endpoint,
        minio_region=args.minio_region,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
    )
    evidence_path = Path(args.evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    artifact_path = evidence_path / "kafka_connect_bronze_sink_response.json"
    artifact_path.write_text(json.dumps(response, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Registered Bronze sink {args.connector_name} at {args.connect_url}.")


if __name__ == "__main__":
    main()
