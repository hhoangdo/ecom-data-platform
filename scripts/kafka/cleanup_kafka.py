from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from vina_bim_shop.kafka.cleanup import cleanup_kafka


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset ADR 01 Kafka topics and optional evidence.")
    parser.add_argument("--bootstrap-server", default="kafka:29092")
    parser.add_argument("--evidence-root", default="evidence/03_kafka_ingestion")
    parser.add_argument("--clean-evidence", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cleanup_kafka(
        runner=lambda command: subprocess.run(command, check=True),
        bootstrap_server=args.bootstrap_server,
        evidence_root=Path(args.evidence_root),
        clean_evidence=args.clean_evidence,
    )
    print("Kafka topics reset and bootstrapped.")


if __name__ == "__main__":
    main()
