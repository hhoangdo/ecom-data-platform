from __future__ import annotations

import argparse
import subprocess

from vina_bim_shop.kafka.bootstrap import bootstrap_topics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create local Kafka topics for ADR 01 ingestion.")
    parser.add_argument("--bootstrap-server", default="kafka:29092")
    parser.add_argument("--topics-file", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bootstrap_topics(
        runner=lambda command: subprocess.run(command, check=True),
        bootstrap_server=args.bootstrap_server,
        topics_file=args.topics_file,
    )


if __name__ == "__main__":
    main()
