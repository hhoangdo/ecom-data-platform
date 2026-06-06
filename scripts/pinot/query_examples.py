from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.pinot.query_examples import run_query_examples


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ADR 05 Pinot dashboard and reconciliation queries.")
    parser.add_argument("--broker-url", default="http://localhost:8000")
    parser.add_argument("--trino-url", default="http://localhost:8080")
    parser.add_argument("--trino-user", default="vina_analyst")
    parser.add_argument("--evidence-root", default="evidence/07_pinot_serving")
    parser.add_argument("--start-ts", default=None)
    parser.add_argument("--end-ts", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = run_query_examples(
        broker_url=args.broker_url,
        trino_url=args.trino_url,
        trino_user=args.trino_user,
        evidence_root=Path(args.evidence_root),
        start_ts=args.start_ts,
        end_ts=args.end_ts,
    )
    print(f"Wrote {len(manifest['artifacts'])} Pinot query artifacts.")


if __name__ == "__main__":
    main()
