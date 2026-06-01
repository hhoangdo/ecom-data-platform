from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.pinot.evidence import capture_evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture ADR 05 Pinot serving evidence.")
    parser.add_argument("--controller-url", default="http://localhost:9003")
    parser.add_argument("--broker-url", default="http://localhost:8000")
    parser.add_argument("--evidence-root", default="evidence/07_pinot_serving")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = capture_evidence(
        controller_url=args.controller_url,
        broker_url=args.broker_url,
        evidence_root=Path(args.evidence_root),
    )
    print(f"Captured ADR 05 evidence with {len(manifest['artifacts'])} artifacts.")


if __name__ == "__main__":
    main()
