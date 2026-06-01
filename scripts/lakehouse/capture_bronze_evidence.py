from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.lakehouse.evidence import capture_bronze_landing_evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture Bronze landing evidence.")
    parser.add_argument("--evidence-root", default="evidence/04_lakehouse")
    parser.add_argument("--listing-path", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = capture_bronze_landing_evidence(
        evidence_root=Path(args.evidence_root),
        listing_path=Path(args.listing_path),
    )
    print(f"Captured Bronze landing evidence with {len(manifest['artifacts'])} artifacts.")


if __name__ == "__main__":
    main()
