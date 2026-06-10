from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.flink.evidence import capture_evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture ADR 04 Flink streaming evidence.")
    parser.add_argument("--evidence-root", default="evidence/06_flink_streaming")
    parser.add_argument("--flink-api-url", default="http://localhost:8086")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = capture_evidence(
        evidence_root=Path(args.evidence_root),
        flink_api_url=args.flink_api_url,
    )
    print(f"Captured ADR 04 evidence with {len(manifest['artifacts'])} artifacts.")


if __name__ == "__main__":
    main()
