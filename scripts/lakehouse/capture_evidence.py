from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.lakehouse.evidence import capture_evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture ADR 02 lakehouse evidence.")
    parser.add_argument("--evidence-root", default="evidence/04_lakehouse")
    parser.add_argument("--minio-endpoint", default="http://localhost:9000")
    parser.add_argument("--trino-url", default="http://localhost:8080")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = capture_evidence(
        evidence_root=Path(args.evidence_root),
        minio_endpoint=args.minio_endpoint,
        trino_url=args.trino_url,
    )
    print(f"Captured ADR 02 evidence with {len(manifest['artifacts'])} artifacts.")


if __name__ == "__main__":
    main()
