from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.pinot.bootstrap import apply_assets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply ADR 05 Pinot schemas and table configs.")
    parser.add_argument("--controller-url", default="http://localhost:9003")
    parser.add_argument("--evidence-root", default="evidence/07_pinot_serving")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = apply_assets(controller_url=args.controller_url, evidence_root=Path(args.evidence_root))
    print(f"Applied {len(manifest['tables'])} Pinot tables.")


if __name__ == "__main__":
    main()
