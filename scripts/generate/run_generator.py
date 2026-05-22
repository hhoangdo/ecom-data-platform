from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.generators.runner import run_generation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Vina Bim Shop Section 01 source data.")
    parser.add_argument("--config", default="configs/generator/base.yaml", help="Path to generator YAML config.")
    parser.add_argument("--scale", choices=["smoke", "medium", "coursework"], default="medium")
    parser.add_argument("--mode", choices=["offline", "streaming", "full"], default="full")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--clean", action="store_true", help="Remove generator-managed outputs before writing.")
    parser.add_argument("--raw-root", default=None, help="Override raw output root for this run.")
    parser.add_argument("--evidence-root", default=None, help="Override evidence output root for this run.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_generation(
        config_path=Path(args.config),
        scale=args.scale,
        mode=args.mode,
        seed=args.seed,
        clean=args.clean,
        raw_root=args.raw_root,
        evidence_root=args.evidence_root,
    )
    print(
        "Generated Section 01 data "
        f"(mode={args.mode}, scale={args.scale}) at {result.raw_root} "
        f"with evidence at {result.evidence_root}."
    )
    for dataset, row_count in sorted(result.row_counts.items()):
        print(f"- {dataset}: {row_count:,} rows")


if __name__ == "__main__":
    main()
