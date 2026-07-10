from __future__ import annotations

import argparse


VARIANTS = (
    "skew-baseline",
    "skew-optimized",
    "high-cardinality-baseline",
    "high-cardinality-optimized",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one standalone Spark optimization experiment.")
    parser.add_argument("--config", default="configs/generator/base.yaml")
    parser.add_argument("--scale", default="coursework", choices=["coursework"])
    parser.add_argument("--variant", required=True, choices=VARIANTS)
    parser.add_argument("--evidence-root", default="evidence/05_spark_batch/optimization")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    from vina_bim_shop.lakehouse.spark.optimization_experiments import run_optimization_variant

    result = run_optimization_variant(
        config_path=args.config,
        scale=args.scale,
        variant=args.variant,
        evidence_root=args.evidence_root,
    )
    print(f"Completed {result['variant']} as {result['application_id']}.")


if __name__ == "__main__":
    main()
