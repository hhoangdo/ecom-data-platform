from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.flink.verification import DEFAULT_BASE_EVIDENCE_ROOT, run_cleanroom_verification


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run selective ADR 04 clean-room reset and verification phases.")
    parser.add_argument(
        "--phase",
        choices=["preflight", "reset", "verify-adr04", "verify-pinot", "cleanup", "all"],
        default="all",
    )
    parser.add_argument("--base-evidence-root", default=str(DEFAULT_BASE_EVIDENCE_ROOT))
    parser.add_argument("--run-root")
    parser.add_argument("--include-pinot", action="store_true")
    parser.add_argument("--poll-timeout-seconds", type=int, default=240)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_cleanroom_verification(
        phase=args.phase,
        base_evidence_root=Path(args.base_evidence_root),
        run_root=Path(args.run_root) if args.run_root else None,
        include_pinot=args.include_pinot,
        poll_timeout_seconds=args.poll_timeout_seconds,
    )
    print(f"Completed clean-room phase {summary['phase']} under {summary['run_root']}.")


if __name__ == "__main__":
    main()
