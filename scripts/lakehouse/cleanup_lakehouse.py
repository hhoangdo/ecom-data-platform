from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stop ADR 02 lakehouse services and optionally remove local evidence.")
    parser.add_argument("--volumes", action="store_true", help="Also remove Docker volumes for MinIO and shared Postgres state.")
    parser.add_argument("--clean-evidence", action="store_true", help="Remove evidence/04_lakehouse.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    command = ["docker", "compose", "--profile", "lakehouse", "down"]
    if args.volumes:
        command.append("-v")
    subprocess.run(command, check=True)

    if args.clean_evidence:
        shutil.rmtree(Path("evidence/04_lakehouse"), ignore_errors=True)


if __name__ == "__main__":
    main()
