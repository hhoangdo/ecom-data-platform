from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PROFILES = {
    "ingestion": {
        "volumes": ["kafka_kraft_data"],
        "description": "Kafka, Schema Registry, Kafka Connect, Kafka UI",
    },
    "lakehouse": {
        "volumes": ["minio_data", "lakehouse_postgres_data"],
        "description": "MinIO, Hive Metastore, Trino, shared Postgres",
    },
    "batch": {
        "volumes": [],
        "description": "Spark master, worker, history server (shares lakehouse volumes)",
    },
    "streaming": {
        "volumes": [],
        "description": "Flink JobManager, TaskManager, job submitter",
    },
    "serving": {
        "volumes": ["pinot_zookeeper_data", "pinot_zookeeper_datalog"],
        "description": "Apache Pinot controller, broker, server, Zookeeper",
    },
    "orchestration": {
        "volumes": [],
        "description": "Airflow webserver, scheduler, init, GX Data Docs",
    },
    "governance": {
        "volumes": [],
        "description": "DataHub GMS, frontend, actions, Elasticsearch",
    },
}

ALL_VOLUMES = [
    "kafka_kraft_data",
    "minio_data",
    "lakehouse_postgres_data",
    "pinot_zookeeper_data",
    "pinot_zookeeper_datalog",
]

GIT_IGNORED_PATHS = [
    "data/raw",
    "data/gold",
    "infra/analytics/dbt/target",
    "infra/analytics/dbt/logs",
    "infra/analytics/dbt/dbt_packages",
    "evidence/runtime",
]

PRESERVED_LOCAL_FILES = {
    "data/raw/.gitignore",
    "data/gold/.gitignore",
}

RESET_LOG_DIR = PROJECT_ROOT / "evidence" / "runtime" / "reset_logs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safe project reset — stops services, removes Docker volumes, optionally cleans local git-ignored data."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be destroyed without taking any action.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt (for scripts/CI).",
    )
    parser.add_argument(
        "--profile",
        action="append",
        choices=list(PROFILES),
        dest="profiles",
        help="Limit reset to specific profiles. Repeatable. Default: all profiles.",
    )
    parser.add_argument(
        "--clean-local-data",
        action="store_true",
        help="Also remove git-ignored local data (data/raw, data/gold, infra/analytics/dbt/target, etc.).",
    )
    parser.add_argument(
        "--keep-volumes",
        action="store_true",
        help="Stop services without removing Docker volumes.",
    )
    return parser.parse_args()


def _run(command: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=capture, text=True, cwd=PROJECT_ROOT)


def _resolve_profiles(selected: list[str] | None) -> list[str]:
    if selected:
        return selected
    return list(PROFILES)


def _build_destruction_plan(profiles: list[str], keep_volumes: bool, clean_local: bool) -> dict:
    services = []
    volume_set: set[str] = set()
    for p in profiles:
        volume_set.update(PROFILES[p]["volumes"])

    if not keep_volumes:
        volumes = sorted(volume_set)
    else:
        volumes = []

    local_paths: list[str] = []
    if clean_local:
        local_paths = GIT_IGNORED_PATHS

    return {
        "profiles": profiles,
        "volumes": volumes,
        "local_data_paths": local_paths,
        "compose_command": f"docker compose --profile {' --profile '.join(profiles)} down{' -v' if not keep_volumes else ''}",
    }


def _print_plan(plan: dict) -> None:
    print("=" * 60)
    print("RESET PLAN")
    print("=" * 60)
    print(f"Profiles to stop: {', '.join(plan['profiles'])}")
    if plan["volumes"]:
        print(f"Docker volumes to remove: {', '.join(plan['volumes'])}")
    else:
        print("Docker volumes: (none — --keep-volumes is set)")
    if plan["local_data_paths"]:
        print(f"Local paths to clean: {', '.join(plan['local_data_paths'])}")
    else:
        print("Local paths: (none — omit --clean-local-data to wipe git-ignored local data)")
    print(f"Compose command: {plan['compose_command']}")
    print("=" * 60)


def _confirm() -> bool:
    print()
    answer = input("Proceed with reset? [y/N] ").strip().lower()
    return answer == "y"


def _stop_and_down(profiles: list[str], keep_volumes: bool) -> None:
    profile_args: list[str] = []
    for p in profiles:
        profile_args.extend(["--profile", p])

    print(f"\nStopping services for profiles: {', '.join(profiles)}")
    _run(["docker", "compose", *profile_args, "stop"])

    down_cmd = ["docker", "compose", *profile_args, "down"]
    if not keep_volumes:
        down_cmd.append("-v")
    print(f"Running: {' '.join(down_cmd)}")
    _run(down_cmd, capture=False)


def _clean_local_paths(paths: list[str]) -> None:
    for rel in paths:
        target = PROJECT_ROOT / rel
        if not target.exists():
            print(f"  Skipping (not found): {rel}")
            continue
        if target.is_dir():
            for item in target.iterdir():
                item_path = rel.replace("\\", "/") + "/" + item.name
                preserved_key = item_path.rstrip("/")
                if preserved_key in PRESERVED_LOCAL_FILES:
                    continue
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                    print(f"  Removed dir: {item_path}")
                else:
                    item.unlink(missing_ok=True)
                    print(f"  Removed file: {item_path}")
        else:
            target.unlink(missing_ok=True)
            print(f"  Removed file: {rel}")


def _write_log(plan: dict, success: bool) -> Path:
    RESET_LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = RESET_LOG_DIR / f"reset_{timestamp}.json"
    log = {
        "timestamp": timestamp,
        "success": success,
        "profiles_stopped": plan["profiles"],
        "volumes_removed": plan["volumes"],
        "local_data_cleaned": plan["local_data_paths"],
    }
    log_path.write_text(json.dumps(log, indent=2) + "\n")
    print(f"\nReset log written: {log_path.relative_to(PROJECT_ROOT)}")
    return log_path


def main() -> None:
    args = parse_args()
    profiles = _resolve_profiles(args.profiles)
    plan = _build_destruction_plan(profiles, args.keep_volumes, args.clean_local_data)

    _print_plan(plan)

    if args.dry_run:
        print("\n--dry-run: No changes made.")
        return

    if not args.force and not _confirm():
        print("Reset aborted.")
        return

    try:
        _stop_and_down(profiles, args.keep_volumes)
        if plan["local_data_paths"]:
            print("\nCleaning local git-ignored data...")
            _clean_local_paths(plan["local_data_paths"])
        _write_log(plan, success=True)
    except Exception as exc:
        _write_log(plan, success=False)
        print(f"\nERROR: Reset failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
