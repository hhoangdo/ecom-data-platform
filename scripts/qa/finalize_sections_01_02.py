from __future__ import annotations

import hashlib
import json
import subprocess
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FINAL_SCALE = "medium"
FINAL_SEED = 42
RAW_ROOT = Path("data/raw")
SECTION01_MANIFEST = Path("evidence/01_data_generator/run_manifest.json")
FINAL_EVIDENCE_ROOT = Path("evidence/final_dataset")
FINAL_ARCHIVE = FINAL_EVIDENCE_ROOT / "vina_bim_shop_medium_raw.zip"
FINAL_MANIFEST = FINAL_EVIDENCE_ROOT / "final_dataset_manifest.json"
DUCKDB_REPRODUCTION_PATH = "data/gold/vina_bim_shop.duckdb"
DUCKDB_EXECUTIVE_MART_PATH = "data/gold/vina_bim_shop_executive.duckdb"


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]

    commands = [
        run_command(
            [
                "uv",
                "run",
                "python",
                "scripts/generate/run_generator.py",
                "--scale",
                FINAL_SCALE,
                "--mode",
                "full",
                "--clean",
                "--seed",
                str(FINAL_SEED),
            ],
            repo_root,
        ),
        run_command(
            ["uv", "run", "python", "scripts/qa/generate_section02_evidence.py"],
            repo_root,
        ),
    ]

    archive_path = repo_root / FINAL_ARCHIVE
    write_raw_archive(repo_root / RAW_ROOT, archive_path)

    section01_manifest = json.loads((repo_root / SECTION01_MANIFEST).read_text(encoding="utf-8"))
    manifest = build_final_dataset_manifest(
        repo_root=repo_root,
        raw_root=repo_root / RAW_ROOT,
        archive_path=archive_path,
        scale=FINAL_SCALE,
        seed=FINAL_SEED,
        row_counts=section01_manifest["row_counts"],
    )
    manifest["commands"] = commands
    (repo_root / FINAL_MANIFEST).parent.mkdir(parents=True, exist_ok=True)
    (repo_root / FINAL_MANIFEST).write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    commands.append(run_command(["uv", "run", "pytest"], repo_root))
    manifest["commands"] = commands
    (repo_root / FINAL_MANIFEST).write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Wrote final dataset archive to {archive_path}")
    print(f"Wrote final dataset manifest to {repo_root / FINAL_MANIFEST}")
    return 0


def run_command(command: list[str], cwd: Path) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
    elapsed = round(time.perf_counter() - started, 3)
    if completed.returncode != 0:
        raise SystemExit(
            f"Command failed: {' '.join(command)}\n"
            f"Exit code: {completed.returncode}\n"
            f"STDOUT:\n{completed.stdout}\n"
            f"STDERR:\n{completed.stderr}"
        )
    return {
        "command": " ".join(command),
        "return_code": completed.returncode,
        "elapsed_seconds": elapsed,
    }


def write_raw_archive(raw_root: Path, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in raw_files(raw_root):
            archive.write(path, path.relative_to(raw_root).as_posix())


def build_final_dataset_manifest(
    *,
    repo_root: Path | None = None,
    raw_root: Path,
    archive_path: Path,
    scale: str,
    seed: int,
    row_counts: dict[str, int],
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scale": scale,
        "seed": seed,
        "raw_root": portable_path(raw_root, repo_root),
        "archive_path": portable_path(archive_path, repo_root),
        "archive_sha256": sha256_file(archive_path),
        "duckdb_reproduction_path": DUCKDB_REPRODUCTION_PATH,
        "duckdb_executive_mart_path": DUCKDB_EXECUTIVE_MART_PATH,
        "duckdb_reproduction_command": "uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt",
        "duckdb_executive_mart_command": (
            "uv run python scripts/spark/export_executive_mart.py "
            "--duckdb-path data/gold/vina_bim_shop_executive.duckdb"
        ),
        "row_counts": row_counts,
        "raw_files": raw_file_manifest(raw_root),
    }


def raw_file_manifest(raw_root: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": path.relative_to(raw_root).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in raw_files(raw_root)
    ]


def raw_files(raw_root: Path) -> list[Path]:
    return sorted(
        path
        for path in raw_root.rglob("*")
        if path.is_file() and path.name not in {".gitkeep", ".gitignore"}
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def portable_path(path: Path, repo_root: Path | None) -> str:
    if repo_root is None:
        return path.as_posix()
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


if __name__ == "__main__":
    raise SystemExit(main())
