import hashlib
import importlib.util
import json
import zipfile
from pathlib import Path


def load_finalizer_module():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "scripts" / "qa" / "finalize_sections_01_02.py"
    spec = importlib.util.spec_from_file_location("finalize_sections_01_02", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_final_dataset_archive_packages_raw_files_only(tmp_path: Path) -> None:
    finalizer = load_finalizer_module()
    raw_root = tmp_path / "data" / "raw"
    raw_file = raw_root / "orders" / "part-000.parquet"
    raw_file.parent.mkdir(parents=True)
    raw_file.write_bytes(b"orders")
    (raw_root / ".gitignore").write_text("*\n!.gitignore\n", encoding="utf-8")
    duckdb_file = tmp_path / "data" / "gold" / "vina_bim_shop.duckdb"
    duckdb_file.parent.mkdir(parents=True)
    duckdb_file.write_bytes(b"not archived")

    archive_path = tmp_path / "evidence" / "final_dataset" / "vina_bim_shop_medium_raw.zip"
    finalizer.write_raw_archive(raw_root, archive_path)

    with zipfile.ZipFile(archive_path) as archive:
        names = archive.namelist()

    assert names == ["orders/part-000.parquet"]


def test_final_dataset_manifest_includes_checksums_and_reproduction_paths(tmp_path: Path) -> None:
    finalizer = load_finalizer_module()
    raw_root = tmp_path / "raw"
    raw_file = raw_root / "kafka_topics" / "commerce_events" / "events.jsonl"
    raw_file.parent.mkdir(parents=True)
    raw_file.write_text('{"event_id":"1"}\n', encoding="utf-8")
    archive_path = tmp_path / "archive.zip"
    archive_path.write_bytes(b"archive")

    manifest = finalizer.build_final_dataset_manifest(
        repo_root=tmp_path,
        raw_root=raw_root,
        archive_path=archive_path,
        scale="medium",
        seed=42,
        row_counts={"kafka_topics": 1},
    )

    assert manifest["scale"] == "medium"
    assert manifest["seed"] == 42
    assert manifest["raw_root"] == "raw"
    assert manifest["archive_path"] == "archive.zip"
    assert manifest["archive_sha256"] == hashlib.sha256(b"archive").hexdigest()
    assert manifest["duckdb_reproduction_path"] == "data/gold/vina_bim_shop.duckdb"
    assert manifest["duckdb_executive_mart_path"] == "data/gold/vina_bim_shop_executive.duckdb"
    assert manifest["row_counts"] == {"kafka_topics": 1}
    assert manifest["raw_files"] == [
        {
            "path": "kafka_topics/commerce_events/events.jsonl",
            "size_bytes": raw_file.stat().st_size,
            "sha256": hashlib.sha256(raw_file.read_bytes()).hexdigest(),
        }
    ]


def test_gitignore_documents_final_dataset_exception() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    gitignore = (repo_root / ".gitignore").read_text(encoding="utf-8")

    assert "evidence/final_dataset/" in gitignore


def test_final_dataset_manifest_artifact_is_valid_when_present() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    manifest_path = repo_root / "evidence" / "final_dataset" / "final_dataset_manifest.json"
    if not manifest_path.exists():
        return

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["scale"] == "medium"
    assert manifest["seed"] == 42
    assert manifest["archive_path"].endswith("vina_bim_shop_medium_raw.zip")
    assert manifest["archive_sha256"]
