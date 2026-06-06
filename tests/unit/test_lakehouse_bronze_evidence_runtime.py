import importlib
import importlib.util
import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_evidence_module():
    spec = importlib.util.find_spec("vina_bim_shop.lakehouse.evidence")
    assert spec is not None, "Expected vina_bim_shop.lakehouse.evidence module for Bronze landing evidence helpers."
    return importlib.import_module("vina_bim_shop.lakehouse.evidence")


def _load_script_module(script_relative_path: str, module_name: str):
    script_path = _repo_root() / script_relative_path
    assert script_path.is_file(), f"Expected script at {script_relative_path}."
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_capture_bronze_landing_evidence_records_batch_and_event_examples(tmp_path: Path) -> None:
    evidence = _load_evidence_module()
    assert hasattr(evidence, "capture_bronze_landing_evidence"), (
        "Expected Bronze evidence helper `capture_bronze_landing_evidence`."
    )

    manifest = evidence.capture_bronze_landing_evidence(
        evidence_root=tmp_path,
        listing_text=(
            "[2026-06-01 00:00:00 UTC]  12KiB bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet\n"
            "[2026-06-01 00:00:01 UTC]  4KiB bronze/events/commerce_events/ingest_date=2026-06-01/000000.jsonl\n"
        ),
    )

    artifact = tmp_path / "bronze_landing_examples.json"
    assert artifact.is_file()
    assert json.loads(artifact.read_text(encoding="utf-8")) == {
        "batch": ["bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet"],
        "events": ["bronze/events/commerce_events/ingest_date=2026-06-01/000000.jsonl"],
    }
    assert manifest["artifacts"] == ["bronze_landing_examples.json"]


def test_capture_bronze_landing_evidence_accepts_bucket_relative_listing_entries(tmp_path: Path) -> None:
    evidence = _load_evidence_module()

    manifest = evidence.capture_bronze_landing_evidence(
        evidence_root=tmp_path,
        listing_text=(
            "[2026-06-01 00:00:00 UTC]  12KiB STANDARD batch/customers/snapshot_date=2026-06-01/part-000.parquet\n"
            "[2026-06-01 00:00:01 UTC]  4KiB STANDARD events/commerce_events/ingest_date=2026-06-01/000000.json\n"
        ),
    )

    artifact = tmp_path / "bronze_landing_examples.json"
    assert artifact.is_file()
    assert json.loads(artifact.read_text(encoding="utf-8")) == {
        "batch": ["bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet"],
        "events": ["bronze/events/commerce_events/ingest_date=2026-06-01/000000.json"],
    }
    assert manifest["artifacts"] == ["bronze_landing_examples.json"]


def test_capture_bronze_evidence_script_writes_examples_artifact(monkeypatch, capsys, tmp_path: Path) -> None:
    module = _load_script_module("scripts/lakehouse/capture_bronze_evidence.py", "capture_bronze_evidence_script")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "capture_bronze_evidence.py",
            "--evidence-root",
            str(tmp_path),
            "--listing-path",
            str(tmp_path / "bronze-listing.txt"),
        ],
    )
    args = module.parse_args()
    assert args.evidence_root == str(tmp_path)
    assert args.listing_path == str(tmp_path / "bronze-listing.txt")

    calls = []

    def fake_capture_bronze_landing_evidence(**kwargs):
        calls.append(kwargs)
        artifact = Path(kwargs["evidence_root"]) / "bronze_landing_examples.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(
            json.dumps(
                {
                    "batch": ["bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet"],
                    "events": ["bronze/events/commerce_events/ingest_date=2026-06-01/000000.jsonl"],
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return {"artifacts": ["bronze_landing_examples.json"]}

    monkeypatch.setattr(module, "capture_bronze_landing_evidence", fake_capture_bronze_landing_evidence)
    module.main()

    assert calls == [{"evidence_root": Path(tmp_path), "listing_path": Path(tmp_path / "bronze-listing.txt")}]
    assert (tmp_path / "bronze_landing_examples.json").is_file()
    assert capsys.readouterr().out.strip() == "Captured Bronze landing evidence with 1 artifacts."
