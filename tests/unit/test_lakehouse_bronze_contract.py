import importlib
import importlib.util
from pathlib import Path


def _load_bronze_module():
    spec = importlib.util.find_spec("vina_bim_shop.lakehouse.bronze")
    assert spec is not None, "Expected vina_bim_shop.lakehouse.bronze module for Bronze raw landing helpers."
    return importlib.import_module("vina_bim_shop.lakehouse.bronze")


def test_bronze_path_helpers_render_batch_and_event_object_keys() -> None:
    bronze = _load_bronze_module()

    required_helpers = [
        "render_batch_snapshot_prefix",
        "render_batch_snapshot_key",
        "render_event_prefix",
        "render_event_key",
    ]
    for helper_name in required_helpers:
        assert hasattr(bronze, helper_name), f"Expected Bronze helper `{helper_name}`."

    assert bronze.render_batch_snapshot_prefix("customers", "2026-06-01") == "bronze/batch/customers/snapshot_date=2026-06-01/"
    assert bronze.render_batch_snapshot_key("customers", "2026-06-01", "part-000.parquet") == (
        "bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet"
    )
    assert bronze.render_event_prefix("commerce_events", "2026-06-01") == (
        "bronze/events/commerce_events/ingest_date=2026-06-01/"
    )
    assert bronze.render_event_key("commerce_events", "2026-06-01", "000000.jsonl") == (
        "bronze/events/commerce_events/ingest_date=2026-06-01/000000.jsonl"
    )


def test_build_batch_snapshot_upload_commands_targets_bronze_batch_layout(tmp_path: Path) -> None:
    bronze = _load_bronze_module()
    assert hasattr(bronze, "build_batch_snapshot_upload_commands"), (
        "Expected Bronze raw export helper `build_batch_snapshot_upload_commands`."
    )

    raw_root = tmp_path / "raw"
    customers_file = raw_root / "customers" / "part-000.parquet"
    orders_file = raw_root / "orders" / "part-001.parquet"
    ignored_event_file = raw_root / "kafka_topics" / "commerce_events" / "events.jsonl"
    ignored_bad_snapshot_file = raw_root / "bad_snapshots" / "bad_snapshots.jsonl"

    for path in [customers_file, orders_file, ignored_event_file, ignored_bad_snapshot_file]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("stub", encoding="utf-8")

    commands = bronze.build_batch_snapshot_upload_commands(
        raw_root=raw_root,
        snapshot_date="2026-06-01",
        minio_alias="LOCAL",
    )

    assert commands == [
        [
            "mc",
            "cp",
            str(customers_file),
            "LOCAL/bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet",
        ],
        [
            "mc",
            "cp",
            str(orders_file),
            "LOCAL/bronze/batch/orders/snapshot_date=2026-06-01/part-001.parquet",
        ],
    ]


def test_lakehouse_evidence_exposes_bronze_layout_examples() -> None:
    from vina_bim_shop.lakehouse import evidence

    assert hasattr(evidence, "bronze_layout_examples"), "Expected Bronze evidence helper `bronze_layout_examples`."
    bronze_layout_examples = getattr(evidence, "bronze_layout_examples")
    assert bronze_layout_examples() == {
        "batch": "bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet",
        "events": "bronze/events/commerce_events/ingest_date=2026-06-01/000000.jsonl",
    }
