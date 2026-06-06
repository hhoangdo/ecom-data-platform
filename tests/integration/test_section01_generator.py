from pathlib import Path

import pandas as pd

from vina_bim_shop.generators.runner import run_generation


def test_smoke_full_generation_writes_contracts_and_evidence(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    raw_root = tmp_path / "raw"
    evidence_root = tmp_path / "evidence"

    result = run_generation(
        config_path=repo_root / "configs" / "generator" / "base.yaml",
        scale="smoke",
        mode="full",
        raw_root=raw_root,
        evidence_root=evidence_root,
        clean=True,
        seed=42,
    )

    expected_datasets = {
        "customers",
        "sellers",
        "products",
        "product_category_map",
        "inventory_snapshots",
        "promotions",
        "orders",
        "order_items",
        "payments",
        "shipments",
    }
    assert expected_datasets.issubset(result.row_counts)
    assert "stream_events" not in result.row_counts

    customers = pd.read_parquet(raw_root / "customers")
    products = pd.read_parquet(raw_root / "products")
    orders = pd.read_parquet(raw_root / "orders")
    order_items = pd.read_parquet(raw_root / "order_items")
    payments = pd.read_parquet(raw_root / "payments")
    shipments = pd.read_parquet(raw_root / "shipments")
    commerce_events = pd.read_json(raw_root / "kafka_topics" / "commerce_events" / "events.jsonl", lines=True)
    dead_letter_events = pd.read_json(raw_root / "kafka_topics" / "dead_letter_events" / "events.jsonl", lines=True)
    bad_snapshots = pd.read_json(raw_root / "bad_snapshots" / "bad_snapshots.jsonl", lines=True)

    assert {"customer_id", "segment", "city", "signup_ts"}.issubset(customers.columns)
    assert {
        "product_id",
        "seller_id",
        "primary_category",
        "primary_subcategory",
        "category_attributes",
    }.issubset(products.columns)
    assert {"order_id", "customer_id", "session_id", "order_timestamp"}.issubset(orders.columns)
    assert {"payment_id", "order_id", "payment_timestamp", "payment_status"}.issubset(payments.columns)
    assert {"shipment_id", "order_id", "shipment_status"}.issubset(shipments.columns)

    assert orders["session_id"].notna().all()
    assert payments["order_id"].nunique() == orders["order_id"].nunique()
    assert shipments["order_id"].nunique() == orders["order_id"].nunique()
    assert pd.to_datetime(payments["payment_timestamp"]).ge(
        pd.to_datetime(orders.set_index("order_id").loc[payments["order_id"], "order_timestamp"]).to_numpy()
    ).all()

    fmcg_elha_share = products["primary_category"].isin(["FMCG", "ELHA"]).mean()
    assert fmcg_elha_share >= 0.55
    assert customers["city"].isin(["Ho Chi Minh City", "Ha Noi"]).mean() >= 0.35

    duplicate_item_rate = order_items.duplicated(
        subset=["order_id", "product_id", "quantity", "unit_price", "discount_amount"]
    ).mean()
    assert 0.005 <= duplicate_item_rate <= 0.05

    assert not (raw_root / "stream_events").exists()
    assert (raw_root / "kafka_topics" / "dead_letter_events" / "events.jsonl").is_file()
    assert (raw_root / "bad_snapshots" / "bad_snapshots.jsonl").is_file()
    assert {
        "product_viewed",
        "add_to_cart",
        "checkout_started",
        "order_placed",
        "payment_failed",
    }.issubset(
        set(commerce_events["event_type"])
    )
    assert pd.to_datetime(commerce_events["created_ts"]).ge(
        pd.to_datetime(commerce_events["event_timestamp"])
    ).all()
    assert commerce_events["event_id"].duplicated().mean() > 0
    assert {
        "missing_required_key",
        "invalid_json",
        "invalid_timestamp",
        "unknown_schema_version",
    }.issubset(set(dead_letter_events["error_reason"]))
    assert {
        "missing_required_key",
        "invalid_json",
        "invalid_timestamp",
        "unknown_schema_version",
    }.issubset(set(bad_snapshots["error_reason"]))
    assert dead_letter_events["raw_payload"].notna().all()
    assert bad_snapshots["raw_record"].notna().all()

    assert (evidence_root / "run_manifest.json").is_file()
    assert (evidence_root / "row_counts.csv").is_file()
    assert (evidence_root / "schema_summary.csv").is_file()
    assert (evidence_root / "quality_metrics.csv").is_file()
    assert (evidence_root / "issue_manifest.csv").is_file()
    assert (evidence_root / "sample_rows" / "orders.csv").is_file()
    assert (evidence_root / "sample_rows" / "bad_snapshots.csv").is_file()
    assert (evidence_root / "sample_rows" / "kafka_topic_dead_letter_events.csv").is_file()
    assert not (evidence_root / "sample_rows" / "stream_events.csv").exists()
    assert (evidence_root / "quality_report.md").is_file()

    quality_metrics = pd.read_csv(evidence_root / "quality_metrics.csv").set_index("metric")["value"]
    assert 0.05 <= quality_metrics["issue_commerce_events_late_arrival"] <= 0.20
    assert quality_metrics["issue_dead_letter_events_invalid_json"] > 0
    assert quality_metrics["issue_bad_snapshots_invalid_timestamp"] > 0
