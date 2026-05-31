from pathlib import Path


def test_section02_documentation_records_core_design_decisions() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    content = (repo_root / "deliverables" / "02_schema_design.md").read_text(encoding="utf-8")

    required_phrases = [
        "JSON event envelopes answer what happened now",
        "Periodic table-state exports answer what state is reliable at checkpoint",
        "dbt-DuckDB is the local execution and test harness",
        "Apache Pinot is fresh but provisional",
        "Trino-served Gold tables are the canonical reconciled truth",
        "official paid revenue",
        "category_cost_rate",
        "obt_order_performance",
        "dead_letter_events",
    ]
    for phrase in required_phrases:
        assert phrase in content

    forbidden_phrases = [
        "legacy flat `stream_events`",
        "raw_stream_events",
        "stg_stream_events",
    ]
    for phrase in forbidden_phrases:
        assert phrase not in content


def test_schema_design_puml_shows_storage_and_serving_contracts() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    content = (repo_root / "architecture" / "diagrams" / "schema_design.puml").read_text(encoding="utf-8")

    required_labels = [
        "raw_kafka_commerce_events",
        "raw_bad_events",
        "stg_orders",
        "dim_category",
        "fact_payment_attempt",
        "fact_inventory_snapshot",
        "obt_order_performance",
        "agg_hourly_reconciled_kpi",
        "feat_customer_unified",
        "pinot_realtime_commerce_metrics_1m",
        "DuckDB executive mart",
    ]
    for label in required_labels:
        assert label in content


def test_dbt_project_declares_expected_model_layers_and_tests() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dbt_root = repo_root / "dbt"

    expected_models = [
        "models/bronze/raw_orders.sql",
        "models/bronze/raw_kafka_commerce_events.sql",
        "models/silver/stg_orders.sql",
        "models/silver/stg_commerce_events.sql",
        "models/gold/fact_order.sql",
        "models/gold/fact_inventory_snapshot.sql",
        "models/gold/obt_order_performance.sql",
        "models/gold/agg_hourly_reconciled_kpi.sql",
        "models/gold/feat_customer_unified.sql",
    ]
    for relative_path in expected_models:
        assert (dbt_root / relative_path).is_file()

    schema_yml = (dbt_root / "models" / "gold" / "schema.yml").read_text(encoding="utf-8")
    assert "relationships" in schema_yml
    assert "accepted_values" in schema_yml
    assert "expression_is_true" in schema_yml
