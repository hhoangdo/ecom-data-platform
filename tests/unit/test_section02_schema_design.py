from pathlib import Path


def test_physical_gold_model_puml_documents_all_layers_and_purposes() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    diagram = repo_root / "architecture" / "diagrams" / "physical_gold_model.puml"

    content = diagram.read_text(encoding="utf-8")

    assert "skinparam backgroundColor white" in content
    for marker in ["<<PK>>", "<<FK>>", "<<AK>>", "<<NK>>", "<<SCD2>>"]:
        assert marker in content

    dbt_model_names = [
        path.stem
        for path in (repo_root / "infra" / "analytics" / "dbt" / "models").rglob("*.sql")
        if path.stem != ".gitkeep"
    ]
    for model_name in dbt_model_names:
        assert model_name in content

    for purpose_label in [
        "derived non-canonical OBT",
        "reconciliation evidence",
        "ML/AI preparation",
        "Bronze and Silver are views; Gold is physically constrained",
    ]:
        assert purpose_label in content


def test_gold_layer_erd_dbml_focuses_on_gold_tables_and_dbml_relationships() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    diagram = repo_root / "architecture" / "diagrams" / "gold_layer_ERD.dbml"

    content = diagram.read_text(encoding="utf-8")

    expected_gold_tables = [
        "agg_hourly_reconciled_kpi",
        "bridge_product_category",
        "dim_category",
        "dim_customer",
        "dim_date",
        "dim_order_status",
        "dim_payment_method",
        "dim_product",
        "dim_promotion",
        "dim_seller",
        "dim_shipment_status",
        "dim_shipping_method",
        "fact_inventory_snapshot",
        "fact_order",
        "fact_order_item",
        "fact_payment_attempt",
        "fact_promotion_application",
        "fact_shipment",
        "feat_customer_90d",
        "feat_customer_unified",
        "feat_stream_60m",
        "obt_order_performance",
    ]
    for table_name in expected_gold_tables:
        assert table_name in content

    for required_dbml_construct in [
        "Project gold_layer_erd",
        "TableGroup dimensions",
        "TableGroup facts",
        "TableGroup serving_features",
        "[pk]",
        "ref: >",
        "[unique]",
    ]:
        assert required_dbml_construct in content

    for relationship in [
        "customer_key bigint [not null, ref: > dim_customer.customer_key]",
        "order_key bigint [not null, ref: > fact_order.order_key]",
        "product_key bigint [not null, ref: > dim_product.product_key]",
        "payment_method_key bigint [not null, ref: > dim_payment_method.payment_method_key]",
        "customer_id varchar [not null, ref: > dim_customer.customer_id]",
    ]:
        assert relationship in content

    for non_gold_label in ["raw_orders", "stg_orders", "logical view lineage", "dbt transform lineage"]:
        assert non_gold_label not in content

    for removed_plantuml_construct in ["@startuml", "skinparam", "<<PK>>", "<<FK>>", "||--o{"]:
        assert removed_plantuml_construct not in content


def test_duckdb_gold_tables_have_physical_constraints_for_dbeaver_erd() -> None:
    import duckdb

    repo_root = Path(__file__).resolve().parents[2]
    db_path = repo_root / "data" / "gold" / "vina_bim_shop.duckdb"

    with duckdb.connect(str(db_path), read_only=True) as connection:
        constraints = connection.execute(
            """
            select
              table_name,
              constraint_type,
              constraint_column_names,
              referenced_table,
              referenced_column_names
            from duckdb_constraints()
            where schema_name = 'gold'
            """
        ).fetchall()

    primary_keys = {
        (table_name, tuple(column_names))
        for table_name, constraint_type, column_names, _, _ in constraints
        if constraint_type == "PRIMARY KEY"
    }
    foreign_keys = {
        (table_name, tuple(column_names), referenced_table, tuple(referenced_columns))
        for table_name, constraint_type, column_names, referenced_table, referenced_columns in constraints
        if constraint_type == "FOREIGN KEY"
    }

    for expected_pk in [
        ("dim_customer", ("customer_key",)),
        ("dim_product", ("product_key",)),
        ("fact_order", ("order_key",)),
        ("fact_order_item", ("order_item_key",)),
        ("fact_payment_attempt", ("payment_attempt_key",)),
        ("bridge_product_category", ("product_key", "category_key")),
        ("agg_hourly_reconciled_kpi", ("metric_hour",)),
        ("feat_customer_90d", ("customer_id", "event_timestamp")),
    ]:
        assert expected_pk in primary_keys

    for expected_fk in [
        ("fact_order", ("customer_key",), "dim_customer", ("customer_key",)),
        ("fact_order", ("order_status_key",), "dim_order_status", ("order_status_key",)),
        ("fact_order_item", ("order_key",), "fact_order", ("order_key",)),
        ("fact_order_item", ("product_key",), "dim_product", ("product_key",)),
        ("fact_payment_attempt", ("payment_method_key",), "dim_payment_method", ("payment_method_key",)),
        ("fact_shipment", ("shipping_method_key",), "dim_shipping_method", ("shipping_method_key",)),
        ("fact_inventory_snapshot", ("seller_key",), "dim_seller", ("seller_key",)),
    ]:
        assert expected_fk in foreign_keys


def test_promotion_sentinel_makes_fact_order_item_promotion_key_non_null() -> None:
    import duckdb

    repo_root = Path(__file__).resolve().parents[2]
    db_path = repo_root / "data" / "gold" / "vina_bim_shop.duckdb"

    with duckdb.connect(str(db_path), read_only=True) as connection:
        sentinel_count = connection.execute(
            "select count(*) from gold.dim_promotion where promotion_id = 'NO_PROMOTION'"
        ).fetchone()[0]
        null_promotion_key_count = connection.execute(
            "select count(*) from gold.fact_order_item where promotion_key is null"
        ).fetchone()[0]

    assert sentinel_count == 1
    assert null_promotion_key_count == 0


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
        "architecture/diagrams/physical_gold_model.puml",
        "architecture/diagrams/physical_gold_model.png",
        "Gold DuckDB tables enforce primary-key and foreign-key constraints",
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
    dbt_root = repo_root / "infra" / "analytics" / "dbt"

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


def test_quarantine_models_read_generated_bad_record_sources() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dbt_root = repo_root / "infra" / "analytics" / "dbt"

    raw_bad_events = dbt_root.joinpath("models", "bronze", "raw_bad_events.sql").read_text(encoding="utf-8")
    raw_bad_snapshots = dbt_root.joinpath("models", "bronze", "raw_bad_snapshots.sql").read_text(encoding="utf-8")
    bronze_schema = dbt_root.joinpath("models", "bronze", "schema.yml").read_text(encoding="utf-8")

    assert "kafka_topics/dead_letter_events/events.jsonl" in raw_bad_events
    assert "bad_snapshots/bad_snapshots.jsonl" in raw_bad_snapshots
    for column_name in ["error_reason", "raw_payload", "source_topic", "raw_record", "source_dataset"]:
        assert column_name in bronze_schema
