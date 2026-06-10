from __future__ import annotations

BRONZE_BATCH_DATASETS = (
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
)

BRONZE_EVENT_TOPICS = (
    "commerce_events",
    "catalog_events",
    "fulfillment_events",
    "ops_events",
)

BRONZE_OPTIONAL_JSON_DATASETS = ("bad_snapshots",)
BRONZE_OPTIONAL_EVENT_TOPICS = ("dead_letter_events",)

SILVER_TABLES = (
    "stg_customers",
    "stg_sellers",
    "stg_products",
    "stg_product_category_map",
    "stg_inventory_snapshots",
    "stg_promotions",
    "stg_orders",
    "stg_order_items",
    "stg_payments",
    "stg_shipments",
    "stg_catalog_events",
    "stg_commerce_events",
    "stg_fulfillment_events",
    "stg_ops_events",
    "stg_bad_snapshots",
)

GOLD_DIMENSION_TABLES = (
    "dim_customer",
    "dim_seller",
    "dim_product",
    "dim_category",
    "dim_date",
    "dim_payment_method",
    "dim_order_status",
    "dim_shipment_status",
    "dim_shipping_method",
    "dim_promotion",
    "bridge_product_category",
)

GOLD_FACT_TABLES = (
    "fact_order",
    "fact_order_item",
    "fact_payment_attempt",
    "fact_shipment",
    "fact_inventory_snapshot",
    "fact_promotion_application",
)

GOLD_SERVING_TABLES = (
    "obt_order_performance",
    "agg_hourly_reconciled_kpi",
    "feat_customer_90d",
    "feat_stream_60m",
    "feat_customer_unified",
)

REQUIRED_GOLD_TABLES = (
    *GOLD_DIMENSION_TABLES,
    *GOLD_FACT_TABLES,
    *GOLD_SERVING_TABLES,
)

SILVER_PARTITIONED_BY = {
    "stg_catalog_events": ("days(event_timestamp)",),
    "stg_commerce_events": ("days(event_timestamp)",),
    "stg_fulfillment_events": ("days(event_timestamp)",),
    "stg_ops_events": ("days(event_timestamp)",),
    "stg_inventory_snapshots": ("days(snapshot_ts)",),
    "stg_bad_snapshots": ("days(ingest_ts)",),
}

GOLD_PARTITIONED_BY = {
    "fact_order": ("order_date_key",),
    "fact_order_item": ("order_date_key",),
    "fact_payment_attempt": ("payment_date_key",),
    "fact_shipment": ("shipment_created_date_key",),
    "fact_inventory_snapshot": ("snapshot_date_key",),
    "obt_order_performance": ("days(order_timestamp)",),
    "agg_hourly_reconciled_kpi": ("days(metric_hour)",),
    "feat_stream_60m": ("days(event_timestamp)",),
}

SOURCE_TOPIC_ACCEPTED_VALUES = {
    "stg_orders.status": ("paid", "payment_failed"),
    "stg_payments.payment_status": ("success", "failed"),
    "stg_commerce_events.event_topic": ("commerce_events",),
    "stg_catalog_events.event_topic": ("catalog_events",),
    "stg_fulfillment_events.event_topic": ("fulfillment_events",),
    "stg_ops_events.event_topic": ("ops_events",),
}
