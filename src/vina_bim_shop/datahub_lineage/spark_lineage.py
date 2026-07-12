from __future__ import annotations

from vina_bim_shop.datahub_lineage.emitter import DataHubLineageEmitter, ice_urn
from vina_bim_shop.lakehouse.spark.constants import (
    GOLD_SERVING_TABLES,
    REQUIRED_GOLD_TABLES,
    SILVER_TABLES,
)

GOLD_UPSTREAM_MAP: dict[str, list[str]] = {
    "dim_customer": ["stg_customers"],
    "dim_seller": ["stg_sellers"],
    "dim_product": ["stg_products", "stg_product_category_map"],
    "dim_category": ["stg_product_category_map"],
    "dim_order_status": ["stg_orders"],
    "dim_shipment_status": ["stg_shipments"],
    "dim_shipping_method": ["stg_shipments"],
    "dim_payment_method": ["stg_payments"],
    "dim_promotion": ["stg_promotions"],
    "fact_order": ["stg_orders"],
    "fact_order_item": ["stg_orders", "stg_order_items", "stg_products"],
    "fact_payment_attempt": ["stg_payments", "stg_orders"],
    "fact_shipment": ["stg_shipments", "stg_orders"],
    "fact_inventory_snapshot": ["stg_inventory_snapshots"],
    "fact_promotion_application": ["stg_promotions", "stg_orders"],
    "obt_order_performance": [
        "stg_orders",
        "stg_order_items",
        "stg_payments",
        "stg_shipments",
    ],
    "agg_hourly_reconciled_kpi": [
        "stg_orders",
        "stg_payments",
        "stg_commerce_events",
    ],
    "feat_customer_90d": ["fact_order"],
    "feat_stream_60m": ["stg_commerce_events"],
    "feat_customer_unified": ["feat_customer_90d", "feat_stream_60m"],
}

MEDALLION_TAGS: dict[str, str] = {
    **{t: "bronze" for t in SILVER_TABLES},
    **{t: "silver" for t in REQUIRED_GOLD_TABLES if t not in GOLD_SERVING_TABLES},
    **{t: "gold" for t in REQUIRED_GOLD_TABLES if t in GOLD_SERVING_TABLES},
}


def emit_spark_batch_lineage(gms_url: str = "http://datahub-gms:8080") -> dict[str, str]:
    emitter = DataHubLineageEmitter(gms_url)
    results: dict[str, str] = {}

    for gold_table in REQUIRED_GOLD_TABLES:
        gold_urn = ice_urn(gold_table)
        upstream_tables = GOLD_UPSTREAM_MAP.get(gold_table, [])
        if not upstream_tables:
            continue

        parent_urns = [ice_urn(t) for t in upstream_tables]
        try:
            emitter.emit_upstream_lineage(gold_urn, parent_urns)
            results[gold_table] = "success"
        except Exception as exc:
            results[gold_table] = f"warning: {exc}"

    for table, tag in MEDALLION_TAGS.items():
        try:
            emitter.emit_tag(ice_urn(table), tag)
        except Exception:
            pass

    for serving_table in GOLD_SERVING_TABLES:
        try:
            emitter.emit_tag(ice_urn(serving_table), "official")
        except Exception:
            pass

    return results
