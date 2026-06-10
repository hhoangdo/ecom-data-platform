from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from vina_bim_shop.lakehouse.spark.constants import REQUIRED_GOLD_TABLES, SOURCE_TOPIC_ACCEPTED_VALUES


def _scalar(spark: SparkSession, query: str) -> Any:
    return spark.sql(query).first()[0]


def _append_check(results: list[dict[str, Any]], *, name: str, success: bool, detail: Any) -> None:
    results.append({"name": name, "success": success, "detail": detail})


def run_pyspark_validations(*, spark: SparkSession, evidence_root: str | Path) -> dict[str, Any]:
    results: list[dict[str, Any]] = []

    for table_name, column_name in [
        ("stg_orders", "order_id"),
        ("stg_order_items", "order_item_id"),
        ("stg_payments", "payment_id"),
        ("stg_shipments", "shipment_id"),
        ("stg_commerce_events", "event_id"),
        ("stg_bad_snapshots", "bad_record_id"),
        ("fact_order", "order_id"),
        ("fact_order_item", "order_item_id"),
        ("fact_payment_attempt", "payment_id"),
        ("fact_shipment", "shipment_id"),
        ("agg_hourly_reconciled_kpi", "metric_hour"),
    ]:
        null_count = _scalar(spark, f"select count(*) from {table_name} where {column_name} is null")
        duplicate_count = _scalar(
            spark,
            f"""
select count(*)
from (
  select {column_name}
  from {table_name}
  group by 1
  having count(*) > 1
)
""",
        )
        _append_check(
            results,
            name=f"{table_name}.{column_name}.not_null",
            success=null_count == 0,
            detail={"null_count": null_count},
        )
        _append_check(
            results,
            name=f"{table_name}.{column_name}.unique",
            success=duplicate_count == 0,
            detail={"duplicate_key_count": duplicate_count},
        )

    for qualified_name, accepted_values in SOURCE_TOPIC_ACCEPTED_VALUES.items():
        table_name, column_name = qualified_name.split(".", maxsplit=1)
        invalid_count = _scalar(
            spark,
            f"""
select count(*)
from {table_name}
where {column_name} is not null
  and {column_name} not in ({", ".join(repr(value) for value in accepted_values)})
""",
        )
        _append_check(
            results,
            name=f"{qualified_name}.accepted_values",
            success=invalid_count == 0,
            detail={"invalid_count": invalid_count, "accepted_values": list(accepted_values)},
        )

    for name, query in [
        (
            "fact_order.customer_key.relationships",
            """
select count(*)
from fact_order fo
left join dim_customer dc
  on fo.customer_key = dc.customer_key
where dc.customer_key is null
""",
        ),
        (
            "fact_order.order_status_key.relationships",
            """
select count(*)
from fact_order fo
left join dim_order_status ds
  on fo.order_status_key = ds.order_status_key
where ds.order_status_key is null
""",
        ),
        (
            "fact_order_item.product_key.relationships",
            """
select count(*)
from fact_order_item fi
left join dim_product dp
  on fi.product_key = dp.product_key
where dp.product_key is null
""",
        ),
        (
            "fact_payment_attempt.payment_method_key.relationships",
            """
select count(*)
from fact_payment_attempt fp
left join dim_payment_method pm
  on fp.payment_method_key = pm.payment_method_key
where pm.payment_method_key is null
""",
        ),
    ]:
        missing_count = _scalar(spark, query)
        _append_check(
            results,
            name=name,
            success=missing_count == 0,
            detail={"missing_count": missing_count},
        )

    non_negative_failures = {
        "fact_order.official_paid_revenue": _scalar(
            spark,
            "select count(*) from fact_order where official_paid_revenue < 0",
        ),
        "fact_order_item.estimated_cost": _scalar(
            spark,
            "select count(*) from fact_order_item where estimated_cost < 0",
        ),
        "fact_order_item.estimated_margin": _scalar(
            spark,
            "select count(*) from fact_order_item where estimated_margin is null",
        ),
    }
    for metric_name, failure_count in non_negative_failures.items():
        _append_check(
            results,
            name=f"{metric_name}.expression_check",
            success=failure_count == 0,
            detail={"failure_count": failure_count},
        )

    dim_brand_null_count = _scalar(
        spark,
        "select count(*) from dim_product where brand is null",
    )
    _append_check(
        results,
        name="dim_product.brand.not_null",
        success=dim_brand_null_count == 0,
        detail={"null_brand_count": dim_brand_null_count},
    )

    paid_revenue_delta = _scalar(
        spark,
        """
with source_totals as (
  select round(sum(order_net_amount), 2) as paid_net_amount
  from stg_orders
  where status = 'paid'
),
gold_totals as (
  select round(sum(official_paid_revenue), 2) as paid_net_amount
  from fact_order
)
select abs(source_totals.paid_net_amount - gold_totals.paid_net_amount)
from source_totals cross join gold_totals
""",
    )
    _append_check(
        results,
        name="fact_order.reconciles_to_stg_orders",
        success=(paid_revenue_delta or 0) <= 0.01,
        detail={"absolute_delta": paid_revenue_delta},
    )

    item_reconcile_failures = _scalar(
        spark,
        """
with item_totals as (
  select order_id, round(sum(net_amount), 2) as item_net_amount
  from stg_order_items
  group by 1
),
order_totals as (
  select order_id, round(order_net_amount, 2) as order_net_amount
  from stg_orders
)
select count(*)
from order_totals
join item_totals using (order_id)
where abs(order_totals.order_net_amount - item_totals.item_net_amount) > 0.01
""",
    )
    _append_check(
        results,
        name="order_item_totals_reconcile",
        success=item_reconcile_failures == 0,
        detail={"mismatched_order_count": item_reconcile_failures},
    )

    observed_tables = {
        row.tableName for row in spark.sql("show tables in iceberg.gold").collect() if not row.isTemporary
    }
    missing_tables = sorted(set(REQUIRED_GOLD_TABLES) - observed_tables)
    _append_check(
        results,
        name="required_gold_inventory",
        success=not missing_tables,
        detail={"missing_tables": missing_tables, "observed_tables": sorted(observed_tables)},
    )

    report = {
        "success": all(result["success"] for result in results),
        "results": results,
    }
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "pyspark_validation_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    if not report["success"]:
        failed = [result["name"] for result in results if not result["success"]]
        raise RuntimeError(f"PySpark validations failed: {', '.join(failed)}")
    return report
