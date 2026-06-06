from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import great_expectations as gx
from pyspark.sql import DataFrame, SparkSession


def _serialize_result(result: Any) -> dict[str, Any]:
    to_json = getattr(result, "to_json_dict", None)
    if callable(to_json):
        payload = to_json()
    else:
        payload = {"repr": repr(result)}
    return json.loads(json.dumps(payload, default=str))


def _validate_dataframe(
    *,
    context: Any,
    datasource_name: str,
    asset_name: str,
    dataframe: DataFrame,
    expectations: list[Any],
) -> list[dict[str, Any]]:
    datasource = context.data_sources.add_spark(name=datasource_name)
    asset = datasource.add_dataframe_asset(name=asset_name)
    batch_definition = asset.add_batch_definition_whole_dataframe(f"{asset_name}_whole_dataframe")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

    results = []
    for expectation in expectations:
        results.append(_serialize_result(batch.validate(expectation)))
    return results


def run_gx_validations(*, spark: SparkSession, evidence_root: str | Path) -> dict[str, Any]:
    context = gx.get_context(mode="ephemeral")
    validations = {
        "stg_orders": _validate_dataframe(
            context=context,
            datasource_name="spark_runtime_stg_orders",
            asset_name="stg_orders_asset",
            dataframe=spark.table("iceberg.silver.stg_orders"),
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
                gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
            ],
        ),
        "fact_order": _validate_dataframe(
            context=context,
            datasource_name="spark_runtime_fact_order",
            asset_name="fact_order_asset",
            dataframe=spark.table("iceberg.gold.fact_order"),
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="official_paid_revenue", min_value=0),
            ],
        ),
        "fact_order_item": _validate_dataframe(
            context=context,
            datasource_name="spark_runtime_fact_order_item",
            asset_name="fact_order_item_asset",
            dataframe=spark.table("iceberg.gold.fact_order_item"),
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull(column="order_item_id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="estimated_margin"),
            ],
        ),
        "agg_hourly_reconciled_kpi": _validate_dataframe(
            context=context,
            datasource_name="spark_runtime_agg_hourly",
            asset_name="agg_hourly_asset",
            dataframe=spark.table("iceberg.gold.agg_hourly_reconciled_kpi"),
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull(column="metric_hour"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="official_paid_revenue", min_value=0),
            ],
        ),
    }

    report = {
        "success": all(
            expectation_result.get("success", False)
            for validation_results in validations.values()
            for expectation_result in validation_results
        ),
        "validations": validations,
    }

    gx_root = Path(evidence_root) / "gx"
    gx_root.mkdir(parents=True, exist_ok=True)
    (gx_root / "validation_results.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if not report["success"]:
        raise RuntimeError("Great Expectations validations failed.")
    return report
