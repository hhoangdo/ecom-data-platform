"""Stable metadata contract for the rubric-facing coursework pipeline."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ENVIRONMENT = "PROD"
AIRFLOW_CLUSTER = "vina-bim-shop-local"
DATAFLOW_ID = "mini_coursework_pipeline"
DATAJOB_IDS = (
    "dp1_raw_to_bronze",
    "dp2_bronze_to_silver_gold",
    "dp3_offline_features",
)

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

CORE_GOLD_TABLES = (
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
    "fact_order",
    "fact_order_item",
    "fact_payment_attempt",
    "fact_shipment",
    "fact_inventory_snapshot",
    "fact_promotion_application",
    "obt_order_performance",
    "agg_hourly_reconciled_kpi",
)

FEATURE_TABLES = (
    "feat_customer_90d",
    "feat_stream_60m",
    "feat_customer_unified",
)


def ice_urn(table_name: str, *, env: str = ENVIRONMENT) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.{table_name},{env})"


def kafka_urn(topic_name: str, *, env: str = ENVIRONMENT) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:kafka,{topic_name},{env})"


def pinot_urn(table_name: str, *, env: str = ENVIRONMENT) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:pinot,{table_name},{env})"


def s3_urn(prefix_name: str, *, env: str = ENVIRONMENT) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{prefix_name},{env})"


def dataflow_urn(dag_id: str = DATAFLOW_ID) -> str:
    return f"urn:li:dataFlow:(airflow,{dag_id},{AIRFLOW_CLUSTER})"


def datajob_urn(dag_id: str, task_id: str = "") -> str:
    flow_urn = dataflow_urn(dag_id)
    return f"urn:li:dataJob:({flow_urn},{task_id})" if task_id else flow_urn


COURSEWORK_DATAFLOW_URN = dataflow_urn()

ASSERTION_TARGETS: dict[str, dict[str, Any]] = {
    "dp1_raw_to_bronze": {
        "representative_output": s3_urn("bronze.batch"),
        "required_schema_fields": ["path"],
        "forbidden_schema_fields": [],
        "assertions": [
            "coursework_dp1_bronze_batch_path_not_null",
            "coursework_dp1_bronze_batch_object_count_min_1",
            "coursework_dp1_bronze_events_path_not_null",
            "coursework_dp1_bronze_events_object_count_min_1",
        ],
    },
    "dp2_bronze_to_silver_gold": {
        "representative_output": ice_urn("fact_order"),
        "required_schema_fields": ["order_id", "official_paid_revenue"],
        "forbidden_schema_fields": [],
        "assertions": ["coursework_dp2_fact_order_rows_min_1"],
    },
    "dp3_offline_features": {
        "representative_output": ice_urn("feat_customer_unified"),
        "required_schema_fields": ["event_timestamp", "created"],
        "forbidden_schema_fields": ["created_ts"],
        "assertions": [
            f"coursework_dp3_{table}_{check}"
            for table in FEATURE_TABLES
            for check in ("row_count_min_1", "has_event_timestamp", "has_created", "has_created_ts_false")
        ],
    },
}

# These schemas are emitted onto the canonical lineage outputs, never onto
# evidence-only duplicate URNs. They are the minimal displayable contract
# fields checked by the Topic 09 evidence gate.
COURSEWORK_SCHEMA_TARGETS: dict[str, list[str]] = {
    s3_urn("bronze.batch"): ["path"],
    s3_urn("bronze.events"): ["path"],
    ice_urn("fact_order"): ["order_id", "official_paid_revenue"],
    **{ice_urn(table): ["event_timestamp", "created"] for table in FEATURE_TABLES},
}


def coursework_pipeline_entities(env: str = ENVIRONMENT) -> dict[str, object]:
    """Return deterministic DataFlow/DataJob specs without importing the SDK."""
    bronze_outputs = [s3_urn("bronze.batch", env=env), s3_urn("bronze.events", env=env)]
    dp1_inputs = [
        kafka_urn(topic, env=env)
        for topic in (
            "commerce_events",
            "catalog_events",
            "fulfillment_events",
            "ops_events",
            "dead_letter_events",
        )
    ]
    dp2_outputs = [*(ice_urn(table, env=env) for table in SILVER_TABLES), *(ice_urn(table, env=env) for table in CORE_GOLD_TABLES)]
    dp3_inputs = [ice_urn("fact_order", env=env), ice_urn("stg_commerce_events", env=env)]
    dp3_outputs = [ice_urn(table, env=env) for table in FEATURE_TABLES]
    return {
        "data_flow": {
            "id": DATAFLOW_ID,
            "urn": dataflow_urn(DATAFLOW_ID),
            "name": "Mini Coursework Pipeline",
            "description": "Airflow TaskGroups for the DP1, DP2, and DP3 coursework stages.",
            "owner": "urn:li:corpuser:airflow",
        },
        "data_jobs": [
            {
                "id": "dp1_raw_to_bronze",
                "urn": datajob_urn(DATAFLOW_ID, "dp1_raw_to_bronze"),
                "name": "DP1: Raw to Bronze",
                "description": "Land raw batch and event inputs in the Bronze MinIO prefixes.",
                "inputs": dp1_inputs,
                "outputs": bronze_outputs,
                "tags": ["bronze", "quality_gate"],
            },
            {
                "id": "dp2_bronze_to_silver_gold",
                "urn": datajob_urn(DATAFLOW_ID, "dp2_bronze_to_silver_gold"),
                "name": "DP2: Bronze to Silver/Gold",
                "description": "Transform Bronze data into all Silver and core Gold Iceberg tables.",
                "inputs": bronze_outputs,
                "outputs": dp2_outputs,
                "tags": ["silver", "gold", "official", "quality_gate"],
            },
            {
                "id": "dp3_offline_features",
                "urn": datajob_urn(DATAFLOW_ID, "dp3_offline_features"),
                "name": "DP3: Offline Features",
                "description": "Compute and validate the three offline feature tables.",
                "inputs": dp3_inputs,
                "outputs": dp3_outputs,
                "tags": ["gold", "official", "quality_gate"],
            },
        ],
    }


def emit_coursework_pipeline_metadata(*, emitter: Any, env: str = ENVIRONMENT) -> dict[str, object]:
    """Emit the stable coursework DataFlow and aggregate DataJobs."""
    entities = coursework_pipeline_entities(env=env)
    data_flow = entities["data_flow"]
    assert isinstance(data_flow, dict)
    data_jobs = entities["data_jobs"]
    assert isinstance(data_jobs, list)

    emitter.emit_dataflow(
        entity_urn=data_flow["urn"],
        name=data_flow["name"],
        description=data_flow["description"],
    )
    emitter.emit_ownership(data_flow["urn"], data_flow["owner"])

    for data_job in data_jobs:
        assert isinstance(data_job, dict)
        emitter.emit_datajob(
            entity_urn=data_job["urn"],
            flow_urn=data_flow["urn"],
            name=data_job["name"],
            description=data_job["description"],
            input_urns=data_job["inputs"],
            output_urns=data_job["outputs"],
        )
        emitter.emit_ownership(data_job["urn"], data_flow["owner"])
        emitter.emit_tags(data_job["urn"], data_job["tags"])

    return {
        "status": "success",
        "dataflow_urn": data_flow["urn"],
        "datajobs_emitted": len(data_jobs),
        "input_output_edges": {
            data_job["id"]: {
                "inputs": data_job["inputs"],
                "outputs": data_job["outputs"],
            }
            for data_job in data_jobs
        },
    }


def emit_coursework_pipeline(*, gms_url: str = "http://datahub-gms:8080", env: str = ENVIRONMENT) -> dict[str, object]:
    """Create the SDK emitter lazily so the pure contract stays locally testable."""
    from vina_bim_shop.datahub_lineage.emitter import DataHubLineageEmitter

    return emit_coursework_pipeline_metadata(emitter=DataHubLineageEmitter(gms_url), env=env)


def coursework_assertion_specs(run_root: Path) -> list[dict[str, object]]:
    """Translate one successful six-stage run into stable dataset assertions."""
    validation_files = {
        "dp1_raw_to_bronze": ("dp1_validate.json", "coursework_bronze_contract.json"),
        "dp2_bronze_to_silver_gold": ("dp2_validate.json", "coursework_core_gold_contract.json"),
        "dp3_offline_features": ("dp3_validate.json", "coursework_feature_contract.json"),
    }
    run_ids: dict[str, str] = {}
    for job_id, (stage_name, quality_name) in validation_files.items():
        stage_path = run_root / stage_name
        quality_path = run_root / "quality" / quality_name
        stage = json.loads(stage_path.read_text(encoding="utf-8"))
        quality = json.loads(quality_path.read_text(encoding="utf-8"))
        if stage.get("state") not in {None, "success"} or quality.get("success") is not True:
            raise ValueError(f"Coursework validation is not successful for {job_id}")
        run_id = stage.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError(f"Coursework validation is missing run_id for {job_id}")
        run_ids[job_id] = run_id

    specs: list[dict[str, object]] = []
    for prefix_name in ("bronze.batch", "bronze.events"):
        prefix_slug = prefix_name.replace(".", "_")
        specs.extend(
            [
                {
                    "job_id": "dp1_raw_to_bronze",
                    "assertion_id": f"coursework_dp1_{prefix_slug}_path_not_null",
                    "dataset_urn": s3_urn(prefix_name),
                    "assertion_type": "expect_column_values_to_not_be_null",
                    "column": "path",
                    "success": True,
                    "run_id": run_ids["dp1_raw_to_bronze"],
                    "source_report": "coursework_bronze_contract.json",
                },
                {
                    "job_id": "dp1_raw_to_bronze",
                    "assertion_id": f"coursework_dp1_{prefix_slug}_object_count_min_1",
                    "dataset_urn": s3_urn(prefix_name),
                    "assertion_type": "expect_table_row_count_to_be_between",
                    "column": "",
                    "success": True,
                    "run_id": run_ids["dp1_raw_to_bronze"],
                    "source_report": "coursework_bronze_contract.json",
                },
            ]
        )
    specs.append(
        {
            "job_id": "dp2_bronze_to_silver_gold",
            "assertion_id": "coursework_dp2_fact_order_rows_min_1",
            "dataset_urn": ice_urn("fact_order"),
            "assertion_type": "expect_column_values_to_be_between",
            "column": "",
            "success": True,
            "run_id": run_ids["dp2_bronze_to_silver_gold"],
            "source_report": "coursework_core_gold_contract.json",
        }
    )
    for table_name in FEATURE_TABLES:
        for check in ("row_count_min_1", "has_event_timestamp", "has_created", "has_created_ts_false"):
            specs.append(
                {
                    "job_id": "dp3_offline_features",
                    "assertion_id": f"coursework_dp3_{table_name}_{check}",
                    "dataset_urn": ice_urn(table_name),
                    "assertion_type": check,
                    "column": "",
                    "success": True,
                    "run_id": run_ids["dp3_offline_features"],
                    "source_report": "coursework_feature_contract.json",
                }
            )
    return specs
