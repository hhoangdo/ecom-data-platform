from __future__ import annotations

import importlib.util
import importlib
import json
from pathlib import Path


def _module():
    path = Path(__file__).resolve().parents[2] / "src" / "vina_bim_shop" / "datahub_lineage" / "coursework_pipelines.py"
    spec = importlib.util.spec_from_file_location("coursework_pipelines", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _job(entities: dict[str, object], job_id: str) -> dict[str, object]:
    jobs = entities["data_jobs"]
    assert isinstance(jobs, list)
    return next(job for job in jobs if job["id"] == job_id)


def test_coursework_pipeline_entities_are_exact_and_idempotent() -> None:
    module = _module()
    first = module.coursework_pipeline_entities()
    second = module.coursework_pipeline_entities()

    assert first == second
    assert first["data_flow"]["id"] == module.DATAFLOW_ID == "mini_coursework_pipeline"
    assert first["data_flow"]["urn"] == module.COURSEWORK_DATAFLOW_URN
    assert module.DATAJOB_IDS == (
        "dp1_raw_to_bronze",
        "dp2_bronze_to_silver_gold",
        "dp3_offline_features",
    )

    dp1 = _job(first, "dp1_raw_to_bronze")
    assert dp1["inputs"] == [
        "urn:li:dataset:(urn:li:dataPlatform:kafka,commerce_events,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:kafka,catalog_events,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:kafka,fulfillment_events,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:kafka,ops_events,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:kafka,dead_letter_events,PROD)",
    ]
    assert dp1["outputs"] == [
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.batch,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.events,PROD)",
    ]

    dp2 = _job(first, "dp2_bronze_to_silver_gold")
    assert dp2["inputs"] == dp1["outputs"]
    assert dp2["outputs"] == [
        *(f"urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.{table},PROD)" for table in (
            "stg_customers", "stg_sellers", "stg_products", "stg_product_category_map",
            "stg_inventory_snapshots", "stg_promotions", "stg_orders", "stg_order_items",
            "stg_payments", "stg_shipments", "stg_catalog_events", "stg_commerce_events",
            "stg_fulfillment_events", "stg_ops_events", "stg_bad_snapshots",
            "dim_customer", "dim_seller", "dim_product", "dim_category", "dim_date",
            "dim_payment_method", "dim_order_status", "dim_shipment_status", "dim_shipping_method",
            "dim_promotion", "bridge_product_category", "fact_order", "fact_order_item",
            "fact_payment_attempt", "fact_shipment", "fact_inventory_snapshot",
            "fact_promotion_application", "obt_order_performance", "agg_hourly_reconciled_kpi",
        )),
    ]

    dp3 = _job(first, "dp3_offline_features")
    assert dp3["inputs"] == [
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.fact_order,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.stg_commerce_events,PROD)",
    ]
    assert dp3["outputs"] == [
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_90d,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_stream_60m,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_unified,PROD)",
    ]

    for job in first["data_jobs"]:
        assert job["inputs"]
        assert job["outputs"]
        assert len(job["inputs"]) == len(set(job["inputs"]))
        assert len(job["outputs"]) == len(set(job["outputs"]))


def test_pure_coursework_contract_does_not_require_the_docker_only_sdk() -> None:
    module = importlib.import_module("vina_bim_shop.datahub_lineage.coursework_pipelines")

    assert module.coursework_pipeline_entities()["data_flow"]["id"] == "mini_coursework_pipeline"


def test_coursework_assertion_targets_are_stable_and_linked_to_representative_outputs() -> None:
    module = _module()
    assertion_targets = module.ASSERTION_TARGETS
    assert assertion_targets["dp1_raw_to_bronze"]["representative_output"] == (
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.batch,PROD)"
    )
    assert assertion_targets["dp2_bronze_to_silver_gold"]["representative_output"] == (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.fact_order,PROD)"
    )
    assert assertion_targets["dp3_offline_features"]["representative_output"] == (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_unified,PROD)"
    )
    assert assertion_targets["dp3_offline_features"]["required_schema_fields"] == [
        "event_timestamp",
        "created",
    ]
    assert assertion_targets["dp3_offline_features"]["forbidden_schema_fields"] == ["created_ts"]
    assert module.COURSEWORK_SCHEMA_TARGETS == {
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.batch,PROD)": ["path"],
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.events,PROD)": ["path"],
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.fact_order,PROD)": [
            "order_id",
            "official_paid_revenue",
        ],
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_90d,PROD)": [
            "event_timestamp",
            "created",
        ],
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_stream_60m,PROD)": [
            "event_timestamp",
            "created",
        ],
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_unified,PROD)": [
            "event_timestamp",
            "created",
        ],
    }


def test_coursework_pipeline_metadata_emits_one_flow_and_three_jobs() -> None:
    class RecordingEmitter:
        def __init__(self) -> None:
            self.dataflows: list[dict[str, object]] = []
            self.datajobs: list[dict[str, object]] = []
            self.owners: list[tuple[str, str]] = []
            self.tags: list[tuple[str, list[str]]] = []

        def emit_dataflow(self, **payload: object) -> None:
            self.dataflows.append(payload)

        def emit_datajob(self, **payload: object) -> None:
            self.datajobs.append(payload)

        def emit_ownership(self, entity_urn: str, owner_urn: str, owner_type: str = "TECHNICAL_OWNER") -> None:
            assert owner_type == "TECHNICAL_OWNER"
            self.owners.append((entity_urn, owner_urn))

        def emit_tags(self, entity_urn: str, tag_names: list[str]) -> None:
            self.tags.append((entity_urn, tag_names))

    module = _module()
    emitter = RecordingEmitter()

    result = module.emit_coursework_pipeline_metadata(emitter=emitter)

    assert result["status"] == "success"
    assert result["dataflow_urn"] == module.COURSEWORK_DATAFLOW_URN
    assert result["datajobs_emitted"] == 3
    assert len(emitter.dataflows) == 1
    assert len(emitter.datajobs) == 3
    assert {job["entity_urn"] for job in emitter.datajobs} == {
        module.datajob_urn(module.DATAFLOW_ID, job_id) for job_id in module.DATAJOB_IDS
    }
    assert all(job["input_urns"] and job["output_urns"] for job in emitter.datajobs)
    assert len(emitter.owners) == 4
    assert len(emitter.tags) == 3


def test_feature_dataset_lineage_matches_the_dp3_sql_dependencies() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "vina_bim_shop"
        / "datahub_lineage"
        / "spark_lineage.py"
    ).read_text(encoding="utf-8")

    assert '"feat_customer_90d": ["fact_order"]' in source
    assert '"feat_stream_60m": ["stg_commerce_events"]' in source
    assert '"feat_customer_unified": ["feat_customer_90d", "feat_stream_60m"]' in source


def test_datahub_1_6_ownership_uses_attribute_lookup_not_enum_subscription() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "vina_bim_shop"
        / "datahub_lineage"
        / "emitter.py"
    ).read_text(encoding="utf-8")

    assert "OwnershipTypeClass[owner_type]" not in source
    assert "getattr(OwnershipTypeClass, owner_type)" in source


def test_coursework_assertion_specs_link_passing_contracts_to_dp_outputs(tmp_path: Path) -> None:
    quality_dir = tmp_path / "quality"
    quality_dir.mkdir()
    for name in (
        "coursework_bronze_contract",
        "coursework_core_gold_contract",
        "coursework_feature_contract",
    ):
        (quality_dir / f"{name}.json").write_text(json.dumps({"success": True}), encoding="utf-8")
    for name in ("dp1_validate", "dp2_validate", "dp3_validate"):
        (tmp_path / f"{name}.json").write_text(
            json.dumps({"run_id": "manual__2026-07-12T00:00:00+00:00"}),
            encoding="utf-8",
        )

    specs = _module().coursework_assertion_specs(tmp_path)

    assert len(specs) == 17
    assert {spec["dataset_urn"] for spec in specs if spec["job_id"] == "dp1_raw_to_bronze"} == {
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.batch,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:s3,bronze.events,PROD)",
    }
    assert {spec["dataset_urn"] for spec in specs if spec["job_id"] == "dp2_bronze_to_silver_gold"} == {
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.fact_order,PROD)"
    }
    assert {spec["dataset_urn"] for spec in specs if spec["job_id"] == "dp3_offline_features"} == {
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_90d,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_stream_60m,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.feat_customer_unified,PROD)",
    }
    assert all(spec["success"] is True for spec in specs)
    assert all(spec["run_id"] == "manual__2026-07-12T00:00:00+00:00" for spec in specs)
