from __future__ import annotations

from pathlib import Path

from vina_bim_shop.lakehouse.spark.constants import (
    BRONZE_OPTIONAL_JSON_DATASETS,
    SILVER_PARTITIONED_BY,
    SILVER_TABLES,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
JOB_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "lakehouse" / "spark" / "job.py"
SQL_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "lakehouse" / "spark" / "sql.py"
VALIDATION_PATH = REPO_ROOT / "src" / "vina_bim_shop" / "lakehouse" / "spark" / "validation.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------------
# 1. Skew policy
# ---------------------------------------------------------------------------------


def test_canonical_batch_path_does_not_salt() -> None:
    """The standalone experiment may salt; the production batch path may not."""
    canonical_paths = (
        REPO_ROOT / "src" / "vina_bim_shop" / "lakehouse" / "spark" / "job.py",
        REPO_ROOT / "src" / "vina_bim_shop" / "lakehouse" / "spark" / "runner.py",
        REPO_ROOT / "scripts" / "spark" / "run_batch.py",
    )
    for path in canonical_paths:
        contents = path.read_text(encoding="utf-8")
        for forbidden in ("salt(", ".salting", "_salt", "salt_explode", "salt_factor"):
            assert forbidden not in contents, f"Unexpected salting reference in {path}: {forbidden}"


def test_spark_uses_pyspark_dataframes_and_spark_sql() -> None:
    job_text = _read(JOB_PATH)
    assert "from pyspark.sql import DataFrame, SparkSession, Window" in job_text
    assert "from pyspark.sql import functions as F" in job_text
    assert "spark.read.parquet(" in job_text
    assert "spark.read.json(" in job_text
    assert "spark.sql(" in job_text
    assert "CREATE OR REPLACE TABLE" in job_text
    assert "MERGE INTO" in job_text


# ---------------------------------------------------------------------------------
# 2. Dedup, windowing, key strategy
# ---------------------------------------------------------------------------------


def test_job_uses_window_partition_by_row_number_for_dedupe() -> None:
    job_text = _read(JOB_PATH)
    assert "Window.partitionBy(" in job_text
    assert "F.row_number().over(window)" in job_text
    assert "orderBy(F.col(order_column).desc())" in job_text


def test_silver_event_builders_preserve_schema_version() -> None:
    job_text = _read(JOB_PATH)
    builder_section = job_text.split("def _build_silver_tables_for_window", 1)[1]
    assert builder_section.count('F.col("schema_version").cast("int").alias("schema_version")') == 4
    assert 'key_columns=["event_id"]' in builder_section
    for event_kind in ("commerce_events", "catalog_events", "fulfillment_events", "ops_events"):
        assert f"raw_kafka_{event_kind}" in builder_section


def test_silver_event_builders_filter_by_event_window() -> None:
    job_text = _read(JOB_PATH)
    builder_section = job_text.split("def _build_silver_tables_for_window", 1)[1]
    assert builder_section.count("(F.col(\"event_timestamp\") >= start)") == 4
    assert builder_section.count("(F.col(\"event_timestamp\") < end)") == 4


# ---------------------------------------------------------------------------------
# 3. Nullable dimensions
# ---------------------------------------------------------------------------------


def test_sql_coalesces_shipping_method_to_unknown() -> None:
    sql_text = _read(SQL_PATH)
    assert sql_text.count("coalesce(shipping_method, 'unknown')") >= 2


def test_sql_normalizes_product_brand_to_unknown() -> None:
    sql_text = _read(SQL_PATH)
    assert "coalesce(p.brand, 'unknown') as brand" in sql_text


def test_silver_stg_products_builder_preserves_raw_brand() -> None:
    """`stg_products` (Silver) is built in job.py by dedupe; brand is not normalized
    at the Silver layer so raw nulls survive for downstream dimension handling."""
    job_text = _read(JOB_PATH)
    builder_text = job_text.split("def _build_silver_tables_for_window", 1)[1]
    assert '"stg_products"' in builder_text
    assert "_dedupe_latest(spark.table(\"raw_products\")" in builder_text
    # The Silver builder does not mention brand at all: it is a passthrough.
    stg_products_block = builder_text.split('"stg_products"', 1)[1].split("),", 1)[0]
    assert "brand" not in stg_products_block.lower()


# ---------------------------------------------------------------------------------
# 4. Schema evolution
# ---------------------------------------------------------------------------------


def test_silver_event_builders_extract_json_fields_via_get_json_object() -> None:
    job_text = _read(JOB_PATH)
    builder_section = job_text.split("def _build_silver_tables_for_window", 1)[1]
    assert "_json_string(" in builder_section
    assert "_json_double(" in builder_section
    assert "_json_int(" in builder_section
    assert 'F.get_json_object' in job_text


# ---------------------------------------------------------------------------------
# 5. Bad events and bad snapshots (quarantine)
# ---------------------------------------------------------------------------------


def test_job_registers_raw_bad_events_view() -> None:
    job_text = _read(JOB_PATH)
    assert "raw_bad_events" in job_text
    assert "_read_optional_dead_letter_events" in job_text


def test_job_registers_raw_bad_snapshots_view() -> None:
    job_text = _read(JOB_PATH)
    assert "raw_bad_snapshots" in job_text
    assert "_read_optional_bad_snapshots" in job_text
    assert "_resolve_bad_snapshots_file" in job_text


def test_resolve_bad_snapshots_file_uses_explicit_path() -> None:
    """The reader accepts a Path-like raw_root; the default honours VBS_RAW_ROOT or
    falls back to "data/raw". The Spark module imports pyspark, so this test pins
    the contract by text only; full import-based coverage runs in pyspark-enabled
    CI via the integration tests."""
    job_text = _read(JOB_PATH)
    assert "def _resolve_bad_snapshots_file(raw_root" in job_text
    assert "Path(os.environ.get(\"VBS_RAW_ROOT\", \"data/raw\"))" in job_text
    assert "raw_root) / \"bad_snapshots\" / \"bad_snapshots.jsonl\"" in job_text


def test_constants_include_stg_bad_snapshots_in_silver_inventory() -> None:
    assert "stg_bad_snapshots" in SILVER_TABLES
    assert "bad_snapshots" in BRONZE_OPTIONAL_JSON_DATASETS
    assert SILVER_PARTITIONED_BY.get("stg_bad_snapshots") == ("days(ingest_ts)",)


def test_validation_pins_stg_bad_snapshots_and_dim_product_brand() -> None:
    validation_text = _read(VALIDATION_PATH)
    assert '("stg_bad_snapshots", "bad_record_id")' in validation_text
    assert "dim_product where brand is null" in validation_text


# ---------------------------------------------------------------------------------
# 6. Schema versions and dedupe keys
# ---------------------------------------------------------------------------------


def test_silver_table_inventory_size_matches_after_change() -> None:
    """`stg_bad_snapshots` was added, so SILVER_TABLES must now contain 15 entries."""
    assert len(SILVER_TABLES) == 15


# ---------------------------------------------------------------------------------
# 7. Category skew tolerance
# ---------------------------------------------------------------------------------


def test_sql_defines_category_cost_rate_per_category() -> None:
    sql_text = _read(SQL_PATH)
    assert "category_cost_rate_sql" in sql_text
    assert "'FMCG'" in sql_text
    assert "'ELHA'" in sql_text
    assert "category_cost_rate" in sql_text


# ---------------------------------------------------------------------------------
# 8. Operational signals
# ---------------------------------------------------------------------------------


def test_job_creates_silver_ops_events() -> None:
    job_text = _read(JOB_PATH)
    builder_section = job_text.split("def _build_silver_tables_for_window", 1)[1]
    assert "stg_ops_events" in builder_section
    assert "burst_event_count" in builder_section
    assert "late_event_count" in builder_section
    assert "duplicate_event_count" in builder_section


# ---------------------------------------------------------------------------------
# 9. Cross-link to solution map
# ---------------------------------------------------------------------------------


def test_deliverable_11_solving_data_challenges_exists_and_links_engines() -> None:
    deliverable = (REPO_ROOT / "deliverables" / "11_solving_data_challenges.md").read_text(encoding="utf-8")
    assert "Spark" in deliverable
    assert "Flink" in deliverable
    assert "standalone optimization experiment" in deliverable.lower()
    assert "canonical Spark batch path does not use salting" in deliverable


def test_data_generator_deliverable_links_to_solution_map() -> None:
    text = (REPO_ROOT / "deliverables" / "01_data_generator.md").read_text(encoding="utf-8")
    assert "11_solving_data_challenges.md" in text


def test_spark_deliverable_links_to_solution_map() -> None:
    text = (REPO_ROOT / "deliverables" / "05_spark_batch.md").read_text(encoding="utf-8")
    assert "11_solving_data_challenges.md" in text
    assert "PySpark" in text or "pyspark" in text.lower()


def test_flink_deliverable_links_to_solution_map() -> None:
    text = (REPO_ROOT / "deliverables" / "06_flink_streaming.md").read_text(encoding="utf-8")
    assert "11_solving_data_challenges.md" in text
    assert "dedupe" in text.lower()
    assert "checkpointing" in text.lower() or "checkpoint" in text.lower()
