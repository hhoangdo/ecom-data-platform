from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from vina_bim_shop.lakehouse.spark.constants import (
    BRONZE_BATCH_DATASETS,
    BRONZE_EVENT_TOPICS,
    GOLD_PARTITIONED_BY,
    REQUIRED_GOLD_TABLES,
    SILVER_PARTITIONED_BY,
    SILVER_TABLES,
)
from vina_bim_shop.lakehouse.spark.gx_validation import run_gx_validations
from vina_bim_shop.lakehouse.spark.layout_profile import (
    COMPACTION_EVIDENCE_BUCKET_COUNT,
    COMPACTION_EVIDENCE_LAYOUT_PROFILE,
    STANDARD_LAYOUT_PROFILE,
    compaction_evidence_target,
    validate_layout_profile,
)
from vina_bim_shop.lakehouse.spark.sql import (
    ordered_core_gold_queries,
    ordered_feature_queries,
    ordered_gold_queries,
)
from vina_bim_shop.lakehouse.spark.validation import run_pyspark_validations
from vina_bim_shop.lakehouse.spark.window import BatchWindow


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Spark batch Iceberg job.")
    parser.add_argument("--start-ts", required=True)
    parser.add_argument("--end-ts", required=True)
    parser.add_argument("--mode", required=True, choices=["hourly", "backfill"])
    parser.add_argument("--evidence-root", default="evidence/05_spark_batch")
    parser.add_argument("--stage", default="full", choices=["full", "core", "features"])
    parser.add_argument(
        "--layout-profile",
        default=STANDARD_LAYOUT_PROFILE,
        choices=[STANDARD_LAYOUT_PROFILE, COMPACTION_EVIDENCE_LAYOUT_PROFILE],
    )
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("vina-bim-shop-batch")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.iceberg.writer.mode", "hash")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://silver/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _window_dates(window: BatchWindow) -> list[str]:
    cursor = window.start_ts.date()
    end_date = (window.end_ts - window.end_ts.resolution).date()
    dates = []
    while cursor <= end_date:
        dates.append(cursor.isoformat())
        cursor = cursor.fromordinal(cursor.toordinal() + 1)
    return dates


def _table_location(layer: str, table_name: str) -> str:
    bucket_env = f"VBS_{layer.upper()}_BUCKET"
    bucket = os.getenv(bucket_env, layer)
    return f"s3a://{bucket}/warehouse/{layer}/{table_name}"


def _path_exists(spark: SparkSession, path_pattern: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(path_pattern)
    fs = path.getFileSystem(hadoop_conf)
    matches = fs.globStatus(path)
    return matches is not None and len(matches) > 0


def _existing_paths(spark: SparkSession, path_patterns: Iterable[str]) -> list[str]:
    return [path for path in path_patterns if _path_exists(spark, path)]


def _batch_paths(dataset: str, dates: Iterable[str]) -> list[str]:
    return [f"s3a://bronze/batch/{dataset}/snapshot_date={snapshot_date}/*" for snapshot_date in dates]


def _event_paths(topic: str, dates: Iterable[str]) -> list[str]:
    return [f"s3a://bronze/events/{topic}/ingest_date={ingest_date}/*" for ingest_date in dates]


def _read_required_parquet(spark: SparkSession, dataset: str, dates: list[str]) -> DataFrame:
    paths = _existing_paths(spark, _batch_paths(dataset, dates))
    if not paths:
        raise RuntimeError(f"Missing Bronze batch inputs for dataset {dataset!r} and dates {dates}.")
    return spark.read.parquet(*paths).withColumn("ingest_ts", F.current_timestamp())


def _read_required_json(spark: SparkSession, *, topic: str, dates: list[str]) -> DataFrame:
    paths = _existing_paths(spark, _event_paths(topic, dates))
    if not paths:
        raise RuntimeError(f"Missing Bronze event inputs for topic {topic!r} and dates {dates}.")
    return spark.read.json(paths).withColumn("ingest_ts", F.current_timestamp())


def _read_optional_dead_letter_events(spark: SparkSession, dates: list[str]) -> DataFrame:
    paths = _existing_paths(spark, _event_paths("dead_letter_events", dates))
    if paths:
        return spark.read.json(paths).select(
            "dlq_id",
            "source_topic",
            "raw_payload",
            "error_reason",
            "event_topic",
            "schema_version",
            F.col("ingest_ts").cast("timestamp").alias("ingest_ts"),
        )
    return spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("dlq_id", T.StringType(), True),
                T.StructField("source_topic", T.StringType(), True),
                T.StructField("raw_payload", T.StringType(), True),
                T.StructField("error_reason", T.StringType(), True),
                T.StructField("event_topic", T.StringType(), True),
                T.StructField("schema_version", T.LongType(), True),
                T.StructField("ingest_ts", T.TimestampType(), True),
            ]
        ),
    )


def _empty_bad_snapshots(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("bad_record_id", T.StringType(), True),
                T.StructField("source_dataset", T.StringType(), True),
                T.StructField("raw_record", T.StringType(), True),
                T.StructField("error_reason", T.StringType(), True),
                T.StructField("ingest_ts", T.TimestampType(), True),
            ]
        ),
    )


def _resolve_bad_snapshots_file(raw_root: str | os.PathLike[str] | None) -> Path:
    if raw_root is not None:
        return Path(raw_root) / "bad_snapshots" / "bad_snapshots.jsonl"
    return Path(os.environ.get("VBS_RAW_ROOT", "data/raw")) / "bad_snapshots" / "bad_snapshots.jsonl"


def _read_optional_bad_snapshots(
    spark: SparkSession,
    raw_root: str | os.PathLike[str] | None = None,
) -> DataFrame:
    snapshot_file = _resolve_bad_snapshots_file(raw_root)
    if not snapshot_file.is_file():
        return _empty_bad_snapshots(spark)
    dataframe = spark.read.json(str(snapshot_file)).withColumn("ingest_ts", F.current_timestamp())
    return dataframe.select(
        F.col("bad_record_id").cast("string").alias("bad_record_id"),
        F.col("source_dataset").cast("string").alias("source_dataset"),
        F.col("raw_record").cast("string").alias("raw_record"),
        F.col("error_reason").cast("string").alias("error_reason"),
        F.col("ingest_ts").cast("timestamp").alias("ingest_ts"),
    )


def _create_raw_views(spark: SparkSession, window: BatchWindow) -> None:
    dates = _window_dates(window)
    for dataset in BRONZE_BATCH_DATASETS:
        _read_required_parquet(spark, dataset, dates).createOrReplaceTempView(f"raw_{dataset}")

    for topic in BRONZE_EVENT_TOPICS:
        _read_required_json(spark, topic=topic, dates=dates).createOrReplaceTempView(f"raw_kafka_{topic}")

    _read_optional_dead_letter_events(spark, dates).createOrReplaceTempView("raw_bad_events")
    _read_optional_bad_snapshots(spark).createOrReplaceTempView("raw_bad_snapshots")


def _dedupe_latest(df: DataFrame, *, key_columns: list[str], order_column: str) -> DataFrame:
    window = Window.partitionBy(*[F.col(column_name) for column_name in key_columns]).orderBy(F.col(order_column).desc())
    return df.withColumn("_row_num", F.row_number().over(window)).where(F.col("_row_num") == 1).drop("_row_num")


def _json_string(column_name: str, path: str):
    return F.get_json_object(F.to_json(F.col(column_name)), path)


def _json_double(column_name: str, path: str):
    return _json_string(column_name, path).cast("double")


def _json_int(column_name: str, path: str):
    return _json_string(column_name, path).cast("int")


def _build_silver_tables_for_window(
    spark: SparkSession,
    *,
    start_ts: str,
    end_ts: str,
) -> list[tuple[str, DataFrame, list[str], str]]:
    start = F.lit(start_ts).cast("timestamp")
    end = F.lit(end_ts).cast("timestamp")
    silver_tables = [
        ("stg_customers", _dedupe_latest(spark.table("raw_customers"), key_columns=["customer_id"], order_column="created_ts"), ["customer_id"], "created_ts"),
        ("stg_sellers", _dedupe_latest(spark.table("raw_sellers"), key_columns=["seller_id"], order_column="created_ts"), ["seller_id"], "created_ts"),
        ("stg_products", _dedupe_latest(spark.table("raw_products"), key_columns=["product_id"], order_column="created_ts"), ["product_id"], "created_ts"),
        (
            "stg_product_category_map",
            _dedupe_latest(
                spark.table("raw_product_category_map"),
                key_columns=["product_id", "category", "subcategory"],
                order_column="assigned_ts",
            ),
            ["product_id", "category", "subcategory"],
            "assigned_ts",
        ),
        (
            "stg_inventory_snapshots",
            _dedupe_latest(spark.table("raw_inventory_snapshots"), key_columns=["snapshot_id"], order_column="snapshot_ts"),
            ["snapshot_id"],
            "snapshot_ts",
        ),
        ("stg_orders", _dedupe_latest(spark.table("raw_orders"), key_columns=["order_id"], order_column="created_ts"), ["order_id"], "created_ts"),
        (
            "stg_order_items",
            _dedupe_latest(spark.table("raw_order_items"), key_columns=["order_item_id"], order_column="created_ts"),
            ["order_item_id"],
            "created_ts",
        ),
        ("stg_payments", _dedupe_latest(spark.table("raw_payments"), key_columns=["payment_id"], order_column="created_ts"), ["payment_id"], "created_ts"),
        (
            "stg_shipments",
            _dedupe_latest(spark.table("raw_shipments"), key_columns=["shipment_id"], order_column="created_ts"),
            ["shipment_id"],
            "created_ts",
        ),
    ]

    promotions = _dedupe_latest(spark.table("raw_promotions"), key_columns=["promotion_id"], order_column="created_ts").withColumn(
        "platform_funding_share",
        F.when(F.col("funding_type") == "platform", F.lit(1.0))
        .when(F.col("funding_type") == "seller", F.lit(0.0))
        .when(F.col("funding_type") == "mixed", F.coalesce(F.get_json_object(F.col("funding_detail"), "$.platform_share").cast("double"), F.lit(0.5)))
        .otherwise(F.lit(0.0)),
    ).withColumn(
        "seller_funding_share",
        F.when(F.col("funding_type") == "platform", F.lit(0.0))
        .when(F.col("funding_type") == "seller", F.lit(1.0))
        .when(F.col("funding_type") == "mixed", F.coalesce(F.get_json_object(F.col("funding_detail"), "$.seller_share").cast("double"), F.lit(0.5)))
        .otherwise(F.lit(1.0)),
    )
    silver_tables.append(("stg_promotions", promotions, ["promotion_id"], "created_ts"))

    commerce_events = _dedupe_latest(
        spark.table("raw_kafka_commerce_events").select(
            "event_id",
            "event_type",
            "event_topic",
            F.col("schema_version").cast("int").alias("schema_version"),
            F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
            F.col("created_ts").cast("timestamp").alias("created_ts"),
            "producer",
            _json_string("correlation_ids", "$.session_id").alias("session_id"),
            _json_string("correlation_ids", "$.anonymous_id").alias("anonymous_id"),
            _json_string("correlation_ids", "$.customer_id").alias("customer_id"),
            F.coalesce(_json_string("correlation_ids", "$.product_id"), _json_string("payload", "$.product_id")).alias("product_id"),
            F.coalesce(_json_string("correlation_ids", "$.order_id"), _json_string("payload", "$.order_id")).alias("order_id"),
            _json_string("correlation_ids", "$.payment_id").alias("payment_id"),
            _json_string("payload", "$.primary_category").alias("primary_category"),
            _json_string("payload", "$.device_type").alias("device_type"),
            _json_string("payload", "$.source").alias("source"),
            _json_string("payload", "$.order_status").alias("order_status"),
            _json_double("payload", "$.order_net_amount").alias("order_net_amount"),
            _json_string("payload", "$.payment_method").alias("payment_method"),
            _json_double("payload", "$.amount").alias("payment_amount"),
            _json_string("payload", "$.failure_reason").alias("failure_reason"),
            "payload",
            "ingest_ts",
        ),
        key_columns=["event_id"],
        order_column="created_ts",
    ).where((F.col("event_timestamp") >= start) & (F.col("event_timestamp") < end))
    silver_tables.append(("stg_commerce_events", commerce_events, ["event_id"], "created_ts"))

    catalog_events = _dedupe_latest(
        spark.table("raw_kafka_catalog_events").select(
            "event_id",
            "event_type",
            "event_topic",
            F.col("schema_version").cast("int").alias("schema_version"),
            F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
            F.col("created_ts").cast("timestamp").alias("created_ts"),
            "producer",
            _json_string("correlation_ids", "$.snapshot_id").alias("snapshot_id"),
            _json_string("correlation_ids", "$.product_id").alias("product_id"),
            _json_string("correlation_ids", "$.seller_id").alias("seller_id"),
            _json_string("correlation_ids", "$.promotion_id").alias("promotion_id"),
            _json_string("payload", "$.category").alias("category"),
            _json_double("payload", "$.stock_on_hand").alias("stock_on_hand"),
            _json_double("payload", "$.discount_rate").alias("discount_rate"),
            "payload",
            "ingest_ts",
        ),
        key_columns=["event_id"],
        order_column="created_ts",
    ).where((F.col("event_timestamp") >= start) & (F.col("event_timestamp") < end))
    silver_tables.append(("stg_catalog_events", catalog_events, ["event_id"], "created_ts"))

    fulfillment_events = _dedupe_latest(
        spark.table("raw_kafka_fulfillment_events").select(
            "event_id",
            "event_type",
            "event_topic",
            F.col("schema_version").cast("int").alias("schema_version"),
            F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
            F.col("created_ts").cast("timestamp").alias("created_ts"),
            "producer",
            _json_string("correlation_ids", "$.shipment_id").alias("shipment_id"),
            _json_string("correlation_ids", "$.order_id").alias("order_id"),
            _json_string("correlation_ids", "$.customer_id").alias("customer_id"),
            _json_string("payload", "$.shipment_status").alias("shipment_status"),
            _json_string("payload", "$.shipping_city").alias("shipping_city"),
            _json_string("payload", "$.shipping_method").alias("shipping_method"),
            "payload",
            "ingest_ts",
        ),
        key_columns=["event_id"],
        order_column="created_ts",
    ).where((F.col("event_timestamp") >= start) & (F.col("event_timestamp") < end))
    silver_tables.append(("stg_fulfillment_events", fulfillment_events, ["event_id"], "created_ts"))

    ops_events = _dedupe_latest(
        spark.table("raw_kafka_ops_events").select(
            "event_id",
            "event_type",
            "event_topic",
            F.col("schema_version").cast("int").alias("schema_version"),
            F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
            F.col("created_ts").cast("timestamp").alias("created_ts"),
            "producer",
            _json_int("payload", "$.burst_event_count").alias("burst_event_count"),
            _json_int("payload", "$.late_event_count").alias("late_event_count"),
            _json_int("payload", "$.duplicate_event_count").alias("duplicate_event_count"),
            "payload",
            "ingest_ts",
        ),
        key_columns=["event_id"],
        order_column="created_ts",
    ).where((F.col("event_timestamp") >= start) & (F.col("event_timestamp") < end))
    silver_tables.append(("stg_ops_events", ops_events, ["event_id"], "created_ts"))

    bad_snapshots = _dedupe_latest(
        spark.table("raw_bad_snapshots"),
        key_columns=["bad_record_id"],
        order_column="ingest_ts",
    )
    silver_tables.append(("stg_bad_snapshots", bad_snapshots, ["bad_record_id"], "ingest_ts"))

    return silver_tables


def _partition_clause(partition_expressions: tuple[str, ...]) -> str:
    if not partition_expressions:
        return ""
    return f"\nPARTITIONED BY ({', '.join(partition_expressions)})"


def _create_namespace(spark: SparkSession, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def _create_table_if_missing(
    *,
    spark: SparkSession,
    full_table_name: str,
    layer: str,
    table_name: str,
    source_view_name: str,
    partition_expressions: tuple[str, ...] = (),
) -> None:
    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {full_table_name}
USING iceberg
{_partition_clause(partition_expressions)}
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM {source_view_name}
""".strip()
    )


def _merge_into_table(
    *,
    spark: SparkSession,
    full_table_name: str,
    source_view_name: str,
    key_columns: list[str],
    order_column: str,
) -> None:
    columns = spark.table(source_view_name).columns
    key_predicate = " AND ".join([f"t.{column_name} <=> s.{column_name}" for column_name in key_columns])
    assignments = ", ".join([f"{column_name} = s.{column_name}" for column_name in columns])
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"s.{column_name}" for column_name in columns])
    spark.sql(
        f"""
MERGE INTO {full_table_name} t
USING {source_view_name} s
ON {key_predicate}
WHEN MATCHED AND s.{order_column} >= t.{order_column} THEN
  UPDATE SET {assignments}
WHEN NOT MATCHED THEN
  INSERT ({insert_columns}) VALUES ({insert_values})
""".strip()
    )


def _persist_silver_tables_for_window(
    spark: SparkSession,
    *,
    start_ts: str,
    end_ts: str,
) -> None:
    _create_namespace(spark, "iceberg.silver")
    for table_name, dataframe, key_columns, order_column in _build_silver_tables_for_window(
        spark,
        start_ts=start_ts,
        end_ts=end_ts,
    ):
        source_view_name = f"__source_{table_name}"
        dataframe.createOrReplaceTempView(source_view_name)
        full_table_name = f"iceberg.silver.{table_name}"
        _create_table_if_missing(
            spark=spark,
            full_table_name=full_table_name,
            layer="silver",
            table_name=table_name,
            source_view_name=source_view_name,
            partition_expressions=SILVER_PARTITIONED_BY.get(table_name, ()),
        )
        _merge_into_table(
            spark=spark,
            full_table_name=full_table_name,
            source_view_name=source_view_name,
            key_columns=key_columns,
            order_column=order_column,
        )
        spark.table(full_table_name).createOrReplaceTempView(table_name)


def _replace_gold_table(
    *,
    spark: SparkSession,
    table_name: str,
    query: str,
) -> None:
    spark.sql(
        f"""
CREATE OR REPLACE TABLE iceberg.gold.{table_name}
USING iceberg
{_partition_clause(GOLD_PARTITIONED_BY.get(table_name, ()))}
TBLPROPERTIES ('format-version'='2')
AS
{query}
""".strip()
    )
    spark.table(f"iceberg.gold.{table_name}").createOrReplaceTempView(table_name)


def _restore_spark_configuration(spark: SparkSession, key: str, previous_value: str | None) -> None:
    if previous_value is None:
        spark.conf.unset(key)
    else:
        spark.conf.set(key, previous_value)


def _compaction_evidence_bucket_rows(
    dataframe: DataFrame,
    *,
    stable_key: str,
    partition_column: str,
) -> tuple[DataFrame, list[dict[str, object]]]:
    bucket_column = "__compaction_evidence_bucket"
    if dataframe.where(F.col(stable_key).isNull() | F.col(partition_column).isNull()).limit(1).count():
        raise ValueError(
            f"Compaction-evidence layout requires non-null {stable_key} and {partition_column} values."
        )
    bucketed = dataframe.withColumn(
        bucket_column,
        F.pmod(F.xxhash64(F.col(stable_key)), F.lit(COMPACTION_EVIDENCE_BUCKET_COUNT)),
    )
    rows = [
        row.asDict(recursive=True)
        for row in (
            bucketed.groupBy(partition_column, bucket_column)
            .count()
            .orderBy(partition_column, bucket_column)
            .collect()
        )
    ]
    partitions_with_both_buckets: dict[object, set[int]] = {}
    for row in rows:
        partitions_with_both_buckets.setdefault(row[partition_column], set()).add(int(row[bucket_column]))
    if not any(len(buckets) == COMPACTION_EVIDENCE_BUCKET_COUNT for buckets in partitions_with_both_buckets.values()):
        raise RuntimeError("Compaction-evidence layout has no partition with both deterministic buckets.")
    return bucketed, rows


def _gold_data_file_partition_counts(
    spark: SparkSession,
    *,
    table_name: str,
    partition_column: str,
) -> list[dict[str, object]]:
    statement = (
        "select "
        f"partition.{partition_column} as partition_value, "
        "count(*) as file_count, "
        "sum(record_count) as record_count, "
        "sum(file_size_in_bytes) as total_bytes "
        f"from iceberg.gold.{table_name}.files "
        "where content = 0 "
        f"group by partition.{partition_column} "
        "order by partition_value"
    )
    return [row.asDict(recursive=True) for row in spark.sql(statement).collect()]


def _replace_gold_table_for_compaction_evidence(
    *,
    spark: SparkSession,
    table_name: str,
    query: str,
    stable_key: str,
    partition_column: str,
) -> dict[str, object]:
    bucketed, bucket_rows = _compaction_evidence_bucket_rows(
        spark.sql(query),
        stable_key=stable_key,
        partition_column=partition_column,
    )
    distribution_key = "spark.sql.iceberg.distribution-mode"
    previous_distribution = spark.conf.get(distribution_key, None)
    try:
        spark.conf.set(distribution_key, "none")
        (
            bucketed.repartitionByRange(
                len(bucket_rows),
                F.col(partition_column),
                F.col("__compaction_evidence_bucket"),
            )
            .sortWithinPartitions(partition_column)
            .drop("__compaction_evidence_bucket")
            .writeTo(f"iceberg.gold.{table_name}")
            .using("iceberg")
            .partitionedBy(F.col(partition_column))
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
    finally:
        _restore_spark_configuration(spark, distribution_key, previous_distribution)

    partition_files = _gold_data_file_partition_counts(
        spark,
        table_name=table_name,
        partition_column=partition_column,
    )
    candidate_partitions = [
        row["partition_value"]
        for row in partition_files
        if int(row["file_count"]) >= COMPACTION_EVIDENCE_BUCKET_COUNT
    ]
    if not candidate_partitions:
        raise RuntimeError(f"Compaction-evidence layout did not create a two-file partition for {table_name}.")
    spark.table(f"iceberg.gold.{table_name}").createOrReplaceTempView(table_name)
    return {
        "table": f"gold.{table_name}",
        "stable_key": stable_key,
        "partition_column": partition_column,
        "bucket_count": COMPACTION_EVIDENCE_BUCKET_COUNT,
        "bucket_row_counts": bucket_rows,
        "data_file_partition_counts": partition_files,
        "candidate_partitions": candidate_partitions,
    }


def _register_persisted_tables(spark: SparkSession, *, namespace: str, table_names: tuple[str, ...]) -> None:
    for table_name in table_names:
        spark.table(f"{namespace}.{table_name}").createOrReplaceTempView(table_name)


def _persist_gold_query_group(
    spark: SparkSession,
    queries: list[tuple[str, str]],
    *,
    layout_profile: str = STANDARD_LAYOUT_PROFILE,
) -> list[dict[str, object]]:
    _create_namespace(spark, "iceberg.gold")
    _register_persisted_tables(spark, namespace="iceberg.silver", table_names=SILVER_TABLES)

    layout_results = []
    for table_name, query in queries:
        target = compaction_evidence_target(table_name)
        if layout_profile == COMPACTION_EVIDENCE_LAYOUT_PROFILE and target:
            stable_key, partition_column = target
            layout_results.append(
                _replace_gold_table_for_compaction_evidence(
                    spark=spark,
                    table_name=table_name,
                    query=query,
                    stable_key=stable_key,
                    partition_column=partition_column,
                )
            )
        else:
            _replace_gold_table(spark=spark, table_name=table_name, query=query)
    return layout_results


def _persist_gold_tables(
    spark: SparkSession,
    *,
    layout_profile: str = STANDARD_LAYOUT_PROFILE,
) -> list[dict[str, object]]:
    return _persist_gold_query_group(spark, ordered_gold_queries(), layout_profile=layout_profile)


def _capture_table_row_counts(
    *,
    spark: SparkSession,
    evidence_root: str | Path,
    gold_tables: tuple[str, ...] = REQUIRED_GOLD_TABLES,
) -> dict[str, dict[str, int]]:
    row_counts = {"silver": {}, "gold": {}}
    for table_name in SILVER_TABLES:
        row_counts["silver"][table_name] = spark.table(f"iceberg.silver.{table_name}").count()
    for table_name in gold_tables:
        row_counts["gold"][table_name] = spark.table(f"iceberg.gold.{table_name}").count()
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "spark_table_row_counts.json").write_text(
        json.dumps(row_counts, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return row_counts


def _write_job_manifest(
    *,
    window: BatchWindow,
    evidence_root: str | Path,
    row_counts: dict[str, dict[str, int]],
    validation_report: dict[str, Any],
    gx_report: dict[str, Any],
    layout_profile: str,
    has_compaction_layout_manifest: bool,
) -> None:
    artifacts = [
        "pyspark_validation_report.json",
        "spark_table_row_counts.json",
        "spark_job_manifest.json",
        "gx/validation_results.json",
    ]
    if has_compaction_layout_manifest:
        artifacts.append("compaction_layout_manifest.json")
    manifest = {
        "window": {
            "start_ts": window.start_ts.isoformat().replace("+00:00", "Z"),
            "end_ts": window.end_ts.isoformat().replace("+00:00", "Z"),
            "mode": window.mode,
        },
        "required_gold_tables": list(REQUIRED_GOLD_TABLES),
        "row_counts": row_counts,
        "pyspark_validation_success": validation_report["success"],
        "gx_validation_success": gx_report["success"],
        "layout_profile": layout_profile,
        "artifacts": artifacts,
    }
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "spark_job_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_compaction_layout_manifest(
    *,
    evidence_root: str | Path,
    layout_results: list[dict[str, object]],
) -> None:
    evidence_path = Path(evidence_root)
    evidence_path.mkdir(parents=True, exist_ok=True)
    (evidence_path / "compaction_layout_manifest.json").write_text(
        json.dumps(
            {
                "layout_profile": COMPACTION_EVIDENCE_LAYOUT_PROFILE,
                "bucket_count": COMPACTION_EVIDENCE_BUCKET_COUNT,
                "tables": layout_results,
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )


def run_job(
    *,
    start_ts: str,
    end_ts: str,
    mode: str,
    evidence_root: str | Path,
    stage: str = "full",
    layout_profile: str = STANDARD_LAYOUT_PROFILE,
) -> dict[str, Any]:
    window = BatchWindow.from_args(start_ts=start_ts, end_ts=end_ts, mode=mode)
    if stage not in {"full", "core", "features"}:
        raise ValueError(f"Unsupported Spark job stage: {stage!r}")
    validate_layout_profile(layout_profile)
    if layout_profile == COMPACTION_EVIDENCE_LAYOUT_PROFILE and stage != "full":
        raise ValueError("Compaction-evidence layout profile requires the full Spark job stage.")
    spark = build_spark_session()
    try:
        layout_results: list[dict[str, object]] = []
        if stage in {"full", "core"}:
            _create_raw_views(spark, window)
            _persist_silver_tables_for_window(
                spark,
                start_ts=window.start_ts.isoformat().replace("+00:00", "Z"),
                end_ts=window.end_ts.isoformat().replace("+00:00", "Z"),
            )
            layout_results = _persist_gold_query_group(
                spark,
                ordered_core_gold_queries(),
                layout_profile=layout_profile,
            )
            if layout_results:
                _write_compaction_layout_manifest(
                    evidence_root=evidence_root,
                    layout_results=layout_results,
                )
        else:
            _register_persisted_tables(spark, namespace="iceberg.silver", table_names=SILVER_TABLES)
            _register_persisted_tables(
                spark,
                namespace="iceberg.gold",
                table_names=tuple(table_name for table_name, _query in ordered_core_gold_queries()),
            )

        if stage in {"full", "features"}:
            _persist_gold_query_group(spark, ordered_feature_queries())

        if stage != "full":
            stage_tables = (
                tuple(table_name for table_name, _query in ordered_core_gold_queries())
                if stage == "core"
                else tuple(table_name for table_name, _query in ordered_feature_queries())
            )
            return {
                "window": {
                    "start_ts": window.start_ts.isoformat().replace("+00:00", "Z"),
                    "end_ts": window.end_ts.isoformat().replace("+00:00", "Z"),
                    "mode": window.mode,
                },
                "stage": stage,
                "layout_profile": layout_profile,
                "row_counts": _capture_table_row_counts(
                    spark=spark,
                    evidence_root=evidence_root,
                    gold_tables=stage_tables,
                ),
            }

        validation_report = run_pyspark_validations(spark=spark, evidence_root=evidence_root)
        gx_report = run_gx_validations(spark=spark, evidence_root=evidence_root)
        row_counts = _capture_table_row_counts(spark=spark, evidence_root=evidence_root)
        _write_job_manifest(
            window=window,
            evidence_root=evidence_root,
            row_counts=row_counts,
            validation_report=validation_report,
            gx_report=gx_report,
            layout_profile=layout_profile,
            has_compaction_layout_manifest=bool(layout_results),
        )
        return {
            "window": {
                "start_ts": window.start_ts.isoformat().replace("+00:00", "Z"),
                "end_ts": window.end_ts.isoformat().replace("+00:00", "Z"),
                "mode": window.mode,
            },
            "row_counts": row_counts,
            "layout_profile": layout_profile,
        }
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    run_job(
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        mode=args.mode,
        evidence_root=args.evidence_root,
        stage=args.stage,
        layout_profile=args.layout_profile,
    )
