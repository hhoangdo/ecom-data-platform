from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vina_bim_shop.generators.config import GeneratorConfig, load_generator_config


HOT_CITIES = ("Ho Chi Minh City", "Ha Noi")
SALT_BUCKETS = 16
VARIANTS = (
    "skew-baseline",
    "skew-optimized",
    "high-cardinality-baseline",
    "high-cardinality-optimized",
)
APPLICATION_NAMES = {
    "skew-baseline": "vina-bim-shop-optimization-skew-baseline",
    "skew-optimized": "vina-bim-shop-optimization-skew-optimized",
    "high-cardinality-baseline": "vina-bim-shop-optimization-high-cardinality-baseline",
    "high-cardinality-optimized": "vina-bim-shop-optimization-high-cardinality-optimized",
}
METRIC_FILENAMES = {
    "skew-baseline": "skew_baseline_metrics.json",
    "skew-optimized": "skew_optimized_metrics.json",
    "high-cardinality-baseline": "high_cardinality_baseline_metrics.json",
    "high-cardinality-optimized": "high_cardinality_optimized_metrics.json",
}
SCREENSHOT_FILENAMES = {
    "skew-baseline": "spark_skew_baseline_history.png",
    "skew-optimized": "spark_skew_optimized_history.png",
    "high-cardinality-baseline": "spark_high_cardinality_baseline_history.png",
    "high-cardinality-optimized": "spark_high_cardinality_optimized_history.png",
}
CARDINALITY_COLUMNS = ("customer_id", "product_id", "order_id", "event_id")


def _city_expression(config: GeneratorConfig):
    cities = list(config.geography["cities"])
    weight_scale = 10_000
    bucket = F.pmod(F.col("_row_id"), F.lit(weight_scale))
    cumulative = 0
    expression = None
    for city in cities[:-1]:
        cumulative += int(round(float(city["weight"]) * weight_scale))
        if expression is None:
            expression = F.when(bucket < F.lit(cumulative), F.lit(str(city["city"])))
        else:
            expression = expression.when(bucket < F.lit(cumulative), F.lit(str(city["city"])))
    if expression is None:
        return F.lit(str(cities[0]["city"]))
    return expression.otherwise(F.lit(str(cities[-1]["city"])))


def build_coursework_experiment_frame(spark: SparkSession, config: GeneratorConfig) -> DataFrame:
    customer_count = int(config.entities["customers"])
    product_count = int(config.entities["products"])
    order_count = int(config.entities["orders"])

    return (
        spark.range(order_count)
        .withColumnRenamed("id", "_row_id")
        .withColumn("city", _city_expression(config))
        .withColumn("_customer_number", F.pmod(F.col("_row_id"), F.lit(customer_count)) + F.lit(1))
        .withColumn("_product_number", F.pmod(F.col("_row_id"), F.lit(product_count)) + F.lit(1))
        .withColumn("_order_number", F.col("_row_id") + F.lit(1))
        .withColumn("customer_id", F.concat(F.lit("CUS-"), F.lpad(F.col("_customer_number"), 6, "0")))
        .withColumn("product_id", F.concat(F.lit("PRD-"), F.lpad(F.col("_product_number"), 6, "0")))
        .withColumn("order_id", F.concat(F.lit("ORD-"), F.lpad(F.col("_order_number"), 6, "0")))
        .withColumn("event_id", F.concat(F.lit("EVT-"), F.lpad(F.col("_order_number"), 6, "0")))
        .withColumn("order_amount_cents", F.pmod(F.col("_row_id") * F.lit(37) + F.lit(1_100), F.lit(500_000)))
        .withColumn(
            "order_amount",
            (F.col("order_amount_cents").cast("decimal(18,2)") / F.lit(100).cast("decimal(18,2)"))
            .cast("decimal(18,2)"),
        )
        .drop("_customer_number", "_product_number", "_order_number")
    )


def add_targeted_salt(frame: DataFrame) -> DataFrame:
    return frame.withColumn(
        "salt_bucket",
        F.when(
            F.col("city").isin(*HOT_CITIES),
            F.pmod(F.xxhash64("customer_id"), F.lit(SALT_BUCKETS)),
        )
        .otherwise(F.lit(0))
        .cast("int"),
    )


def _amount_string(amount_cents: int) -> str:
    return f"{amount_cents // 100}.{amount_cents % 100:02d}"


def _city_result_rows(frame: DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "city": row["city"],
            "order_count": int(row["order_count"]),
            "order_amount": _amount_string(int(row["order_amount_cents"])),
        }
        for row in frame.orderBy("city").collect()
    ]


def _partition_profile(frame: DataFrame) -> dict[str, Any]:
    rows = [
        {"partition_id": int(row["partition_id"]), "row_count": int(row["row_count"])}
        for row in (
            frame.withColumn("partition_id", F.spark_partition_id())
            .groupBy("partition_id")
            .agg(F.count(F.lit(1)).alias("row_count"))
            .orderBy("partition_id")
            .collect()
        )
    ]
    counts = [row["row_count"] for row in rows]
    return {
        "partition_count": int(frame.rdd.getNumPartitions()),
        "non_empty_partition_count": len(rows),
        "minimum_rows": min(counts),
        "maximum_rows": max(counts),
        "rows_by_partition": rows,
    }


def _runtime_metadata(spark: SparkSession, variant: str, config: GeneratorConfig) -> dict[str, Any]:
    aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled").strip().lower()
    event_log_enabled = spark.sparkContext.getConf().get("spark.eventLog.enabled", "false").strip().lower()
    event_log_dir = spark.sparkContext.getConf().get("spark.eventLog.dir", "")
    application_name = spark.sparkContext.appName
    if aqe_enabled != "false":
        raise RuntimeError("Optimization comparisons require spark.sql.adaptive.enabled=false.")
    if event_log_enabled != "true" or event_log_dir != "s3a://checkpoints/spark-events":
        raise RuntimeError("Optimization comparisons require the configured Spark event-log destination.")
    if application_name != APPLICATION_NAMES[variant]:
        raise RuntimeError(f"Expected Spark application name {APPLICATION_NAMES[variant]!r}, got {application_name!r}.")

    city_weights = {str(city["city"]): float(city["weight"]) for city in config.geography["cities"]}
    return {
        "application_id": spark.sparkContext.applicationId,
        "application_name": application_name,
        "aqe_enabled": False,
        "event_log_enabled": True,
        "event_log_dir": event_log_dir,
        "input_contract": {
            "config_path": "configs/generator/base.yaml",
            "scale": config.scale,
            "random_seed": config.random_seed,
            "history_days": config.history_days,
            "entity_counts": {
                "customers": int(config.entities["customers"]),
                "products": int(config.entities["products"]),
                "orders": int(config.entities["orders"]),
            },
            "city_weights": city_weights,
        },
    }


def _expected_cardinality(config: GeneratorConfig) -> dict[str, int]:
    return {
        "customer_id": int(config.entities["customers"]),
        "product_id": int(config.entities["products"]),
        "order_id": int(config.entities["orders"]),
        "event_id": int(config.entities["orders"]),
    }


def _skew_metrics(frame: DataFrame, *, optimized: bool) -> dict[str, Any]:
    prepared = (
        add_targeted_salt(frame).repartition(16, "city", "salt_bucket")
        if optimized
        else frame.repartition(16, "city")
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        profile = _partition_profile(prepared)
        if optimized:
            salt_profile = {
                row["city"]: [int(value) for value in row["salt_buckets"]]
                for row in (
                    prepared.groupBy("city")
                    .agg(F.sort_array(F.collect_set("salt_bucket")).alias("salt_buckets"))
                    .collect()
                )
            }
        else:
            salt_profile = None
        started = time.perf_counter()
        if optimized:
            result = (
                prepared.groupBy("city", "salt_bucket")
                .agg(
                    F.count(F.lit(1)).alias("order_count"),
                    F.sum("order_amount_cents").alias("order_amount_cents"),
                )
                .groupBy("city")
                .agg(
                    F.sum("order_count").alias("order_count"),
                    F.sum("order_amount_cents").alias("order_amount_cents"),
                )
            )
        else:
            result = prepared.groupBy("city").agg(
                F.count(F.lit(1)).alias("order_count"),
                F.sum("order_amount_cents").alias("order_amount_cents"),
            )
        city_results = _city_result_rows(result)
        elapsed_ms = round((time.perf_counter() - started) * 1_000, 3)
    finally:
        prepared.unpersist()

    metrics = {
        "input_rows": sum(row["order_count"] for row in city_results),
        "pre_aggregate_repartition": {
            "columns": ["city", "salt_bucket"] if optimized else ["city"],
            **profile,
        },
        "elapsed_ms": elapsed_ms,
        "city_results": city_results,
    }
    if optimized:
        metrics["hot_keys"] = list(HOT_CITIES)
        metrics["salt_bucket_count"] = SALT_BUCKETS
        metrics["city_salt_buckets"] = salt_profile
    return metrics


def _cardinality_metrics(frame: DataFrame, *, optimized: bool) -> dict[str, Any]:
    prepared = (
        frame.repartition(32, "order_id") if optimized else frame.repartition(8, "city")
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        profile = _partition_profile(prepared)
        aggregations = []
        for column in CARDINALITY_COLUMNS:
            aggregations.append(F.approx_count_distinct(column).alias(f"{column}_approx"))
            aggregations.append(F.countDistinct(column).alias(f"{column}_exact"))
        started = time.perf_counter()
        row = prepared.agg(*aggregations).first().asDict()
        elapsed_ms = round((time.perf_counter() - started) * 1_000, 3)
    finally:
        prepared.unpersist()

    return {
        "input_rows": sum(row["row_count"] for row in profile["rows_by_partition"]),
        "pre_aggregate_repartition": {
            "columns": ["order_id"] if optimized else ["city"],
            **profile,
        },
        "elapsed_ms": elapsed_ms,
        "cardinality": {
            column: {
                "approx_count_distinct": int(row[f"{column}_approx"]),
                "exact_count_distinct": int(row[f"{column}_exact"]),
            }
            for column in CARDINALITY_COLUMNS
        },
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"Required baseline artifact does not exist: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_same_input_contract(baseline: dict[str, Any], candidate: dict[str, Any]) -> None:
    if baseline["input_contract"] != candidate["input_contract"]:
        raise RuntimeError("Baseline and optimized variants do not use the same deterministic input contract.")


def _write_skew_equivalence(evidence_path: Path, metrics: dict[str, Any]) -> None:
    baseline = _load_json(evidence_path / METRIC_FILENAMES["skew-baseline"])
    _assert_same_input_contract(baseline, metrics)
    if baseline["city_results"] != metrics["city_results"]:
        raise RuntimeError("Salted skew results differ from the baseline city aggregates.")
    _write_json(
        evidence_path / "skew_equivalence.json",
        {
            "baseline_application_id": baseline["application_id"],
            "optimized_application_id": metrics["application_id"],
            "compared_fields": ["city", "order_count", "order_amount"],
            "success": True,
        },
    )


def _write_cardinality_equivalence(evidence_path: Path, metrics: dict[str, Any]) -> None:
    baseline = _load_json(evidence_path / METRIC_FILENAMES["high-cardinality-baseline"])
    _assert_same_input_contract(baseline, metrics)
    comparisons = {
        column: {
            "baseline": baseline["cardinality"][column]["exact_count_distinct"],
            "optimized": metrics["cardinality"][column]["exact_count_distinct"],
        }
        for column in CARDINALITY_COLUMNS
    }
    if any(values["baseline"] != values["optimized"] for values in comparisons.values()):
        raise RuntimeError("High-cardinality exact distinct results differ from the baseline.")
    _write_json(
        evidence_path / "high_cardinality_equivalence.json",
        {
            "baseline_application_id": baseline["application_id"],
            "optimized_application_id": metrics["application_id"],
            "comparisons": comparisons,
            "success": True,
        },
    )


def _update_run_manifest(evidence_path: Path, metrics: dict[str, Any]) -> None:
    manifest_path = evidence_path / "run_manifest.json"
    manifest = _load_json(manifest_path) if manifest_path.is_file() else {"variants": {}}
    manifest["canonical_batch_semantics_changed"] = False
    manifest["variants"][metrics["variant"]] = {
        "application_id": metrics["application_id"],
        "application_name": metrics["application_name"],
        "metrics_path": METRIC_FILENAMES[metrics["variant"]],
        "screenshot_path": f"../screenshots/{SCREENSHOT_FILENAMES[metrics['variant']]}",
    }
    _write_json(manifest_path, manifest)


def _write_optimization_report(evidence_path: Path) -> None:
    metric_paths = [evidence_path / filename for filename in METRIC_FILENAMES.values()]
    if not all(path.is_file() for path in metric_paths):
        return
    skew_equivalence = _load_json(evidence_path / "skew_equivalence.json")
    cardinality_equivalence = _load_json(evidence_path / "high_cardinality_equivalence.json")
    skew_baseline = _load_json(evidence_path / METRIC_FILENAMES["skew-baseline"])
    skew_optimized = _load_json(evidence_path / METRIC_FILENAMES["skew-optimized"])
    cardinality_baseline = _load_json(evidence_path / METRIC_FILENAMES["high-cardinality-baseline"])
    cardinality_optimized = _load_json(evidence_path / METRIC_FILENAMES["high-cardinality-optimized"])
    lines = [
        "# Spark Optimization Experiment Report",
        "",
        "Controlled standalone coursework-profile experiment; it does not read or write canonical batch tables.",
        "AQE was disabled for every compared submission. Elapsed time is observed evidence, not a guaranteed speedup.",
        "",
        "## Skew",
        "",
        f"- Baseline elapsed: {skew_baseline['elapsed_ms']} ms",
        f"- Optimized elapsed: {skew_optimized['elapsed_ms']} ms",
        f"- Exact city aggregate equality: {skew_equivalence['success']}",
        f"- Hot keys: {', '.join(HOT_CITIES)}; deterministic salt buckets: {SALT_BUCKETS}",
        "",
        "## High Cardinality",
        "",
        f"- Baseline elapsed: {cardinality_baseline['elapsed_ms']} ms",
        f"- Optimized elapsed: {cardinality_optimized['elapsed_ms']} ms",
        f"- Exact all-ID equality: {cardinality_equivalence['success']}",
        "",
        "| ID | Baseline exact | Optimized exact | Baseline approximate | Optimized approximate |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for column in CARDINALITY_COLUMNS:
        baseline_values = cardinality_baseline["cardinality"][column]
        optimized_values = cardinality_optimized["cardinality"][column]
        lines.append(
            "| "
            f"{column} | {baseline_values['exact_count_distinct']} | {optimized_values['exact_count_distinct']} "
            f"| {baseline_values['approx_count_distinct']} | {optimized_values['approx_count_distinct']} |"
        )
    (evidence_path / "optimization_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_optimization_variant(
    config_path: str | Path,
    scale: str,
    variant: str,
    evidence_root: str | Path,
) -> dict[str, Any]:
    if variant not in VARIANTS:
        raise ValueError(f"Unknown optimization variant {variant!r}.")
    if scale != "coursework":
        raise ValueError("Optimization experiments require the coursework scale.")

    config = load_generator_config(config_path, scale=scale)
    evidence_path = Path(evidence_root)
    spark = SparkSession.builder.getOrCreate()
    try:
        metrics = _runtime_metadata(spark, variant, config)
        frame = build_coursework_experiment_frame(spark, config)
        if variant.startswith("skew"):
            metrics.update(_skew_metrics(frame, optimized=variant == "skew-optimized"))
        else:
            metrics.update(_cardinality_metrics(frame, optimized=variant == "high-cardinality-optimized"))
            metrics["expected_exact_cardinality"] = _expected_cardinality(config)
            if any(
                metrics["cardinality"][column]["exact_count_distinct"] != expected
                for column, expected in metrics["expected_exact_cardinality"].items()
            ):
                raise RuntimeError("Experiment cardinality does not match the deterministic coursework input contract.")
        metrics["variant"] = variant
        _write_json(evidence_path / METRIC_FILENAMES[variant], metrics)
        if variant == "skew-optimized":
            _write_skew_equivalence(evidence_path, metrics)
        if variant == "high-cardinality-optimized":
            _write_cardinality_equivalence(evidence_path, metrics)
        _update_run_manifest(evidence_path, metrics)
        _write_optimization_report(evidence_path)
        return metrics
    finally:
        spark.stop()
