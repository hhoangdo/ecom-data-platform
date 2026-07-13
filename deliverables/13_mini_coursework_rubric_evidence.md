# Mini-Coursework Rubric Evidence

This reviewer index follows the workbook rows exactly. The [machine-readable manifest](../evidence/final_integration/mini_coursework_rubric_manifest.json) is authoritative for status and SHA-256 integrity; run its documented verifier before relying on any `Satisfied` entry.

| Row | Status | Implementation | Evidence | Review note |
| ---: | --- | --- | --- | --- |
| 2 | Satisfied | [README](../README.md) | [API coverage](../evidence/final_integration/public_documentation_coverage.json) | Central navigation, diagram conventions, and declared API policy. |
| 3 | Satisfied | [Kafka Connect Dockerfile](../infra/kafka/connect/Dockerfile) | [Image comparison](../evidence/00_engineering_fundamentals/kafka_connect_image_comparison.json) | Measured size reduction. |
| 4 | Satisfied | [Kafka Connect Dockerfile](../infra/kafka/connect/Dockerfile) | [Plugin smoke](../evidence/00_engineering_fundamentals/kafka_connect_plugin_smoke.json) | Multi-stage runtime proof. |
| 5 | Satisfied | [Generator config](../configs/generator/base.yaml) | [Generator summary](../evidence/01_data_generator/rubric_evidence_summary.md) | Offline skew. |
| 6 | Satisfied | [Generator runner](../src/vina_bim_shop/generators/runner.py) | [Cardinality evidence](../evidence/01_data_generator/cardinality_summary.csv) | Approximate distinct IDs. |
| 7 | Satisfied | [Generator config](../configs/generator/base.yaml) | [Schema summary](../evidence/01_data_generator/schema_version_summary.csv) | Schema evolution. |
| 8 | Satisfied | [Generator config](../configs/generator/base.yaml) | [Quality report](../evidence/01_data_generator/quality_report.md) | Offline duplicates. |
| 9 | Satisfied | [Generator config](../configs/generator/base.yaml) | [Generator manifest](../evidence/01_data_generator/run_manifest.json) | Configured offline profile. |
| 10 | Satisfied | [Generator runner](../src/vina_bim_shop/generators/runner.py) | [Generator summary](../evidence/01_data_generator/rubric_evidence_summary.md) | Bronze-ready raw storage. |
| 11 | Satisfied | [Generator config](../configs/generator/base.yaml) | [Generator summary](../evidence/01_data_generator/rubric_evidence_summary.md) | Streaming burst. |
| 12 | Satisfied | [Generator config](../configs/generator/base.yaml) | [Generator summary](../evidence/01_data_generator/rubric_evidence_summary.md) | Late arrivals. |
| 13 | Satisfied | [Generator config](../configs/generator/base.yaml) | [Generator summary](../evidence/01_data_generator/rubric_evidence_summary.md) | Streaming duplicates. |
| 14 | Satisfied | [Generator config](../configs/generator/base.yaml) | [Generator manifest](../evidence/01_data_generator/run_manifest.json) | Configured streaming profile. |
| 15 | Satisfied | [Spark experiments](../scripts/spark/run_optimization_experiments.py) | [Spark manifest](../evidence/05_spark_batch/optimization/run_manifest.json) | Baseline and optimized applications. |
| 16 | Satisfied | [Spark experiments](../scripts/spark/run_optimization_experiments.py) | [Skew equivalence](../evidence/05_spark_batch/optimization/skew_equivalence.json) | Targeted skew mitigation. |
| 17 | Satisfied | [Spark experiments](../scripts/spark/run_optimization_experiments.py) | [Cardinality equivalence](../evidence/05_spark_batch/optimization/high_cardinality_equivalence.json) | High-cardinality repartition experiment. |
| 18 | Satisfied | [Spark runner](../src/vina_bim_shop/lakehouse/spark/runner.py) | [Spark report](../evidence/05_spark_batch/optimization/optimization_report.md) | Schema evolution handling. |
| 19 | Satisfied | [Spark runner](../src/vina_bim_shop/lakehouse/spark/runner.py) | [Spark report](../evidence/05_spark_batch/optimization/optimization_report.md) | Duplicate and quarantine handling. |
| 20 | Satisfied | [Airflow batch DAG](../infra/orchestration/airflow/dags/hourly_batch_lakehouse.py) | [Spark manifest](../evidence/05_spark_batch/optimization/run_manifest.json) | Airflow batch integration. |
| 21 | Satisfied | [Flink baseline experiment](../src/vina_bim_shop/flink/baseline_experiment.py) | [Comparison](../evidence/06_flink_streaming/optimization/comparison.json) | Baseline versus optimized. |
| 22 | Satisfied | [Flink baseline experiment](../src/vina_bim_shop/flink/baseline_experiment.py) | [Challenge samples](../evidence/06_flink_streaming/optimization/challenge_samples.json) | Burst handling. |
| 23 | Satisfied | [Flink baseline experiment](../src/vina_bim_shop/flink/baseline_experiment.py) | [Challenge samples](../evidence/06_flink_streaming/optimization/challenge_samples.json) | Late-event correction. |
| 24 | Satisfied | [Flink baseline experiment](../src/vina_bim_shop/flink/baseline_experiment.py) | [Challenge samples](../evidence/06_flink_streaming/optimization/challenge_samples.json) | Streaming duplicates. |
| 25 | Satisfied | [Flink runtime](../src/vina_bim_shop/flink/runtime.py) | [Comparison](../evidence/06_flink_streaming/optimization/comparison.json) | Event-time window proof. |
| 26 | Satisfied | [Iceberg optimizer](../scripts/lakehouse/optimize_iceberg.py) | [Compaction result](../evidence/04_lakehouse/optimization/compaction_results.json) | Controlled smoke-scale compaction; not a general performance claim. |
| 27 | Satisfied | [DuckDB benchmark](../scripts/analytics/benchmark_duckdb_index.py) | [Benchmark](../evidence/04_lakehouse/optimization/query_benchmark.json) | Reproducible benchmark proof. |
| 28 | Satisfied | [Coursework orchestration](../src/vina_bim_shop/orchestration/mini_coursework_pipeline.py) | [DP1 ingest](../evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z/dp1_ingest.json) | Raw-to-Bronze ingest. |
| 29 | Satisfied | [Coursework orchestration](../src/vina_bim_shop/orchestration/mini_coursework_pipeline.py) | [DP1 validation](../evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z/dp1_validate.json) | Bronze GX contract. |
| 30 | Satisfied | [Coursework orchestration](../src/vina_bim_shop/orchestration/mini_coursework_pipeline.py) | [DP2 transform](../evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z/dp2_transform.json) | Bronze to Silver/Gold. |
| 31 | Satisfied | [Coursework orchestration](../src/vina_bim_shop/orchestration/mini_coursework_pipeline.py) | [DP2 validation](../evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z/dp2_validate.json) | Gold GX contract. |
| 32 | Satisfied | [Coursework orchestration](../src/vina_bim_shop/orchestration/mini_coursework_pipeline.py) | [DP3 compute](../evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z/dp3_compute.json) | Offline feature compute. |
| 33 | Satisfied | [Coursework orchestration](../src/vina_bim_shop/orchestration/mini_coursework_pipeline.py) | [DP3 validation](../evidence/08_airflow_gx/coursework_pipeline/topic07_20260711T124521Z/dp3_validate.json) | Feature contract. |
| 34 | Satisfied | [DataHub emitter](../src/vina_bim_shop/datahub_lineage/emitter.py) | [DP1 lineage](../evidence/09_datahub_governance/screenshots/datahub_dp1_lineage.png) | Indexed UI lineage. |
| 35 | Satisfied | [DataHub emitter](../src/vina_bim_shop/datahub_lineage/emitter.py) | [DP1 contract](../evidence/09_datahub_governance/screenshots/datahub_dp1_contract.png) | Indexed UI contract. |
| 36 | Satisfied | [DataHub emitter](../src/vina_bim_shop/datahub_lineage/emitter.py) | [DP2 lineage](../evidence/09_datahub_governance/screenshots/datahub_dp2_lineage.png) | Indexed UI lineage. |
| 37 | Satisfied | [DataHub emitter](../src/vina_bim_shop/datahub_lineage/emitter.py) | [DP2 contract](../evidence/09_datahub_governance/screenshots/datahub_dp2_contract.png) | Indexed UI contract. |
| 38 | Satisfied | [DataHub emitter](../src/vina_bim_shop/datahub_lineage/emitter.py) | [DP3 lineage](../evidence/09_datahub_governance/screenshots/datahub_dp3_lineage.png) | Indexed UI lineage. |
| 39 | Satisfied | [DataHub emitter](../src/vina_bim_shop/datahub_lineage/emitter.py) | [DP3 contract](../evidence/09_datahub_governance/screenshots/datahub_dp3_contract.png) | Indexed UI contract. |
| 40 | Satisfied | [All-zone ERD](../architecture/diagrams/schema_design.puml) | [Schema screenshot](../evidence/02_schema_design/screenshots/schema_design.png) | Bronze/Silver/Gold ERD. |
| 41 | Satisfied | [Dimension model](../infra/analytics/dbt/models/gold/dim_customer.sql) | [Schema inventory](../evidence/02_schema_design/schema_inventory.csv) | SCD2-compatible columns only. |
| 42 | Satisfied | [Feature contract](../infra/analytics/dbt/models/gold/_features.yml) | [dbt catalog](../evidence/02_schema_design/dbt_catalog_summary.csv) | Exact `event_timestamp` and `created` fields. |
| 43 | Satisfied | [Gold ERD source](../architecture/diagrams/erd/physical_gold_model.puml) | [Gold ERD render](../architecture/diagrams/erd/physical_gold_model.png) | Dimension/fact relationships. |
| 44 | Satisfied | [Schema deliverable](02_schema_design.md) | [Schema manifest](../evidence/02_schema_design/run_manifest.json) | Layer naming conventions. |
| 45 | Satisfied | [Novel ideas capture](../scripts/qa/capture_novel_ideas.py) | [Idea 1](../evidence/10_novel_ideas/idea_1_duckdb_dbt.json) | DuckDB/dbt local analytics. |
| 46 | Satisfied | [Novel ideas capture](../scripts/qa/capture_novel_ideas.py) | [Idea 2](../evidence/10_novel_ideas/idea_2_pinot_realtime.json) | Pinot realtime serving. |

Residual limitations remain evidence-scoped: the Topic 05 compaction is a controlled smoke-scale layout proof, DuckDB is a local parity path, and Pinot is provisional rather than canonical historical truth.
