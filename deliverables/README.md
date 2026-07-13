# `deliverables`

This folder contains the official coursework writeups. Read these files as the narrative explanation of each platform stage, not as executable source code or raw proof output.

## Suggested Review Order

| File | Purpose |
| --- | --- |
| `01_data_generator.md` | Synthetic source data generator design and outputs. |
| `02_schema_design.md` | Bronze/Silver/Gold schema design and data dictionary. |
| `03_data_generator_improvement.md` | Generator improvement notes. |
| `03_kafka_ingestion.md` | Kafka ingestion, schemas, topics, and Bronze landing. |
| `04_lakehouse.md` | MinIO, Hive Metastore, Trino, and lakehouse foundation. |
| `04.1_ml_design.md` | ML feature and modeling design. |
| `04.2_llm_design.md` | LLM design and governance considerations. |
| `05_spark_batch.md` | Spark batch processing and reconciled Gold outputs. |
| `06_flink_streaming.md` | Flink streaming jobs and derived realtime topics. |
| `07_pinot_serving.md` | Pinot serving layer and realtime query examples. |
| `08_airflow_gx_orchestration.md` | Airflow orchestration and Great Expectations validation. |
| `09_datahub_governance.md` | DataHub metadata, lineage, tags, and assertions. |
| `10_duckdb_dbt_local_analytics.md` | Local DuckDB/dbt analytics and parity path. |
| `12_novel_ideas.md` | Ordered DuckDB/dbt and Pinot novel-idea proof package. |
| `13_mini_coursework_rubric_evidence.md` | Row-ordered Mini-Coursework implementation and evidence navigation. |
| `11_solving_data_challenges.md` | Data challenge handling and tradeoffs. |

## How It Differs From Similar Folders

| Folder | Role |
| --- | --- |
| `deliverables/` | Human-readable coursework narrative and stage-level explanation. |
| `evidence/` | Captured proof artifacts such as manifests, reports, screenshots, health checks, and query outputs. |
| `src/vina_bim_shop/` and `infra/` | Implementation and runtime assets that make the deliverables reproducible. |

When reviewing sequentially, read a deliverable first, then inspect the matching numbered folder under `../evidence/`.
