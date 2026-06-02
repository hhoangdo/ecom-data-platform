# UI Screenshots

Browser MCP tool unavailable in this session. Capture screenshots manually:

| # | Service | URL | What to capture |
|---|---|---|---|
| 1 | Kafka UI | http://localhost:8084 | Topic list + drill into commerce_events message |
| 2 | Schema Registry | http://localhost:8081/subjects | JSON response (or use curl) |
| 3 | Kafka Connect | http://localhost:8083/connectors | Connector status |
| 4 | MinIO Console | http://localhost:9001 | Bucket list (buckets: bronze, silver, gold, checkpoints, evidence) |
| 5 | Trino UI | http://localhost:8080 | Query history + run: SELECT count(*) FROM iceberg.gold.fact_order |
| 6 | Spark Master | http://localhost:8085 | Active workers + running app |
| 7 | Spark History | http://localhost:18080 | Completed applications |
| 8 | Flink UI | http://localhost:8086 | Running jobs list + checkpoint stats |
| 9 | Pinot UI | http://localhost:9003 | (controller) or query via: SELECT count(*) FROM pinot_realtime_commerce_metrics_1m |
| 10 | Airflow UI | http://localhost:8082 | DAG list + graph view of bootstrap_topics |
| 11 | GX Data Docs | http://localhost:8088 | Validation results page |
| 12 | DataHub UI | http://localhost:9002 | Search for 'fact_order' + lineage graph |

All 7 profiles started successfully on 2026-06-02. See `../service_health.json`.
