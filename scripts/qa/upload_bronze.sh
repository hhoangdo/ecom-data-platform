#!/bin/sh
set -eu
mc alias set local http://minio:9000 vina_minio vina_minio_password

for ds in customers sellers products product_category_map inventory_snapshots promotions orders order_items payments shipments; do
  if [ -f "/rawdata/${ds}/part-000.parquet" ]; then
    mc cp "/rawdata/${ds}/part-000.parquet" "local/bronze/batch/${ds}/snapshot_date=2026-05-01/part-000.parquet"
    echo "OK: ${ds}"
  fi
done

for topic in commerce_events catalog_events fulfillment_events ops_events dead_letter_events; do
  if [ -f "/rawdata/kafka_topics/${topic}/events.jsonl" ]; then
    mc cp "/rawdata/kafka_topics/${topic}/events.jsonl" "local/bronze/events/${topic}/ingest_date=2026-05-01/events.jsonl"
    echo "OK: ${topic}"
  fi
done

echo "--- ALL DONE ---"
