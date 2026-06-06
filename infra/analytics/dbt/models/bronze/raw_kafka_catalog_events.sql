select
  *,
  current_timestamp as ingest_ts
from read_json_auto('{{ var("raw_root", "data/raw") }}/kafka_topics/catalog_events/events.jsonl')
