select
  dlq_id,
  source_topic,
  raw_payload,
  error_reason,
  event_topic,
  schema_version,
  cast(ingest_ts as timestamp) as ingest_ts
from read_json_auto('{{ var("raw_root", "data/raw") }}/kafka_topics/dead_letter_events/events.jsonl')
