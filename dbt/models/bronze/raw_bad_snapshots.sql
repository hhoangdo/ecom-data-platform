select
  bad_record_id,
  source_dataset,
  raw_record,
  error_reason,
  cast(ingest_ts as timestamp) as ingest_ts
from read_json_auto('{{ var("raw_root", "data/raw") }}/bad_snapshots/bad_snapshots.jsonl')
