select
  *,
  current_timestamp as ingest_ts
from read_parquet('{{ var("raw_root", "data/raw") }}/product_category_map/*.parquet')
