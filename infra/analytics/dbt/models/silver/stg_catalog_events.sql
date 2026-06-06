with ranked as (
  select
    event_id,
    event_type,
    event_topic,
    try_cast(schema_version as integer) as schema_version,
    try_cast(event_timestamp as timestamp) as event_timestamp,
    try_cast(created_ts as timestamp) as created_ts,
    producer,
    {{ json_string('correlation_ids', '$.snapshot_id') }} as snapshot_id,
    {{ json_string('correlation_ids', '$.product_id') }} as product_id,
    {{ json_string('correlation_ids', '$.seller_id') }} as seller_id,
    {{ json_string('correlation_ids', '$.promotion_id') }} as promotion_id,
    {{ json_string('payload', '$.category') }} as category,
    {{ json_double('payload', '$.stock_on_hand') }} as stock_on_hand,
    {{ json_double('payload', '$.discount_rate') }} as discount_rate,
    payload,
    ingest_ts,
    row_number() over (partition by event_id order by try_cast(created_ts as timestamp) desc) as rn
  from {{ ref('raw_kafka_catalog_events') }}
)
select * exclude (rn)
from ranked
where rn = 1
