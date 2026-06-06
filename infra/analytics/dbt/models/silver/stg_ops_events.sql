with ranked as (
  select
    event_id,
    event_type,
    event_topic,
    try_cast(schema_version as integer) as schema_version,
    try_cast(event_timestamp as timestamp) as event_timestamp,
    try_cast(created_ts as timestamp) as created_ts,
    producer,
    {{ json_int('payload', '$.burst_event_count') }} as burst_event_count,
    {{ json_int('payload', '$.late_event_count') }} as late_event_count,
    {{ json_int('payload', '$.duplicate_event_count') }} as duplicate_event_count,
    payload,
    ingest_ts,
    row_number() over (partition by event_id order by try_cast(created_ts as timestamp) desc) as rn
  from {{ ref('raw_kafka_ops_events') }}
)
select * exclude (rn)
from ranked
where rn = 1
