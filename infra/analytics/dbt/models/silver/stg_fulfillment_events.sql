with ranked as (
  select
    event_id,
    event_type,
    event_topic,
    try_cast(schema_version as integer) as schema_version,
    try_cast(event_timestamp as timestamp) as event_timestamp,
    try_cast(created_ts as timestamp) as created_ts,
    producer,
    {{ json_string('correlation_ids', '$.shipment_id') }} as shipment_id,
    {{ json_string('correlation_ids', '$.order_id') }} as order_id,
    {{ json_string('correlation_ids', '$.customer_id') }} as customer_id,
    {{ json_string('payload', '$.shipment_status') }} as shipment_status,
    {{ json_string('payload', '$.shipping_city') }} as shipping_city,
    {{ json_string('payload', '$.shipping_method') }} as shipping_method,
    payload,
    ingest_ts,
    row_number() over (partition by event_id order by try_cast(created_ts as timestamp) desc) as rn
  from {{ ref('raw_kafka_fulfillment_events') }}
)
select * exclude (rn)
from ranked
where rn = 1
