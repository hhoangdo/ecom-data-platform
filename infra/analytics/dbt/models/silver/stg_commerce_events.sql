with ranked as (
  select
    event_id,
    event_type,
    event_topic,
    try_cast(schema_version as integer) as schema_version,
    try_cast(event_timestamp as timestamp) as event_timestamp,
    try_cast(created_ts as timestamp) as created_ts,
    producer,
    {{ json_string('correlation_ids', '$.session_id') }} as session_id,
    {{ json_string('correlation_ids', '$.anonymous_id') }} as anonymous_id,
    {{ json_string('correlation_ids', '$.customer_id') }} as customer_id,
    coalesce({{ json_string('correlation_ids', '$.product_id') }}, {{ json_string('payload', '$.product_id') }}) as product_id,
    coalesce({{ json_string('correlation_ids', '$.order_id') }}, {{ json_string('payload', '$.order_id') }}) as order_id,
    {{ json_string('correlation_ids', '$.payment_id') }} as payment_id,
    {{ json_string('payload', '$.primary_category') }} as primary_category,
    {{ json_string('payload', '$.device_type') }} as device_type,
    {{ json_string('payload', '$.source') }} as source,
    {{ json_string('payload', '$.order_status') }} as order_status,
    {{ json_double('payload', '$.order_net_amount') }} as order_net_amount,
    {{ json_string('payload', '$.payment_method') }} as payment_method,
    {{ json_double('payload', '$.amount') }} as payment_amount,
    {{ json_string('payload', '$.failure_reason') }} as failure_reason,
    payload,
    ingest_ts,
    row_number() over (partition by event_id order by try_cast(created_ts as timestamp) desc) as rn
  from {{ ref('raw_kafka_commerce_events') }}
)
select * exclude (rn)
from ranked
where rn = 1
