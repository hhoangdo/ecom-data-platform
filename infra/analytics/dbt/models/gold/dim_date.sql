with dates as (
  select cast(order_timestamp as date) as calendar_date from {{ ref('stg_orders') }}
  union
  select cast(payment_timestamp as date) as calendar_date from {{ ref('stg_payments') }}
  union
  select cast(created_ts as date) as calendar_date from {{ ref('stg_shipments') }}
  union
  select cast(snapshot_ts as date) as calendar_date from {{ ref('stg_inventory_snapshots') }}
)
select
  cast(strftime(calendar_date, '%Y%m%d') as integer) as date_key,
  calendar_date,
  cast(strftime(calendar_date, '%w') as integer) as day_of_week,
  cast(strftime(calendar_date, '%m') as integer) as month,
  cast(strftime(calendar_date, '%Y') as integer) as year,
  cast(strftime(calendar_date, '%w') as integer) in (0, 6) as is_weekend
from dates
where calendar_date is not null
