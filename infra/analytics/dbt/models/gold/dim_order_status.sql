select
  row_number() over (order by status) as order_status_key,
  status as order_status
from (
  select distinct status
  from {{ ref('stg_orders') }}
  where status is not null
)
