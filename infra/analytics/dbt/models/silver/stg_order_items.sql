with ranked as (
  select
    *,
    row_number() over (partition by order_item_id order by created_ts desc) as rn
  from {{ ref('raw_order_items') }}
)
select * exclude (rn)
from ranked
where rn = 1
