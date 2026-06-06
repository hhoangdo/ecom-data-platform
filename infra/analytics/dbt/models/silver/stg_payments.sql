with ranked as (
  select
    *,
    row_number() over (partition by payment_id order by created_ts desc) as rn
  from {{ ref('raw_payments') }}
)
select * exclude (rn)
from ranked
where rn = 1
