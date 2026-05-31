with ranked as (
  select
    *,
    row_number() over (partition by shipment_id order by created_ts desc) as rn
  from {{ ref('raw_shipments') }}
)
select * exclude (rn)
from ranked
where rn = 1
