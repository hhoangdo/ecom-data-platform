with ranked as (
  select
    *,
    row_number() over (partition by snapshot_id order by snapshot_ts desc) as rn
  from {{ ref('raw_inventory_snapshots') }}
)
select * exclude (rn)
from ranked
where rn = 1
