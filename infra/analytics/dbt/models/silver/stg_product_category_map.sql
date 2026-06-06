with ranked as (
  select
    *,
    row_number() over (
      partition by product_id, category, subcategory
      order by assigned_ts desc
    ) as rn
  from {{ ref('raw_product_category_map') }}
)
select * exclude (rn)
from ranked
where rn = 1
