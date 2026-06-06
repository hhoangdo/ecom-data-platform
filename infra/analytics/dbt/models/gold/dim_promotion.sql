with promotions as (
  select
    promotion_id,
    promotion_name,
    funding_type,
    funding_detail,
    seller_id,
    category,
    discount_rate,
    platform_funding_share,
    seller_funding_share,
    promotion_start_ts,
    promotion_end_ts,
    created_ts
  from {{ ref('stg_promotions') }}
),
no_promotion as (
  select
    'NO_PROMOTION' as promotion_id,
    'No promotion applied' as promotion_name,
    'none' as funding_type,
    cast(null as varchar) as funding_detail,
    cast(null as varchar) as seller_id,
    'none' as category,
    0.0 as discount_rate,
    0.0 as platform_funding_share,
    0.0 as seller_funding_share,
    cast(null as timestamp) as promotion_start_ts,
    cast(null as timestamp) as promotion_end_ts,
    cast(null as timestamp) as created_ts
),
unioned as (
  select * from promotions
  union all
  select * from no_promotion
)
select
  row_number() over (order by promotion_id) as promotion_key,
  promotion_id,
  promotion_name,
  funding_type,
  funding_detail,
  seller_id,
  category,
  discount_rate,
  platform_funding_share,
  seller_funding_share,
  promotion_start_ts,
  promotion_end_ts,
  created_ts
from unioned
