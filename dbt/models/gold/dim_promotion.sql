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
from {{ ref('stg_promotions') }}
