select
  row_number() over (order by seller_id) as seller_key,
  seller_id,
  seller_name,
  seller_tier,
  primary_category,
  city,
  region,
  seller_rating,
  fulfillment_speed_days,
  inventory_reliability,
  price_band,
  is_official_store,
  created_ts,
  created_ts as valid_from_ts,
  cast(null as timestamp) as valid_to_ts,
  true as is_current
from {{ ref('stg_sellers') }}
