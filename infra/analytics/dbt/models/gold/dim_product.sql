select
  row_number() over (order by p.product_id) as product_key,
  p.product_id,
  s.seller_key,
  p.seller_id,
  p.primary_category,
  p.primary_subcategory,
  p.brand,
  p.product_name,
  p.base_price,
  p.price_band,
  p.is_active,
  p.fulfillment_channel,
  p.category_attributes,
  p.created_ts,
  p.created_ts as valid_from_ts,
  cast(null as timestamp) as valid_to_ts,
  true as is_current
from {{ ref('stg_products') }} p
left join {{ ref('dim_seller') }} s
  on p.seller_id = s.seller_id
