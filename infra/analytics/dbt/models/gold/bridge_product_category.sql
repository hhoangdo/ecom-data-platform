select
  p.product_key,
  p.product_id,
  c.category_key,
  m.category,
  m.subcategory,
  m.is_primary,
  m.assigned_ts
from {{ ref('stg_product_category_map') }} m
join {{ ref('dim_product') }} p
  on m.product_id = p.product_id
join {{ ref('dim_category') }} c
  on m.category = c.category
 and m.subcategory = c.subcategory
