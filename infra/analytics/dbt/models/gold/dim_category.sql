with categories as (
  select distinct category, subcategory
  from {{ ref('stg_product_category_map') }}
)
select
  row_number() over (order by category, subcategory) as category_key,
  category,
  subcategory,
  {{ category_cost_rate('category') }} as category_cost_rate
from categories
