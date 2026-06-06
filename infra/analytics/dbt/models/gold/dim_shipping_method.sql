select
  row_number() over (order by coalesce(shipping_method, 'unknown')) as shipping_method_key,
  coalesce(shipping_method, 'unknown') as shipping_method
from (
  select distinct shipping_method
  from {{ ref('stg_shipments') }}
)
