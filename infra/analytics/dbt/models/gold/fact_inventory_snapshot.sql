select
  row_number() over (order by i.snapshot_id) as inventory_snapshot_key,
  i.snapshot_id,
  p.product_key,
  s.seller_key,
  cast(strftime(cast(i.snapshot_ts as date), '%Y%m%d') as integer) as snapshot_date_key,
  i.product_id,
  i.seller_id,
  i.snapshot_ts,
  i.stock_on_hand,
  i.reserved_stock,
  i.stock_on_hand - i.reserved_stock as available_stock
from {{ ref('stg_inventory_snapshots') }} i
left join {{ ref('dim_product') }} p
  on i.product_id = p.product_id
left join {{ ref('dim_seller') }} s
  on i.seller_id = s.seller_id
