with payment_rollup as (
  select
    order_id,
    max(payment_status = 'success') as has_successful_payment
  from {{ ref('stg_payments') }}
  group by 1
)
select
  row_number() over (order by oi.order_item_id) as order_item_key,
  oi.order_item_id,
  oi.order_id,
  fo.order_key,
  fo.customer_key,
  p.product_key,
  c.category_key,
  pr.promotion_key,
  cast(strftime(cast(o.order_timestamp as date), '%Y%m%d') as integer) as order_date_key,
  oi.product_id,
  oi.seller_id,
  oi.primary_category,
  oi.primary_subcategory,
  oi.quantity,
  oi.unit_price,
  oi.gross_amount,
  oi.discount_amount,
  oi.net_amount,
  {{ category_cost_rate('oi.primary_category') }} as category_cost_rate,
  case when o.status = 'paid' and coalesce(pay.has_successful_payment, false) then oi.net_amount else 0 end as line_paid_revenue,
  case when o.status = 'paid' and coalesce(pay.has_successful_payment, false) then oi.gross_amount else 0 end as line_gmv,
  case when o.status = 'paid' and coalesce(pay.has_successful_payment, false) then oi.net_amount * {{ category_cost_rate('oi.primary_category') }} else 0 end as estimated_cost,
  case when o.status = 'paid' and coalesce(pay.has_successful_payment, false) then oi.net_amount * (1 - {{ category_cost_rate('oi.primary_category') }}) else 0 end as estimated_margin,
  oi.promotion_id,
  oi.created_ts
from {{ ref('stg_order_items') }} oi
join {{ ref('stg_orders') }} o
  on oi.order_id = o.order_id
left join payment_rollup pay
  on oi.order_id = pay.order_id
left join {{ ref('fact_order') }} fo
  on oi.order_id = fo.order_id
left join {{ ref('dim_product') }} p
  on oi.product_id = p.product_id
left join {{ ref('dim_category') }} c
  on oi.primary_category = c.category
 and oi.primary_subcategory = c.subcategory
left join {{ ref('dim_promotion') }} pr
  on coalesce(oi.promotion_id, 'NO_PROMOTION') = pr.promotion_id
