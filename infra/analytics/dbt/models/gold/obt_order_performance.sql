with item_rollup as (
  select
    order_id,
    sum(quantity) as total_quantity,
    sum(estimated_cost) as estimated_cost,
    sum(estimated_margin) as estimated_margin,
    count(distinct product_id) as distinct_products,
    count(distinct seller_id) as distinct_sellers
  from {{ ref('fact_order_item') }}
  group by 1
),
payment_rollup as (
  select
    order_id,
    max(payment_timestamp) as last_payment_ts,
    max(case when is_payment_success then payment_method end) as successful_payment_method,
    sum(case when is_payment_success then 1 else 0 end) as successful_payment_attempts,
    count(*) as payment_attempts
  from {{ ref('fact_payment_attempt') }}
  group by 1
),
shipment_rollup as (
  select
    order_id,
    max(shipment_status) as shipment_status,
    max(is_delivery_delayed) as is_delivery_delayed,
    max(is_payment_blocked) as is_payment_blocked
  from {{ ref('fact_shipment') }}
  group by 1
)
select
  fo.order_id,
  fo.order_timestamp,
  fo.customer_id,
  dc.segment,
  dc.city as customer_city,
  fo.primary_category,
  fo.order_status,
  fo.shipping_city,
  fo.shipping_region,
  fo.shipping_method,
  fo.promotion_id,
  fo.coupon_code,
  fo.item_count,
  coalesce(ir.total_quantity, 0) as total_quantity,
  coalesce(ir.distinct_products, 0) as distinct_products,
  coalesce(ir.distinct_sellers, 0) as distinct_sellers,
  fo.order_gross_amount,
  fo.order_discount_amount,
  fo.order_net_amount,
  fo.official_paid_revenue,
  fo.gross_merchandise_value,
  coalesce(ir.estimated_cost, 0) as estimated_cost,
  coalesce(ir.estimated_margin, 0) as estimated_margin,
  fo.has_successful_payment,
  fo.is_paid_order,
  coalesce(pr.payment_attempts, 0) as payment_attempts,
  coalesce(pr.successful_payment_attempts, 0) as successful_payment_attempts,
  pr.successful_payment_method,
  pr.last_payment_ts,
  sr.shipment_status,
  coalesce(sr.is_delivery_delayed, false) as is_delivery_delayed,
  coalesce(sr.is_payment_blocked, false) as is_payment_blocked
from {{ ref('fact_order') }} fo
left join {{ ref('dim_customer') }} dc
  on fo.customer_key = dc.customer_key
left join item_rollup ir
  on fo.order_id = ir.order_id
left join payment_rollup pr
  on fo.order_id = pr.order_id
left join shipment_rollup sr
  on fo.order_id = sr.order_id
