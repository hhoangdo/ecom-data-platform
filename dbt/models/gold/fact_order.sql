with payment_rollup as (
  select
    order_id,
    max(payment_status = 'success') as has_successful_payment,
    count(*) as payment_attempt_count
  from {{ ref('stg_payments') }}
  group by 1
)
select
  row_number() over (order by o.order_id) as order_key,
  o.order_id,
  c.customer_key,
  cast(strftime(cast(o.order_timestamp as date), '%Y%m%d') as integer) as order_date_key,
  os.order_status_key,
  o.customer_id,
  o.session_id,
  o.anonymous_id,
  o.order_timestamp,
  o.created_ts,
  o.primary_category,
  o.status as order_status,
  o.shipping_city,
  o.shipping_region,
  o.shipping_method,
  o.fulfillment_channel,
  o.promotion_id,
  o.coupon_code,
  o.item_count,
  o.order_gross_amount,
  o.order_discount_amount,
  o.order_net_amount,
  coalesce(p.has_successful_payment, false) as has_successful_payment,
  o.status = 'paid' and coalesce(p.has_successful_payment, false) as is_paid_order,
  case when o.status = 'paid' and coalesce(p.has_successful_payment, false) then o.order_net_amount else 0 end as official_paid_revenue,
  case when o.status = 'paid' and coalesce(p.has_successful_payment, false) then o.order_gross_amount else 0 end as gross_merchandise_value,
  coalesce(p.payment_attempt_count, 0) as payment_attempt_count
from {{ ref('stg_orders') }} o
left join {{ ref('dim_customer') }} c
  on o.customer_id = c.customer_id
left join {{ ref('dim_order_status') }} os
  on o.status = os.order_status
left join payment_rollup p
  on o.order_id = p.order_id
