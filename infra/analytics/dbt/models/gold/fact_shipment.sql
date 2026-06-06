select
  row_number() over (order by s.shipment_id) as shipment_key,
  s.shipment_id,
  s.order_id,
  fo.order_key,
  c.customer_key,
  ss.shipment_status_key,
  sm.shipping_method_key,
  cast(strftime(cast(s.created_ts as date), '%Y%m%d') as integer) as shipment_created_date_key,
  s.customer_id,
  s.shipping_city,
  s.shipping_region,
  coalesce(s.shipping_method, 'unknown') as shipping_method,
  s.shipment_status,
  s.shipment_status = 'delayed' as is_delivery_delayed,
  s.shipment_status = 'blocked_payment_failed' as is_payment_blocked,
  s.handoff_ts,
  s.estimated_delivery_ts,
  s.created_ts
from {{ ref('stg_shipments') }} s
left join {{ ref('fact_order') }} fo
  on s.order_id = fo.order_id
left join {{ ref('dim_customer') }} c
  on s.customer_id = c.customer_id
left join {{ ref('dim_shipment_status') }} ss
  on s.shipment_status = ss.shipment_status
left join {{ ref('dim_shipping_method') }} sm
  on coalesce(s.shipping_method, 'unknown') = sm.shipping_method
