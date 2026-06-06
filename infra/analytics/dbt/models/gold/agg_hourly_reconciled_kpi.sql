with order_hourly as (
  select
    date_trunc('hour', order_timestamp) as metric_hour,
    count(*) as order_count,
    sum(case when is_paid_order then 1 else 0 end) as paid_order_count,
    sum(order_gross_amount) as gross_order_amount,
    sum(order_discount_amount) as discount_amount,
    sum(official_paid_revenue) as official_paid_revenue,
    sum(gross_merchandise_value) as gross_merchandise_value,
    sum(estimated_cost) as estimated_cost,
    sum(estimated_margin) as estimated_margin,
    sum(case when order_status = 'payment_failed' then 1 else 0 end) as cancelled_or_failed_order_count
  from {{ ref('obt_order_performance') }}
  group by 1
),
payment_hourly as (
  select
    date_trunc('hour', payment_timestamp) as metric_hour,
    count(*) as payment_attempt_count,
    sum(case when is_payment_success then 1 else 0 end) as payment_success_count
  from {{ ref('fact_payment_attempt') }}
  group by 1
),
shipment_hourly as (
  select
    date_trunc('hour', created_ts) as metric_hour,
    count(*) as shipment_count,
    sum(case when is_delivery_delayed then 1 else 0 end) as delayed_shipment_count
  from {{ ref('fact_shipment') }}
  group by 1
),
event_hourly as (
  select
    date_trunc('hour', event_timestamp) as metric_hour,
    sum(case when event_type = 'checkout_started' then 1 else 0 end) as checkout_started_count,
    sum(case when event_type = 'order_placed' then 1 else 0 end) as order_placed_event_count
  from {{ ref('stg_commerce_events') }}
  group by 1
)
select
  o.metric_hour,
  o.order_count,
  o.paid_order_count,
  o.gross_order_amount,
  o.discount_amount,
  o.official_paid_revenue,
  o.gross_merchandise_value,
  o.estimated_cost,
  o.estimated_margin,
  case when o.paid_order_count > 0 then o.official_paid_revenue / o.paid_order_count else 0 end as average_order_value,
  o.cancelled_or_failed_order_count,
  case when o.order_count > 0 then o.cancelled_or_failed_order_count::double / o.order_count else 0 end as cancellation_rate,
  coalesce(p.payment_attempt_count, 0) as payment_attempt_count,
  coalesce(p.payment_success_count, 0) as payment_success_count,
  case when coalesce(p.payment_attempt_count, 0) > 0 then p.payment_success_count::double / p.payment_attempt_count else 0 end as payment_success_rate,
  coalesce(s.shipment_count, 0) as shipment_count,
  coalesce(s.delayed_shipment_count, 0) as delayed_shipment_count,
  case when coalesce(s.shipment_count, 0) > 0 then s.delayed_shipment_count::double / s.shipment_count else 0 end as delivery_delay_rate,
  coalesce(e.checkout_started_count, 0) as checkout_started_count,
  coalesce(e.order_placed_event_count, 0) as order_placed_event_count,
  case when coalesce(e.checkout_started_count, 0) > 0 then e.order_placed_event_count::double / e.checkout_started_count else 0 end as conversion_rate
from order_hourly o
left join payment_hourly p using (metric_hour)
left join shipment_hourly s using (metric_hour)
left join event_hourly e using (metric_hour)
