with latest_stream as (
  select *
  from (
    select
      *,
      row_number() over (partition by customer_id order by event_timestamp desc) as rn
    from {{ ref('feat_stream_60m') }}
  )
  where rn = 1
)
select
  c.customer_id,
  greatest(c.event_timestamp, coalesce(s.event_timestamp, c.event_timestamp)) as event_timestamp,
  c.f_customer_total_orders_90d,
  c.f_customer_paid_revenue_90d,
  c.f_customer_avg_order_value_90d,
  c.f_customer_distinct_categories_90d,
  coalesce(s.f_stream_views_60m, 0) as f_stream_views_60m,
  coalesce(s.f_stream_add_to_cart_60m, 0) as f_stream_add_to_cart_60m,
  coalesce(s.f_stream_checkout_started_60m, 0) as f_stream_checkout_started_60m,
  coalesce(s.f_stream_order_placed_60m, 0) as f_stream_order_placed_60m,
  coalesce(s.f_stream_cart_to_purchase_ratio_60m, 0) as f_stream_cart_to_purchase_ratio_60m,
  greatest(c.created, coalesce(s.created, c.created)) as created
from {{ ref('feat_customer_90d') }} c
left join latest_stream s
  on c.customer_id = s.customer_id
