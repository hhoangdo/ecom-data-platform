select
  customer_id,
  date_trunc('hour', event_timestamp) as event_timestamp,
  sum(case when event_type = 'product_viewed' then 1 else 0 end) as f_stream_views_60m,
  sum(case when event_type = 'add_to_cart' then 1 else 0 end) as f_stream_add_to_cart_60m,
  sum(case when event_type = 'checkout_started' then 1 else 0 end) as f_stream_checkout_started_60m,
  sum(case when event_type = 'order_placed' then 1 else 0 end) as f_stream_order_placed_60m,
  case
    when sum(case when event_type = 'add_to_cart' then 1 else 0 end) > 0
    then sum(case when event_type = 'order_placed' then 1 else 0 end)::double
       / sum(case when event_type = 'add_to_cart' then 1 else 0 end)
    else 0
  end as f_stream_cart_to_purchase_ratio_60m,
  max(created_ts) as created
from {{ ref('stg_commerce_events') }}
where customer_id is not null
group by 1, 2
