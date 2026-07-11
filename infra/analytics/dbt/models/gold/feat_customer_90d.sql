with customer_orders as (
  select
    fo.customer_id,
    max(fo.order_timestamp) as event_timestamp,
    count(*) as f_customer_total_orders_90d,
    sum(fo.official_paid_revenue) as f_customer_paid_revenue_90d,
    avg(nullif(fo.official_paid_revenue, 0)) as f_customer_avg_order_value_90d,
    count(distinct fo.primary_category) as f_customer_distinct_categories_90d,
    max(fo.created_ts) as created
  from {{ ref('fact_order') }} fo
  group by 1
)
select
  customer_id,
  event_timestamp,
  coalesce(f_customer_total_orders_90d, 0) as f_customer_total_orders_90d,
  coalesce(f_customer_paid_revenue_90d, 0) as f_customer_paid_revenue_90d,
  coalesce(f_customer_avg_order_value_90d, 0) as f_customer_avg_order_value_90d,
  coalesce(f_customer_distinct_categories_90d, 0) as f_customer_distinct_categories_90d,
  created
from customer_orders
where customer_id is not null
