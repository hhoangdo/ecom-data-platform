from __future__ import annotations


def category_cost_rate_sql(expression: str) -> str:
    return f"""case
  when {expression} = 'FMCG' then 0.72
  when {expression} = 'ELHA' then 0.82
  when {expression} = 'Fashion' then 0.55
  when {expression} = 'Home & Living' then 0.62
  else 0.65
end"""


def _legacy_ordered_gold_queries() -> list[tuple[str, str]]:
    category_rate = category_cost_rate_sql("category")
    item_category_rate = category_cost_rate_sql("oi.primary_category")
    return [
        (
            "dim_customer",
            """
select
  row_number() over (order by customer_id) as customer_key,
  customer_id,
  anonymous_id,
  signup_ts,
  country,
  region,
  city,
  city_code,
  segment,
  marketing_opt_in,
  preferred_device,
  acquisition_channel,
  created_ts,
  created_ts as valid_from_ts,
  cast(null as timestamp) as valid_to_ts,
  true as is_current
from stg_customers
""",
        ),
        (
            "dim_seller",
            """
select
  row_number() over (order by seller_id) as seller_key,
  seller_id,
  seller_name,
  seller_tier,
  primary_category,
  city,
  region,
  seller_rating,
  fulfillment_speed_days,
  inventory_reliability,
  price_band,
  is_official_store,
  created_ts,
  created_ts as valid_from_ts,
  cast(null as timestamp) as valid_to_ts,
  true as is_current
from stg_sellers
""",
        ),
        (
            "dim_product",
            """
select
  row_number() over (order by p.product_id) as product_key,
  p.product_id,
  s.seller_key,
  p.seller_id,
  p.primary_category,
  p.primary_subcategory,
  coalesce(p.brand, 'unknown') as brand,
  p.product_name,
  p.base_price,
  p.price_band,
  p.is_active,
  p.fulfillment_channel,
  p.category_attributes,
  p.created_ts,
  p.created_ts as valid_from_ts,
  cast(null as timestamp) as valid_to_ts,
  true as is_current
from stg_products p
left join dim_seller s
  on p.seller_id = s.seller_id
""",
        ),
        (
            "dim_category",
            f"""
with categories as (
  select distinct category, subcategory
  from stg_product_category_map
)
select
  row_number() over (order by category, subcategory) as category_key,
  category,
  subcategory,
  {category_rate} as category_cost_rate
from categories
""",
        ),
        (
            "dim_date",
            """
with dates as (
  select cast(order_timestamp as date) as calendar_date from stg_orders
  union
  select cast(payment_timestamp as date) as calendar_date from stg_payments
  union
  select cast(created_ts as date) as calendar_date from stg_shipments
  union
  select cast(snapshot_ts as date) as calendar_date from stg_inventory_snapshots
)
select
  cast(date_format(calendar_date, 'yyyyMMdd') as int) as date_key,
  calendar_date,
  case when dayofweek(calendar_date) = 1 then 0 else dayofweek(calendar_date) - 1 end as day_of_week,
  month(calendar_date) as month,
  year(calendar_date) as year,
  dayofweek(calendar_date) in (1, 7) as is_weekend
from dates
where calendar_date is not null
""",
        ),
        (
            "dim_payment_method",
            """
select
  row_number() over (order by payment_method) as payment_method_key,
  payment_method
from (
  select distinct payment_method
  from stg_payments
  where payment_method is not null
)
""",
        ),
        (
            "dim_order_status",
            """
select
  row_number() over (order by status) as order_status_key,
  status as order_status
from (
  select distinct status
  from stg_orders
  where status is not null
)
""",
        ),
        (
            "dim_shipment_status",
            """
select
  row_number() over (order by shipment_status) as shipment_status_key,
  shipment_status
from (
  select distinct shipment_status
  from stg_shipments
  where shipment_status is not null
)
""",
        ),
        (
            "dim_shipping_method",
            """
select
  row_number() over (order by coalesce(shipping_method, 'unknown')) as shipping_method_key,
  coalesce(shipping_method, 'unknown') as shipping_method
from (
  select distinct shipping_method
  from stg_shipments
)
""",
        ),
        (
            "dim_promotion",
            """
with promotions as (
  select
    promotion_id,
    promotion_name,
    funding_type,
    funding_detail,
    seller_id,
    category,
    discount_rate,
    platform_funding_share,
    seller_funding_share,
    promotion_start_ts,
    promotion_end_ts,
    created_ts
  from stg_promotions
),
no_promotion as (
  select
    'NO_PROMOTION' as promotion_id,
    'No promotion applied' as promotion_name,
    'none' as funding_type,
    cast(null as string) as funding_detail,
    cast(null as string) as seller_id,
    'none' as category,
    0.0 as discount_rate,
    0.0 as platform_funding_share,
    0.0 as seller_funding_share,
    cast(null as timestamp) as promotion_start_ts,
    cast(null as timestamp) as promotion_end_ts,
    cast(null as timestamp) as created_ts
),
unioned as (
  select * from promotions
  union all
  select * from no_promotion
)
select
  row_number() over (order by promotion_id) as promotion_key,
  promotion_id,
  promotion_name,
  funding_type,
  funding_detail,
  seller_id,
  category,
  discount_rate,
  platform_funding_share,
  seller_funding_share,
  promotion_start_ts,
  promotion_end_ts,
  created_ts
from unioned
""",
        ),
        (
            "bridge_product_category",
            """
select
  p.product_key,
  p.product_id,
  c.category_key,
  m.category,
  m.subcategory,
  m.is_primary,
  m.assigned_ts
from stg_product_category_map m
join dim_product p
  on m.product_id = p.product_id
join dim_category c
  on m.category = c.category
 and m.subcategory = c.subcategory
""",
        ),
        (
            "fact_order",
            """
with payment_rollup as (
  select
    order_id,
    max(case when payment_status = 'success' then 1 else 0 end) = 1 as has_successful_payment,
    count(*) as payment_attempt_count
  from stg_payments
  group by 1
)
select
  row_number() over (order by o.order_id) as order_key,
  o.order_id,
  c.customer_key,
  cast(date_format(cast(o.order_timestamp as date), 'yyyyMMdd') as int) as order_date_key,
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
from stg_orders o
left join dim_customer c
  on o.customer_id = c.customer_id
left join dim_order_status os
  on o.status = os.order_status
left join payment_rollup p
  on o.order_id = p.order_id
""",
        ),
        (
            "fact_order_item",
            f"""
with payment_rollup as (
  select
    order_id,
    max(case when payment_status = 'success' then 1 else 0 end) = 1 as has_successful_payment
  from stg_payments
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
  cast(date_format(cast(o.order_timestamp as date), 'yyyyMMdd') as int) as order_date_key,
  oi.product_id,
  oi.seller_id,
  oi.primary_category,
  oi.primary_subcategory,
  oi.quantity,
  oi.unit_price,
  oi.gross_amount,
  oi.discount_amount,
  oi.net_amount,
  {item_category_rate} as category_cost_rate,
  case when o.status = 'paid' and coalesce(pay.has_successful_payment, false) then oi.net_amount else 0 end as line_paid_revenue,
  case when o.status = 'paid' and coalesce(pay.has_successful_payment, false) then oi.gross_amount else 0 end as line_gmv,
  case when o.status = 'paid' and coalesce(pay.has_successful_payment, false) then oi.net_amount * {item_category_rate} else 0 end as estimated_cost,
  case when o.status = 'paid' and coalesce(pay.has_successful_payment, false) then oi.net_amount * (1 - {item_category_rate}) else 0 end as estimated_margin,
  oi.promotion_id,
  oi.created_ts
from stg_order_items oi
join stg_orders o
  on oi.order_id = o.order_id
left join payment_rollup pay
  on oi.order_id = pay.order_id
left join fact_order fo
  on oi.order_id = fo.order_id
left join dim_product p
  on oi.product_id = p.product_id
left join dim_category c
  on oi.primary_category = c.category
 and oi.primary_subcategory = c.subcategory
left join dim_promotion pr
  on coalesce(oi.promotion_id, 'NO_PROMOTION') = pr.promotion_id
""",
        ),
        (
            "fact_payment_attempt",
            """
select
  row_number() over (order by p.payment_id) as payment_attempt_key,
  p.payment_id,
  p.order_id,
  fo.order_key,
  c.customer_key,
  cast(date_format(cast(p.payment_timestamp as date), 'yyyyMMdd') as int) as payment_date_key,
  pm.payment_method_key,
  p.customer_id,
  p.payment_timestamp,
  p.created_ts,
  p.payment_method,
  p.amount,
  p.payment_status,
  p.payment_status = 'success' as is_payment_success,
  p.payment_status = 'failed' as is_payment_failed,
  p.failure_reason
from stg_payments p
left join fact_order fo
  on p.order_id = fo.order_id
left join dim_customer c
  on p.customer_id = c.customer_id
left join dim_payment_method pm
  on p.payment_method = pm.payment_method
""",
        ),
        (
            "fact_shipment",
            """
select
  row_number() over (order by s.shipment_id) as shipment_key,
  s.shipment_id,
  s.order_id,
  fo.order_key,
  c.customer_key,
  ss.shipment_status_key,
  sm.shipping_method_key,
  cast(date_format(cast(s.created_ts as date), 'yyyyMMdd') as int) as shipment_created_date_key,
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
from stg_shipments s
left join fact_order fo
  on s.order_id = fo.order_id
left join dim_customer c
  on s.customer_id = c.customer_id
left join dim_shipment_status ss
  on s.shipment_status = ss.shipment_status
left join dim_shipping_method sm
  on coalesce(s.shipping_method, 'unknown') = sm.shipping_method
""",
        ),
        (
            "fact_inventory_snapshot",
            """
select
  row_number() over (order by i.snapshot_id) as inventory_snapshot_key,
  i.snapshot_id,
  p.product_key,
  s.seller_key,
  cast(date_format(cast(i.snapshot_ts as date), 'yyyyMMdd') as int) as snapshot_date_key,
  i.product_id,
  i.seller_id,
  i.snapshot_ts,
  i.stock_on_hand,
  i.reserved_stock,
  i.stock_on_hand - i.reserved_stock as available_stock
from stg_inventory_snapshots i
left join dim_product p
  on i.product_id = p.product_id
left join dim_seller s
  on i.seller_id = s.seller_id
""",
        ),
        (
            "fact_promotion_application",
            """
select
  row_number() over (order by oi.order_item_id) as promotion_application_key,
  oi.order_item_id,
  oi.order_id,
  fo.order_key,
  fo.customer_key,
  pr.promotion_key,
  oi.promotion_id,
  oi.gross_amount,
  oi.discount_amount,
  oi.discount_amount * coalesce(pr.platform_funding_share, 0.0) as platform_discount_amount,
  oi.discount_amount * coalesce(pr.seller_funding_share, 1.0) as seller_discount_amount,
  pr.funding_type,
  pr.platform_funding_share,
  pr.seller_funding_share
from stg_order_items oi
join fact_order fo
  on oi.order_id = fo.order_id
left join dim_promotion pr
  on oi.promotion_id = pr.promotion_id
where oi.promotion_id is not null
""",
        ),
        (
            "feat_customer_90d",
            """
with customer_orders as (
  select
    fo.customer_id,
    max(fo.order_timestamp) as event_timestamp,
    count(*) as f_customer_total_orders_90d,
    sum(fo.official_paid_revenue) as f_customer_paid_revenue_90d,
    avg(nullif(fo.official_paid_revenue, 0)) as f_customer_avg_order_value_90d,
    count(distinct fo.primary_category) as f_customer_distinct_categories_90d,
    max(fo.created_ts) as created
  from fact_order fo
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
""",
        ),
        (
            "feat_stream_60m",
            """
select
  customer_id,
  date_trunc('hour', event_timestamp) as event_timestamp,
  sum(case when event_type = 'product_viewed' then 1 else 0 end) as f_stream_views_60m,
  sum(case when event_type = 'add_to_cart' then 1 else 0 end) as f_stream_add_to_cart_60m,
  sum(case when event_type = 'checkout_started' then 1 else 0 end) as f_stream_checkout_started_60m,
  sum(case when event_type = 'order_placed' then 1 else 0 end) as f_stream_order_placed_60m,
  case
    when sum(case when event_type = 'add_to_cart' then 1 else 0 end) > 0
    then cast(sum(case when event_type = 'order_placed' then 1 else 0 end) as double)
       / sum(case when event_type = 'add_to_cart' then 1 else 0 end)
    else 0
  end as f_stream_cart_to_purchase_ratio_60m,
  max(created_ts) as created
from stg_commerce_events
where customer_id is not null
group by 1, 2
""",
        ),
        (
            "feat_customer_unified",
            """
with latest_stream as (
  select *
  from (
    select
      *,
      row_number() over (partition by customer_id order by event_timestamp desc) as rn
    from feat_stream_60m
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
from feat_customer_90d c
left join latest_stream s
  on c.customer_id = s.customer_id
""",
        ),
        (
            "obt_order_performance",
            """
with item_rollup as (
  select
    order_id,
    sum(quantity) as total_quantity,
    sum(estimated_cost) as estimated_cost,
    sum(estimated_margin) as estimated_margin,
    count(distinct product_id) as distinct_products,
    count(distinct seller_id) as distinct_sellers
  from fact_order_item
  group by 1
),
payment_rollup as (
  select
    order_id,
    max(payment_timestamp) as last_payment_ts,
    max(case when is_payment_success then payment_method end) as successful_payment_method,
    sum(case when is_payment_success then 1 else 0 end) as successful_payment_attempts,
    count(*) as payment_attempts
  from fact_payment_attempt
  group by 1
),
shipment_rollup as (
  select
    order_id,
    max(shipment_status) as shipment_status,
    max(case when is_delivery_delayed then 1 else 0 end) = 1 as is_delivery_delayed,
    max(case when is_payment_blocked then 1 else 0 end) = 1 as is_payment_blocked
  from fact_shipment
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
from fact_order fo
left join dim_customer dc
  on fo.customer_key = dc.customer_key
left join item_rollup ir
  on fo.order_id = ir.order_id
left join payment_rollup pr
  on fo.order_id = pr.order_id
left join shipment_rollup sr
  on fo.order_id = sr.order_id
""",
        ),
        (
            "agg_hourly_reconciled_kpi",
            """
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
  from obt_order_performance
  group by 1
),
payment_hourly as (
  select
    date_trunc('hour', payment_timestamp) as metric_hour,
    count(*) as payment_attempt_count,
    sum(case when is_payment_success then 1 else 0 end) as payment_success_count
  from fact_payment_attempt
  group by 1
),
shipment_hourly as (
  select
    date_trunc('hour', created_ts) as metric_hour,
    count(*) as shipment_count,
    sum(case when is_delivery_delayed then 1 else 0 end) as delayed_shipment_count
  from fact_shipment
  group by 1
),
event_hourly as (
  select
    date_trunc('hour', event_timestamp) as metric_hour,
    sum(case when event_type = 'checkout_started' then 1 else 0 end) as checkout_started_count,
    sum(case when event_type = 'order_placed' then 1 else 0 end) as order_placed_event_count
  from stg_commerce_events
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
  case when o.order_count > 0 then cast(o.cancelled_or_failed_order_count as double) / o.order_count else 0 end as cancellation_rate,
  coalesce(p.payment_attempt_count, 0) as payment_attempt_count,
  coalesce(p.payment_success_count, 0) as payment_success_count,
  case when coalesce(p.payment_attempt_count, 0) > 0 then cast(p.payment_success_count as double) / p.payment_attempt_count else 0 end as payment_success_rate,
  coalesce(s.shipment_count, 0) as shipment_count,
  coalesce(s.delayed_shipment_count, 0) as delayed_shipment_count,
  case when coalesce(s.shipment_count, 0) > 0 then cast(s.delayed_shipment_count as double) / s.shipment_count else 0 end as delivery_delay_rate,
  coalesce(e.checkout_started_count, 0) as checkout_started_count,
  coalesce(e.order_placed_event_count, 0) as order_placed_event_count,
  case when coalesce(e.checkout_started_count, 0) > 0 then cast(e.order_placed_event_count as double) / e.checkout_started_count else 0 end as conversion_rate
from order_hourly o
left join payment_hourly p using (metric_hour)
left join shipment_hourly s using (metric_hour)
left join event_hourly e using (metric_hour)
""",
        ),
    ]


def ordered_core_gold_queries() -> list[tuple[str, str]]:
    """Return every non-feature Gold query in deterministic dependency order."""
    return [
        (table_name, query)
        for table_name, query in _legacy_ordered_gold_queries()
        if not table_name.startswith("feat_")
    ]


def ordered_feature_queries() -> list[tuple[str, str]]:
    """Return the three offline-feature Gold queries in their public order."""
    return [
        (table_name, query)
        for table_name, query in _legacy_ordered_gold_queries()
        if table_name.startswith("feat_")
    ]


def ordered_gold_queries() -> list[tuple[str, str]]:
    """Preserve the full Gold API as the core stage followed by DP3 features."""
    return [*ordered_core_gold_queries(), *ordered_feature_queries()]
