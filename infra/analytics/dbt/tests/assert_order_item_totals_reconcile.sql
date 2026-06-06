with item_totals as (
    select order_id, round(sum(net_amount), 2) as item_net_amount
    from {{ ref('stg_order_items') }}
    group by 1
),
order_totals as (
    select order_id, round(order_net_amount, 2) as order_net_amount
    from {{ ref('stg_orders') }}
)
select order_totals.order_id
from order_totals
join item_totals using (order_id)
where abs(order_totals.order_net_amount - item_totals.item_net_amount) > 0.01
