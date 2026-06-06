with source_totals as (
    select round(sum(order_net_amount), 2) as paid_net_amount
    from {{ ref('stg_orders') }}
    where status = 'paid'
),
gold_totals as (
    select round(sum(official_paid_revenue), 2) as paid_net_amount
    from {{ ref('fact_order') }}
)
select *
from source_totals, gold_totals
where abs(source_totals.paid_net_amount - gold_totals.paid_net_amount) > 0.01
