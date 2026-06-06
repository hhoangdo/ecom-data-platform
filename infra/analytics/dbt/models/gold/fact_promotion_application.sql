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
from {{ ref('stg_order_items') }} oi
join {{ ref('fact_order') }} fo
  on oi.order_id = fo.order_id
left join {{ ref('dim_promotion') }} pr
  on oi.promotion_id = pr.promotion_id
where oi.promotion_id is not null
