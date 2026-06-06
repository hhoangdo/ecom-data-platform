select
  row_number() over (order by p.payment_id) as payment_attempt_key,
  p.payment_id,
  p.order_id,
  fo.order_key,
  c.customer_key,
  cast(strftime(cast(p.payment_timestamp as date), '%Y%m%d') as integer) as payment_date_key,
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
from {{ ref('stg_payments') }} p
left join {{ ref('fact_order') }} fo
  on p.order_id = fo.order_id
left join {{ ref('dim_customer') }} c
  on p.customer_id = c.customer_id
left join {{ ref('dim_payment_method') }} pm
  on p.payment_method = pm.payment_method
