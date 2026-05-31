select
  row_number() over (order by payment_method) as payment_method_key,
  payment_method
from (
  select distinct payment_method
  from {{ ref('stg_payments') }}
  where payment_method is not null
)
