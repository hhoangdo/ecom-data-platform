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
from {{ ref('stg_customers') }}
