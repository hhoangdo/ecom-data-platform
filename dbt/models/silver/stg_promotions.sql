with ranked as (
  select
    *,
    row_number() over (partition by promotion_id order by created_ts desc) as rn
  from {{ ref('raw_promotions') }}
),
deduped as (
  select * exclude (rn)
  from ranked
  where rn = 1
)
select
  *,
  case
    when funding_type = 'platform' then 1.0
    when funding_type = 'seller' then 0.0
    when funding_type = 'mixed' then coalesce(try_cast(json_extract_string(funding_detail, '$.platform_share') as double), 0.5)
    else 0.0
  end as platform_funding_share,
  case
    when funding_type = 'platform' then 0.0
    when funding_type = 'seller' then 1.0
    when funding_type = 'mixed' then coalesce(try_cast(json_extract_string(funding_detail, '$.seller_share') as double), 0.5)
    else 1.0
  end as seller_funding_share
from deduped
