select
  row_number() over (order by shipment_status) as shipment_status_key,
  shipment_status
from (
  select distinct shipment_status
  from {{ ref('stg_shipments') }}
  where shipment_status is not null
)
