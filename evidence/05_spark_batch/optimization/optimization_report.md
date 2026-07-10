# Spark Optimization Experiment Report

Controlled standalone coursework-profile experiment; it does not read or write canonical batch tables.
AQE was disabled for every compared submission. Elapsed time is observed evidence, not a guaranteed speedup.

## Skew

- Baseline elapsed: 1412.994 ms
- Optimized elapsed: 1392.164 ms
- Exact city aggregate equality: True
- Hot keys: Ho Chi Minh City, Ha Noi; deterministic salt buckets: 16

## High Cardinality

- Baseline elapsed: 7337.401 ms
- Optimized elapsed: 8512.983 ms
- Exact all-ID equality: True

| ID | Baseline exact | Optimized exact | Baseline approximate | Optimized approximate |
| --- | ---: | ---: | ---: | ---: |
| customer_id | 50000 | 50000 | 49401 | 49401 |
| product_id | 30000 | 30000 | 31069 | 31069 |
| order_id | 150000 | 150000 | 142797 | 142797 |
| event_id | 150000 | 150000 | 146766 | 146766 |
