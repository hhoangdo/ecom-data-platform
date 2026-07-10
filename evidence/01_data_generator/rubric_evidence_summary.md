# Section 01 Generator Rubric Evidence Summary

- Source configuration: `configs/generator/base.yaml`
- Generated mode: `medium` scale, `60` history days, seed `42`.
- Approximate distinct counts use DuckDB `approx_count_distinct`; uniqueness ratios are capped at `1.0` because an approximate estimate can exceed its population.

## Row 5 - Offline Skew

| Evidence | Configured value | Observed result |
| --- | --- | --- |
| HCMC and Ha Noi customer skew target | `0.45` | `0.453` |
| City weights | `Ho Chi Minh City=0.27; Ha Noi=0.18; Da Nang=0.09; Hai Phong=0.07; Can Tho=0.07; Binh Duong=0.07; Dong Nai=0.06; Hue=0.05; Nha Trang=0.05; Other VN=0.09` | See customer-share metric above |
| Category weights | `FMCG=0.36; ELHA=0.3; Fashion=0.2; Home & Living=0.14` | `0.66933` |

## Row 6 - Offline High Cardinality

| Entity | ID column | Row count | DuckDB approximate distinct count | Uniqueness ratio |
| --- | --- | ---: | ---: | ---: |
| customers | customer_id | 12,000 | 12,422 | 1.00000 |
| products | product_id | 6,000 | 7,538 | 1.00000 |
| orders | order_id | 45,000 | 55,332 | 1.00000 |
| events | event_id | 639,537 | 749,707 | 1.00000 |

## Row 7 - Offline Schema Evolution

| Evidence | Configured value | Observed result |
| --- | --- | --- |
| Schema evolution cutoff ratio | `0.45` | `0.45367` |
| Missing brand rate | `0.02` | `0.021` |
| Missing shipping method rate | `0.03` | `0.03062` |

| Topic | Schema version | Event rows |
| --- | ---: | ---: |
| catalog_events | 1 | 69,160 |
| commerce_events | 1 | 442,451 |
| dead_letter_events | 1 | 4 |
| fulfillment_events | 1 | 127,921 |
| ops_events | 1 | 5 |

## Row 8 - Offline Duplicates

| Evidence | Configured value | Observed result |
| --- | --- | --- |
| Offline order-item duplicate rate | `0.02` | `0.0196` |

## Row 9 - Offline Generator Configuration

| Profile | History days | Entity counts |
| --- | ---: | --- |
| smoke | 7 | customers=800; sellers=80; products=600; orders=1800; promotions=16 |
| medium | 60 | customers=12000; sellers=600; products=6000; orders=45000; promotions=80 |
| coursework | 90 | customers=50000; sellers=2500; products=30000; orders=150000; promotions=180 |

## Row 10 - Bronze Input Storage

| Evidence | Configured value | Observed result |
| --- | --- | --- |
| Raw output root | `data/raw` | Parquet snapshots and Kafka topic JSONL are generated under this root |
| Kafka topic inputs | `catalog_events; commerce_events; fulfillment_events; ops_events; dead_letter_events` | dead_letter_events is the Bronze quarantine input |

## Row 11 - Streaming Burst

| Evidence | Configured value | Observed result |
| --- | --- | --- |
| Burst windows | `12:00-12:20; 20:00-20:20` | `5` ops event rows |

## Row 12 - Streaming Late Arrivals

| Evidence | Configured value | Observed result |
| --- | --- | --- |
| Late-arrival rate | `0.12` | `0.11959` |
| Late delay minutes | `5-45` | Measured through the late-arrival issue metric |
| Missing device type rate | `0.04` | `0.03988` |

## Row 13 - Streaming Duplicates

| Evidence | Configured value | Observed result |
| --- | --- | --- |
| Streaming duplicate rate | `0.015` | `0.01478` |

## Row 14 - Streaming Generator Configuration

| Evidence | Configured value | Observed result |
| --- | --- | --- |
| Selected scale profile | `medium` | `60` history days |
| Source configuration | `configs/generator/base.yaml` | All reported controls above are loaded from this YAML mapping |
