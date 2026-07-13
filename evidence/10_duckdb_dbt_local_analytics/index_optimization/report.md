# DuckDB ART Index Benchmark

The benchmark database is an isolated copy of `gold.fact_order`; the canonical dbt database was read-only.

| Variant | Median ms | Result hash |
| --- | ---: | --- |
| Baseline | 1.429 | ecfe42c8ba49a03ee49642ccc7a043cdba1230fecebc2602c69b73ee2c74771f |
| Indexed | 1.122 | ecfe42c8ba49a03ee49642ccc7a043cdba1230fecebc2602c69b73ee2c74771f |

Explain plan changed: False.
Local timings are observed evidence and do not promise an index speedup.
