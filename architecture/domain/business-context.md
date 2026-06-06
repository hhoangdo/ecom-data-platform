# Vina Bim Shop Business Context

## Purpose

This document anchors the business framing for the implemented coursework platform. The domain context keeps source generation, schema design, serving policy, and governance evidence aligned to the same marketplace operating model.

## Marketplace Model

`vina-bim-shop` is a synthetic multi-seller e-commerce marketplace inspired by Vietnamese Shopee-style retail behavior. It supports analytics, operational reporting, feature-table preparation, and governance evidence without copying a live production taxonomy.

## Core Actors

| Actor | Responsibility |
| --- | --- |
| `customer` | Browses products, searches, adds items to cart, checks out, pays, and receives shipments. |
| `seller` | Owns listings, category placement, price, inventory, fulfillment channel, and seller performance traits. |
| `platform` | Owns taxonomy, promotions, source contracts, data pipelines, canonical reporting definitions, and governance. |
| `logistics_provider` | Emits shipment creation, handoff, delay, delivery, and payment-blocked fulfillment events. |
| `payment_provider` | Records payment attempts, successful payments, failures, and failure reasons. |

## Business Questions Supported

| Question | Implemented analytical support |
| --- | --- |
| Which categories, sellers, and products drive orders, GMV, official paid revenue, and repeat purchases? | Gold facts, dimensions, `obt_order_performance`, and `agg_hourly_reconciled_kpi`. |
| Where do users drop out between browsing, cart, checkout, order placement, and payment? | `commerce_events`, `stg_commerce_events`, `feat_stream_60m`, Flink metrics, and Pinot realtime tables. |
| Which payment, inventory, or shipment issues create operational risk? | Payment/shipment facts, fulfillment events, ops events, and `realtime_ops_alerts`. |
| Which local evidence can reviewers inspect without running the full stack? | dbt-DuckDB parity file, DuckDB Executive Mart, final dataset package, and committed evidence reports. |
| How can metadata and lineage be inspected across the platform? | DataHub governance evidence for Kafka, MinIO/S3, Trino/Iceberg, dbt, Spark, Flink, and GX assertions. |

## Scope Boundaries

- The project models one synthetic marketplace, not multiple marketplaces.
- International tax, returns, refunds, true COGS, and seller finance workflows are not implemented.
- Sensitive personal data is not generated; identifiers are synthetic and geography is coarse.
- Drift scenario implementation, ML model training/serving, and LLM application design are outside the current platform evidence.
