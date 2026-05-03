# Vina Bim Shop Business Context

## Purpose

This document anchors the business framing for the coursework project so that data generation and schema choices stay aligned with the same operating model.

## Marketplace Model

`vina-bim-shop` is a synthetic multi-seller e-commerce marketplace inspired by Shopee-style retail behavior. It is designed to support analytics, operational reporting, and future AI use cases without copying a live production taxonomy one-to-one.

## Core Actors

- `customer`: generates browse, cart, order, and payment behavior
- `seller`: owns product listings, pricing, and inventory
- `platform`: owns taxonomy, promotions, pipeline logic, and reporting standards
- `logistics_provider`: drives shipment status changes and delivery performance
- `payment_provider`: records payment attempts and failures

## Questions the Data Model Must Support

- Which categories and sellers contribute most to orders, GMV, and repeat purchase behavior?
- Where do customers drop out between browse, cart, checkout, and successful payment?
- Which inventory, payment, or shipment issues create downstream business risk?
- Which stable features should be exposed later for ML or LLM-driven workflows?
