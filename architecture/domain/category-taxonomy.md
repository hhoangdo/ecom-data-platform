# Vina Bim Shop Category Taxonomy

## Taxonomy Policy

This taxonomy is Shopee-inspired but project-owned. It is intentionally stable so generated source data, Gold dimensions, feature tables, and evidence reports use the same category vocabulary across local runs.

The machine-readable companion file is:

- `../../data/reference/taxonomy/taxonomy_snapshot.yaml`

## Level-1 Categories And Subcategories

### FMCG

- `Health & Beauty`
- `Mom & Baby`
- `Household Goods`
- `Groceries`
- `Pet Care`

### ELHA

`ELHA` means `Electronics and Home Appliances`.

- `Mobile & Gadgets`
- `Computers & Accessories`
- `TVs & Audio`
- `Home Appliances`
- `Smart Devices & Cameras`

### Fashion

- `Women's Fashion`
- `Men's Fashion`
- `Footwear`
- `Bags & Luggage`
- `Fashion Accessories`

### Home & Living

- `Kitchen & Dining`
- `Bedding & Bath`
- `Furniture`
- `Home Decor`
- `Storage & Organization`

## Implementation Notes

| Asset | Role |
| --- | --- |
| `products.primary_category` and `products.primary_subcategory` | Main product taxonomy assignment used by product dimensions and order facts. |
| `product_category_map` | Implemented category bridge source with primary and secondary product-category assignments. |
| `dim_category` | Gold category/subcategory dimension with `category_cost_rate` for estimated margin logic. |
| `bridge_product_category` | Gold many-to-many bridge between products and categories. |

Category identifiers and names should stay stable once generated because they feed revenue rollups, margin estimates, product features, DataHub metadata, and reviewer-facing evidence.
