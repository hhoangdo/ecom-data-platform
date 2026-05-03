# Vina Bim Shop Category Taxonomy

## Taxonomy Policy

This taxonomy is Shopee-inspired but project-owned. It is intentionally simplified so that the coursework can keep a stable category snapshot across all sections.

The machine-readable companion file is:

- `data/reference/taxonomy/taxonomy_snapshot.yaml`

## Level-1 Categories and Subcategories

### FMCG

- `Health & Beauty`
- `Mom & Baby`
- `Household Goods`
- `Groceries`
- `Pet Care`

### ELHA

`ELHA` is treated as `Electronics and Home Appliances`.

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

## Modeling Notes

- Products belong to one primary subcategory in the base generator.
- A separate `product_category_map` dataset is still reserved so the design can later support many-to-many taxonomy assignments if needed.
- Category identifiers should stay stable once generated so that Gold dimensions and future feature tables do not churn unnecessarily.
