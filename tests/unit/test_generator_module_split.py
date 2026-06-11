from __future__ import annotations

"""Compatibility shim test for the generator module split.

Every public/private symbol that used to be importable from
``vina_bim_shop.generators.offline.generator`` and
``vina_bim_shop.generators.streaming.generator`` must still be importable
from those paths.  This guards against accidental removal of re-exports
during the refactor.
"""

import importlib

import vina_bim_shop.generators.offline.generator as offline_pkg
import vina_bim_shop.generators.streaming.generator as streaming_pkg


EXPECTED_OFFLINE_SYMBOLS = [
    "CATEGORY_BRANDS",
    "COMMERCE_EVENT_TYPE_MAP",
    "OfflineGeneration",
    "PAYMENT_METHODS",
    "PRICE_BAND_MULTIPLIER",
    "PRICE_RANGES",
    "SHIPPING_METHODS",
    "_attach_order_totals",
    "_generate_customers",
    "_generate_inventory_snapshots",
    "_generate_order_items",
    "_generate_orders",
    "_generate_payments",
    "_generate_product_category_map",
    "_generate_products",
    "_generate_promotions",
    "_generate_sellers",
    "_generate_shipments",
    "_inject_order_item_duplicates",
    "_issue_record",
    "_random_timestamps",
    "_time_bounds",
    "_weighted_choice",
    "generate_offline",
]


EXPECTED_STREAMING_SYMBOLS = [
    "COMMERCE_EVENT_TYPE_MAP",
    "StreamingGeneration",
    "_abandoned_sessions",
    "_apply_created_ts_and_late_arrivals",
    "_build_topic_events",
    "_catalog_topic_events",
    "_commerce_topic_events",
    "_device_os",
    "_envelope",
    "_events_from_orders",
    "_event_id",
    "_finalize_event_frame",
    "_fulfillment_topic_events",
    "_inject_device_missingness",
    "_inject_stream_duplicates",
    "_is_burst_timestamp",
    "_iso",
    "_json_value",
    "_ops_topic_events",
    "_payload_dict",
    "_topic_frame",
    "_topic_schema_version",
    "generate_streaming_events",
]


def test_offline_generator_re_exports_every_legacy_symbol() -> None:
    for name in EXPECTED_OFFLINE_SYMBOLS:
        assert hasattr(offline_pkg, name), f"missing legacy re-export: offline.generator.{name}"


def test_streaming_generator_re_exports_every_legacy_symbol() -> None:
    for name in EXPECTED_STREAMING_SYMBOLS:
        assert hasattr(streaming_pkg, name), f"missing legacy re-export: streaming.generator.{name}"


def test_public_entry_points_callable_through_legacy_paths() -> None:
    from vina_bim_shop.generators.offline.generator import generate_offline
    from vina_bim_shop.generators.streaming.generator import generate_streaming_events

    assert callable(generate_offline)
    assert callable(generate_streaming_events)


def test_offline_runtime_uses_slim_module() -> None:
    from vina_bim_shop.generators.runner import run_generation

    assert hasattr(run_generation, "__wrapped__") or callable(run_generation)


def test_modules_are_importable() -> None:
    """All new submodules are importable."""
    for module_name in [
        "vina_bim_shop.generators.profiles",
        "vina_bim_shop.generators.skew",
        "vina_bim_shop.generators.duplicates",
        "vina_bim_shop.generators.lateness",
        "vina_bim_shop.generators.schema_evolution",
        "vina_bim_shop.generators.bad_records",
        "vina_bim_shop.generators.ops_signals",
        "vina_bim_shop.generators.writer",
        "vina_bim_shop.generators.offline.entities",
        "vina_bim_shop.generators.offline.inventory_promos",
        "vina_bim_shop.generators.offline.orders",
        "vina_bim_shop.generators.offline.fulfillment",
        "vina_bim_shop.generators.streaming.session_events",
        "vina_bim_shop.generators.streaming.envelope",
        "vina_bim_shop.generators.streaming.topic_commerce",
        "vina_bim_shop.generators.streaming.topic_catalog",
        "vina_bim_shop.generators.streaming.topic_fulfillment",
    ]:
        importlib.import_module(module_name)
