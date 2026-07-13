from __future__ import annotations


STANDARD_LAYOUT_PROFILE = "standard"
COMPACTION_EVIDENCE_LAYOUT_PROFILE = "compaction-evidence"
COMPACTION_EVIDENCE_BUCKET_COUNT = 2

_COMPACTION_EVIDENCE_TARGETS = {
    "fact_order": ("order_id", "order_date_key"),
    "fact_order_item": ("order_item_id", "order_date_key"),
}


def validate_layout_profile(layout_profile: str) -> str:
    if layout_profile not in {STANDARD_LAYOUT_PROFILE, COMPACTION_EVIDENCE_LAYOUT_PROFILE}:
        raise ValueError(f"Unsupported Spark layout profile: {layout_profile!r}")
    return layout_profile


def compaction_evidence_target(table_name: str) -> tuple[str, str] | None:
    return _COMPACTION_EVIDENCE_TARGETS.get(table_name)
