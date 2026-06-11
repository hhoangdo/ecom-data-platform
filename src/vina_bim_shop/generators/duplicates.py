from __future__ import annotations

from typing import Any

from vina_bim_shop.generators.config import GeneratorConfig


def issue_record(dataset: str, issue_type: str, affected_rows: int, observed_rate: float) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "issue_type": issue_type,
        "affected_rows": int(affected_rows),
        "observed_rate": round(float(observed_rate), 5),
    }


def inject_order_item_duplicates(
    config: GeneratorConfig,
    rng: Any,
    order_items: Any,
) -> tuple[Any, list[dict[str, Any]]]:
    import pandas as pd

    duplicate_rate = float(config.quality["offline_duplicate_rate"])
    n_dupes = max(1, int(len(order_items) * duplicate_rate))
    duplicate_rows = order_items.iloc[rng.choice(order_items.index.to_numpy(), size=n_dupes, replace=False)].copy()
    output = pd.concat([order_items, duplicate_rows], ignore_index=True)
    return output, [issue_record("order_items", "exact_duplicate_payload", n_dupes, n_dupes / len(output))]


def inject_stream_duplicates(
    config: GeneratorConfig,
    rng: Any,
    events: Any,
) -> tuple[Any, list[dict[str, Any]]]:
    import pandas as pd

    duplicate_rate = float(config.quality["stream_duplicate_rate"])
    n_dupes = max(1, int(len(events) * duplicate_rate))
    duplicate_rows = events.iloc[rng.choice(events.index.to_numpy(), size=n_dupes, replace=False)].copy()
    output = pd.concat([events, duplicate_rows], ignore_index=True)
    return output, [
        {
            "dataset": "commerce_events",
            "issue_type": "exact_duplicate_event_payload",
            "affected_rows": int(n_dupes),
            "observed_rate": round(float(n_dupes / len(output)), 5),
        }
    ]


def quarantine_issue_records(dataset: str, records: Any) -> list[dict[str, Any]]:
    total = max(1, len(records))
    return [
        {
            "dataset": dataset,
            "issue_type": str(error_reason),
            "affected_rows": int(count),
            "observed_rate": round(float(count / total), 5),
        }
        for error_reason, count in records["error_reason"].value_counts().sort_index().items()
    ]
