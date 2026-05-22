from __future__ import annotations

import re

import pandas as pd


def _safe_hint(value: object) -> str:
    return re.sub(r"[^A-Z0-9]+", "-", str(value).upper()).strip("-")


def sequential_ids(prefix: str, hint: pd.Series | str, values: pd.Series) -> pd.Series:
    hints = hint if isinstance(hint, pd.Series) else pd.Series([hint] * len(values), index=values.index)
    safe_hints = hints.astype(str).map(_safe_hint)
    return prefix + "-" + safe_hints + "-" + values.astype(int).astype(str).str.zfill(8)


def dated_ids(prefix: str, hint: pd.Series, timestamps: pd.Series, values: pd.Series) -> pd.Series:
    date_part = pd.to_datetime(timestamps).dt.strftime("%Y%m%d")
    safe_hints = hint.astype(str).map(_safe_hint)
    return prefix + "-" + safe_hints + "-" + date_part + "-" + values.astype(int).astype(str).str.zfill(8)
