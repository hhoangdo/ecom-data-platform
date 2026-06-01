from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


def parse_iso8601_utc(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError(f"Expected timezone-aware timestamp, got {value!r}.")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class BatchWindow:
    start_ts: datetime
    end_ts: datetime
    mode: str

    @classmethod
    def from_args(cls, *, start_ts: str, end_ts: str, mode: str) -> "BatchWindow":
        start = parse_iso8601_utc(start_ts)
        end = parse_iso8601_utc(end_ts)
        normalized_mode = mode.strip().lower()
        if normalized_mode not in {"hourly", "backfill"}:
            raise ValueError(f"Unsupported batch mode: {mode!r}.")
        if end <= start:
            raise ValueError("Expected end-ts to be greater than start-ts.")
        if normalized_mode == "hourly" and end - start != timedelta(hours=1):
            raise ValueError("Hourly mode requires an exact one-hour UTC window.")
        return cls(start_ts=start, end_ts=end, mode=normalized_mode)

    def to_cli_args(self) -> list[str]:
        return [
            "--start-ts",
            self.start_ts.isoformat().replace("+00:00", "Z"),
            "--end-ts",
            self.end_ts.isoformat().replace("+00:00", "Z"),
            "--mode",
            self.mode,
        ]
