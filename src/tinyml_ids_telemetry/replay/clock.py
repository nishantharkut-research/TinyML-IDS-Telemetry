"""Deterministic synthetic replay time."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True, slots=True)
class SyntheticClock:
    """Map original zero-based source offsets to deterministic UTC times."""

    start_utc: datetime
    interval: timedelta

    def __post_init__(self) -> None:
        if self.start_utc.tzinfo is None or self.start_utc.utcoffset() is None:
            raise ValueError("start_utc must be timezone-aware")
        if self.interval <= timedelta(0):
            raise ValueError("interval must be positive")
        object.__setattr__(self, "start_utc", self.start_utc.astimezone(UTC))

    def at_offset(self, source_offset: int) -> datetime:
        """Return UTC synthetic time for an original source offset."""
        if type(source_offset) is not int or source_offset < 0:
            raise ValueError("source_offset must be a non-negative integer")
        return self.start_utc + self.interval * source_offset
