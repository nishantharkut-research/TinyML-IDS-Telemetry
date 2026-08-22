"""Exact integer aggregation primitives."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IntCountSum:
    """Immutable count and exact integer sum."""

    count: int = 0
    total: int = 0

    def __post_init__(self) -> None:
        self._validate_integer("count", self.count)
        self._validate_integer("total", self.total)
        if self.count < 0:
            raise ValueError("count must be non-negative")

    def add(self, value: int) -> IntCountSum:
        """Return an aggregate containing one additional integer value."""
        self._validate_integer("value", value)
        return IntCountSum(count=self.count + 1, total=self.total + value)

    def merge(self, other: IntCountSum) -> IntCountSum:
        """Return the exact combination of two aggregates."""
        if not isinstance(other, IntCountSum):
            raise TypeError("other must be an IntCountSum")
        return IntCountSum(count=self.count + other.count, total=self.total + other.total)

    @staticmethod
    def _validate_integer(name: str, value: object) -> None:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an integer")
