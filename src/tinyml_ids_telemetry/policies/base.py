"""Shared retention decision contract."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from numbers import Real
from typing import Protocol

from tinyml_ids_telemetry.contracts.event import TelemetryEvent


@dataclass(frozen=True, slots=True)
class RetentionDecision:
    policy_id: str
    retained: bool
    reason: str
    inclusion_probability: float
    inverse_probability_weight: float

    def __post_init__(self) -> None:
        if not self.policy_id or not self.reason:
            raise ValueError("policy_id and reason must be non-empty")
        if not isinstance(self.retained, bool):
            raise TypeError("retained must be a bool")
        for name, value in (
            ("inclusion_probability", self.inclusion_probability),
            ("inverse_probability_weight", self.inverse_probability_weight),
        ):
            if isinstance(value, bool) or not isinstance(value, Real):
                raise TypeError(f"{name} must be a real number")
            if not isfinite(value):
                raise ValueError(f"{name} must be finite")
        if not 0 < self.inclusion_probability <= 1:
            raise ValueError("inclusion_probability must be in (0, 1]")
        if self.inverse_probability_weight < 1:
            raise ValueError("inverse_probability_weight must be at least one")
        if self.inverse_probability_weight != 1.0 / self.inclusion_probability:
            raise ValueError(
                "inverse_probability_weight must be the reciprocal of inclusion_probability"
            )


class RetentionPolicy(Protocol):
    policy_id: str

    def decide(self, event: TelemetryEvent) -> RetentionDecision: ...
