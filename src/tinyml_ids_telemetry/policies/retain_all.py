"""Lossless retention baseline."""

from __future__ import annotations

from tinyml_ids_telemetry.contracts.event import TelemetryEvent
from tinyml_ids_telemetry.policies.base import RetentionDecision


class RetainAllPolicy:
    policy_id = "retain-all-v1"

    def decide(self, event: TelemetryEvent) -> RetentionDecision:
        """Retain every event accepted by the canonical event contract."""
        return RetentionDecision(
            policy_id=self.policy_id,
            retained=True,
            reason="retain_all",
            inclusion_probability=1.0,
            inverse_probability_weight=1.0,
        )
