"""Offline labeled evaluation of telemetry retention decisions."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from tinyml_ids_telemetry.contracts.event import TelemetryEvent
from tinyml_ids_telemetry.policies.base import RetentionDecision


@dataclass(frozen=True, slots=True)
class RetentionEvaluation:
    """Counts describing one policy's retention of labeled events."""

    policy_id: str
    total_events: int
    retained_events: int
    predicted_attack_events: int
    retained_predicted_attack_events: int
    true_attack_events: int
    retained_true_attack_events: int
    missed_attack_events: int
    retained_missed_attack_events: int


def evaluate_retention(
    event_decisions: Iterable[tuple[TelemetryEvent, RetentionDecision]],
) -> RetentionEvaluation:
    """Evaluate retention decisions against offline event labels."""
    policy_id: str | None = None
    source_event_ids: set[str] = set()
    total_events = 0
    retained_events = 0
    predicted_attack_events = 0
    retained_predicted_attack_events = 0
    true_attack_events = 0
    retained_true_attack_events = 0
    missed_attack_events = 0
    retained_missed_attack_events = 0

    for event, decision in event_decisions:
        if event.source_event_id in source_event_ids:
            raise ValueError(f"duplicate source_event_id: {event.source_event_id}")
        source_event_ids.add(event.source_event_id)

        if policy_id is None:
            policy_id = decision.policy_id
        elif decision.policy_id != policy_id:
            raise ValueError("mixed policy IDs are not supported")

        predicted_attack = event.predicted_class_id != event.normal_class_id
        true_attack = event.true_class_id != event.normal_class_id
        missed_attack = true_attack and not predicted_attack

        total_events += 1
        retained_events += int(decision.retained)
        predicted_attack_events += int(predicted_attack)
        retained_predicted_attack_events += int(decision.retained and predicted_attack)
        true_attack_events += int(true_attack)
        retained_true_attack_events += int(decision.retained and true_attack)
        missed_attack_events += int(missed_attack)
        retained_missed_attack_events += int(decision.retained and missed_attack)

    if policy_id is None:
        raise ValueError("retention evaluation requires at least one event")

    return RetentionEvaluation(
        policy_id=policy_id,
        total_events=total_events,
        retained_events=retained_events,
        predicted_attack_events=predicted_attack_events,
        retained_predicted_attack_events=retained_predicted_attack_events,
        true_attack_events=true_attack_events,
        retained_true_attack_events=retained_true_attack_events,
        missed_attack_events=missed_attack_events,
        retained_missed_attack_events=retained_missed_attack_events,
    )
