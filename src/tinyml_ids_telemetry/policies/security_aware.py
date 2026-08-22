"""Static alert-preserving telemetry-retention policy."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import cast

from tinyml_ids_telemetry.contracts.event import TelemetryEvent
from tinyml_ids_telemetry.policies.base import RetentionDecision

_IGNORED_COUNTERS = (
    "ignored_datagram_count",
    "ignored_wrong_endpoint_count",
    "ignored_bad_length_count",
    "ignored_non_ascii_count",
    "ignored_malformed_count",
    "ignored_bad_prefix_count",
    "ignored_bad_identity_count",
    "ignored_bad_attempt_count",
    "ignored_bad_inner_hex_count",
    "ignored_bad_crc_count",
    "ignored_bad_inner_text_count",
    "ignored_wrong_session_count",
    "ignored_wrong_stage_count",
    "ignored_wrong_transaction_count",
    "ignored_wrong_attempt_count",
)
_THRESHOLD_SNAPSHOT_ID = "d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff"
_THRESHOLD_QUANTILE_METHOD = "nearest-rank-p10-predicted-normal-per-model"
_MARGIN_THRESHOLDS = {
    "student_A": Decimal("6.330078125"),
    "student_B": Decimal("5.701171875"),
}
_LOGIT_FRAC_BITS = {
    "student_A": 9,
    "student_B": 9,
}
_NORMAL_SAMPLE_NUMERATOR = 1
_NORMAL_SAMPLE_DENOMINATOR = 10


class SecurityAwareStaticPolicy:
    policy_id = "security-aware-static-v1"

    def __init__(
        self,
        *,
        margin_thresholds: Mapping[str, Decimal],
        normal_sample_numerator: int,
        normal_sample_denominator: int,
    ) -> None:
        thresholds = dict(margin_thresholds)
        if thresholds != _MARGIN_THRESHOLDS:
            raise ValueError("margin thresholds must exactly match security-aware-static-v1")
        if (
            isinstance(normal_sample_numerator, bool)
            or not isinstance(normal_sample_numerator, int)
            or isinstance(normal_sample_denominator, bool)
            or not isinstance(normal_sample_denominator, int)
            or normal_sample_numerator != _NORMAL_SAMPLE_NUMERATOR
            or normal_sample_denominator != _NORMAL_SAMPLE_DENOMINATOR
        ):
            raise ValueError("normal sample fraction must exactly match security-aware-static-v1")
        self._margin_thresholds = thresholds
        self._logit_frac_bits = _LOGIT_FRAC_BITS.copy()
        self._sample_numerator = normal_sample_numerator
        self._sample_denominator = normal_sample_denominator

    def decide(self, event: TelemetryEvent) -> RetentionDecision:
        """Return an online decision without consulting the event's true class."""
        threshold = self._margin_thresholds.get(event.student_model)
        if threshold is None:
            raise ValueError(f"No margin threshold for model: {event.student_model}")
        if (
            isinstance(event.logit_frac_bits, bool)
            or not isinstance(event.logit_frac_bits, int)
            or event.logit_frac_bits != self._logit_frac_bits[event.student_model]
        ):
            raise ValueError(f"logit_frac_bits mismatch for model: {event.student_model}")

        if event.predicted_class_id != event.normal_class_id:
            return self._mandatory("predicted_attack")
        if event.status != "OK":
            return self._mandatory("device_status")
        if self._has_protocol_anomaly(event):
            return self._mandatory("protocol_anomaly")

        if self._normalized_margin(event) < threshold:
            return self._mandatory("low_margin")

        probability = self._sample_numerator / self._sample_denominator
        retained = self._sample_bucket(event) < self._sample_numerator
        return RetentionDecision(
            policy_id=self.policy_id,
            retained=retained,
            reason="normal_sample" if retained else "normal_not_sampled",
            inclusion_probability=probability,
            inverse_probability_weight=1.0 / probability,
        )

    def _mandatory(self, reason: str) -> RetentionDecision:
        return RetentionDecision(
            policy_id=self.policy_id,
            retained=True,
            reason=reason,
            inclusion_probability=1.0,
            inverse_probability_weight=1.0,
        )

    def _sample_bucket(self, event: TelemetryEvent) -> int:
        payload = f"{self.policy_id}|{event.source_event_id}".encode("ascii")
        value = int.from_bytes(sha256(payload).digest()[:8], "big")
        return value % self._sample_denominator

    @staticmethod
    def _normalized_margin(event: TelemetryEvent) -> Decimal:
        top_two = sorted(event.fixed_logits, reverse=True)[:2]
        return Decimal(top_two[0] - top_two[1]) / Decimal(1 << event.logit_frac_bits)

    @staticmethod
    def _has_protocol_anomaly(event: TelemetryEvent) -> bool:
        return (
            event.attempts > 1
            or event.response_timeout_count > 0
            or any(getattr(event, field) > 0 for field in _IGNORED_COUNTERS)
        )


def load_security_aware_policy(path: str | Path) -> SecurityAwareStaticPolicy:
    """Load the static policy from a versioned TOML contract."""
    with Path(path).open("rb") as handle:
        data = cast(dict[str, object], tomllib.load(handle))
    version = data.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version != 1:
        raise ValueError("Unsupported retention policy configuration version")
    raw_section = data.get("security_aware_static")
    if not isinstance(raw_section, dict):
        raise ValueError("security_aware_static must be a table")
    section = cast(dict[str, object], raw_section)
    if section.get("policy_id") != SecurityAwareStaticPolicy.policy_id:
        raise ValueError("security_aware_static policy_id mismatch")
    if section.get("threshold_snapshot_id") != _THRESHOLD_SNAPSHOT_ID:
        raise ValueError("security_aware_static threshold_snapshot_id mismatch")
    if section.get("threshold_quantile_method") != _THRESHOLD_QUANTILE_METHOD:
        raise ValueError("security_aware_static threshold_quantile_method mismatch")
    raw_logit_frac_bits = section.get("logit_frac_bits")
    if not isinstance(raw_logit_frac_bits, dict):
        raise ValueError("security_aware_static logit_frac_bits mismatch")
    if set(raw_logit_frac_bits) != set(_LOGIT_FRAC_BITS):
        raise ValueError("security_aware_static logit_frac_bits mismatch")
    if any(
        isinstance(value, bool) or not isinstance(value, int)
        for value in raw_logit_frac_bits.values()
    ):
        raise ValueError("security_aware_static logit_frac_bits mismatch")
    if raw_logit_frac_bits != _LOGIT_FRAC_BITS:
        raise ValueError("security_aware_static logit_frac_bits mismatch")
    numerator = section.get("normal_sample_numerator")
    denominator = section.get("normal_sample_denominator")
    if not isinstance(numerator, int) or isinstance(numerator, bool):
        raise ValueError("normal_sample_numerator must be an integer")
    if not isinstance(denominator, int) or isinstance(denominator, bool):
        raise ValueError("normal_sample_denominator must be an integer")
    raw_thresholds = section.get("margin_thresholds")
    if not isinstance(raw_thresholds, dict):
        raise ValueError("margin_thresholds must be a table")
    thresholds: dict[str, Decimal] = {}
    for model, raw_threshold in raw_thresholds.items():
        if not isinstance(model, str) or not isinstance(raw_threshold, str):
            raise ValueError("margin thresholds must map model names to decimal strings")
        try:
            thresholds[model] = Decimal(raw_threshold)
        except InvalidOperation as error:
            raise ValueError(f"Invalid margin threshold for model: {model}") from error
    return SecurityAwareStaticPolicy(
        margin_thresholds=thresholds,
        normal_sample_numerator=numerator,
        normal_sample_denominator=denominator,
    )
