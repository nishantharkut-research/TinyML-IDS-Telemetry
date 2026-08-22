"""Canonical source telemetry event contract."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    """One immutable, hardware-executed source row joined to its reference."""

    SCHEMA_VERSION: ClassVar[str] = "telemetry-event-v1"

    source_event_id: str
    lineage_id: str
    source_session_id: str
    source_stage_id: str
    row_id: int
    source_row_index: int
    board: str
    student_model: str
    status: str
    true_class_id: int
    predicted_class_id: int
    fixed_predicted_class_id: int
    fp32_predicted_class_id: int
    class_names: tuple[str, ...]
    normal_class_id: int
    fixed_logits: tuple[int, ...]
    logit_frac_bits: int
    preprocess_us: int
    inference_us: int
    total_us: int
    attempts: int
    response_timeout_count: int
    ignored_datagram_count: int
    ignored_wrong_endpoint_count: int
    ignored_bad_length_count: int
    ignored_non_ascii_count: int
    ignored_malformed_count: int
    ignored_bad_prefix_count: int
    ignored_bad_identity_count: int
    ignored_bad_attempt_count: int
    ignored_bad_inner_hex_count: int
    ignored_bad_crc_count: int
    ignored_bad_inner_text_count: int
    ignored_wrong_session_count: int
    ignored_wrong_stage_count: int
    ignored_wrong_transaction_count: int
    ignored_wrong_attempt_count: int
    successful_request_bytes: int
    request_datagram_bytes_sent: int
    response_bytes: int
    host_observed_datagram_rtt_us: int
    transaction_elapsed_us: int

    def __post_init__(self) -> None:
        self._validate_digest("source_event_id", self.source_event_id)
        self._validate_digest("lineage_id", self.lineage_id)

        if not self.source_session_id:
            raise ValueError("source_session_id must be non-empty")
        if not self.source_stage_id:
            raise ValueError("source_stage_id must be non-empty")
        if not self.board:
            raise ValueError("board must be non-empty")
        if not self.student_model:
            raise ValueError("student_model must be non-empty")
        if not self.status:
            raise ValueError("status must be non-empty")

        self._validate_non_negative("row_id", self.row_id)
        self._validate_non_negative("source_row_index", self.source_row_index)

        if len(self.class_names) != 5 or len(set(self.class_names)) != 5:
            raise ValueError("class_names must contain five unique classes")
        if len(self.fixed_logits) != len(self.class_names):
            raise ValueError("fixed_logits must contain one value per class")

        for name, class_id in (
            ("true_class_id", self.true_class_id),
            ("predicted_class_id", self.predicted_class_id),
            ("fixed_predicted_class_id", self.fixed_predicted_class_id),
            ("fp32_predicted_class_id", self.fp32_predicted_class_id),
            ("normal_class_id", self.normal_class_id),
        ):
            if not 0 <= class_id < len(self.class_names):
                raise ValueError(f"{name} is outside the class mapping")

        if self.class_names[self.normal_class_id].casefold() != "normal":
            raise ValueError("normal_class_id does not identify the Normal class")

        first_argmax = max(range(len(self.fixed_logits)), key=self.fixed_logits.__getitem__)
        if self.predicted_class_id != first_argmax:
            raise ValueError("predicted_class_id must equal the first fixed-logit argmax")
        if self.fixed_predicted_class_id != self.predicted_class_id:
            raise ValueError("fixed_predicted_class_id must equal the MCU prediction")

        if self.logit_frac_bits < 0:
            raise ValueError("logit_frac_bits must be non-negative")

        for name in (
            "preprocess_us",
            "inference_us",
            "total_us",
            "response_timeout_count",
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
            "successful_request_bytes",
            "request_datagram_bytes_sent",
            "response_bytes",
            "host_observed_datagram_rtt_us",
            "transaction_elapsed_us",
        ):
            self._validate_non_negative(name, getattr(self, name))

        if self.attempts < 1:
            raise ValueError("attempts must be at least one")
        if self.total_us != self.preprocess_us + self.inference_us:
            raise ValueError("total_us must equal preprocess_us plus inference_us")

    @staticmethod
    def _validate_digest(name: str, value: str) -> None:
        if _SHA256_PATTERN.fullmatch(value) is None:
            raise ValueError(f"{name} must be a lowercase SHA-256 digest")

    @staticmethod
    def _validate_non_negative(name: str, value: int) -> None:
        if value < 0:
            raise ValueError(f"{name} must be non-negative")

    @property
    def normalized_logit_margin(self) -> float:
        """Return the top-two fixed-logit margin after Q-format scaling."""
        top_two = sorted(self.fixed_logits, reverse=True)[:2]
        return (top_two[0] - top_two[1]) / (1 << self.logit_frac_bits)

    def to_dict(self) -> dict[str, object]:
        """Serialize the event to its versioned JSON-compatible representation."""
        return {
            "schema_version": self.SCHEMA_VERSION,
            "source_event_id": self.source_event_id,
            "lineage_id": self.lineage_id,
            "source_session_id": self.source_session_id,
            "source_stage_id": self.source_stage_id,
            "row_id": self.row_id,
            "source_row_index": self.source_row_index,
            "board": self.board,
            "student_model": self.student_model,
            "status": self.status,
            "true_class_id": self.true_class_id,
            "true_class_name": self.class_names[self.true_class_id],
            "predicted_class_id": self.predicted_class_id,
            "predicted_class_name": self.class_names[self.predicted_class_id],
            "fixed_predicted_class_id": self.fixed_predicted_class_id,
            "fp32_predicted_class_id": self.fp32_predicted_class_id,
            "normal_class_id": self.normal_class_id,
            "is_predicted_attack": self.predicted_class_id != self.normal_class_id,
            "fixed_logits": list(self.fixed_logits),
            "logit_frac_bits": self.logit_frac_bits,
            "normalized_logit_margin": self.normalized_logit_margin,
            "preprocess_us": self.preprocess_us,
            "inference_us": self.inference_us,
            "total_us": self.total_us,
            "attempts": self.attempts,
            "response_timeout_count": self.response_timeout_count,
            "ignored_datagram_count": self.ignored_datagram_count,
            "ignored_wrong_endpoint_count": self.ignored_wrong_endpoint_count,
            "ignored_bad_length_count": self.ignored_bad_length_count,
            "ignored_non_ascii_count": self.ignored_non_ascii_count,
            "ignored_malformed_count": self.ignored_malformed_count,
            "ignored_bad_prefix_count": self.ignored_bad_prefix_count,
            "ignored_bad_identity_count": self.ignored_bad_identity_count,
            "ignored_bad_attempt_count": self.ignored_bad_attempt_count,
            "ignored_bad_inner_hex_count": self.ignored_bad_inner_hex_count,
            "ignored_bad_crc_count": self.ignored_bad_crc_count,
            "ignored_bad_inner_text_count": self.ignored_bad_inner_text_count,
            "ignored_wrong_session_count": self.ignored_wrong_session_count,
            "ignored_wrong_stage_count": self.ignored_wrong_stage_count,
            "ignored_wrong_transaction_count": self.ignored_wrong_transaction_count,
            "ignored_wrong_attempt_count": self.ignored_wrong_attempt_count,
            "successful_request_bytes": self.successful_request_bytes,
            "request_datagram_bytes_sent": self.request_datagram_bytes_sent,
            "response_bytes": self.response_bytes,
            "host_observed_datagram_rtt_us": self.host_observed_datagram_rtt_us,
            "transaction_elapsed_us": self.transaction_elapsed_us,
            "mcu_fixed_prediction_match": True,
            "mcu_fixed_logits_match": True,
        }
