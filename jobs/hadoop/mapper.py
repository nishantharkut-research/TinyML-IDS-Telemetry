"""Map canonical telemetry JSONL to exact integer metric partials."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TextIO, cast

_CLASS_NAMES = ("Blackhole", "Flooding", "Grayhole", "Normal", "TDMA")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_POLICY_IDS = ("retain-all-v1", "security-aware-static-v1")
_THRESHOLD_SNAPSHOT_ID = "d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff"
_THRESHOLD_QUANTILE_METHOD = "nearest-rank-p10-predicted-normal-per-model"
_EXPECTED_MARGIN_THRESHOLDS = {
    "student_A": Decimal("6.330078125"),
    "student_B": Decimal("5.701171875"),
}
_EXPECTED_LOGIT_FRAC_BITS = {"student_A": 9, "student_B": 9}
_IGNORED_COUNTER_FIELDS = (
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
_NON_NEGATIVE_INTEGER_FIELDS = (
    "row_id",
    "source_row_index",
    "logit_frac_bits",
    "preprocess_us",
    "inference_us",
    "total_us",
    "response_timeout_count",
    *_IGNORED_COUNTER_FIELDS,
    "successful_request_bytes",
    "request_datagram_bytes_sent",
    "response_bytes",
    "host_observed_datagram_rtt_us",
    "transaction_elapsed_us",
)
_EVENT_FIELDS = frozenset(
    {
        "schema_version",
        "source_event_id",
        "lineage_id",
        "source_session_id",
        "source_stage_id",
        "row_id",
        "source_row_index",
        "board",
        "student_model",
        "status",
        "true_class_id",
        "true_class_name",
        "predicted_class_id",
        "predicted_class_name",
        "fixed_predicted_class_id",
        "fp32_predicted_class_id",
        "normal_class_id",
        "is_predicted_attack",
        "fixed_logits",
        "logit_frac_bits",
        "normalized_logit_margin",
        "preprocess_us",
        "inference_us",
        "total_us",
        "attempts",
        "response_timeout_count",
        *_IGNORED_COUNTER_FIELDS,
        "successful_request_bytes",
        "request_datagram_bytes_sent",
        "response_bytes",
        "host_observed_datagram_rtt_us",
        "transaction_elapsed_us",
        "mcu_fixed_prediction_match",
        "mcu_fixed_logits_match",
    }
)


class MapperError(ValueError):
    """The mapper input or policy contract is invalid."""


@dataclass(frozen=True, slots=True)
class _PolicyConfig:
    margin_units: Mapping[str, int]
    logit_frac_bits: Mapping[str, int]
    sample_numerator: int
    sample_denominator: int


@dataclass(frozen=True, slots=True)
class _Decision:
    policy_id: str
    retained: bool
    reason: str
    probability_numerator: int
    probability_denominator: int


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _exact_int(value: object, context: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise MapperError(f"{context} must be an integer greater than or equal to {minimum}")
    return value


def _load_policy_config(path: str | Path) -> _PolicyConfig:
    try:
        with Path(path).open("rb") as handle:
            data = cast(dict[str, object], tomllib.load(handle))
    except OSError as error:
        raise MapperError("retention policy configuration could not be read") from error
    except tomllib.TOMLDecodeError as error:
        raise MapperError(f"invalid retention policy configuration: {error}") from error

    if set(data) != {"version", "retain_all", "security_aware_static"}:
        raise MapperError("retention policy configuration must contain exactly two policies")
    if _exact_int(data["version"], "policy configuration version") != 1:
        raise MapperError("unsupported retention policy configuration version")
    if data["retain_all"] != {"policy_id": _POLICY_IDS[0]}:
        raise MapperError("retain_all policy contract mismatch")

    security = data["security_aware_static"]
    expected_keys = {
        "policy_id",
        "normal_sample_numerator",
        "normal_sample_denominator",
        "threshold_snapshot_id",
        "threshold_quantile_method",
        "logit_frac_bits",
        "margin_thresholds",
    }
    if not isinstance(security, dict) or set(security) != expected_keys:
        raise MapperError("security_aware_static policy contract mismatch")
    if security["policy_id"] != _POLICY_IDS[1]:
        raise MapperError("security_aware_static policy_id mismatch")
    if security["threshold_snapshot_id"] != _THRESHOLD_SNAPSHOT_ID:
        raise MapperError("security_aware_static threshold_snapshot_id mismatch")
    if security["threshold_quantile_method"] != _THRESHOLD_QUANTILE_METHOD:
        raise MapperError("security_aware_static threshold_quantile_method mismatch")

    numerator = _exact_int(
        security["normal_sample_numerator"], "normal_sample_numerator", minimum=1
    )
    denominator = _exact_int(
        security["normal_sample_denominator"], "normal_sample_denominator", minimum=1
    )
    if (numerator, denominator) != (1, 10):
        raise MapperError("normal sample fraction must exactly match security-aware-static-v1")

    raw_bits = security["logit_frac_bits"]
    if not isinstance(raw_bits, dict) or set(raw_bits) != set(_EXPECTED_LOGIT_FRAC_BITS):
        raise MapperError("security_aware_static logit_frac_bits mismatch")
    bits = {
        model: _exact_int(value, f"logit_frac_bits for {model}")
        for model, value in raw_bits.items()
    }
    if bits != _EXPECTED_LOGIT_FRAC_BITS:
        raise MapperError("security_aware_static logit_frac_bits mismatch")

    raw_thresholds = security["margin_thresholds"]
    if not isinstance(raw_thresholds, dict) or set(raw_thresholds) != set(
        _EXPECTED_MARGIN_THRESHOLDS
    ):
        raise MapperError("security_aware_static margin thresholds mismatch")
    thresholds: dict[str, Decimal] = {}
    for model, raw_threshold in raw_thresholds.items():
        if not isinstance(raw_threshold, str):
            raise MapperError("margin thresholds must be decimal strings")
        try:
            thresholds[model] = Decimal(raw_threshold)
        except InvalidOperation as error:
            raise MapperError(f"invalid margin threshold for model: {model}") from error
    if thresholds != _EXPECTED_MARGIN_THRESHOLDS:
        raise MapperError("margin thresholds must exactly match security-aware-static-v1")
    margin_units = {
        model: int(threshold * (1 << bits[model])) for model, threshold in thresholds.items()
    }
    if margin_units != {"student_A": 3241, "student_B": 2919}:
        raise MapperError("margin thresholds must be exact Q9 values")
    return _PolicyConfig(margin_units, bits, numerator, denominator)


def _reject_constant(value: str) -> object:
    raise MapperError(f"non-finite JSON number is not permitted: {value}")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise MapperError(f"duplicate JSON property: {name}")
        result[name] = value
    return result


def _parse_event(line: str, line_number: int, config: _PolicyConfig) -> dict[str, object]:
    raw = line.rstrip("\r\n")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, MapperError) as error:
        raise MapperError(
            f"input line {line_number} must contain valid strict JSON: {error}"
        ) from error
    if not isinstance(value, dict):
        raise MapperError(f"input line {line_number} must be a JSON object")
    if raw != _canonical_json(value):
        raise MapperError(f"input line {line_number} must be canonical compact sorted JSON")
    event = cast(dict[str, object], value)
    missing = sorted(_EVENT_FIELDS - set(event))
    extra = sorted(set(event) - _EVENT_FIELDS)
    if missing:
        raise MapperError(f"input line {line_number} is missing required field {missing[0]}")
    if extra:
        raise MapperError(f"input line {line_number} contains unexpected field {extra[0]}")
    if event["schema_version"] != "telemetry-event-v1":
        raise MapperError(f"input line {line_number} has unsupported schema_version")

    for name in ("source_event_id", "lineage_id"):
        value = event[name]
        if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
            raise MapperError(f"input line {line_number} {name} must be a lowercase SHA-256")
    for name in ("source_session_id", "source_stage_id", "board", "student_model", "status"):
        value = event[name]
        if not isinstance(value, str) or not value.strip():
            raise MapperError(f"input line {line_number} {name} must be a non-empty string")
    for name in _NON_NEGATIVE_INTEGER_FIELDS:
        _exact_int(event[name], f"input line {line_number} {name}")
    attempts = _exact_int(event["attempts"], f"input line {line_number} attempts", minimum=1)
    for name in (
        "true_class_id",
        "predicted_class_id",
        "fixed_predicted_class_id",
        "fp32_predicted_class_id",
        "normal_class_id",
    ):
        class_id = _exact_int(event[name], f"input line {line_number} {name}")
        if class_id > 4:
            raise MapperError(f"input line {line_number} {name} must be between 0 and 4")

    logits = event["fixed_logits"]
    if (
        not isinstance(logits, list)
        or len(logits) != 5
        or any(type(logit) is not int for logit in logits)
    ):
        raise MapperError(f"input line {line_number} fixed_logits must be five integers")
    margin = event["normalized_logit_margin"]
    if (
        isinstance(margin, bool)
        or not isinstance(margin, (int, float))
        or not math.isfinite(margin)
        or margin < 0
    ):
        raise MapperError(
            f"input line {line_number} normalized_logit_margin must be a finite non-negative number"
        )
    for name in ("is_predicted_attack", "mcu_fixed_prediction_match", "mcu_fixed_logits_match"):
        if type(event[name]) is not bool:
            raise MapperError(f"input line {line_number} {name} must be a boolean")
    if (
        event["mcu_fixed_prediction_match"] is not True
        or event["mcu_fixed_logits_match"] is not True
    ):
        raise MapperError(f"input line {line_number} MCU equivalence fields must be true")

    model = cast(str, event["student_model"])
    if model not in config.margin_units:
        raise MapperError(f"input line {line_number} has unsupported student_model")
    if event["logit_frac_bits"] != config.logit_frac_bits[model]:
        raise MapperError(f"input line {line_number} logit_frac_bits mismatch")
    if event["normal_class_id"] != 3:
        raise MapperError(f"input line {line_number} normal_class_id must be 3")
    predicted = cast(int, event["predicted_class_id"])
    true_class = cast(int, event["true_class_id"])
    first_argmax = max(range(5), key=cast(list[int], logits).__getitem__)
    if predicted != first_argmax or event["fixed_predicted_class_id"] != predicted:
        raise MapperError(f"input line {line_number} has inconsistent prediction")
    if event["true_class_name"] != _CLASS_NAMES[true_class]:
        raise MapperError(f"input line {line_number} has inconsistent true_class_name")
    if event["predicted_class_name"] != _CLASS_NAMES[predicted]:
        raise MapperError(f"input line {line_number} has inconsistent predicted_class_name")
    predicted_attack = predicted != 3
    if event["is_predicted_attack"] is not predicted_attack:
        raise MapperError(f"input line {line_number} has inconsistent is_predicted_attack")
    top_two = sorted(cast(list[int], logits), reverse=True)[:2]
    expected_margin = (top_two[0] - top_two[1]) / (1 << config.logit_frac_bits[model])
    if margin != expected_margin:
        raise MapperError(f"input line {line_number} has inconsistent normalized_logit_margin")
    if event["total_us"] != cast(int, event["preprocess_us"]) + cast(int, event["inference_us"]):
        raise MapperError(f"input line {line_number} has inconsistent total_us")
    assert attempts >= 1
    return event


def _security_decision(event: Mapping[str, object], config: _PolicyConfig) -> _Decision:
    predicted = cast(int, event["predicted_class_id"])
    normal = cast(int, event["normal_class_id"])
    if predicted != normal:
        reason = "predicted_attack"
    elif event["status"] != "OK":
        reason = "device_status"
    elif (
        cast(int, event["attempts"]) > 1
        or cast(int, event["response_timeout_count"]) > 0
        or any(cast(int, event[name]) > 0 for name in _IGNORED_COUNTER_FIELDS)
    ):
        reason = "protocol_anomaly"
    else:
        logits = sorted(cast(list[int], event["fixed_logits"]), reverse=True)
        margin_units = logits[0] - logits[1]
        model = cast(str, event["student_model"])
        if margin_units < config.margin_units[model]:
            reason = "low_margin"
        else:
            payload = f"{_POLICY_IDS[1]}|{event['source_event_id']}".encode("ascii")
            bucket = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") % 10
            reason = "normal_sample" if bucket < config.sample_numerator else "normal_not_sampled"
    mandatory = reason in {"predicted_attack", "device_status", "protocol_anomaly", "low_margin"}
    retained = reason != "normal_not_sampled"
    return _Decision(_POLICY_IDS[1], retained, reason, 1, 1 if mandatory else 10)


def _metric_key(
    metric: str,
    dimensions: Sequence[tuple[str, object]] = (),
    *,
    statistical_unit: str = "hardware_execution",
) -> str:
    return _canonical_json(
        {
            "dimensions": [[name, value] for name, value in dimensions],
            "metric": metric,
            "statistical_unit": statistical_unit,
        }
    )


def _emit(
    target: TextIO,
    metric: str,
    value: int,
    dimensions: Sequence[tuple[str, object]] = (),
    *,
    statistical_unit: str = "hardware_execution",
) -> None:
    target.write(f"{_metric_key(metric, dimensions, statistical_unit=statistical_unit)}\t{value}\n")


def _emit_event(event: Mapping[str, object], target: TextIO, config: _PolicyConfig) -> None:
    canonical_payload = _canonical_json(event).encode("ascii")
    payload_sha256 = hashlib.sha256(canonical_payload).hexdigest()
    _emit(
        target,
        "source_event_occurrence_count",
        1,
        (
            ("source_event_id", event["source_event_id"]),
            ("lineage_id", event["lineage_id"]),
            ("row_id", event["row_id"]),
            ("source_row_index", event["source_row_index"]),
            ("payload_sha256", payload_sha256),
        ),
        statistical_unit="canonical_input_record",
    )
    _emit(
        target,
        "lineage_row_occurrence_count",
        1,
        (
            ("lineage_id", event["lineage_id"]),
            ("row_id", event["row_id"]),
            ("source_event_id", event["source_event_id"]),
            ("payload_sha256", payload_sha256),
        ),
        statistical_unit="canonical_input_record",
    )
    _emit(
        target,
        "predictive_source_row_occurrence_count",
        1,
        (
            ("source_row_index", event["source_row_index"]),
            ("lineage_id", event["lineage_id"]),
            ("source_event_id", event["source_event_id"]),
            ("payload_sha256", payload_sha256),
        ),
        statistical_unit="predictive_source_row",
    )

    lineage = (
        ("lineage_id", event["lineage_id"]),
        ("board", event["board"]),
        ("student_model", event["student_model"]),
    )
    count_dimensions = (
        (),
        lineage,
        (("board", event["board"]),),
        (("student_model", event["student_model"]),),
        (("true_class_id", event["true_class_id"]), ("true_class_name", event["true_class_name"])),
        (
            ("predicted_class_id", event["predicted_class_id"]),
            ("predicted_class_name", event["predicted_class_name"]),
        ),
        (("board", event["board"]), ("student_model", event["student_model"])),
    )
    for dimensions in count_dimensions:
        _emit(target, "hardware_execution_count", 1, dimensions)

    operational = (
        ("successful_execution_count", int(event["status"] == "OK")),
        ("predicted_attack_execution_count", int(cast(bool, event["is_predicted_attack"]))),
        ("retry_event_count", int(cast(int, event["attempts"]) > 1)),
        ("retry_count", cast(int, event["attempts"]) - 1),
        ("response_timeout_count_sum", cast(int, event["response_timeout_count"])),
        *((f"{name}_sum", cast(int, event[name])) for name in _IGNORED_COUNTER_FIELDS),
        ("preprocess_us_sum", cast(int, event["preprocess_us"])),
        ("inference_us_sum", cast(int, event["inference_us"])),
        ("total_us_sum", cast(int, event["total_us"])),
        (
            "request_datagram_bytes_sent_sum",
            cast(int, event["request_datagram_bytes_sent"]),
        ),
        ("response_bytes_sum", cast(int, event["response_bytes"])),
        (
            "host_observed_datagram_rtt_us_sum",
            cast(int, event["host_observed_datagram_rtt_us"]),
        ),
        ("transaction_elapsed_us_sum", cast(int, event["transaction_elapsed_us"])),
    )
    for metric, value in operational:
        _emit(target, metric, value, lineage)

    decisions = (
        _Decision(_POLICY_IDS[0], True, "retain_all", 1, 1),
        _security_decision(event, config),
    )
    predicted_attack = cast(bool, event["is_predicted_attack"])
    actual_attack = cast(int, event["true_class_id"]) != cast(int, event["normal_class_id"])
    for decision in decisions:
        policy = (("policy_id", decision.policy_id),)
        retention_metrics = (
            ("hardware_execution_count", 1),
            ("retained_hardware_execution_count", int(decision.retained)),
            ("predicted_attack_execution_count", int(predicted_attack)),
            (
                "retained_predicted_attack_execution_count",
                int(decision.retained and predicted_attack),
            ),
            ("actual_attack_execution_count", int(actual_attack)),
            ("retained_actual_attack_execution_count", int(decision.retained and actual_attack)),
            ("inclusion_probability_numerator_sum", decision.probability_numerator),
            ("inclusion_probability_denominator_sum", decision.probability_denominator),
        )
        for metric, value in retention_metrics:
            _emit(target, metric, value, policy)
        _emit(
            target,
            "decision_count",
            1,
            (
                ("policy_id", decision.policy_id),
                ("reason", decision.reason),
                ("retained", decision.retained),
            ),
        )


def run(source: TextIO, target: TextIO, policy_config: str | Path) -> None:
    """Read canonical event JSONL incrementally and emit mapper TSV records."""
    config = _load_policy_config(policy_config)
    observed = 0
    for line_number, line in enumerate(source, start=1):
        observed += 1
        if not line.strip():
            raise MapperError(f"input line {line_number} must not be blank")
        event = _parse_event(line, line_number, config)
        _emit_event(event, target, config)
    if observed == 0:
        raise MapperError("mapper input must contain at least one telemetry event")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Hadoop Streaming mapper over standard input and output."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy-config", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        run(sys.stdin, sys.stdout, args.policy_config)
    except MapperError as error:
        print(f"mapper failed: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
