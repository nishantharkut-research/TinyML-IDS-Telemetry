"""Reduce sorted Hadoop mapper partials to canonical exact metric rows."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from typing import TextIO, cast

_SHA256 = re.compile(r"[0-9a-f]{64}")
_CLASS_NAMES = ("Blackhole", "Flooding", "Grayhole", "Normal", "TDMA")
_POLICY_IDS = {"retain-all-v1", "security-aware-static-v1"}
_OCCURRENCE_METRICS = {
    "source_event_occurrence_count",
    "lineage_row_occurrence_count",
    "predictive_source_row_occurrence_count",
}
_LINEAGE_DIMENSIONS = ("lineage_id", "board", "student_model")
_POLICY_DIMENSIONS = ("policy_id",)
_OPERATIONAL_METRICS = {
    "successful_execution_count",
    "predicted_attack_execution_count",
    "retry_event_count",
    "retry_count",
    "response_timeout_count_sum",
    "ignored_datagram_count_sum",
    "ignored_wrong_endpoint_count_sum",
    "ignored_bad_length_count_sum",
    "ignored_non_ascii_count_sum",
    "ignored_malformed_count_sum",
    "ignored_bad_prefix_count_sum",
    "ignored_bad_identity_count_sum",
    "ignored_bad_attempt_count_sum",
    "ignored_bad_inner_hex_count_sum",
    "ignored_bad_crc_count_sum",
    "ignored_bad_inner_text_count_sum",
    "ignored_wrong_session_count_sum",
    "ignored_wrong_stage_count_sum",
    "ignored_wrong_transaction_count_sum",
    "ignored_wrong_attempt_count_sum",
    "preprocess_us_sum",
    "inference_us_sum",
    "total_us_sum",
    "request_datagram_bytes_sent_sum",
    "response_bytes_sum",
    "host_observed_datagram_rtt_us_sum",
    "transaction_elapsed_us_sum",
}
_RETENTION_METRICS = {
    "retained_hardware_execution_count",
    "predicted_attack_execution_count",
    "retained_predicted_attack_execution_count",
    "actual_attack_execution_count",
    "retained_actual_attack_execution_count",
    "inclusion_probability_numerator_sum",
    "inclusion_probability_denominator_sum",
}
_HARDWARE_COUNT_DIMENSIONS = {
    (),
    _LINEAGE_DIMENSIONS,
    ("board",),
    ("student_model",),
    ("true_class_id", "true_class_name"),
    ("predicted_class_id", "predicted_class_name"),
    ("board", "student_model"),
    _POLICY_DIMENSIONS,
}
_SECURITY_RETAINED_REASONS = {
    "predicted_attack",
    "device_status",
    "protocol_anomaly",
    "low_margin",
    "normal_sample",
}


class ReducerError(ValueError):
    """The reducer input does not satisfy the mapper-output contract."""


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _reject_constant(value: str) -> object:
    raise ReducerError(f"non-finite JSON number is not permitted: {value}")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ReducerError(f"duplicate JSON property: {name}")
        result[name] = value
    return result


def _parse_json(raw: str, context: str) -> object:
    try:
        return json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, ReducerError) as error:
        raise ReducerError(f"{context} must be strict JSON: {error}") from error


def _required_string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReducerError(f"{context} must be a non-empty string")
    return value


def _required_digest(value: object, context: str) -> str:
    text = _required_string(value, context)
    if _SHA256.fullmatch(text) is None:
        raise ReducerError(f"{context} must be a lowercase SHA-256 digest")
    return text


def _required_class(value: object, context: str) -> int:
    if type(value) is not int or not 0 <= value <= 4:
        raise ReducerError(f"{context} must be an integer between 0 and 4")
    return value


def _validate_dimension_values(dimensions: Mapping[str, object]) -> None:
    for name in ("board", "student_model", "reason"):
        if name in dimensions:
            _required_string(dimensions[name], f"dimension {name}")
    for name in ("lineage_id", "source_event_id", "payload_sha256"):
        if name in dimensions:
            _required_digest(dimensions[name], f"dimension {name}")
    for name in ("row_id", "source_row_index"):
        if name in dimensions:
            value = dimensions[name]
            if type(value) is not int or value < 0:
                raise ReducerError(f"dimension {name} must be a non-negative integer")
    if "student_model" in dimensions and dimensions["student_model"] not in {
        "student_A",
        "student_B",
    }:
        raise ReducerError("dimension student_model is unsupported")
    for id_name, class_name in (
        ("true_class_id", "true_class_name"),
        ("predicted_class_id", "predicted_class_name"),
    ):
        if id_name in dimensions:
            class_id = _required_class(dimensions[id_name], f"dimension {id_name}")
            if dimensions[class_name] != _CLASS_NAMES[class_id]:
                raise ReducerError(f"dimension {class_name} is inconsistent")
    if "policy_id" in dimensions and dimensions["policy_id"] not in _POLICY_IDS:
        raise ReducerError("dimension policy_id is unsupported")
    if "retained" in dimensions and type(dimensions["retained"]) is not bool:
        raise ReducerError("dimension retained must be a boolean")


def validate_mapper_key(value: object) -> dict[str, object]:
    """Validate and return one exact Hadoop mapper metric key."""
    if not isinstance(value, dict) or set(value) != {
        "dimensions",
        "metric",
        "statistical_unit",
    }:
        raise ReducerError("mapper key must contain exactly dimensions, metric, statistical_unit")
    key = cast(dict[str, object], value)
    metric = _required_string(key["metric"], "mapper key metric")
    unit = _required_string(key["statistical_unit"], "mapper key statistical_unit")
    raw_dimensions = key["dimensions"]
    if not isinstance(raw_dimensions, list):
        raise ReducerError("mapper key dimensions must be an array of name/value pairs")
    pairs: list[tuple[str, object]] = []
    for index, pair in enumerate(raw_dimensions):
        if not isinstance(pair, list) or len(pair) != 2:
            raise ReducerError(f"mapper key dimensions[{index}] must be a name/value pair")
        name = _required_string(pair[0], f"mapper key dimensions[{index}] name")
        pairs.append((name, pair[1]))
    names = tuple(name for name, _ in pairs)
    if len(set(names)) != len(names):
        raise ReducerError("mapper key dimension names must be unique")
    dimensions = dict(pairs)
    _validate_dimension_values(dimensions)

    valid = False
    if metric == "source_event_occurrence_count":
        valid = unit == "canonical_input_record" and names == (
            "source_event_id",
            "lineage_id",
            "row_id",
            "source_row_index",
            "payload_sha256",
        )
    elif metric == "lineage_row_occurrence_count":
        valid = unit == "canonical_input_record" and names == (
            "lineage_id",
            "row_id",
            "source_event_id",
            "payload_sha256",
        )
    elif metric == "predictive_source_row_occurrence_count":
        valid = unit == "predictive_source_row" and names == (
            "source_row_index",
            "lineage_id",
            "source_event_id",
            "payload_sha256",
        )
    elif unit == "hardware_execution":
        if metric == "hardware_execution_count":
            valid = names in _HARDWARE_COUNT_DIMENSIONS
        elif metric == "decision_count":
            valid = names == ("policy_id", "reason", "retained")
        else:
            valid = (metric in _OPERATIONAL_METRICS and names == _LINEAGE_DIMENSIONS) or (
                metric in _RETENTION_METRICS and names == _POLICY_DIMENSIONS
            )
    if not valid:
        raise ReducerError("mapper key metric, unit, and dimensions do not match an exact schema")
    if metric == "decision_count":
        policy_id = dimensions["policy_id"]
        reason = dimensions["reason"]
        retained = dimensions["retained"]
        if policy_id == "retain-all-v1":
            if (reason, retained) != ("retain_all", True):
                raise ReducerError("retain-all-v1 decision dimensions are inconsistent")
        elif reason == "normal_not_sampled":
            if retained is not False:
                raise ReducerError("normal_not_sampled decision must not be retained")
        elif reason not in _SECURITY_RETAINED_REASONS or retained is not True:
            raise ReducerError("security-aware-static-v1 decision dimensions are inconsistent")
    return key


def _parse_record(line: str, line_number: int) -> tuple[str, dict[str, object], int]:
    raw = line.rstrip("\r\n")
    if not raw:
        raise ReducerError(f"input line {line_number} must not be blank")
    if raw.count("\t") != 1:
        raise ReducerError(f"input line {line_number} must contain exactly one tab")
    raw_key, raw_partial = raw.split("\t")
    key_value = _parse_json(raw_key, f"input line {line_number} key")
    key = validate_mapper_key(key_value)
    if raw_key != _canonical_json(key):
        raise ReducerError(f"input line {line_number} key must be canonical compact JSON")
    partial_value = _parse_json(raw_partial, f"input line {line_number} partial")
    if type(partial_value) is not int or partial_value < 0:
        raise ReducerError(f"input line {line_number} partial must be a non-negative integer")
    if (
        key["metric"]
        in {
            "source_event_occurrence_count",
            "hardware_execution_count",
            "decision_count",
        }
        and partial_value == 0
    ):
        raise ReducerError(f"input line {line_number} count partial must be a positive integer")
    if key["metric"] in _OCCURRENCE_METRICS and partial_value != 1:
        raise ReducerError(f"input line {line_number} occurrence partial must equal one")
    if raw_partial != _canonical_json(partial_value):
        raise ReducerError(f"input line {line_number} partial must be canonical compact JSON")
    return raw_key, key, partial_value


def _write_row(target: TextIO, key: Mapping[str, object], value: int) -> None:
    raw_dimensions = cast(list[list[object]], key["dimensions"])
    dimensions = {cast(str, pair[0]): pair[1] for pair in raw_dimensions}
    target.write(
        _canonical_json(
            {
                "dimensions": dimensions,
                "metric": key["metric"],
                "statistical_unit": key["statistical_unit"],
                "value": value,
            }
        )
        + "\n"
    )


def run(source: TextIO, target: TextIO) -> None:
    """Aggregate sorted mapper records while retaining only the current key."""
    current_raw_key: str | None = None
    current_key: dict[str, object] | None = None
    current_sum = 0
    last_source_id: str | None = None
    last_payload_sha256: str | None = None
    last_lineage_row: tuple[str, int] | None = None
    last_lineage_identity: tuple[str, str] | None = None
    last_predictive_row: tuple[int, str] | None = None
    last_predictive_identity: tuple[str, str] | None = None
    observed = 0

    for line_number, line in enumerate(source, start=1):
        observed += 1
        raw_key, key, partial = _parse_record(line, line_number)
        if current_raw_key is not None and raw_key < current_raw_key:
            raise ReducerError(f"input line {line_number} is not sorted by mapper key")

        raw_dimensions = cast(list[list[object]], key["dimensions"])
        dimensions: dict[str, object] = {cast(str, pair[0]): pair[1] for pair in raw_dimensions}
        if key["metric"] == "source_event_occurrence_count":
            source_id = cast(str, dimensions["source_event_id"])
            payload_sha256 = cast(str, dimensions["payload_sha256"])
            if source_id == last_source_id:
                if payload_sha256 != last_payload_sha256:
                    raise ReducerError(f"source_event_id {source_id} has conflicting payloads")
                raise ReducerError(f"repeated source_event_id {source_id}")
            last_source_id = source_id
            last_payload_sha256 = payload_sha256
        elif key["metric"] == "lineage_row_occurrence_count":
            lineage_row = (
                cast(str, dimensions["lineage_id"]),
                cast(int, dimensions["row_id"]),
            )
            lineage_identity = (
                cast(str, dimensions["source_event_id"]),
                cast(str, dimensions["payload_sha256"]),
            )
            if lineage_row == last_lineage_row:
                if lineage_identity != last_lineage_identity:
                    raise ReducerError(f"conflicting lineage row {lineage_row}")
                raise ReducerError(f"repeated lineage row {lineage_row}")
            last_lineage_row = lineage_row
            last_lineage_identity = lineage_identity
        elif key["metric"] == "predictive_source_row_occurrence_count":
            predictive_row = (
                cast(int, dimensions["source_row_index"]),
                cast(str, dimensions["lineage_id"]),
            )
            predictive_identity = (
                cast(str, dimensions["source_event_id"]),
                cast(str, dimensions["payload_sha256"]),
            )
            if predictive_row == last_predictive_row:
                if predictive_identity != last_predictive_identity:
                    raise ReducerError(f"conflicting predictive source row {predictive_row}")
                raise ReducerError(f"repeated predictive source row {predictive_row}")
            last_predictive_row = predictive_row
            last_predictive_identity = predictive_identity

        if raw_key != current_raw_key:
            if current_key is not None:
                _write_row(target, current_key, current_sum)
            current_raw_key = raw_key
            current_key = key
            current_sum = partial
        else:
            current_sum += partial

    if observed == 0:
        raise ReducerError("reducer input must contain at least one mapper record")
    assert current_key is not None
    _write_row(target, current_key, current_sum)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Hadoop Streaming reducer over standard input and output."""
    argparse.ArgumentParser(description=__doc__).parse_args(argv)
    try:
        run(sys.stdin, sys.stdout)
    except ReducerError as error:
        print(f"reducer failed: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
