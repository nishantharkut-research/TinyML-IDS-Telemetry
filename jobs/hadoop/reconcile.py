"""Reconcile finalized Spark aggregates with Hadoop Streaming metric JSONL."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TextIO, cast

from jobs.hadoop.oracle_manifest import (
    OracleManifestError,
    validate_finalized_run_manifest_mapping,
    validate_hadoop_oracle_manifest_mapping,
)
from jobs.hadoop.reducer import ReducerError, validate_mapper_key
from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceContractError,
    ensure_output_outside_evidence_roots,
)

_LINEAGE_DIMENSIONS = ("lineage_id", "board", "student_model")
_POLICY_DIMENSIONS = ("policy_id",)
_CLASS_NAMES = ("Blackhole", "Flooding", "Grayhole", "Normal", "TDMA")
_EXECUTION_DIMENSION_FIELDS = ("board", "student_model", "class_id", "class_name")
_EXECUTION_DIMENSION_SHAPES = {
    "board": frozenset({"board"}),
    "student_model": frozenset({"student_model"}),
    "true_class": frozenset({"class_id", "class_name"}),
    "predicted_class": frozenset({"class_id", "class_name"}),
    "board_student_model": frozenset({"board", "student_model"}),
}
_EXECUTION_DIMENSION_MAPPER_SHAPES = frozenset(
    {
        frozenset({"board"}),
        frozenset({"student_model"}),
        frozenset({"true_class_id", "true_class_name"}),
        frozenset({"predicted_class_id", "predicted_class_name"}),
        frozenset({"board", "student_model"}),
    }
)
_PERCENTILE = re.compile(r"(?:percentile|(?:^|_)p\d+(?:_|$))", re.IGNORECASE)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_OCCURRENCE_METRICS = frozenset(
    {
        "source_event_occurrence_count",
        "lineage_row_occurrence_count",
        "predictive_source_row_occurrence_count",
    }
)
_OPERATIONAL_METRICS = (
    "hardware_execution_count",
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
)
_PREDICTIVE_METRICS = (
    "hardware_execution_count",
    "execution_lineage_count",
    "unique_predictive_row_count",
)
_PREDICTIVE_METRIC_UNITS = {
    "hardware_execution_count": "hardware_execution",
    "execution_lineage_count": "execution_lineage",
    "unique_predictive_row_count": "predictive_source_row",
}
_RETENTION_METRICS = (
    "hardware_execution_count",
    "retained_hardware_execution_count",
    "predicted_attack_execution_count",
    "retained_predicted_attack_execution_count",
    "actual_attack_execution_count",
    "retained_actual_attack_execution_count",
    "inclusion_probability_numerator_sum",
    "inclusion_probability_denominator_sum",
)
_DATASET_SCHEMAS = {
    "operational_metrics": (
        ("replay_run_id", *_LINEAGE_DIMENSIONS),
        "hardware_execution",
        _OPERATIONAL_METRICS,
    ),
    "retention_metrics": (
        ("replay_run_id", *_POLICY_DIMENSIONS),
        "hardware_execution",
        _RETENTION_METRICS,
    ),
    "retention_reason_counts": (
        ("replay_run_id", "policy_id", "reason", "retained"),
        "hardware_execution",
        ("decision_count",),
    ),
}
_SPARK_AGGREGATE_NAMES = frozenset(
    {*_DATASET_SCHEMAS, "execution_dimension_counts", "predictive_row_metrics"}
)
_PERCENTILE_SHAPES = frozenset(
    (
        unit,
        frozenset(name for name in dimension_names if name != "replay_run_id"),
    )
    for dimension_names, unit, _ in _DATASET_SCHEMAS.values()
)


class ReconciliationError(ValueError):
    """The aggregate inputs or publication contract are invalid."""


Number = int | float
JsonScalar = str | int | bool


@dataclass(frozen=True, slots=True)
class _MetricKey:
    dimensions: tuple[tuple[str, JsonScalar], ...]
    metric: str
    statistical_unit: str

    @classmethod
    def create(
        cls,
        dimensions: Mapping[str, JsonScalar],
        metric: str,
        statistical_unit: str,
    ) -> _MetricKey:
        return cls(tuple(sorted(dimensions.items())), metric, statistical_unit)

    def as_dict(self) -> dict[str, object]:
        return {
            "dimensions": dict(self.dimensions),
            "metric": self.metric,
            "statistical_unit": self.statistical_unit,
        }


@dataclass(frozen=True, slots=True)
class _MetricRow:
    key: _MetricKey
    value: Number
    mapper_key: str


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def canonical_report(report: Mapping[str, object]) -> str:
    """Serialize one deterministic compact JSON reconciliation report."""
    return _canonical_json(report) + "\n"


def _reject_constant(value: str) -> object:
    raise ReconciliationError(f"non-finite JSON number is not permitted: {value}")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ReconciliationError(f"duplicate JSON property: {name}")
        result[name] = value
    return result


def _is_percentile(field: str) -> bool:
    return _PERCENTILE.search(field) is not None


def _number(value: object, context: str) -> Number:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReconciliationError(f"{context} must be a finite number")
    if isinstance(value, float) and not math.isfinite(value):
        raise ReconciliationError(f"{context} must be a finite number")
    return value


def _non_negative_integer(value: object, context: str) -> int:
    if type(value) is not int or value < 0:
        raise ReconciliationError(f"{context} must be a non-negative integer")
    return value


def _ordered_dimension_names(metric: str, dimensions: Mapping[str, JsonScalar]) -> tuple[str, ...]:
    names = set(dimensions)
    if metric == "source_event_occurrence_count":
        return (
            "source_event_id",
            "lineage_id",
            "row_id",
            "source_row_index",
            "payload_sha256",
        )
    if metric == "lineage_row_occurrence_count":
        return ("lineage_id", "row_id", "source_event_id", "payload_sha256")
    if metric == "predictive_source_row_occurrence_count":
        return ("source_row_index", "lineage_id", "source_event_id", "payload_sha256")
    if names == set(_LINEAGE_DIMENSIONS):
        return _LINEAGE_DIMENSIONS
    if names == {"policy_id", "reason", "retained"}:
        return ("policy_id", "reason", "retained")
    if names == {"policy_id"}:
        return _POLICY_DIMENSIONS
    if names == {"true_class_id", "true_class_name"}:
        return ("true_class_id", "true_class_name")
    if names == {"predicted_class_id", "predicted_class_name"}:
        return ("predicted_class_id", "predicted_class_name")
    if names == {"board", "student_model"}:
        return ("board", "student_model")
    if names == {"board"}:
        return ("board",)
    if names == {"student_model"}:
        return ("student_model",)
    return tuple(sorted(names))


def _parse_hadoop_row(raw_line: str, line_number: int) -> _MetricRow:
    framed = raw_line.rstrip("\r\n")
    if not framed:
        raise ReconciliationError(f"Hadoop metric line {line_number} must not be blank")
    raw = framed[:-1] if framed.endswith("\t") else framed
    if not raw or "\t" in raw:
        raise ReconciliationError(
            f"Hadoop metric line {line_number} has invalid text-output framing"
        )
    try:
        parsed = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, ReconciliationError) as error:
        raise ReconciliationError(
            f"Hadoop metric line {line_number} must contain strict JSON: {error}"
        ) from error
    if not isinstance(parsed, dict) or set(parsed) != {
        "dimensions",
        "metric",
        "statistical_unit",
        "value",
    }:
        raise ReconciliationError(
            f"Hadoop metric line {line_number} must contain exactly dimensions, metric, "
            "statistical_unit, and value"
        )
    if raw != _canonical_json(parsed):
        raise ReconciliationError(
            f"Hadoop metric line {line_number} must be canonical compact JSON"
        )
    raw_dimensions = parsed["dimensions"]
    if not isinstance(raw_dimensions, dict):
        raise ReconciliationError(f"Hadoop metric line {line_number} dimensions must be an object")
    dimensions: dict[str, JsonScalar] = {}
    for name, value in raw_dimensions.items():
        if not isinstance(name, str) or not name.strip():
            raise ReconciliationError(
                f"Hadoop metric line {line_number} dimension names must be non-empty strings"
            )
        if type(value) not in {str, int, bool}:
            raise ReconciliationError(
                f"Hadoop metric line {line_number} dimension {name} has an invalid type"
            )
        dimensions[name] = cast(JsonScalar, value)
    metric = parsed["metric"]
    unit = parsed["statistical_unit"]
    if not isinstance(metric, str) or not metric.strip():
        raise ReconciliationError(f"Hadoop metric line {line_number} metric must be non-empty")
    if not isinstance(unit, str) or not unit.strip():
        raise ReconciliationError(
            f"Hadoop metric line {line_number} statistical_unit must be non-empty"
        )
    value = (
        _number(parsed["value"], f"Hadoop metric line {line_number} percentile value")
        if _is_percentile(metric)
        else _non_negative_integer(parsed["value"], f"Hadoop metric line {line_number} value")
    )
    if metric in _OCCURRENCE_METRICS and value != 1:
        raise ReconciliationError(
            f"Hadoop metric line {line_number} {metric} value must equal exactly one"
        )
    ordered_names = _ordered_dimension_names(metric, dimensions)
    if set(ordered_names) != set(dimensions):
        raise ReconciliationError(
            f"Hadoop metric line {line_number} dimensions do not match the metric schema"
        )
    reducer_key = {
        "dimensions": [[name, dimensions[name]] for name in ordered_names],
        "metric": metric,
        "statistical_unit": unit,
    }
    if not _is_percentile(metric):
        try:
            validate_mapper_key(reducer_key)
        except ReducerError as error:
            raise ReconciliationError(
                f"Hadoop metric line {line_number} does not match the reducer schema: {error}"
            ) from error
    return _MetricRow(
        _MetricKey.create(dimensions, metric, unit),
        value,
        _canonical_json(reducer_key),
    )


def _validate_spark_dimension(name: str, value: object, context: str) -> JsonScalar:
    if name == "retained":
        if type(value) is not bool:
            raise ReconciliationError(f"{context} retained must be a boolean")
        return value
    if not isinstance(value, str) or not value.strip():
        raise ReconciliationError(f"{context} {name} must be a non-empty string")
    if name == "lineage_id" and re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ReconciliationError(f"{context} lineage_id must be a lowercase SHA-256 digest")
    return value


def _decimal(value: Number, context: str) -> Decimal:
    try:
        return Decimal(str(value))
    except InvalidOperation as error:
        raise ReconciliationError(f"{context} must be a finite number") from error


def _spark_metrics(
    aggregates: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[dict[_MetricKey, Number], set[str], str]:
    if set(aggregates) != _SPARK_AGGREGATE_NAMES:
        raise ReconciliationError(
            "Spark aggregates must contain exactly execution_dimension_counts, "
            "operational_metrics, predictive_row_metrics, retention_metrics, and "
            "retention_reason_counts"
        )
    metrics: dict[_MetricKey, Number] = {}
    run_ids: set[str] = set()
    percentile_fields: set[str] = set()
    for dataset, (dimension_names, expected_unit, metric_names) in _DATASET_SCHEMAS.items():
        rows = aggregates[dataset]
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise ReconciliationError(f"Spark {dataset} must be a sequence of aggregate rows")
        required = {*dimension_names, "statistical_unit", *metric_names}
        for index, raw_row in enumerate(rows):
            context = f"Spark {dataset} row {index + 1}"
            if not isinstance(raw_row, Mapping):
                raise ReconciliationError(f"{context} must be an object")
            missing = sorted(required - set(raw_row))
            if missing:
                raise ReconciliationError(f"{context} is missing required field {missing[0]}")
            extras = sorted(set(raw_row) - required)
            invalid_extras = [
                field
                for field in extras
                if not field.endswith("_mean") and not _is_percentile(field)
            ]
            if invalid_extras:
                raise ReconciliationError(
                    f"{context} contains unexpected field {invalid_extras[0]}"
                )
            if raw_row["statistical_unit"] != expected_unit:
                raise ReconciliationError(f"{context} statistical_unit must equal {expected_unit}")
            dimensions: dict[str, JsonScalar] = {}
            for name in dimension_names:
                value = _validate_spark_dimension(name, raw_row[name], context)
                if name == "replay_run_id":
                    run_ids.add(cast(str, value))
                else:
                    dimensions[name] = value
            count = _non_negative_integer(
                raw_row.get("hardware_execution_count", 0),
                f"{context} hardware_execution_count",
            )
            for metric in metric_names:
                value = _non_negative_integer(raw_row[metric], f"{context} {metric}")
                key = _MetricKey.create(dimensions, metric, expected_unit)
                if key in metrics:
                    raise ReconciliationError(
                        f"duplicate Spark metric identity: {_canonical_json(key.as_dict())}"
                    )
                metrics[key] = value
            for field in extras:
                numeric = _number(raw_row[field], f"{context} {field}")
                if field.endswith("_mean"):
                    sum_field = f"{field.removesuffix('_mean')}_sum"
                    if sum_field not in raw_row or count == 0:
                        raise ReconciliationError(
                            f"{context} {field} must be derived from a matching exact sum and count"
                        )
                    expected_mean = cast(int, raw_row[sum_field]) / count
                    if numeric != expected_mean:
                        raise ReconciliationError(
                            f"{context} {field} must be derived from {sum_field} and "
                            "hardware_execution_count"
                        )
                    continue
                percentile_fields.add(field)
                key = _MetricKey.create(dimensions, field, expected_unit)
                if key in metrics:
                    raise ReconciliationError(
                        f"duplicate Spark metric identity: {_canonical_json(key.as_dict())}"
                    )
                metrics[key] = numeric

    dataset = "predictive_row_metrics"
    rows = aggregates[dataset]
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ReconciliationError(f"Spark {dataset} must be a sequence of aggregate rows")
    required = {"replay_run_id", "metric", "statistical_unit", "value"}
    for index, raw_row in enumerate(rows):
        context = f"Spark {dataset} row {index + 1}"
        if not isinstance(raw_row, Mapping):
            raise ReconciliationError(f"{context} must be an object")
        missing = sorted(required - set(raw_row))
        if missing:
            raise ReconciliationError(f"{context} is missing required field {missing[0]}")
        extras = sorted(set(raw_row) - required)
        if extras:
            raise ReconciliationError(f"{context} contains unexpected field {extras[0]}")
        replay_run_id = _validate_spark_dimension(
            "replay_run_id", raw_row["replay_run_id"], context
        )
        run_ids.add(cast(str, replay_run_id))
        raw_metric = raw_row["metric"]
        if not isinstance(raw_metric, str) or raw_metric not in _PREDICTIVE_METRIC_UNITS:
            raise ReconciliationError(f"{context} has unsupported metric")
        metric = raw_metric
        expected_unit = _PREDICTIVE_METRIC_UNITS[metric]
        if raw_row["statistical_unit"] != expected_unit:
            raise ReconciliationError(f"{context} statistical_unit must equal {expected_unit}")
        value = _non_negative_integer(raw_row["value"], f"{context} value")
        key = _MetricKey.create({}, metric, expected_unit)
        if key in metrics:
            raise ReconciliationError(
                f"duplicate Spark metric identity: {_canonical_json(key.as_dict())}"
            )
        metrics[key] = value

    dataset = "execution_dimension_counts"
    rows = aggregates[dataset]
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ReconciliationError(f"Spark {dataset} must be a sequence of aggregate rows")
    required = {
        "replay_run_id",
        "dimension_kind",
        *_EXECUTION_DIMENSION_FIELDS,
        "statistical_unit",
        "hardware_execution_count",
    }
    for index, raw_row in enumerate(rows):
        context = f"Spark {dataset} row {index + 1}"
        if not isinstance(raw_row, Mapping):
            raise ReconciliationError(f"{context} must be an object")
        missing = sorted(required - set(raw_row))
        if missing:
            raise ReconciliationError(f"{context} is missing required field {missing[0]}")
        extras = sorted(set(raw_row) - required)
        if extras:
            raise ReconciliationError(f"{context} contains unexpected field {extras[0]}")
        replay_run_id = _validate_spark_dimension(
            "replay_run_id", raw_row["replay_run_id"], context
        )
        run_ids.add(cast(str, replay_run_id))
        if raw_row["statistical_unit"] != "hardware_execution":
            raise ReconciliationError(f"{context} statistical_unit must equal hardware_execution")
        dimension_kind = raw_row["dimension_kind"]
        if not isinstance(dimension_kind, str) or not dimension_kind.strip():
            raise ReconciliationError(f"{context} dimension_kind must be a non-empty string")
        if dimension_kind not in _EXECUTION_DIMENSION_SHAPES:
            raise ReconciliationError(f"{context} has unsupported dimension_kind")
        expected_shape = _EXECUTION_DIMENSION_SHAPES[dimension_kind]
        observed_shape = frozenset(
            field for field in _EXECUTION_DIMENSION_FIELDS if raw_row[field] is not None
        )
        if observed_shape != expected_shape:
            raise ReconciliationError(f"{context} {dimension_kind} has invalid dimension shape")

        execution_dimensions: dict[str, JsonScalar]
        if dimension_kind == "board":
            execution_dimensions = {
                "board": _validate_spark_dimension("board", raw_row["board"], context)
            }
        elif dimension_kind == "student_model":
            execution_dimensions = {
                "student_model": _validate_spark_dimension(
                    "student_model", raw_row["student_model"], context
                )
            }
        elif dimension_kind == "board_student_model":
            execution_dimensions = {
                "board": _validate_spark_dimension("board", raw_row["board"], context),
                "student_model": _validate_spark_dimension(
                    "student_model", raw_row["student_model"], context
                ),
            }
        else:
            class_id = raw_row["class_id"]
            if type(class_id) is not int or not 0 <= class_id <= 4:
                raise ReconciliationError(f"{context} class_id must be an integer between 0 and 4")
            class_name = _validate_spark_dimension("class_name", raw_row["class_name"], context)
            if class_name != _CLASS_NAMES[class_id]:
                raise ReconciliationError(f"{context} class_name is inconsistent with class_id")
            prefix = "true" if dimension_kind == "true_class" else "predicted"
            execution_dimensions = {
                f"{prefix}_class_id": class_id,
                f"{prefix}_class_name": class_name,
            }
        value = _non_negative_integer(
            raw_row["hardware_execution_count"],
            f"{context} hardware_execution_count",
        )
        key = _MetricKey.create(
            execution_dimensions, "hardware_execution_count", "hardware_execution"
        )
        if key in metrics:
            raise ReconciliationError(
                f"duplicate Spark metric identity: {_canonical_json(key.as_dict())}"
            )
        metrics[key] = value
    if len(run_ids) != 1:
        raise ReconciliationError("Spark aggregate rows must describe exactly one replay_run_id")
    return metrics, percentile_fields, next(iter(run_ids))


def _validated_tolerances(
    percentile_tolerances: Mapping[str, Number] | None,
) -> dict[str, Number]:
    tolerances: dict[str, Number] = {}
    for field, raw_tolerance in (percentile_tolerances or {}).items():
        if not isinstance(field, str) or not field.strip() or not _is_percentile(field):
            raise ReconciliationError("percentile tolerances must be keyed by percentile field")
        tolerance = _number(raw_tolerance, f"absolute tolerance for {field}")
        if tolerance < 0:
            raise ReconciliationError(f"absolute tolerance for {field} must be non-negative")
        tolerances[field] = tolerance
    return tolerances


def _is_comparable_hadoop_metric(key: _MetricKey, percentile_fields: set[str]) -> bool:
    dimensions = {name for name, _ in key.dimensions}
    if key.statistical_unit == "hardware_execution":
        if (
            key.metric == "hardware_execution_count"
            and frozenset(dimensions) in _EXECUTION_DIMENSION_MAPPER_SHAPES
        ):
            return True
        if dimensions == set(_LINEAGE_DIMENSIONS):
            return key.metric in _OPERATIONAL_METRICS or key.metric in percentile_fields
        if dimensions == set(_POLICY_DIMENSIONS):
            return key.metric in _RETENTION_METRICS
        if dimensions == {"policy_id", "reason", "retained"}:
            return key.metric == "decision_count"
    return (
        key.statistical_unit
        in {
            "hardware_execution",
            "execution_lineage",
            "predictive_source_row",
        }
        and not dimensions
        and (
            _PREDICTIVE_METRIC_UNITS.get(key.metric) == key.statistical_unit
            or key.metric in percentile_fields
        )
    )


def _validate_percentile_shape(key: _MetricKey) -> None:
    shape = (key.statistical_unit, frozenset(name for name, _ in key.dimensions))
    if shape not in _PERCENTILE_SHAPES:
        raise ReconciliationError(
            "percentile dimensions and statistical_unit do not match a Spark aggregate schema"
        )
    for name, value in key.dimensions:
        _validate_spark_dimension(name, value, "Hadoop percentile dimension")


def _metric_sort_key(key: _MetricKey) -> str:
    return _canonical_json(key.as_dict())


def reconcile(
    spark_aggregates: Mapping[str, Sequence[Mapping[str, object]]],
    hadoop_metric_lines: Iterable[str],
    *,
    finalized_run_manifest: Mapping[str, object],
    hadoop_oracle_manifest: Mapping[str, object],
    hadoop_metrics_sha256: str,
    spark_aggregates_sha256: str,
    percentile_tolerances: Mapping[str, Number] | None = None,
) -> dict[str, object]:
    """Compare exact metrics and derive statistical-unit counts from Hadoop rows."""
    try:
        finalized_provenance = validate_finalized_run_manifest_mapping(finalized_run_manifest)
        hadoop_provenance = validate_hadoop_oracle_manifest_mapping(hadoop_oracle_manifest)
    except OracleManifestError as error:
        raise ReconciliationError(str(error)) from error
    if (
        not isinstance(hadoop_metrics_sha256, str)
        or _SHA256.fullmatch(hadoop_metrics_sha256) is None
    ):
        raise ReconciliationError(
            "actual Hadoop metrics SHA-256 must be a lowercase SHA-256 digest"
        )
    if (
        not isinstance(spark_aggregates_sha256, str)
        or _SHA256.fullmatch(spark_aggregates_sha256) is None
    ):
        raise ReconciliationError(
            "actual Spark aggregate SHA-256 must be a lowercase SHA-256 digest"
        )
    tolerances = _validated_tolerances(percentile_tolerances)
    spark, percentile_fields, spark_replay_run_id = _spark_metrics(spark_aggregates)
    hadoop: dict[_MetricKey, Number] = {}
    previous_mapper_key: str | None = None
    source_rows = 0
    lineage_occurrence_rows = 0
    predictive_occurrence_rows = 0
    source_identities: dict[str, tuple[str, int, int, str]] = {}
    lineage_identities: dict[str, tuple[str, int, str]] = {}
    predictive_identities: dict[str, tuple[str, int, str]] = {}
    lineage_ids: set[str] = set()
    predictive_row_indices: set[int] = set()
    hadoop_percentile_fields: set[str] = set()
    global_hardware_count: int | None = None
    observed = 0

    for line_number, raw_line in enumerate(hadoop_metric_lines, start=1):
        observed += 1
        row = _parse_hadoop_row(raw_line, line_number)
        if previous_mapper_key is not None and row.mapper_key < previous_mapper_key:
            raise ReconciliationError("Hadoop metric rows must retain reducer key order")
        if row.mapper_key == previous_mapper_key:
            raise ReconciliationError(
                f"duplicate Hadoop metric identity: {_canonical_json(row.key.as_dict())}"
            )
        previous_mapper_key = row.mapper_key
        dimensions = dict(row.key.dimensions)
        if _is_percentile(row.key.metric):
            _validate_percentile_shape(row.key)
            hadoop_percentile_fields.add(row.key.metric)
        if row.key.metric == "source_event_occurrence_count":
            source_rows += 1
            source_identities[cast(str, dimensions["source_event_id"])] = (
                cast(str, dimensions["lineage_id"]),
                cast(int, dimensions["row_id"]),
                cast(int, dimensions["source_row_index"]),
                cast(str, dimensions["payload_sha256"]),
            )
        elif row.key.metric == "lineage_row_occurrence_count":
            lineage_occurrence_rows += 1
            lineage_ids.add(cast(str, dimensions["lineage_id"]))
            lineage_identities[cast(str, dimensions["source_event_id"])] = (
                cast(str, dimensions["lineage_id"]),
                cast(int, dimensions["row_id"]),
                cast(str, dimensions["payload_sha256"]),
            )
        elif row.key.metric == "predictive_source_row_occurrence_count":
            predictive_occurrence_rows += 1
            predictive_row_indices.add(cast(int, dimensions["source_row_index"]))
            predictive_identities[cast(str, dimensions["source_event_id"])] = (
                cast(str, dimensions["lineage_id"]),
                cast(int, dimensions["source_row_index"]),
                cast(str, dimensions["payload_sha256"]),
            )
        elif (
            row.key.metric == "hardware_execution_count"
            and row.key.statistical_unit == "hardware_execution"
            and not dimensions
        ):
            global_hardware_count = cast(int, row.value)
        if _is_comparable_hadoop_metric(
            row.key,
            percentile_fields | hadoop_percentile_fields,
        ):
            hadoop[row.key] = row.value

    all_percentile_fields = percentile_fields | hadoop_percentile_fields
    missing_tolerances = sorted(all_percentile_fields - tolerances.keys())
    if missing_tolerances:
        raise ReconciliationError(
            f"percentile field {missing_tolerances[0]} requires an explicit absolute tolerance"
        )
    if observed == 0:
        raise ReconciliationError("Hadoop metric input must contain at least one row")
    if source_rows == 0:
        raise ReconciliationError("Hadoop metrics are missing source event occurrence rows")
    if not lineage_ids:
        raise ReconciliationError("Hadoop metrics are missing lineage row occurrence rows")
    if not predictive_row_indices:
        raise ReconciliationError(
            "Hadoop metrics are missing predictive source row occurrence rows"
        )
    if global_hardware_count is None:
        raise ReconciliationError("Hadoop metrics are missing the global hardware execution count")

    derived = {
        "hardware_execution_count": global_hardware_count,
        "execution_lineage_count": len(lineage_ids),
        "unique_predictive_row_count": len(predictive_row_indices),
    }
    for metric, value in derived.items():
        hadoop[_MetricKey.create({}, metric, _PREDICTIVE_METRIC_UNITS[metric])] = value
    summary = {
        "duplicate_count": 0,
        "execution_lineage_count": len(lineage_ids),
        "hardware_execution_count": global_hardware_count,
        "total_canonical_input_records": source_rows,
        "unique_canonical_records": source_rows,
        "unique_predictive_row_count": len(predictive_row_indices),
    }

    differences: list[dict[str, object]] = []
    all_source_ids = set(source_identities) | set(lineage_identities) | set(predictive_identities)
    for source_event_id in sorted(all_source_ids):
        source_identity = source_identities.get(source_event_id)
        lineage_identity = lineage_identities.get(source_event_id)
        predictive_identity = predictive_identities.get(source_event_id)
        if (
            source_identity is None
            or lineage_identity is None
            or predictive_identity is None
            or source_identity[0] != lineage_identity[0]
            or source_identity[0] != predictive_identity[0]
            or source_identity[1] != lineage_identity[1]
            or source_identity[2] != predictive_identity[1]
            or source_identity[3] != lineage_identity[2]
            or source_identity[3] != predictive_identity[2]
        ):
            differences.append(
                {
                    "kind": "canonical_event_identity_mismatch",
                    "source_event_id": source_event_id,
                }
            )
    canonical_event_identity = sha256()
    for source_event_id in sorted(source_identities):
        lineage_id, row_id, source_row_index, payload_sha256 = source_identities[source_event_id]
        canonical_event_identity.update(
            f"{source_event_id}|{lineage_id}|{row_id}|{source_row_index}|{payload_sha256}\n".encode(
                "ascii"
            )
        )
    canonical_event_identity_sha256 = canonical_event_identity.hexdigest()
    provenance_checks = (
        (
            "replay_run_id",
            finalized_provenance.replay_run_id,
            spark_replay_run_id,
        ),
        (
            "snapshot_id",
            finalized_provenance.snapshot_id,
            hadoop_provenance.snapshot_id,
        ),
        (
            "events_sha256",
            finalized_provenance.events_sha256,
            hadoop_provenance.events_sha256,
        ),
        (
            "policy_config_sha256",
            finalized_provenance.policy_config_sha256,
            hadoop_provenance.policy_config_sha256,
        ),
        (
            "metrics_sha256",
            hadoop_provenance.metrics_sha256,
            hadoop_metrics_sha256,
        ),
        (
            "canonical_event_identity_sha256",
            finalized_provenance.canonical_event_identity_sha256,
            canonical_event_identity_sha256,
        ),
        (
            "hadoop_manifest_canonical_event_identity_sha256",
            hadoop_provenance.canonical_event_identity_sha256,
            canonical_event_identity_sha256,
        ),
        (
            "spark_aggregates_sha256",
            finalized_provenance.spark_aggregates_sha256,
            spark_aggregates_sha256,
        ),
        (
            "metrics_row_count",
            hadoop_provenance.metrics_row_count,
            observed,
        ),
        (
            "source_event_count",
            finalized_provenance.source_event_count,
            source_rows,
        ),
        (
            "snapshot_event_count",
            hadoop_provenance.snapshot_event_count,
            source_rows,
        ),
    )
    differences.extend(
        {
            "actual_value": actual,
            "expected_value": expected,
            "field": field,
            "kind": "provenance_mismatch",
        }
        for field, expected, actual in provenance_checks
        if actual != expected
    )
    cardinalities = {
        "hardware_execution_count": global_hardware_count,
        "lineage_row_occurrence_count": lineage_occurrence_rows,
        "predictive_source_row_occurrence_count": predictive_occurrence_rows,
    }
    differences.extend(
        {
            "actual": actual,
            "expected": source_rows,
            "field": field,
            "kind": "internal_cardinality_mismatch",
            "reference_field": "unique_canonical_records",
        }
        for field, actual in cardinalities.items()
        if actual != source_rows
    )
    differences.extend(
        {"kind": "missing_hadoop_metric", "metric_key": key.as_dict()}
        for key in sorted(spark.keys() - hadoop.keys(), key=_metric_sort_key)
    )
    differences.extend(
        {"kind": "extra_hadoop_metric", "metric_key": key.as_dict()}
        for key in sorted(hadoop.keys() - spark.keys(), key=_metric_sort_key)
    )
    for key in sorted(spark.keys() & hadoop.keys(), key=_metric_sort_key):
        spark_value = spark[key]
        hadoop_value = hadoop[key]
        if key.metric in all_percentile_fields:
            tolerance = tolerances[key.metric]
            difference = abs(_decimal(spark_value, key.metric) - _decimal(hadoop_value, key.metric))
            if difference > _decimal(tolerance, key.metric):
                differences.append(
                    {
                        "absolute_tolerance": tolerance,
                        "field": key.metric,
                        "hadoop_value": hadoop_value,
                        "kind": "percentile_tolerance_exceeded",
                        "metric_key": key.as_dict(),
                        "spark_value": spark_value,
                    }
                )
        elif spark_value != hadoop_value:
            differences.append(
                {
                    "field": key.metric,
                    "hadoop_value": hadoop_value,
                    "kind": "value_mismatch",
                    "metric_key": key.as_dict(),
                    "spark_value": spark_value,
                }
            )
    differences.sort(key=_canonical_json)
    return {
        "differences": differences,
        "matched": not differences,
        "provenance": {
            "finalized_run_manifest": finalized_provenance.compact_provenance(),
            "hadoop_oracle_manifest": hadoop_provenance.compact_provenance(),
            "observed_hadoop_metrics": {
                "metrics_row_count": observed,
                "metrics_sha256": hadoop_metrics_sha256,
            },
            "spark_aggregate": {
                "replay_run_id": spark_replay_run_id,
                "spark_aggregates_sha256": spark_aggregates_sha256,
            },
        },
        "schema_version": "hadoop-spark-reconciliation-v1",
        "summary": summary,
    }


def publish_report(
    output_path: Path,
    report: Mapping[str, object],
    *,
    environment: Mapping[str, str] | None = None,
) -> Path:
    """Atomically publish a valid report without clobbering an existing path."""
    env = os.environ if environment is None else environment
    roots = tuple(
        Path(value.strip())
        for name in ("CUKD_XAI_ROOT", "TINYML_IDS_EVIDENCE_ROOT")
        if (value := env.get(name, "")).strip()
    )
    try:
        output = ensure_output_outside_evidence_roots(output_path, roots)
    except EvidenceContractError as error:
        raise ReconciliationError(str(error)) from error
    if output.exists():
        raise FileExistsError(f"reconciliation report already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = NamedTemporaryFile(  # noqa: SIM115 - publication owns the handle lifecycle.
        mode="w+b",
        prefix=f".{output.name}.",
        suffix=".tmp",
        dir=output.parent,
        delete=False,
    )
    temporary_path = Path(temporary.name)
    try:
        temporary.write(canonical_report(report).encode("utf-8"))
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary.close()
        try:
            os.link(temporary_path, output)
        except FileExistsError as error:
            raise FileExistsError(f"reconciliation report already exists: {output}") from error
        temporary_path.unlink()
    except BaseException:
        if not temporary.closed:
            temporary.close()
        temporary_path.unlink(missing_ok=True)
        raise
    return output


def _load_spark_aggregates(source: TextIO) -> Mapping[str, Sequence[Mapping[str, object]]]:
    raw = source.read()
    if raw.endswith("\r\n"):
        raw = raw[:-2]
    elif raw.endswith("\n"):
        raw = raw[:-1]
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, ReconciliationError) as error:
        raise ReconciliationError(
            f"Spark aggregate input must contain strict JSON: {error}"
        ) from error
    if not isinstance(value, dict):
        raise ReconciliationError("Spark aggregate input must be a JSON object")
    if raw != _canonical_json(value):
        raise ReconciliationError("Spark aggregate input must be canonical compact sorted JSON")
    return cast(Mapping[str, Sequence[Mapping[str, object]]], value)


def _load_canonical_manifest(path: Path, context: str) -> Mapping[str, object]:
    payload = path.read_bytes()
    try:
        raw = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ReconciliationError(f"{context} must contain UTF-8 JSON") from error
    if not raw.endswith("\n") or raw.endswith("\n\n"):
        raise ReconciliationError(f"{context} must be canonical compact sorted JSON")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, ReconciliationError) as error:
        raise ReconciliationError(f"{context} must contain strict JSON: {error}") from error
    if not isinstance(value, dict) or raw != _canonical_json(value) + "\n":
        raise ReconciliationError(f"{context} must be canonical compact sorted JSON")
    return value


def _parse_tolerances(values: Sequence[str]) -> dict[str, Number]:
    tolerances: dict[str, Number] = {}
    for raw in values:
        field, separator, raw_value = raw.partition("=")
        if not separator or not field or field in tolerances:
            raise ReconciliationError(
                "--percentile-tolerance must use one unique FIELD=ABSOLUTE_TOLERANCE value"
            )
        try:
            value = float(raw_value)
        except ValueError as error:
            raise ReconciliationError(f"invalid percentile tolerance for {field}") from error
        tolerances[field] = value
    return tolerances


def main(argv: Sequence[str] | None = None) -> int:
    """Run reconciliation over serialized Spark aggregate rows and Hadoop JSONL."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-aggregates", required=True, type=Path)
    parser.add_argument("--hadoop-metrics", required=True, type=Path)
    parser.add_argument("--finalized-run-manifest", required=True, type=Path)
    parser.add_argument("--hadoop-run-manifest", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--percentile-tolerance", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        spark_payload = args.spark_aggregates.read_bytes()
        try:
            spark_text = spark_payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ReconciliationError("Spark aggregate input must contain UTF-8 JSON") from error
        spark = _load_spark_aggregates(StringIO(spark_text))
        spark_aggregates_sha256 = sha256(spark_payload).hexdigest()
        finalized_manifest = _load_canonical_manifest(
            args.finalized_run_manifest, "finalized run manifest"
        )
        hadoop_manifest = _load_canonical_manifest(
            args.hadoop_run_manifest, "Hadoop oracle manifest"
        )
        tolerances = _parse_tolerances(args.percentile_tolerance)
        hadoop_payload = args.hadoop_metrics.read_bytes()
        hadoop_metrics_sha256 = sha256(hadoop_payload).hexdigest()
        try:
            hadoop_text = hadoop_payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ReconciliationError("Hadoop metric input must contain UTF-8 JSONL") from error
        report = reconcile(
            spark,
            StringIO(hadoop_text),
            finalized_run_manifest=finalized_manifest,
            hadoop_oracle_manifest=hadoop_manifest,
            hadoop_metrics_sha256=hadoop_metrics_sha256,
            spark_aggregates_sha256=spark_aggregates_sha256,
            percentile_tolerances=tolerances,
        )
        payload = canonical_report(report)
        matched = cast(bool, report["matched"])
        if args.output is not None:
            publish_report(args.output, report)
        elif matched:
            sys.stdout.write(payload)
        else:
            sys.stderr.write(payload)
        return 0 if matched else 1
    except (FileExistsError, OSError, ReconciliationError) as error:
        print(f"reconciliation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
