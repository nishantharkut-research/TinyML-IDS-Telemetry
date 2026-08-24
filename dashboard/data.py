"""Fail-closed access to finalized Spark aggregate publications."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import cast

_RUN_DIRECTORY = re.compile(r"replay_run_id=([A-Za-z0-9][A-Za-z0-9._-]{0,127})")
_OUTPUTS = frozenset(
    {
        "canonical_gold",
        "execution_dimension_counts",
        "operational_metrics",
        "policy_decisions",
        "predictive_row_metrics",
        "retained_security_aware",
        "retention_metrics",
        "retention_reason_counts",
    }
)
_AGGREGATES = _OUTPUTS - {
    "canonical_gold",
    "policy_decisions",
    "retained_security_aware",
}


class FinalizedRunError(ValueError):
    """A dashboard input is incomplete, inconsistent, or not finalized."""


@dataclass(frozen=True)
class FinalizedRun:
    """Validated read-only aggregate view of one replay run."""

    run_directory: Path
    replay_run_id: str
    source_event_count: int
    delivery_count: int
    aggregates: Mapping[str, tuple[Mapping[str, object], ...]]


@dataclass(frozen=True)
class CourseReportSummary:
    """Reconciliation state bound to one finalized replay run."""

    replay_run_id: str
    reconciliation_matched: bool
    execution_lineage_count: int
    hardware_execution_count: int
    replay_delivery_count: int
    unique_predictive_row_count: int
    replay_executed: bool
    pipeline_total_seconds: float
    acknowledged_replay_delivery_rate: float | None
    finalized_source_event_rate: float
    canonical_gold_bytes: int
    retained_security_aware_bytes: int


def _read_json_object(path: Path, label: str) -> tuple[dict[str, object], bytes]:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise FinalizedRunError(f"unable to read {label}: {path}") from error
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FinalizedRunError(f"{label} is not valid JSON") from error
    if not isinstance(value, dict):
        raise FinalizedRunError(f"{label} must be a JSON object")
    return cast(dict[str, object], value), payload


def _nonnegative_int(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        raise FinalizedRunError(f"finalized manifest {field} must be a nonnegative integer")
    return value


def _output_row_count(outputs: Mapping[str, object], output: str) -> int:
    raw_metadata = outputs.get(output)
    if not isinstance(raw_metadata, Mapping):
        raise FinalizedRunError(f"finalized manifest {output} metadata must be an object")
    metadata = cast(Mapping[str, object], raw_metadata)
    return _nonnegative_int(metadata.get("row_count"), f"{output} row_count")


def load_finalized_run(run_directory: Path) -> FinalizedRun:
    """Load aggregate tables only after validating the finalized publication contract."""
    run = run_directory.expanduser().resolve()
    match = _RUN_DIRECTORY.fullmatch(run.name)
    if not run.is_dir() or match is None:
        raise FinalizedRunError("run directory must be named replay_run_id=<safe-id>")

    manifest, _ = _read_json_object(run / "finalized_run_manifest.json", "finalized manifest")
    if manifest.get("schema_version") != "telemetry-finalized-run-v1":
        raise FinalizedRunError("finalized manifest has an unsupported schema_version")
    replay_run_id = manifest.get("replay_run_id")
    if replay_run_id != match.group(1):
        raise FinalizedRunError("finalized manifest replay_run_id does not match its directory")

    raw_outputs = manifest.get("outputs")
    if not isinstance(raw_outputs, Mapping) or set(raw_outputs) != _OUTPUTS:
        raise FinalizedRunError("finalized manifest must declare the exact output set")
    outputs = cast(Mapping[str, object], raw_outputs)
    output_row_counts = {output: _output_row_count(outputs, output) for output in sorted(_OUTPUTS)}
    for output in sorted(_OUTPUTS):
        if not (run / output / "_SUCCESS").is_file():
            raise FinalizedRunError(f"{output} is missing its _SUCCESS marker")

    aggregate_document, aggregate_payload = _read_json_object(
        run / "spark_aggregates.json", "Spark aggregate document"
    )
    expected_hash = manifest.get("spark_aggregates_sha256")
    if not isinstance(expected_hash, str) or sha256(aggregate_payload).hexdigest() != expected_hash:
        raise FinalizedRunError("Spark aggregate document SHA-256 does not match the manifest")
    if set(aggregate_document) != _AGGREGATES:
        raise FinalizedRunError("Spark aggregate document must contain the exact aggregate tables")

    tables: dict[str, tuple[Mapping[str, object], ...]] = {}
    for table_name in sorted(_AGGREGATES):
        raw_rows = aggregate_document[table_name]
        if not isinstance(raw_rows, list) or any(not isinstance(row, dict) for row in raw_rows):
            raise FinalizedRunError(f"aggregate table {table_name} must contain JSON objects")
        rows = cast(list[dict[str, object]], raw_rows)
        if any(row.get("replay_run_id") != replay_run_id for row in rows):
            raise FinalizedRunError(
                f"aggregate table {table_name} contains a foreign replay_run_id"
            )
        if len(rows) != output_row_counts[table_name]:
            raise FinalizedRunError(
                f"aggregate table {table_name} row_count does not match the finalized manifest"
            )
        tables[table_name] = tuple(MappingProxyType(dict(row)) for row in rows)

    source_event_count = _nonnegative_int(manifest.get("source_event_count"), "source_event_count")
    delivery_count = _nonnegative_int(manifest.get("delivery_count"), "delivery_count")
    if source_event_count == 0 or delivery_count < source_event_count:
        raise FinalizedRunError("finalized manifest has inconsistent source and delivery counts")
    if output_row_counts["canonical_gold"] != source_event_count:
        raise FinalizedRunError("canonical_gold row_count does not match source_event_count")
    if output_row_counts["policy_decisions"] != 2 * source_event_count:
        raise FinalizedRunError(
            "policy_decisions row_count does not match the two declared policies"
        )
    if output_row_counts["retained_security_aware"] > source_event_count:
        raise FinalizedRunError("retained_security_aware row_count exceeds source_event_count")

    return FinalizedRun(
        run_directory=run,
        replay_run_id=cast(str, replay_run_id),
        source_event_count=source_event_count,
        delivery_count=delivery_count,
        aggregates=MappingProxyType(tables),
    )


def _report_count(counts: Mapping[str, object], field: str) -> int:
    value = counts.get(field)
    if type(value) is not int or value < 0:
        raise FinalizedRunError(f"course report {field} must be a nonnegative integer")
    return value


def _report_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FinalizedRunError(f"course report {field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise FinalizedRunError(f"course report {field} must be finite and nonnegative")
    return result


def load_course_report(report_path: Path, run: FinalizedRun) -> CourseReportSummary:
    """Load a successful Spark-Hadoop reconciliation bound to ``run``."""
    report, _ = _read_json_object(report_path.expanduser().resolve(), "course report")
    if report.get("schema_version") != "tinyml-ids-course-report-v1":
        raise FinalizedRunError("course report has an unsupported schema_version")
    replay_run_id = report.get("replay_run_id")
    if replay_run_id != run.replay_run_id:
        raise FinalizedRunError("course report replay_run_id does not match the finalized run")

    raw_counts = report.get("counts")
    if not isinstance(raw_counts, Mapping):
        raise FinalizedRunError("course report counts must be a JSON object")
    counts = cast(Mapping[str, object], raw_counts)
    lineages = _report_count(counts, "execution_lineage_count")
    hardware = _report_count(counts, "hardware_execution_count")
    deliveries = _report_count(counts, "replay_delivery_count")
    predictive_rows = _report_count(counts, "unique_predictive_row_count")
    if hardware != run.source_event_count or deliveries != run.delivery_count:
        raise FinalizedRunError("course report counts do not match the finalized run")
    if lineages == 0 or predictive_rows == 0:
        raise FinalizedRunError("course report evidence counts are inconsistent")

    raw_reconciliation = report.get("reconciliation")
    if not isinstance(raw_reconciliation, Mapping):
        raise FinalizedRunError("course report reconciliation must be a JSON object")
    reconciliation = cast(Mapping[str, object], raw_reconciliation)
    if reconciliation.get("matched") is not True or reconciliation.get("difference_count") != 0:
        raise FinalizedRunError("course report does not record exact reconciliation")

    raw_pipeline = report.get("pipeline")
    if not isinstance(raw_pipeline, Mapping):
        raise FinalizedRunError("course report pipeline must be a JSON object")
    pipeline = cast(Mapping[str, object], raw_pipeline)
    replay_executed = pipeline.get("replay_executed")
    if type(replay_executed) is not bool:
        raise FinalizedRunError("course report replay_executed must be boolean")

    raw_phases = pipeline.get("phase_seconds")
    if not isinstance(raw_phases, Mapping):
        raise FinalizedRunError("course report phase_seconds must be a JSON object")
    total_seconds = _report_number(raw_phases.get("total"), "phase_seconds.total")
    _report_number(raw_phases.get("replay"), "phase_seconds.replay")
    if total_seconds == 0:
        raise FinalizedRunError("course report pipeline total duration must be positive")

    raw_rates = pipeline.get("rates_per_second")
    if not isinstance(raw_rates, Mapping):
        raise FinalizedRunError("course report rates_per_second must be a JSON object")
    acknowledged_value = raw_rates.get("acknowledged_replay_delivery_rate")
    acknowledged_rate = (
        None
        if acknowledged_value is None
        else _report_number(
            acknowledged_value,
            "rates_per_second.acknowledged_replay_delivery_rate",
        )
    )
    if replay_executed is (acknowledged_rate is None):
        raise FinalizedRunError("course report replay rate conflicts with replay execution state")
    finalized_rate = _report_number(
        raw_rates.get("finalized_source_event_rate"),
        "rates_per_second.finalized_source_event_rate",
    )

    raw_storage = pipeline.get("storage_bytes")
    if not isinstance(raw_storage, Mapping):
        raise FinalizedRunError("course report storage_bytes must be a JSON object")
    storage = cast(Mapping[str, object], raw_storage)
    canonical_bytes = _report_count(storage, "finalized/canonical_gold")
    retained_bytes = _report_count(storage, "finalized/retained_security_aware")
    if canonical_bytes == 0:
        raise FinalizedRunError("course report canonical Gold storage bytes must be positive")

    return CourseReportSummary(
        replay_run_id=run.replay_run_id,
        reconciliation_matched=True,
        execution_lineage_count=lineages,
        hardware_execution_count=hardware,
        replay_delivery_count=deliveries,
        unique_predictive_row_count=predictive_rows,
        replay_executed=replay_executed,
        pipeline_total_seconds=total_seconds,
        acknowledged_replay_delivery_rate=acknowledged_rate,
        finalized_source_event_rate=finalized_rate,
        canonical_gold_bytes=canonical_bytes,
        retained_security_aware_bytes=retained_bytes,
    )
