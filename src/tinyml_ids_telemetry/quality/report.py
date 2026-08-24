"""Machine-readable and human-readable reports for finalized course runs."""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceContractError,
    ensure_output_outside_evidence_roots,
)


class FinalizedRunView(Protocol):
    """The report fields supplied by the read-only dashboard data layer."""

    @property
    def replay_run_id(self) -> str: ...

    @property
    def source_event_count(self) -> int: ...

    @property
    def delivery_count(self) -> int: ...

    @property
    def aggregates(self) -> Mapping[str, tuple[Mapping[str, object], ...]]: ...


class CourseReportError(ValueError):
    """Finalized evidence cannot support a course report."""


@dataclass(frozen=True)
class CourseReportPaths:
    """Published report files."""

    json_path: Path
    markdown_path: Path


def _count(summary: Mapping[str, object], field: str) -> int:
    value = summary.get(field)
    if type(value) is not int or value < 0:
        raise CourseReportError(f"reconciliation summary {field} must be nonnegative")
    return value


def _retention_rows(run: FinalizedRunView) -> list[dict[str, object]]:
    rows = run.aggregates.get("retention_metrics")
    if rows is None:
        raise CourseReportError("finalized aggregates are missing retention_metrics")
    result: list[dict[str, object]] = []
    for row in rows:
        if row.get("replay_run_id") != run.replay_run_id:
            raise CourseReportError("retention metric belongs to another replay run")
        result.append(dict(row))
    return sorted(result, key=lambda row: str(row.get("policy_id", "")))


def _nonnegative_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CourseReportError(f"pipeline execution {field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise CourseReportError(f"pipeline execution {field} must be finite and nonnegative")
    return result


def _pipeline_metrics(
    run: FinalizedRunView,
    execution: Mapping[str, object],
) -> dict[str, object]:
    if execution.get("schema_version") != "telemetry-pipeline-execution-v1":
        raise CourseReportError("unsupported pipeline execution schema_version")
    if execution.get("replay_run_id") != run.replay_run_id:
        raise CourseReportError("pipeline execution replay_run_id does not match the finalized run")
    replay_executed = execution.get("replay_executed")
    if type(replay_executed) is not bool:
        raise CourseReportError("pipeline execution replay_executed must be boolean")

    raw_counts = execution.get("counts")
    if not isinstance(raw_counts, Mapping):
        raise CourseReportError("pipeline execution counts must be an object")
    counts = cast(Mapping[str, object], raw_counts)
    snapshot_sources = _count(counts, "snapshot_source_event_count")
    scheduled_sources = _count(counts, "scheduled_source_event_count")
    scheduled_deliveries = _count(counts, "scheduled_delivery_count")
    acknowledged_deliveries = _count(counts, "acknowledged_delivery_count")
    finalized_sources = _count(counts, "finalized_source_event_count")
    finalized_deliveries = _count(counts, "finalized_delivery_count")
    if (
        snapshot_sources != run.source_event_count
        or finalized_sources != run.source_event_count
        or finalized_deliveries != run.delivery_count
        or scheduled_sources > snapshot_sources
        or scheduled_deliveries < scheduled_sources
        or acknowledged_deliveries != scheduled_deliveries
    ):
        raise CourseReportError("pipeline execution counts do not match the finalized run")

    raw_phases = execution.get("phase_seconds")
    if not isinstance(raw_phases, Mapping):
        raise CourseReportError("pipeline execution phase_seconds must be an object")
    phases = {
        str(name): _nonnegative_number(value, f"phase_seconds.{name}")
        for name, value in raw_phases.items()
    }
    if (
        not {"replay", "total"}.issubset(phases)
        or phases["total"] == 0
        or phases["replay"] > phases["total"]
    ):
        raise CourseReportError("pipeline execution phase_seconds are inconsistent")

    raw_rates = execution.get("rates_per_second")
    if not isinstance(raw_rates, Mapping):
        raise CourseReportError("pipeline execution rates_per_second must be an object")
    acknowledged_value = raw_rates.get("acknowledged_replay_delivery_rate")
    if acknowledged_value is None:
        acknowledged_rate = None
    else:
        acknowledged_rate = _nonnegative_number(
            acknowledged_value,
            "rates_per_second.acknowledged_replay_delivery_rate",
        )
    finalized_rate = _nonnegative_number(
        raw_rates.get("finalized_source_event_rate"),
        "rates_per_second.finalized_source_event_rate",
    )
    if replay_executed != (acknowledged_rate is not None):
        raise CourseReportError("pipeline execution replay rate conflicts with replay state")

    raw_storage = execution.get("storage_bytes")
    if not isinstance(raw_storage, Mapping):
        raise CourseReportError("pipeline execution storage_bytes must be an object")
    storage = {
        str(name): _count(cast(Mapping[str, object], raw_storage), str(name))
        for name in raw_storage
    }
    required_storage = {
        "finalized/canonical_gold",
        "finalized/retained_security_aware",
    }
    if not required_storage.issubset(storage):
        raise CourseReportError("pipeline execution storage_bytes are incomplete")

    return {
        "replay_executed": replay_executed,
        "phase_seconds": phases,
        "rates_per_second": {
            "acknowledged_replay_delivery_rate": acknowledged_rate,
            "finalized_source_event_rate": finalized_rate,
        },
        "storage_bytes": storage,
    }


def build_course_report(
    run: FinalizedRunView,
    reconciliation: Mapping[str, object],
    pipeline_execution: Mapping[str, object],
) -> dict[str, object]:
    """Build a report only from a finalized run and a matching batch oracle."""
    if reconciliation.get("schema_version") != "hadoop-spark-reconciliation-v1":
        raise CourseReportError("unsupported reconciliation schema_version")
    differences = reconciliation.get("differences")
    reconciliation_failed = reconciliation.get("matched") is not True
    if reconciliation_failed or not isinstance(differences, list) or differences:
        raise CourseReportError("reconciliation did not match")
    raw_provenance = reconciliation.get("provenance")
    if not isinstance(raw_provenance, Mapping):
        raise CourseReportError("reconciliation provenance must be an object")
    provenance = cast(Mapping[str, object], raw_provenance)
    raw_spark = provenance.get("spark_aggregate")
    if not isinstance(raw_spark, Mapping):
        raise CourseReportError("reconciliation Spark provenance must be an object")
    spark_provenance = cast(Mapping[str, object], raw_spark)
    if spark_provenance.get("replay_run_id") != run.replay_run_id:
        raise CourseReportError("reconciliation replay_run_id does not match the finalized run")
    raw_summary = reconciliation.get("summary")
    if not isinstance(raw_summary, Mapping):
        raise CourseReportError("reconciliation summary must be an object")
    summary = cast(Mapping[str, object], raw_summary)
    hardware_executions = _count(summary, "hardware_execution_count")
    lineages = _count(summary, "execution_lineage_count")
    predictive_rows = _count(summary, "unique_predictive_row_count")
    if hardware_executions != run.source_event_count:
        raise CourseReportError("reconciliation hardware count does not match the finalized run")
    if lineages == 0 or predictive_rows == 0 or run.delivery_count < hardware_executions:
        raise CourseReportError("finalized run counts are inconsistent")

    return {
        "schema_version": "tinyml-ids-course-report-v1",
        "replay_run_id": run.replay_run_id,
        "counts": {
            "execution_lineage_count": lineages,
            "hardware_execution_count": hardware_executions,
            "replay_delivery_count": run.delivery_count,
            "unique_predictive_row_count": predictive_rows,
        },
        "reconciliation": {"difference_count": 0, "matched": True},
        "pipeline": _pipeline_metrics(run, pipeline_execution),
        "retention_metrics": _retention_rows(run),
        "interpretation": {
            "event_time": "synthetic analytics replay time",
            "source_unit": "hardware execution",
            "sample_boundary": (
                "hardware executions across board-model lineages reuse the same predictive rows"
            ),
            "transport_boundary": (
                "pipeline throughput is analytics replay throughput, not physical device throughput"
            ),
        },
    }


def render_course_report_markdown(report: Mapping[str, object]) -> str:
    """Render a concise course report with explicit statistical units."""
    counts = cast(Mapping[str, int], report["counts"])
    reconciliation = cast(Mapping[str, object], report["reconciliation"])
    hardware = counts["hardware_execution_count"]
    predictive = counts["unique_predictive_row_count"]
    deliveries = counts["replay_delivery_count"]
    lineages = counts["execution_lineage_count"]
    matched = "passed" if reconciliation["matched"] is True else "failed"
    pipeline = cast(Mapping[str, object], report["pipeline"])
    phases = cast(Mapping[str, float], pipeline["phase_seconds"])
    rates = cast(Mapping[str, float | None], pipeline["rates_per_second"])
    storage = cast(Mapping[str, int], pipeline["storage_bytes"])
    acknowledged_rate = rates["acknowledged_replay_delivery_rate"]
    acknowledged_line = (
        f"- {acknowledged_rate:,.2f} replay deliveries/s acknowledged by the producer\n"
        if acknowledged_rate is not None
        else "- replay delivery rate not measured during receipt-based recovery\n"
    )
    return (
        "# Course experiment report\n\n"
        f"**Replay run:** `{report['replay_run_id']}`\n\n"
        "## Evidence units\n\n"
        f"- {hardware:,} hardware executions across {lineages:,} board-model lineages\n"
        f"- {predictive:,} predictive source rows\n"
        f"- {deliveries:,} replay deliveries received by the analytics pipeline\n\n"
        f"The hardware records reuse one shared predictive sequence. They are not {hardware:,} "
        "independent predictive samples. Replay delivery count may be higher because controlled "
        "fault schedules can inject duplicate deliveries.\n\n"
        "## Verification\n\n"
        f"Spark and Hadoop exact reconciliation: **{matched}**.\n\n"
        "## Pipeline measurement\n\n"
        f"- {phases['total']:,.3f} s from Spark startup through cleanup\n"
        f"{acknowledged_line}"
        f"- {rates['finalized_source_event_rate']:,.2f} finalized source events/s\n"
        f"- {storage['finalized/canonical_gold']:,} bytes for canonical Gold Parquet\n"
        f"- {storage['finalized/retained_security_aware']:,} bytes for the security-aware "
        "retained Parquet view\n\n"
        "Event timestamps describe synthetic analytics replay time. Throughput describes the "
        "analytics pipeline and does not measure physical device throughput.\n"
    )


def _canonical_report(report: Mapping[str, object]) -> bytes:
    return (json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def publish_course_report(
    output_directory: Path,
    report: Mapping[str, object],
    *,
    environment: Mapping[str, str] | None = None,
) -> CourseReportPaths:
    """Atomically publish canonical JSON and Markdown without clobbering output."""
    env = os.environ if environment is None else environment
    protected = tuple(
        Path(value.strip())
        for name in ("CUKD_XAI_ROOT", "TINYML_IDS_EVIDENCE_ROOT")
        if (value := env.get(name, "")).strip()
    )
    try:
        output = ensure_output_outside_evidence_roots(output_directory, protected)
    except EvidenceContractError as error:
        raise CourseReportError(str(error)) from error
    if output.exists():
        raise FileExistsError(f"course report output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        (staging / "course_report.json").write_bytes(_canonical_report(report))
        (staging / "course_report.md").write_text(
            render_course_report_markdown(report), encoding="utf-8", newline="\n"
        )
        staging.rename(output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return CourseReportPaths(
        json_path=output / "course_report.json",
        markdown_path=output / "course_report.md",
    )
