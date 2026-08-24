"""Machine-readable and human-readable reports for finalized course runs."""

from __future__ import annotations

import json
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


def build_course_report(
    run: FinalizedRunView,
    reconciliation: Mapping[str, object],
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
