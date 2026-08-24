from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from dashboard.data import FinalizedRun
from tinyml_ids_telemetry.cli.report import run_report_command
from tinyml_ids_telemetry.quality.report import (
    CourseReportError,
    build_course_report,
    publish_course_report,
    render_course_report_markdown,
)


def _run(tmp_path: Path) -> FinalizedRun:
    return FinalizedRun(
        run_directory=tmp_path / "replay_run_id=course-demo",
        replay_run_id="course-demo",
        source_event_count=225_204,
        delivery_count=247_725,
        aggregates={
            "execution_dimension_counts": (),
            "operational_metrics": (),
            "predictive_row_metrics": (),
            "retention_metrics": (
                {
                    "policy_id": "retain-all-v1",
                    "hardware_execution_count": 225_204,
                    "retained_hardware_execution_count": 225_204,
                    "predicted_attack_execution_count": 100_000,
                    "retained_predicted_attack_execution_count": 100_000,
                    "actual_attack_execution_count": 110_000,
                    "retained_actual_attack_execution_count": 110_000,
                    "replay_run_id": "course-demo",
                    "statistical_unit": "hardware_execution",
                },
            ),
            "retention_reason_counts": (),
        },
    )


def _reconciliation(*, matched: bool = True) -> dict[str, object]:
    return {
        "schema_version": "hadoop-spark-reconciliation-v1",
        "matched": matched,
        "differences": [] if matched else [{"kind": "value_mismatch"}],
        "provenance": {
            "spark_aggregate": {
                "replay_run_id": "course-demo",
                "spark_aggregates_sha256": "a" * 64,
            }
        },
        "summary": {
            "duplicate_count": 0,
            "execution_lineage_count": 4,
            "hardware_execution_count": 225_204,
            "total_canonical_input_records": 225_204,
            "unique_canonical_records": 225_204,
            "unique_predictive_row_count": 56_301,
        },
    }


def _pipeline_execution() -> dict[str, object]:
    return {
        "schema_version": "telemetry-pipeline-execution-v1",
        "replay_run_id": "course-demo",
        "replay_executed": True,
        "counts": {
            "snapshot_source_event_count": 225_204,
            "scheduled_source_event_count": 225_204,
            "scheduled_delivery_count": 247_725,
            "acknowledged_delivery_count": 247_725,
            "finalized_source_event_count": 225_204,
            "finalized_delivery_count": 247_725,
        },
        "phase_seconds": {"replay": 100.0, "total": 150.0},
        "rates_per_second": {
            "acknowledged_replay_delivery_rate": 2477.25,
            "finalized_source_event_rate": 1501.36,
        },
        "storage_bytes": {
            "finalized/canonical_gold": 10_000_000,
            "finalized/retained_security_aware": 4_000_000,
        },
    }


def test_course_report_separates_deliveries_executions_and_predictive_rows(
    tmp_path: Path,
) -> None:
    report = build_course_report(_run(tmp_path), _reconciliation(), _pipeline_execution())

    assert report["schema_version"] == "tinyml-ids-course-report-v1"
    assert report["counts"] == {
        "execution_lineage_count": 4,
        "hardware_execution_count": 225_204,
        "replay_delivery_count": 247_725,
        "unique_predictive_row_count": 56_301,
    }
    assert report["reconciliation"] == {"difference_count": 0, "matched": True}
    assert report["pipeline"]["rates_per_second"]["acknowledged_replay_delivery_rate"] == 2477.25
    assert report["pipeline"]["storage_bytes"]["finalized/retained_security_aware"] == 4_000_000

    markdown = render_course_report_markdown(report)
    assert "225,204 hardware executions" in markdown
    assert "56,301 predictive source rows" in markdown
    assert "247,725 replay deliveries" in markdown
    assert "225,204 samples" not in markdown
    assert "not 225,204 independent predictive samples" in markdown
    assert "2,477.25 replay deliveries/s" in markdown
    assert "4,000,000 bytes" in markdown


def test_course_report_rejects_failed_reconciliation(tmp_path: Path) -> None:
    with pytest.raises(CourseReportError, match="reconciliation did not match"):
        build_course_report(
            _run(tmp_path),
            _reconciliation(matched=False),
            _pipeline_execution(),
        )


def test_course_report_rejects_reconciliation_from_another_run(tmp_path: Path) -> None:
    reconciliation = _reconciliation()
    provenance = reconciliation["provenance"]
    assert isinstance(provenance, dict)
    spark = provenance["spark_aggregate"]
    assert isinstance(spark, dict)
    spark["replay_run_id"] = "another-run"

    with pytest.raises(CourseReportError, match="replay_run_id"):
        build_course_report(_run(tmp_path), reconciliation, _pipeline_execution())


def test_course_report_rejects_pipeline_execution_from_another_run(tmp_path: Path) -> None:
    execution = _pipeline_execution()
    execution["replay_run_id"] = "another-run"

    with pytest.raises(CourseReportError, match="pipeline execution replay_run_id"):
        build_course_report(_run(tmp_path), _reconciliation(), execution)


def test_course_report_accepts_receipt_recovery_counts(tmp_path: Path) -> None:
    execution = _pipeline_execution()
    execution["replay_executed"] = False
    counts = execution["counts"]
    assert isinstance(counts, dict)
    counts["scheduled_source_event_count"] = 100_000
    counts["scheduled_delivery_count"] = 110_000
    counts["acknowledged_delivery_count"] = 110_000
    rates = execution["rates_per_second"]
    assert isinstance(rates, dict)
    rates["acknowledged_replay_delivery_rate"] = None

    report = build_course_report(_run(tmp_path), _reconciliation(), execution)

    assert report["pipeline"]["replay_executed"] is False


def test_course_report_rejects_rate_for_receipt_recovery(tmp_path: Path) -> None:
    execution = _pipeline_execution()
    execution["replay_executed"] = False

    with pytest.raises(CourseReportError, match="replay rate conflicts"):
        build_course_report(_run(tmp_path), _reconciliation(), execution)


def test_course_report_rejects_replay_duration_above_total(tmp_path: Path) -> None:
    execution = _pipeline_execution()
    phases = execution["phase_seconds"]
    assert isinstance(phases, dict)
    phases["replay"] = 151.0

    with pytest.raises(CourseReportError, match="phase_seconds are inconsistent"):
        build_course_report(_run(tmp_path), _reconciliation(), execution)


def test_publish_course_report_writes_canonical_json_and_markdown(tmp_path: Path) -> None:
    report = build_course_report(_run(tmp_path), _reconciliation(), _pipeline_execution())
    output = tmp_path / "report"

    paths = publish_course_report(output, report)

    assert paths.json_path == output / "course_report.json"
    assert paths.markdown_path == output / "course_report.md"
    assert json.loads(paths.json_path.read_text(encoding="ascii")) == report
    assert paths.markdown_path.read_text(encoding="utf-8").startswith("# Course experiment report")
    with pytest.raises(FileExistsError):
        publish_course_report(output, report)


def test_report_command_publishes_from_finalized_run_and_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tinyml_ids_telemetry.cli import report as module

    reconciliation_path = tmp_path / "reconciliation.json"
    reconciliation_path.write_text(
        json.dumps(_reconciliation(), sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    pipeline_execution_path = tmp_path / "pipeline_execution.json"
    pipeline_execution_path.write_text(
        json.dumps(_pipeline_execution(), sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    output = tmp_path / "published"
    monkeypatch.setattr(module, "load_finalized_run", lambda path: _run(tmp_path))

    result = run_report_command(
        Namespace(
            finalized_run=tmp_path / "finalized",
            reconciliation=reconciliation_path,
            pipeline_execution=pipeline_execution_path,
            output=output,
        )
    )

    assert result == 0
    assert (output / "course_report.json").is_file()
    assert (output / "course_report.md").is_file()
