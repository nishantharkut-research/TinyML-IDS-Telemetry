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


def test_course_report_separates_deliveries_executions_and_predictive_rows(
    tmp_path: Path,
) -> None:
    report = build_course_report(_run(tmp_path), _reconciliation())

    assert report["schema_version"] == "tinyml-ids-course-report-v1"
    assert report["counts"] == {
        "execution_lineage_count": 4,
        "hardware_execution_count": 225_204,
        "replay_delivery_count": 247_725,
        "unique_predictive_row_count": 56_301,
    }
    assert report["reconciliation"] == {"difference_count": 0, "matched": True}

    markdown = render_course_report_markdown(report)
    assert "225,204 hardware executions" in markdown
    assert "56,301 predictive source rows" in markdown
    assert "247,725 replay deliveries" in markdown
    assert "225,204 samples" not in markdown
    assert "not 225,204 independent predictive samples" in markdown


def test_course_report_rejects_failed_reconciliation(tmp_path: Path) -> None:
    with pytest.raises(CourseReportError, match="reconciliation did not match"):
        build_course_report(_run(tmp_path), _reconciliation(matched=False))


def test_course_report_rejects_reconciliation_from_another_run(tmp_path: Path) -> None:
    reconciliation = _reconciliation()
    provenance = reconciliation["provenance"]
    assert isinstance(provenance, dict)
    spark = provenance["spark_aggregate"]
    assert isinstance(spark, dict)
    spark["replay_run_id"] = "another-run"

    with pytest.raises(CourseReportError, match="replay_run_id"):
        build_course_report(_run(tmp_path), reconciliation)


def test_publish_course_report_writes_canonical_json_and_markdown(tmp_path: Path) -> None:
    report = build_course_report(_run(tmp_path), _reconciliation())
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
    output = tmp_path / "published"
    monkeypatch.setattr(module, "load_finalized_run", lambda path: _run(tmp_path))

    result = run_report_command(
        Namespace(
            finalized_run=tmp_path / "finalized",
            reconciliation=reconciliation_path,
            output=output,
        )
    )

    assert result == 0
    assert (output / "course_report.json").is_file()
    assert (output / "course_report.md").is_file()
