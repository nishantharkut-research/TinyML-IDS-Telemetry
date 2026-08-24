from __future__ import annotations

import json
import subprocess
import sys
from hashlib import sha256
from pathlib import Path

import pytest

from dashboard.app import build_dashboard_views
from dashboard.data import (
    CourseReportSummary,
    FinalizedRun,
    FinalizedRunError,
    load_course_report,
    load_finalized_run,
)

OUTPUTS = {
    "canonical_gold",
    "execution_dimension_counts",
    "operational_metrics",
    "policy_decisions",
    "predictive_row_metrics",
    "retained_security_aware",
    "retention_metrics",
    "retention_reason_counts",
}


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def _finalized_run(tmp_path: Path) -> Path:
    run = tmp_path / "replay_run_id=course-demo"
    run.mkdir()
    aggregates = {
        "execution_dimension_counts": [],
        "operational_metrics": [
            {
                "board": "esp32c3",
                "hardware_execution_count": 56_301,
                "lineage_id": "a" * 64,
                "replay_run_id": "course-demo",
                "statistical_unit": "hardware_execution",
                "student_model": "student_A",
            }
        ],
        "predictive_row_metrics": [
            {
                "metric": "unique_predictive_row_count",
                "replay_run_id": "course-demo",
                "statistical_unit": "predictive_source_row",
                "value": 56_301,
            }
        ],
        "retention_metrics": [],
        "retention_reason_counts": [],
    }
    aggregate_payload = _canonical(aggregates)
    (run / "spark_aggregates.json").write_bytes(aggregate_payload)
    for output in OUTPUTS:
        output_directory = run / output
        output_directory.mkdir()
        (output_directory / "_SUCCESS").touch()
    manifest = {
        "schema_version": "telemetry-finalized-run-v1",
        "replay_run_id": "course-demo",
        "source_event_count": 225_204,
        "delivery_count": 225_204,
        "spark_aggregates_sha256": sha256(aggregate_payload).hexdigest(),
        "outputs": {
            "canonical_gold": {"row_count": 225_204},
            "execution_dimension_counts": {"row_count": 0},
            "operational_metrics": {"row_count": 1},
            "policy_decisions": {"row_count": 450_408},
            "predictive_row_metrics": {"row_count": 1},
            "retained_security_aware": {"row_count": 100_000},
            "retention_metrics": {"row_count": 0},
            "retention_reason_counts": {"row_count": 0},
        },
        "statistical_unit": (
            "rows are hardware executions; repeated source_row_index values across lineages "
            "are not independent predictive rows"
        ),
    }
    (run / "finalized_run_manifest.json").write_bytes(_canonical(manifest))
    return run


def _course_report() -> dict[str, object]:
    return {
        "schema_version": "tinyml-ids-course-report-v1",
        "replay_run_id": "course-demo",
        "counts": {
            "execution_lineage_count": 4,
            "hardware_execution_count": 225_204,
            "replay_delivery_count": 225_204,
            "unique_predictive_row_count": 56_301,
        },
        "reconciliation": {"difference_count": 0, "matched": True},
        "pipeline": {
            "replay_executed": True,
            "phase_seconds": {"replay": 100.0, "total": 150.0},
            "rates_per_second": {
                "acknowledged_replay_delivery_rate": 2252.04,
                "finalized_source_event_rate": 1501.36,
            },
            "storage_bytes": {
                "finalized/canonical_gold": 10_000_000,
                "finalized/retained_security_aware": 4_000_000,
            },
        },
    }


def test_load_finalized_run_reads_only_manifest_bound_aggregates(tmp_path: Path) -> None:
    run = _finalized_run(tmp_path)

    result = load_finalized_run(run)

    assert result.replay_run_id == "course-demo"
    assert result.source_event_count == 225_204
    assert result.delivery_count == 225_204
    assert result.aggregates["predictive_row_metrics"][0]["value"] == 56_301


def test_load_finalized_run_rejects_incomplete_publication(tmp_path: Path) -> None:
    run = _finalized_run(tmp_path)
    (run / "operational_metrics" / "_SUCCESS").unlink()

    with pytest.raises(FinalizedRunError, match=r"operational_metrics.*_SUCCESS"):
        load_finalized_run(run)


def test_load_finalized_run_rejects_aggregate_mutation(tmp_path: Path) -> None:
    run = _finalized_run(tmp_path)
    (run / "spark_aggregates.json").write_text("{}\n", encoding="ascii")

    with pytest.raises(FinalizedRunError, match="SHA-256"):
        load_finalized_run(run)


def test_load_finalized_run_rejects_manifest_row_count_mismatch(tmp_path: Path) -> None:
    run = _finalized_run(tmp_path)
    manifest_path = run / "finalized_run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    manifest["outputs"]["operational_metrics"]["row_count"] = 2
    manifest_path.write_bytes(_canonical(manifest))

    with pytest.raises(FinalizedRunError, match="operational_metrics row_count"):
        load_finalized_run(run)


def test_load_course_report_requires_matching_reconciled_run(tmp_path: Path) -> None:
    run = load_finalized_run(_finalized_run(tmp_path))
    report_path = tmp_path / "course_report.json"
    report_path.write_bytes(_canonical(_course_report()))

    result = load_course_report(report_path, run)

    assert result.reconciliation_matched is True
    assert result.unique_predictive_row_count == 56_301
    assert result.pipeline_total_seconds == 150.0
    assert result.acknowledged_replay_delivery_rate == 2252.04
    assert result.retained_security_aware_bytes == 4_000_000


def test_load_course_report_accepts_retained_file_overhead(tmp_path: Path) -> None:
    run = load_finalized_run(_finalized_run(tmp_path))
    report_path = tmp_path / "course_report.json"
    report = _course_report()
    pipeline = report["pipeline"]
    assert isinstance(pipeline, dict)
    storage = pipeline["storage_bytes"]
    assert isinstance(storage, dict)
    storage["finalized/retained_security_aware"] = 11_000_000
    report_path.write_bytes(_canonical(report))

    result = load_course_report(report_path, run)

    assert result.retained_security_aware_bytes == 11_000_000


def test_load_course_report_rejects_foreign_run(tmp_path: Path) -> None:
    run = load_finalized_run(_finalized_run(tmp_path))
    report_path = tmp_path / "course_report.json"
    report_path.write_bytes(
        _canonical(
            {
                "schema_version": "tinyml-ids-course-report-v1",
                "replay_run_id": "another-run",
                "counts": {},
                "reconciliation": {"difference_count": 0, "matched": True},
            }
        )
    )

    with pytest.raises(FinalizedRunError, match="replay_run_id"):
        load_course_report(report_path, run)


def test_dashboard_views_derive_only_declared_finalized_metrics(tmp_path: Path) -> None:
    replay_run_id = "course-demo"
    run = FinalizedRun(
        run_directory=tmp_path / f"replay_run_id={replay_run_id}",
        replay_run_id=replay_run_id,
        source_event_count=100,
        delivery_count=110,
        aggregates={
            "execution_dimension_counts": (
                {
                    "replay_run_id": replay_run_id,
                    "dimension_kind": "predicted_class",
                    "class_id": 0,
                    "class_name": "Normal",
                    "hardware_execution_count": 75,
                },
                {
                    "replay_run_id": replay_run_id,
                    "dimension_kind": "predicted_class",
                    "class_id": 1,
                    "class_name": "Flooding",
                    "hardware_execution_count": 25,
                },
            ),
            "operational_metrics": (
                {
                    "replay_run_id": replay_run_id,
                    "board": "esp32c3",
                    "student_model": "student_A",
                    "hardware_execution_count": 100,
                    "successful_execution_count": 98,
                    "predicted_attack_execution_count": 25,
                    "retry_event_count": 3,
                    "retry_count": 4,
                    "response_timeout_count_sum": 1,
                    "preprocess_us_sum": 100,
                    "inference_us_sum": 1_500,
                    "total_us_sum": 2_000,
                    "host_observed_datagram_rtt_us_sum": 20_000,
                    "transaction_elapsed_us_sum": 25_000,
                },
            ),
            "predictive_row_metrics": (
                {
                    "replay_run_id": replay_run_id,
                    "metric": "execution_lineage_count",
                    "value": 4,
                },
                {
                    "replay_run_id": replay_run_id,
                    "metric": "unique_predictive_row_count",
                    "value": 25,
                },
            ),
            "retention_metrics": (
                {
                    "replay_run_id": replay_run_id,
                    "policy_id": "retain-all-v1",
                    "hardware_execution_count": 100,
                    "retained_hardware_execution_count": 100,
                    "actual_attack_execution_count": 20,
                    "retained_actual_attack_execution_count": 20,
                },
            ),
            "retention_reason_counts": (
                {
                    "replay_run_id": replay_run_id,
                    "policy_id": "retain-all-v1",
                    "reason": "retain_all",
                    "retained": True,
                    "decision_count": 100,
                },
            ),
        },
    )

    report = CourseReportSummary(
        replay_run_id=replay_run_id,
        reconciliation_matched=True,
        execution_lineage_count=4,
        hardware_execution_count=100,
        replay_delivery_count=110,
        unique_predictive_row_count=25,
        replay_executed=True,
        pipeline_total_seconds=8.0,
        acknowledged_replay_delivery_rate=22.0,
        finalized_source_event_rate=12.5,
        canonical_gold_bytes=5_000,
        retained_security_aware_bytes=2_000,
    )

    views = build_dashboard_views(run, report)

    assert views.overview["duplicate_delivery_count"] == 10
    assert views.overview["unique_predictive_row_count"] == 25
    assert views.overview["execution_lineage_count"] == 4
    assert views.operations[0]["mean_inference_us"] == 15.0
    assert views.operations[0]["mean_total_us"] == 20.0
    assert views.quality["failed_execution_count"] == 2
    assert views.quality["retry_count"] == 4
    assert views.predicted_classes[1]["class_name"] == "Flooding"
    assert views.retention[0]["retention_rate"] == 1.0
    assert views.pipeline["total_seconds"] == 8.0
    assert views.pipeline["retained_storage_ratio"] == 0.4


def test_streamlit_entrypoint_imports_from_outside_checkout(tmp_path: Path) -> None:
    app = Path(__file__).resolve().parents[3] / "dashboard" / "app.py"
    run_name = "tinyml_dashboard_import_probe"
    command = f"import runpy; runpy.run_path({str(app)!r}, run_name={run_name!r})"

    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
