from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

from dashboard.data import FinalizedRunError, load_finalized_run

OUTPUTS = {
    "canonical_gold",
    "execution_dimension_counts",
    "operational_metrics",
    "policy_decisions",
    "predictive_row_metrics",
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
        "outputs": {name: {"row_count": 1} for name in sorted(OUTPUTS)},
        "statistical_unit": (
            "rows are hardware executions; repeated source_row_index values across lineages "
            "are not independent predictive rows"
        ),
    }
    (run / "finalized_run_manifest.json").write_bytes(_canonical(manifest))
    return run


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
