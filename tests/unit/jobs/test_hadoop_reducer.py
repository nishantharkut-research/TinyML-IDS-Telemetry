from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from jobs.hadoop.mapper import run as map_run
from jobs.hadoop.reducer import ReducerError
from jobs.hadoop.reducer import run as reduce_run
from tests.helpers.event_factory import make_event

PROJECT_ROOT = Path(__file__).parents[3]
POLICY_CONFIG = PROJECT_ROOT / "configs" / "retention-policies.toml"


def _mapped(*events: dict[str, object]) -> str:
    source = io.StringIO(
        "".join(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n" for event in events)
    )
    target = io.StringIO()
    map_run(source, target, POLICY_CONFIG)
    return "".join(f"{line}\n" for line in sorted(target.getvalue().splitlines()))


def _reduced(mapped: str) -> tuple[str, list[dict[str, object]]]:
    target = io.StringIO()
    reduce_run(io.StringIO(mapped), target)
    output = target.getvalue()
    rows = [json.loads(line) for line in output.splitlines()]
    for raw, row in zip(output.splitlines(), rows, strict=True):
        assert raw == json.dumps(row, sort_keys=True, separators=(",", ":"))
    return output, rows


def _key(
    metric: str,
    *dimensions: tuple[str, object],
    unit: str = "hardware_execution",
) -> str:
    return json.dumps(
        {
            "dimensions": [[name, value] for name, value in dimensions],
            "metric": metric,
            "statistical_unit": unit,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def test_reducer_streams_sorted_partials_into_canonical_metric_rows() -> None:
    first = make_event(
        row_id=1,
        predicted_class_id=0,
        true_class_id=0,
        attempts=2,
        response_timeout_count=1,
    ).to_dict()
    second = make_event(row_id=2, true_class_id=0).to_dict()
    mapped = _mapped(first, second)

    first_output, rows = _reduced(mapped)
    second_output, _ = _reduced(mapped)

    totals: dict[str, int] = {}
    for line in mapped.splitlines():
        raw_key, raw_value = line.split("\t")
        totals[raw_key] = totals.get(raw_key, 0) + int(raw_value)
    expected_rows = set()
    for raw_key, value in totals.items():
        key = json.loads(raw_key)
        expected_rows.add(
            json.dumps(
                {
                    "dimensions": dict(key["dimensions"]),
                    "metric": key["metric"],
                    "statistical_unit": key["statistical_unit"],
                    "value": value,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    assert first_output == second_output
    assert first_output.endswith("\n")
    assert len(rows) == len(set(first_output.splitlines())) == 60
    assert set(first_output.splitlines()) == expected_rows


def test_reducer_rejects_repeated_source_event_with_identical_payload() -> None:
    source_id = "1" * 64
    key = _key(
        "source_event_occurrence_count",
        ("source_event_id", source_id),
        ("lineage_id", "3" * 64),
        ("row_id", 7),
        ("source_row_index", 11),
        ("payload_sha256", "a" * 64),
        unit="canonical_input_record",
    )

    with pytest.raises(ReducerError, match="repeated source_event_id"):
        reduce_run(io.StringIO(f"{key}\t1\n{key}\t1\n"), io.StringIO())


def test_reducer_fails_the_complete_mapper_stream_for_a_duplicate_event() -> None:
    event = make_event(row_id=7).to_dict()

    with pytest.raises(ReducerError, match=r"repeated (lineage row|source_event_id)"):
        reduce_run(io.StringIO(_mapped(event, event)), io.StringIO())


def test_reducer_rejects_different_payloads_for_one_source_event() -> None:
    source_id = "1" * 64
    lines = []
    for payload in ("a" * 64, "b" * 64):
        key = _key(
            "source_event_occurrence_count",
            ("source_event_id", source_id),
            ("lineage_id", "3" * 64),
            ("row_id", 7),
            ("source_row_index", 11),
            ("payload_sha256", payload),
            unit="canonical_input_record",
        )
        lines.append(f"{key}\t1\n")

    with pytest.raises(ReducerError, match="conflicting payloads"):
        reduce_run(io.StringIO("".join(sorted(lines))), io.StringIO())


@pytest.mark.parametrize(
    ("metric", "dimensions", "unit", "message"),
    [
        (
            "lineage_row_occurrence_count",
            (
                ("lineage_id", "1" * 64),
                ("row_id", 7),
                ("source_event_id", "2" * 64),
                ("payload_sha256", "3" * 64),
            ),
            "canonical_input_record",
            "repeated lineage row",
        ),
        (
            "predictive_source_row_occurrence_count",
            (
                ("source_row_index", 7),
                ("lineage_id", "1" * 64),
                ("source_event_id", "2" * 64),
                ("payload_sha256", "3" * 64),
            ),
            "predictive_source_row",
            "repeated predictive source row",
        ),
    ],
)
def test_reducer_rejects_repeated_lineage_and_predictive_occurrences(
    metric: str,
    dimensions: tuple[tuple[str, object], ...],
    unit: str,
    message: str,
) -> None:
    key = _key(metric, *dimensions, unit=unit)

    with pytest.raises(ReducerError, match=message):
        reduce_run(io.StringIO(f"{key}\t1\n{key}\t1\n"), io.StringIO())


@pytest.mark.parametrize(
    ("metric", "first_dimensions", "second_dimensions", "unit", "message"),
    [
        (
            "lineage_row_occurrence_count",
            (
                ("lineage_id", "1" * 64),
                ("row_id", 7),
                ("source_event_id", "2" * 64),
                ("payload_sha256", "3" * 64),
            ),
            (
                ("lineage_id", "1" * 64),
                ("row_id", 7),
                ("source_event_id", "4" * 64),
                ("payload_sha256", "5" * 64),
            ),
            "canonical_input_record",
            "conflicting lineage row",
        ),
        (
            "predictive_source_row_occurrence_count",
            (
                ("source_row_index", 7),
                ("lineage_id", "1" * 64),
                ("source_event_id", "2" * 64),
                ("payload_sha256", "3" * 64),
            ),
            (
                ("source_row_index", 7),
                ("lineage_id", "1" * 64),
                ("source_event_id", "4" * 64),
                ("payload_sha256", "5" * 64),
            ),
            "predictive_source_row",
            "conflicting predictive source row",
        ),
    ],
)
def test_reducer_rejects_conflicting_lineage_and_predictive_occurrences(
    metric: str,
    first_dimensions: tuple[tuple[str, object], ...],
    second_dimensions: tuple[tuple[str, object], ...],
    unit: str,
    message: str,
) -> None:
    lines = [
        f"{_key(metric, *first_dimensions, unit=unit)}\t1\n",
        f"{_key(metric, *second_dimensions, unit=unit)}\t1\n",
    ]

    with pytest.raises(ReducerError, match=message):
        reduce_run(io.StringIO("".join(sorted(lines))), io.StringIO())


def test_reducer_requires_lexically_sorted_canonical_keys() -> None:
    unique_lines = sorted(set(_mapped(make_event().to_dict()).splitlines()))

    with pytest.raises(ReducerError, match="not sorted"):
        reduce_run(io.StringIO(f"{unique_lines[-1]}\n{unique_lines[0]}\n"), io.StringIO())


@pytest.mark.parametrize(
    ("record", "message"),
    [
        ("", "at least one mapper record"),
        ("\n", "must not be blank"),
        ("not-json\t1\n", "strict JSON"),
        (f"{_key('hardware_execution_count')}\n", "exactly one tab"),
        (f"{_key('hardware_execution_count')}\ttrue\n", "non-negative integer"),
        (f"{_key('hardware_execution_count')}\t-1\n", "non-negative integer"),
        (f"{_key('hardware_execution_count')}\t1.0\n", "non-negative integer"),
        (f"{_key('made_up_metric')}\t1\n", "do not match an exact schema"),
        (
            f"{_key('retry_count', ('board', 'fixture-board'))}\t1\n",
            "do not match an exact schema",
        ),
        (
            _key(
                "decision_count",
                ("policy_id", "retain-all-v1"),
                ("reason", "wrong"),
                ("retained", True),
            )
            + "\t1\n",
            "retain-all-v1 decision dimensions are inconsistent",
        ),
    ],
)
def test_reducer_fails_closed_on_invalid_key_and_partial_schemas(record: str, message: str) -> None:
    with pytest.raises(ReducerError, match=message):
        reduce_run(io.StringIO(record), io.StringIO())


def test_reducer_rejects_noncanonical_json_key() -> None:
    key = json.dumps(
        {
            "dimensions": [],
            "metric": "hardware_execution_count",
            "statistical_unit": "hardware_execution",
        }
    )

    with pytest.raises(ReducerError, match="canonical compact JSON"):
        reduce_run(io.StringIO(f"{key}\t1\n"), io.StringIO())


def test_reducer_rejects_zero_source_occurrence_partial() -> None:
    key = _key(
        "source_event_occurrence_count",
        ("source_event_id", "1" * 64),
        ("lineage_id", "3" * 64),
        ("row_id", 7),
        ("source_row_index", 11),
        ("payload_sha256", "2" * 64),
        unit="canonical_input_record",
    )

    with pytest.raises(ReducerError, match="positive integer"):
        reduce_run(io.StringIO(f"{key}\t0\n"), io.StringIO())


@pytest.mark.parametrize(
    ("metric", "dimensions", "unit"),
    [
        (
            "source_event_occurrence_count",
            (
                ("source_event_id", "1" * 64),
                ("lineage_id", "3" * 64),
                ("row_id", 7),
                ("source_row_index", 11),
                ("payload_sha256", "2" * 64),
            ),
            "canonical_input_record",
        ),
        (
            "lineage_row_occurrence_count",
            (
                ("lineage_id", "1" * 64),
                ("row_id", 7),
                ("source_event_id", "2" * 64),
                ("payload_sha256", "3" * 64),
            ),
            "canonical_input_record",
        ),
        (
            "predictive_source_row_occurrence_count",
            (
                ("source_row_index", 7),
                ("lineage_id", "1" * 64),
                ("source_event_id", "2" * 64),
                ("payload_sha256", "3" * 64),
            ),
            "predictive_source_row",
        ),
    ],
)
def test_reducer_rejects_preaggregated_occurrence_partials(
    metric: str, dimensions: tuple[tuple[str, object], ...], unit: str
) -> None:
    key = _key(metric, *dimensions, unit=unit)

    with pytest.raises(ReducerError, match="must equal one"):
        reduce_run(io.StringIO(f"{key}\t2\n"), io.StringIO())


def test_reducer_emits_completed_keys_incrementally_before_later_unsorted_input() -> None:
    unique_lines = sorted(set(_mapped(make_event().to_dict()).splitlines()))
    target = io.StringIO()

    with pytest.raises(ReducerError, match="not sorted"):
        reduce_run(
            io.StringIO(f"{unique_lines[0]}\n{unique_lines[1]}\n{unique_lines[0]}\n"),
            target,
        )

    assert target.getvalue().startswith("{")


def test_reducer_cli_rejects_unexpected_arguments_before_reading_stdin() -> None:
    completed = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "jobs" / "hadoop" / "reducer.py"), "unexpected"],
        cwd=PROJECT_ROOT,
        input=_mapped(make_event().to_dict()),
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "unrecognized arguments: unexpected" in completed.stderr
    assert completed.stdout == ""


def test_reducer_cli_reports_contract_failures_without_tracebacks_or_paths() -> None:
    completed = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "jobs" / "hadoop" / "reducer.py")],
        cwd=PROJECT_ROOT,
        input="not-json\t1\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 2
    assert completed.stdout == ""
    assert completed.stderr.startswith("reducer failed: ")
    assert "strict JSON" in completed.stderr
    assert "Traceback" not in completed.stderr
    assert str(PROJECT_ROOT) not in completed.stderr
