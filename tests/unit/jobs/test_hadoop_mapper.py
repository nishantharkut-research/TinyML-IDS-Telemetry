from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from jobs.hadoop.mapper import MapperError, run
from tests.helpers.event_factory import make_event

PROJECT_ROOT = Path(__file__).parents[3]
POLICY_CONFIG = PROJECT_ROOT / "configs" / "retention-policies.toml"
_IGNORED_COUNTER_FIELDS = (
    "ignored_datagram_count",
    "ignored_wrong_endpoint_count",
    "ignored_bad_length_count",
    "ignored_non_ascii_count",
    "ignored_malformed_count",
    "ignored_bad_prefix_count",
    "ignored_bad_identity_count",
    "ignored_bad_attempt_count",
    "ignored_bad_inner_hex_count",
    "ignored_bad_crc_count",
    "ignored_bad_inner_text_count",
    "ignored_wrong_session_count",
    "ignored_wrong_stage_count",
    "ignored_wrong_transaction_count",
    "ignored_wrong_attempt_count",
)


def _key(metric: str, *dimensions: tuple[str, object], unit: str = "hardware_execution") -> str:
    return json.dumps(
        {
            "dimensions": [[name, value] for name, value in dimensions],
            "metric": metric,
            "statistical_unit": unit,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _map(event: dict[str, object]) -> tuple[str, dict[str, int]]:
    source = io.StringIO(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
    target = io.StringIO()
    run(source, target, POLICY_CONFIG)
    output = target.getvalue()
    records: dict[str, int] = {}
    for line in output.splitlines():
        key, value = line.split("\t")
        assert key == json.dumps(json.loads(key), sort_keys=True, separators=(",", ":"))
        assert value == json.dumps(json.loads(value), separators=(",", ":"))
        records[key] = int(value)
    return output, records


def _rows(event: dict[str, object]) -> list[tuple[dict[str, object], int]]:
    output, _ = _map(event)
    rows: list[tuple[dict[str, object], int]] = []
    for line in output.splitlines():
        raw_key, raw_value = line.split("\t")
        rows.append((json.loads(raw_key), json.loads(raw_value)))
    return rows


def test_mapper_emits_canonical_exact_metrics_for_one_hardware_execution() -> None:
    event = make_event(
        predicted_class_id=0,
        true_class_id=0,
        attempts=2,
        response_timeout_count=1,
        ignored_datagram_count=2,
    ).to_dict()
    for value, name in enumerate(_IGNORED_COUNTER_FIELDS[1:], start=1):
        event[name] = value

    output, records = _map(event)

    payload = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("ascii")
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    lineage = (
        ("lineage_id", event["lineage_id"]),
        ("board", event["board"]),
        ("student_model", event["student_model"]),
    )
    expected = {
        _key(
            "source_event_occurrence_count",
            ("source_event_id", event["source_event_id"]),
            ("lineage_id", event["lineage_id"]),
            ("row_id", event["row_id"]),
            ("source_row_index", event["source_row_index"]),
            ("payload_sha256", payload_sha256),
            unit="canonical_input_record",
        ): 1,
        _key(
            "lineage_row_occurrence_count",
            ("lineage_id", event["lineage_id"]),
            ("row_id", event["row_id"]),
            ("source_event_id", event["source_event_id"]),
            ("payload_sha256", payload_sha256),
            unit="canonical_input_record",
        ): 1,
        _key(
            "predictive_source_row_occurrence_count",
            ("source_row_index", event["source_row_index"]),
            ("lineage_id", event["lineage_id"]),
            ("source_event_id", event["source_event_id"]),
            ("payload_sha256", payload_sha256),
            unit="predictive_source_row",
        ): 1,
        _key("hardware_execution_count"): 1,
        _key("hardware_execution_count", *lineage): 1,
        _key("hardware_execution_count", ("board", event["board"])): 1,
        _key("hardware_execution_count", ("student_model", event["student_model"])): 1,
        _key(
            "hardware_execution_count",
            ("true_class_id", event["true_class_id"]),
            ("true_class_name", event["true_class_name"]),
        ): 1,
        _key(
            "hardware_execution_count",
            ("predicted_class_id", event["predicted_class_id"]),
            ("predicted_class_name", event["predicted_class_name"]),
        ): 1,
        _key(
            "hardware_execution_count",
            ("board", event["board"]),
            ("student_model", event["student_model"]),
        ): 1,
        _key("successful_execution_count", *lineage): 1,
        _key("predicted_attack_execution_count", *lineage): 1,
        _key("retry_event_count", *lineage): 1,
        _key("retry_count", *lineage): 1,
        _key("response_timeout_count_sum", *lineage): 1,
        _key("preprocess_us_sum", *lineage): 10,
        _key("inference_us_sum", *lineage): 20,
        _key("total_us_sum", *lineage): 30,
        _key("request_datagram_bytes_sent_sum", *lineage): 200,
        _key("response_bytes_sum", *lineage): 160,
        _key("host_observed_datagram_rtt_us_sum", *lineage): 1_000,
        _key("transaction_elapsed_us_sum", *lineage): 1_000,
    }
    for name in _IGNORED_COUNTER_FIELDS:
        expected[_key(f"{name}_sum", *lineage)] = cast(int, event[name])
    for policy_id, reason in (
        ("retain-all-v1", "retain_all"),
        ("security-aware-static-v1", "predicted_attack"),
    ):
        policy = (("policy_id", policy_id),)
        for metric, value in (
            ("hardware_execution_count", 1),
            ("retained_hardware_execution_count", 1),
            ("predicted_attack_execution_count", 1),
            ("retained_predicted_attack_execution_count", 1),
            ("actual_attack_execution_count", 1),
            ("retained_actual_attack_execution_count", 1),
            ("inclusion_probability_numerator_sum", 1),
            ("inclusion_probability_denominator_sum", 1),
        ):
            expected[_key(metric, *policy)] = value
        expected[
            _key(
                "decision_count",
                ("policy_id", policy_id),
                ("reason", reason),
                ("retained", True),
            )
        ] = 1

    assert output.endswith("\n")
    raw_keys = [line.split("\t", 1)[0] for line in output.splitlines()]
    assert len(raw_keys) == len(set(raw_keys)) == 55
    assert records == expected


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda event: event.pop("status"), "missing required field status"),
        (lambda event: event.__setitem__("extra", 1), "unexpected field extra"),
        (lambda event: event.__setitem__("attempts", True), "attempts must be an integer"),
        (
            lambda event: event.__setitem__("fixed_logits", [False, 0, 0, 1, 0]),
            "fixed_logits must be five integers",
        ),
        (lambda event: event.__setitem__("total_us", 31), "inconsistent total_us"),
        (
            lambda event: event.__setitem__("normalized_logit_margin", 999),
            "inconsistent normalized_logit_margin",
        ),
        (
            lambda event: event.__setitem__("is_predicted_attack", True),
            "inconsistent is_predicted_attack",
        ),
        (
            lambda event: event.__setitem__("predicted_class_name", "Flooding"),
            "inconsistent predicted_class_name",
        ),
        (
            lambda event: event.__setitem__("mcu_fixed_logits_match", False),
            "MCU equivalence fields must be true",
        ),
        (
            lambda event: event.__setitem__("student_model", "student_C"),
            "unsupported student_model",
        ),
        (lambda event: event.__setitem__("logit_frac_bits", 8), "logit_frac_bits mismatch"),
    ],
)
def test_mapper_fails_closed_on_invalid_events(
    mutation: Callable[[dict[str, object]], object], message: str
) -> None:
    event = make_event().to_dict()
    mutation(event)

    with pytest.raises(MapperError, match=message):
        _map(event)


@pytest.mark.parametrize("line", ["", "not-json\n", "[]\n", '{"status":"OK","status":"BAD"}\n'])
def test_mapper_rejects_blank_malformed_non_object_and_duplicate_property_json(line: str) -> None:
    with pytest.raises(MapperError):
        run(io.StringIO(line), io.StringIO(), POLICY_CONFIG)


def test_mapper_rejects_whitespace_and_unsorted_noncanonical_event_json() -> None:
    event = make_event().to_dict()
    canonical = json.dumps(event, sort_keys=True, separators=(",", ":"))
    unsorted = json.dumps(event, sort_keys=False, separators=(",", ":"))
    assert unsorted != canonical

    for raw in (f" {canonical}\n", f"{canonical} \n", f"{unsorted}\n"):
        with pytest.raises(MapperError, match="canonical compact sorted JSON"):
            run(io.StringIO(raw), io.StringIO(), POLICY_CONFIG)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("version = 1", "version = true", "version must be an integer"),
        (
            "normal_sample_denominator = 10",
            "normal_sample_denominator = 11",
            "normal sample fraction",
        ),
        (
            'student_A = "6.330078125"',
            'student_A = "6.0"',
            "margin thresholds must exactly match",
        ),
        ("student_A = 9\nstudent_B = 9", "student_A = true\nstudent_B = 9", "must be an integer"),
    ],
)
def test_mapper_independently_rejects_wrong_policy_contract(
    tmp_path: Path, old: str, new: str, message: str
) -> None:
    config = POLICY_CONFIG.read_text(encoding="utf-8")
    invalid = tmp_path / "retention-policies.toml"
    invalid.write_text(config.replace(old, new), encoding="utf-8")

    with pytest.raises(MapperError, match=message):
        run(io.StringIO(""), io.StringIO(), invalid)


def _security_reason(event: dict[str, object]) -> tuple[str, bool]:
    for key, _ in _rows(event):
        dimensions = _dimensions(key)
        if key["metric"] == "decision_count" and dimensions.get("policy_id") == (
            "security-aware-static-v1"
        ):
            return str(dimensions["reason"]), bool(dimensions["retained"])
    raise AssertionError("security-aware decision row not emitted")


@pytest.mark.parametrize(
    ("event", "expected_reason"),
    [
        (
            make_event(predicted_class_id=0, status="ERROR", attempts=2).to_dict(),
            "predicted_attack",
        ),
        (make_event(status="ERROR", attempts=2).to_dict(), "device_status"),
        (make_event(attempts=2, margin_units=1).to_dict(), "protocol_anomaly"),
        (make_event(margin_units=3_240).to_dict(), "low_margin"),
        (
            make_event(student_model="student_B", margin_units=2_918).to_dict(),
            "low_margin",
        ),
    ],
)
def test_mapper_matches_mandatory_policy_reason_precedence(
    event: dict[str, object], expected_reason: str
) -> None:
    assert _security_reason(event) == (expected_reason, True)


@pytest.mark.parametrize(
    ("student_model", "threshold", "margin_units"),
    [
        ("student_A", 3_241, 3_240),
        ("student_A", 3_241, 3_241),
        ("student_A", 3_241, 3_242),
        ("student_B", 2_919, 2_918),
        ("student_B", 2_919, 2_919),
        ("student_B", 2_919, 2_920),
    ],
)
def test_mapper_uses_strict_model_q9_margin_boundaries(
    student_model: str, threshold: int, margin_units: int
) -> None:
    event = make_event(
        row_id=40 if student_model == "student_A" else 41,
        student_model=student_model,
        margin_units=margin_units,
    ).to_dict()

    reason, retained = _security_reason(event)

    if margin_units == threshold - 1:
        assert (reason, retained) == ("low_margin", True)
    else:
        payload = f"security-aware-static-v1|{event['source_event_id']}".encode("ascii")
        sampled = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") % 10 == 0
        assert (reason, retained) == (
            "normal_sample" if sampled else "normal_not_sampled",
            sampled,
        )


def test_mapper_uses_unsigned_first_sha256_u64_for_one_in_ten_sampling() -> None:
    policy_id = "security-aware-static-v1"
    selected = next(
        f"{value:064x}"
        for value in range(1, 10_000)
        if int.from_bytes(
            hashlib.sha256(f"{policy_id}|{value:064x}".encode("ascii")).digest()[:8], "big"
        )
        % 10
        == 0
    )
    rejected = next(
        f"{value:064x}"
        for value in range(1, 10_000)
        if int.from_bytes(
            hashlib.sha256(f"{policy_id}|{value:064x}".encode("ascii")).digest()[:8], "big"
        )
        % 10
        != 0
    )

    selected_event = make_event(row_id=1, margin_units=7_000).to_dict()
    selected_event["source_event_id"] = selected
    rejected_event = make_event(row_id=2, margin_units=7_000).to_dict()
    rejected_event["source_event_id"] = rejected

    assert _security_reason(selected_event) == (
        "normal_sample",
        True,
    )
    assert _security_reason(rejected_event) == (
        "normal_not_sampled",
        False,
    )


def test_true_class_never_changes_online_policy_decision() -> None:
    source_id = "f" * 64
    normal = make_event(row_id=3, true_class_id=3, margin_units=7_000).to_dict()
    normal["source_event_id"] = source_id
    attack = dict(normal)
    attack["true_class_id"] = 0
    attack["true_class_name"] = "Blackhole"

    normal_rows = _rows(normal)
    attack_rows = _rows(attack)

    def online(rows: list[tuple[dict[str, object], int]]) -> set[tuple[str, int]]:
        selected: set[tuple[str, int]] = set()
        for key, value in rows:
            dimensions = _dimensions(key)
            if dimensions.get("policy_id") == "security-aware-static-v1" and key["metric"] not in {
                "actual_attack_execution_count",
                "retained_actual_attack_execution_count",
            }:
                selected.add((_canonical_test_key(key), value))
        return selected

    assert online(normal_rows) == online(attack_rows)
    assert _metric_value(normal_rows, "actual_attack_execution_count") == 0
    assert _metric_value(attack_rows, "actual_attack_execution_count") == 1


def _canonical_test_key(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _dimensions(key: dict[str, object]) -> dict[str, object]:
    return dict(cast(list[tuple[str, object]], key["dimensions"]))


def _metric_value(rows: list[tuple[dict[str, object], int]], metric: str) -> int:
    return next(
        value
        for key, value in rows
        if key["metric"] == metric
        and _dimensions(key).get("policy_id") == "security-aware-static-v1"
    )


def test_mapper_processes_stdin_incrementally_before_a_later_failure() -> None:
    valid = json.dumps(make_event().to_dict(), sort_keys=True, separators=(",", ":"))
    target = io.StringIO()

    with pytest.raises(MapperError, match="input line 2"):
        run(io.StringIO(f"{valid}\nnot-json\n"), target, POLICY_CONFIG)

    assert target.getvalue().startswith("{")


@pytest.mark.parametrize(
    ("input_text", "policy_config", "message"),
    [
        ("not-json\n", POLICY_CONFIG, "strict JSON"),
        ("", PROJECT_ROOT / "missing-retention-policies.toml", "could not be read"),
    ],
)
def test_mapper_cli_reports_contract_failures_without_tracebacks_or_paths(
    input_text: str,
    policy_config: Path,
    message: str,
) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "jobs" / "hadoop" / "mapper.py"),
            "--policy-config",
            str(policy_config),
        ],
        cwd=PROJECT_ROOT,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 2
    assert completed.stdout == ""
    assert completed.stderr.startswith("mapper failed: ")
    assert message in completed.stderr
    assert "Traceback" not in completed.stderr
    assert str(PROJECT_ROOT) not in completed.stderr
