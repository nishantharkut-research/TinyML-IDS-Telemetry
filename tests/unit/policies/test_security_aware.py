from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.contracts.event import TelemetryEvent
from tinyml_ids_telemetry.policies.security_aware import (
    SecurityAwareStaticPolicy,
    load_security_aware_policy,
)


def _policy() -> SecurityAwareStaticPolicy:
    return SecurityAwareStaticPolicy(
        margin_thresholds={
            "student_A": Decimal("6.330078125"),
            "student_B": Decimal("5.701171875"),
        },
        normal_sample_numerator=1,
        normal_sample_denominator=10,
    )


def _modified_repository_config(tmp_path: Path, old: str, new: str) -> Path:
    config = Path("configs/retention-policies.toml").read_text(encoding="utf-8")
    assert old in config
    changed_config = tmp_path / "retention-policies.toml"
    changed_config.write_text(config.replace(old, new), encoding="utf-8")
    return changed_config


def _repository_config_with_logit_mapping(tmp_path: Path, mapping: str) -> Path:
    config = Path("configs/retention-policies.toml").read_text(encoding="utf-8")
    canonical_table = "[security_aware_static.logit_frac_bits]\nstudent_A = 9\nstudent_B = 9\n\n"
    replacement_table = f"[security_aware_static.logit_frac_bits]\n{mapping}\n\n"
    if canonical_table in config:
        config = config.replace(canonical_table, replacement_table)
    else:
        config = config.replace(
            "[security_aware_static.margin_thresholds]",
            f"{replacement_table}[security_aware_static.margin_thresholds]",
        )
    changed_config = tmp_path / "retention-policies.toml"
    changed_config.write_text(config, encoding="utf-8")
    return changed_config


def test_predicted_attack_is_always_retained() -> None:
    decision = _policy().decide(make_event(predicted_class_id=0, true_class_id=3))

    assert decision.retained is True
    assert decision.reason == "predicted_attack"
    assert decision.inclusion_probability == 1.0
    assert decision.inverse_probability_weight == 1.0


def test_non_ok_device_response_is_always_retained() -> None:
    decision = _policy().decide(make_event(status="FAILED"))

    assert decision.retained is True
    assert decision.reason == "device_status"


@pytest.mark.parametrize(
    "event",
    [
        make_event(attempts=2),
        make_event(response_timeout_count=1),
        make_event(ignored_datagram_count=1),
        replace(make_event(), ignored_bad_crc_count=1),
    ],
)
def test_protocol_anomaly_is_always_retained(event: TelemetryEvent) -> None:
    decision = _policy().decide(event)

    assert decision.retained is True
    assert decision.reason == "protocol_anomaly"


def test_model_specific_low_margin_normal_is_retained() -> None:
    student_a = _policy().decide(make_event(student_model="student_A", margin_units=512))
    student_b = _policy().decide(make_event(student_model="student_B", margin_units=1_024))

    assert student_a.retained is True
    assert student_a.reason == "low_margin"
    assert student_b.retained is True
    assert student_b.reason == "low_margin"


def test_remaining_normal_sampling_is_deterministic_and_weighted() -> None:
    policy = _policy()
    decisions = [policy.decide(make_event(row_id=row_id)) for row_id in range(100)]
    sampled = next(decision for decision in decisions if decision.retained)
    dropped = next(decision for decision in decisions if not decision.retained)

    assert decisions == [policy.decide(make_event(row_id=row_id)) for row_id in range(100)]
    assert sampled.reason == "normal_sample"
    assert dropped.reason == "normal_not_sampled"
    assert sampled.inclusion_probability == 0.1
    assert sampled.inverse_probability_weight == 10.0
    assert dropped.inclusion_probability == 0.1
    assert dropped.inverse_probability_weight == 10.0


def test_missing_model_margin_threshold_fails_closed() -> None:
    with pytest.raises(ValueError, match="margin threshold"):
        _policy().decide(make_event(student_model="student_C"))


@pytest.mark.parametrize(
    "event",
    [
        make_event(student_model="student_C", predicted_class_id=0),
        make_event(student_model="student_C", status="FAILED"),
        make_event(student_model="student_C", attempts=2),
    ],
)
def test_missing_model_threshold_fails_before_mandatory_retention(
    event: TelemetryEvent,
) -> None:
    with pytest.raises(ValueError, match="margin threshold"):
        _policy().decide(event)


def test_wrong_logit_scale_fails_before_retention() -> None:
    event = make_event(logit_frac_bits=8, predicted_class_id=0)

    with pytest.raises(ValueError, match="logit_frac_bits"):
        _policy().decide(event)


@pytest.mark.parametrize("logit_frac_bits", [9.0, True])
def test_non_integer_logit_scale_fails_before_retention(logit_frac_bits: object) -> None:
    event = make_event(  # type: ignore[arg-type]
        logit_frac_bits=logit_frac_bits,
        predicted_class_id=0,
    )

    with pytest.raises(ValueError, match="logit_frac_bits"):
        _policy().decide(event)


def test_repository_policy_config_loads_exact_model_thresholds() -> None:
    policy = load_security_aware_policy("configs/retention-policies.toml")

    below_a = policy.decide(make_event(student_model="student_A", margin_units=3_240))
    at_a = policy.decide(make_event(student_model="student_A", margin_units=3_241))
    below_b = policy.decide(make_event(student_model="student_B", margin_units=2_918))
    at_b = policy.decide(make_event(student_model="student_B", margin_units=2_919))

    assert below_a.reason == "low_margin"
    assert at_a.reason != "low_margin"
    assert below_b.reason == "low_margin"
    assert at_b.reason != "low_margin"


def test_policy_config_rejects_threshold_provenance_mismatch(tmp_path: Path) -> None:
    mismatched_config = _modified_repository_config(
        tmp_path,
        "d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff",
        "0" * 64,
    )

    with pytest.raises(ValueError, match="threshold_snapshot_id mismatch"):
        load_security_aware_policy(mismatched_config)


def test_policy_config_rejects_changed_threshold_quantile_method(tmp_path: Path) -> None:
    changed_config = _modified_repository_config(
        tmp_path,
        "nearest-rank-p10-predicted-normal-per-model",
        "linear-p10-predicted-normal-per-model",
    )

    with pytest.raises(ValueError, match="threshold_quantile_method mismatch"):
        load_security_aware_policy(changed_config)


def test_policy_config_rejects_changed_threshold(tmp_path: Path) -> None:
    changed_config = _modified_repository_config(
        tmp_path,
        'student_A = "6.330078125"',
        'student_A = "6.0"',
    )

    with pytest.raises(ValueError, match="margin thresholds"):
        load_security_aware_policy(changed_config)


def test_policy_config_rejects_changed_sample_fraction(tmp_path: Path) -> None:
    changed_config = _modified_repository_config(
        tmp_path,
        "normal_sample_numerator = 1",
        "normal_sample_numerator = 2",
    )

    with pytest.raises(ValueError, match="normal sample fraction"):
        load_security_aware_policy(changed_config)


def test_policy_config_rejects_changed_sample_denominator(tmp_path: Path) -> None:
    changed_config = _modified_repository_config(
        tmp_path,
        "normal_sample_denominator = 10",
        "normal_sample_denominator = 20",
    )

    with pytest.raises(ValueError, match="normal sample fraction"):
        load_security_aware_policy(changed_config)


@pytest.mark.parametrize(
    "mapping",
    [
        "student_A = 8\nstudent_B = 9",
        "student_A = 9",
        "student_A = 9\nstudent_B = 9\nstudent_C = 9",
    ],
)
def test_policy_config_rejects_changed_logit_scale_mapping(
    tmp_path: Path,
    mapping: str,
) -> None:
    changed_config = _repository_config_with_logit_mapping(tmp_path, mapping)

    with pytest.raises(ValueError, match="logit_frac_bits"):
        load_security_aware_policy(changed_config)


@pytest.mark.parametrize(
    "mapping",
    [
        "student_A = 9.0\nstudent_B = 9",
        "student_A = true\nstudent_B = 9",
    ],
)
def test_policy_config_rejects_non_integer_logit_scales(
    tmp_path: Path,
    mapping: str,
) -> None:
    changed_config = _repository_config_with_logit_mapping(tmp_path, mapping)

    with pytest.raises(ValueError, match="logit_frac_bits"):
        load_security_aware_policy(changed_config)


@pytest.mark.parametrize("version", ["true", "1.0"])
def test_policy_config_rejects_non_integer_version(tmp_path: Path, version: str) -> None:
    changed_config = _modified_repository_config(tmp_path, "version = 1", f"version = {version}")

    with pytest.raises(ValueError, match="version"):
        load_security_aware_policy(changed_config)


@pytest.mark.parametrize(
    "thresholds",
    [
        {"student_A": Decimal("6.330078125")},
        {
            "student_A": Decimal("6.330078125"),
            "student_B": Decimal("5.701171875"),
            "student_C": Decimal("1.0"),
        },
    ],
)
def test_policy_rejects_changed_model_key_set(thresholds: dict[str, Decimal]) -> None:
    with pytest.raises(ValueError, match="margin thresholds"):
        SecurityAwareStaticPolicy(
            margin_thresholds=thresholds,
            normal_sample_numerator=1,
            normal_sample_denominator=10,
        )
