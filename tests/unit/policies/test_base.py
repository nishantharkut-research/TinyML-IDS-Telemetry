from __future__ import annotations

import pytest

from tinyml_ids_telemetry.policies.base import RetentionDecision


def _decision(
    *,
    retained: bool = True,
    inclusion_probability: float = 1.0,
    inverse_probability_weight: float = 1.0,
) -> RetentionDecision:
    return RetentionDecision(
        policy_id="fixture-policy-v1",
        retained=retained,
        reason="fixture",
        inclusion_probability=inclusion_probability,
        inverse_probability_weight=inverse_probability_weight,
    )


def test_retained_must_be_a_bool() -> None:
    with pytest.raises(TypeError, match="retained"):
        _decision(retained=2)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("field", ["inclusion_probability", "inverse_probability_weight"])
def test_numeric_fields_must_be_finite(field: str, value: float) -> None:
    arguments = {field: value}

    with pytest.raises(ValueError, match=field):
        _decision(**arguments)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [True, "1"])
@pytest.mark.parametrize("field", ["inclusion_probability", "inverse_probability_weight"])
def test_numeric_fields_must_be_real_numbers(field: str, value: object) -> None:
    arguments = {field: value}

    with pytest.raises(TypeError, match=field):
        _decision(**arguments)  # type: ignore[arg-type]


def test_inverse_probability_weight_must_be_reciprocal() -> None:
    with pytest.raises(ValueError, match="reciprocal"):
        _decision(inclusion_probability=0.25, inverse_probability_weight=10.0)


def test_inverse_probability_weight_rejects_near_reciprocal() -> None:
    with pytest.raises(ValueError, match="reciprocal"):
        _decision(inclusion_probability=0.1, inverse_probability_weight=10.000000009)


@pytest.mark.parametrize(
    ("probability", "weight"),
    [(1.0, 1.0), (0.25, 4.0), (0.1, 10.0)],
)
def test_valid_retention_decisions_are_accepted(probability: float, weight: float) -> None:
    decision = _decision(
        inclusion_probability=probability,
        inverse_probability_weight=weight,
    )

    assert decision.inclusion_probability == probability
    assert decision.inverse_probability_weight == weight
