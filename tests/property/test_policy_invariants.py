from __future__ import annotations

from dataclasses import replace

from hypothesis import given
from hypothesis import strategies as st

from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.policies.retain_all import RetainAllPolicy
from tinyml_ids_telemetry.policies.security_aware import load_security_aware_policy


@given(
    row_id=st.integers(min_value=0, max_value=1_000_000),
    student_model=st.sampled_from(("student_A", "student_B")),
    predicted_class_id=st.integers(min_value=0, max_value=4),
    true_class_id=st.integers(min_value=0, max_value=4),
    replacement_true_class_id=st.integers(min_value=0, max_value=4),
    margin_units=st.integers(min_value=1, max_value=20_000),
    status=st.sampled_from(("OK", "FAILED")),
    attempts=st.integers(min_value=1, max_value=3),
    response_timeout_count=st.integers(min_value=0, max_value=2),
    ignored_datagram_count=st.integers(min_value=0, max_value=2),
)
def test_online_policy_does_not_depend_on_true_class(
    row_id: int,
    student_model: str,
    predicted_class_id: int,
    true_class_id: int,
    replacement_true_class_id: int,
    margin_units: int,
    status: str,
    attempts: int,
    response_timeout_count: int,
    ignored_datagram_count: int,
) -> None:
    policy = load_security_aware_policy("configs/retention-policies.toml")
    event = make_event(
        row_id=row_id,
        student_model=student_model,
        predicted_class_id=predicted_class_id,
        true_class_id=true_class_id,
        margin_units=margin_units,
        status=status,
        attempts=attempts,
        response_timeout_count=response_timeout_count,
        ignored_datagram_count=ignored_datagram_count,
    )

    assert policy.decide(event) == policy.decide(
        replace(event, true_class_id=replacement_true_class_id)
    )


@given(
    row_id=st.integers(min_value=0, max_value=1_000_000),
    predicted_class_id=st.integers(min_value=0, max_value=4),
    true_class_id=st.integers(min_value=0, max_value=4),
)
def test_retain_all_never_drops_a_valid_event(
    row_id: int,
    predicted_class_id: int,
    true_class_id: int,
) -> None:
    event = make_event(
        row_id=row_id,
        predicted_class_id=predicted_class_id,
        true_class_id=true_class_id,
    )

    assert RetainAllPolicy().decide(event).retained is True
