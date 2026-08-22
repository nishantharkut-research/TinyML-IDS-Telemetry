from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.policies.retain_all import RetainAllPolicy


def test_retain_all_keeps_every_valid_event() -> None:
    policy = RetainAllPolicy()

    decision = policy.decide(make_event())

    assert decision.policy_id == "retain-all-v1"
    assert decision.retained is True
    assert decision.reason == "retain_all"
    assert decision.inclusion_probability == 1.0
    assert decision.inverse_probability_weight == 1.0
