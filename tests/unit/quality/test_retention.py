import pytest

from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.policies.base import RetentionDecision
from tinyml_ids_telemetry.quality.retention import evaluate_retention


def _decision(
    retained: bool,
    *,
    policy_id: str = "fixture-policy-v1",
) -> RetentionDecision:
    return RetentionDecision(
        policy_id=policy_id,
        retained=retained,
        reason="fixture",
        inclusion_probability=1.0,
        inverse_probability_weight=1.0,
    )


def test_offline_evaluation_separates_predicted_and_true_attack_retention() -> None:
    normal = make_event(row_id=0, true_class_id=3, predicted_class_id=3)
    detected_attack = make_event(row_id=1, true_class_id=0, predicted_class_id=0)
    missed_attack = make_event(row_id=2, true_class_id=0, predicted_class_id=3)

    result = evaluate_retention(
        [
            (normal, _decision(False)),
            (detected_attack, _decision(True)),
            (missed_attack, _decision(False)),
        ]
    )

    assert result.policy_id == "fixture-policy-v1"
    assert result.total_events == 3
    assert result.retained_events == 1
    assert result.predicted_attack_events == 1
    assert result.retained_predicted_attack_events == 1
    assert result.true_attack_events == 2
    assert result.retained_true_attack_events == 1
    assert result.missed_attack_events == 1
    assert result.retained_missed_attack_events == 0


def test_offline_evaluation_rejects_duplicate_source_events() -> None:
    event = make_event()

    try:
        evaluate_retention([(event, _decision(True)), (event, _decision(True))])
    except ValueError as error:
        assert "duplicate source_event_id" in str(error)
    else:
        raise AssertionError("duplicate source event was accepted")


def test_offline_evaluation_rejects_mixed_policy_ids() -> None:
    first = make_event(row_id=0)
    second = make_event(row_id=1)

    with pytest.raises(ValueError, match="mixed policy IDs"):
        evaluate_retention(
            [
                (first, _decision(True)),
                (second, _decision(True, policy_id="other-policy-v1")),
            ]
        )


def test_offline_evaluation_distinguishes_same_row_across_boards() -> None:
    first = make_event(board="board-A")
    second = make_event(board="board-B")

    result = evaluate_retention([(first, _decision(True)), (second, _decision(True))])

    assert first.lineage_id != second.lineage_id
    assert first.source_event_id != second.source_event_id
    assert result.total_events == 2
    assert result.retained_events == 2
