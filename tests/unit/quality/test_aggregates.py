import pytest

from tinyml_ids_telemetry.quality.aggregates import IntCountSum


def test_integer_count_sum_merges_exactly_and_associatively() -> None:
    first = IntCountSum().add(10).add(20)
    second = IntCountSum().add(30)
    third = IntCountSum().add(40).add(50)

    assert first.merge(second) == IntCountSum(count=3, total=60)
    assert first.merge(second).merge(third) == first.merge(second.merge(third))
    assert first.merge(IntCountSum()) == first


@pytest.mark.parametrize("value", [True, 1.5, "1"])
def test_integer_count_sum_rejects_non_integer_values(value: object) -> None:
    with pytest.raises(TypeError, match="integer"):
        IntCountSum().add(value)  # type: ignore[arg-type]
