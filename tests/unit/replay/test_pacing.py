from __future__ import annotations

import pytest


class _FakeTimer:
    def __init__(self, *, oversleep: float = 0.0) -> None:
        self.now = 100.0
        self.oversleep = oversleep
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, duration: float) -> None:
        self.sleeps.append(duration)
        self.now += duration + self.oversleep


@pytest.mark.parametrize(
    ("rate_per_second", "expected_sleeps"),
    [
        (2, [0.5, 0.5]),
        (20, [0.05, 0.05]),
    ],
)
def test_delivery_pacer_supports_normal_and_accelerated_absolute_deadlines(
    rate_per_second: int,
    expected_sleeps: list[float],
) -> None:
    from tinyml_ids_telemetry.replay.pacing import DeliveryPacer

    timer = _FakeTimer()
    pacer = DeliveryPacer(
        rate_per_second=rate_per_second,
        monotonic=timer.monotonic,
        sleep=timer.sleep,
    )

    for delivery_index in range(3):
        pacer.wait_for_delivery(delivery_index)

    assert timer.sleeps == pytest.approx(expected_sleeps)


def test_delivery_pacer_does_not_accumulate_sleep_drift() -> None:
    from tinyml_ids_telemetry.replay.pacing import DeliveryPacer

    timer = _FakeTimer(oversleep=0.01)
    pacer = DeliveryPacer(
        rate_per_second=2,
        monotonic=timer.monotonic,
        sleep=timer.sleep,
    )

    for delivery_index in range(4):
        pacer.wait_for_delivery(delivery_index)

    assert timer.sleeps == pytest.approx([0.5, 0.49, 0.49])


def test_delivery_pacer_rejects_invalid_rates_before_reading_time() -> None:
    from tinyml_ids_telemetry.replay.pacing import DeliveryPacer

    def monotonic() -> float:
        pytest.fail("monotonic clock was read")

    with pytest.raises(ValueError, match="positive integer"):
        DeliveryPacer(rate_per_second=0, monotonic=monotonic)
