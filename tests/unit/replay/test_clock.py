from datetime import UTC, datetime, timedelta, timezone

import pytest

from tinyml_ids_telemetry.replay.clock import SyntheticClock


def test_synthetic_time_uses_original_zero_based_source_offset() -> None:
    clock = SyntheticClock(
        start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        interval=timedelta(milliseconds=250),
    )

    assert clock.at_offset(0) == datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    assert clock.at_offset(7) == datetime(2026, 8, 23, 12, 0, 1, 750000, tzinfo=UTC)


def test_synthetic_time_is_normalized_to_utc() -> None:
    india = timezone(timedelta(hours=5, minutes=30))
    clock = SyntheticClock(
        start_utc=datetime(2026, 8, 23, 17, 30, tzinfo=india),
        interval=timedelta(seconds=1),
    )

    assert clock.at_offset(2) == datetime(2026, 8, 23, 12, 0, 2, tzinfo=UTC)


@pytest.mark.parametrize(
    ("start", "interval", "message"),
    [
        (datetime(2026, 8, 23, 12, 0), timedelta(seconds=1), "timezone-aware"),
        (datetime(2026, 8, 23, 12, 0, tzinfo=UTC), timedelta(0), "positive"),
        (datetime(2026, 8, 23, 12, 0, tzinfo=UTC), timedelta(seconds=-1), "positive"),
    ],
)
def test_clock_rejects_invalid_configuration(
    start: datetime,
    interval: timedelta,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        SyntheticClock(start_utc=start, interval=interval)


def test_clock_rejects_negative_source_offset() -> None:
    clock = SyntheticClock(
        start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        interval=timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="source_offset"):
        clock.at_offset(-1)


@pytest.mark.parametrize("source_offset", [True, 1.0, "1"])
def test_clock_rejects_non_exact_integer_source_offset(source_offset: object) -> None:
    clock = SyntheticClock(
        start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        interval=timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="source_offset must be a non-negative integer"):
        clock.at_offset(source_offset)  # type: ignore[arg-type]
