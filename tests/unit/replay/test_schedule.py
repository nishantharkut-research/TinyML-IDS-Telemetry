from __future__ import annotations

import json
from collections.abc import Iterator
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.replay.clock import SyntheticClock
from tinyml_ids_telemetry.replay.schedule import DeliveryEnvelope, iter_replay_schedule

START = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
RUN_ID = "fixture-replay-run"


def _clock() -> SyntheticClock:
    return SyntheticClock(start_utc=START, interval=timedelta(milliseconds=250))


def _events(count: int) -> list[dict[str, object]]:
    return [make_event(row_id=offset).to_dict() for offset in range(count)]


def _direct_envelope(
    *,
    event: dict[str, object] | None = None,
    **changes: object,
) -> DeliveryEnvelope:
    metadata_source = _events(1)[0]
    source = metadata_source if event is None else event
    scheduled = next(iter_replay_schedule([metadata_source], replay_run_id=RUN_ID, clock=_clock()))
    values: dict[str, object] = {
        "replay_run_id": scheduled.replay_run_id,
        "delivery_id": scheduled.delivery_id,
        "source_offset": scheduled.source_offset,
        "emission_index": scheduled.emission_index,
        "duplicate_ordinal": scheduled.duplicate_ordinal,
        "synthetic_event_time": scheduled.synthetic_event_time,
        "event": source,
    }
    values.update(changes)
    return DeliveryEnvelope(**values)  # type: ignore[arg-type]


def test_replay_wraps_source_event_without_mutating_or_aliasing_it() -> None:
    source = _events(1)[0]
    original = deepcopy(source)

    envelope = next(iter_replay_schedule([source], replay_run_id=RUN_ID, clock=_clock()))

    assert source == original
    assert envelope.to_dict()["event"] == original
    assert envelope.source_event_id == source["source_event_id"]
    assert "replay_run_id" not in source
    source["status"] = "changed-after-scheduling"
    assert envelope.event["status"] == "OK"
    with pytest.raises(TypeError):
        envelope.event["status"] = "cannot-mutate-envelope"  # type: ignore[index]


def test_delivery_envelope_exposes_versioned_metadata_and_canonical_json() -> None:
    envelope = next(iter_replay_schedule(_events(1), replay_run_id=RUN_ID, clock=_clock()))

    payload = envelope.to_dict()

    assert payload == {
        "schema_version": "telemetry-replay-envelope-v1",
        "replay_run_id": RUN_ID,
        "delivery_id": envelope.delivery_id,
        "source_offset": 0,
        "emission_index": 0,
        "duplicate_ordinal": 0,
        "synthetic_event_time": "2026-08-23T12:00:00Z",
        "event": envelope.to_dict()["event"],
    }
    assert envelope.to_json_bytes() == json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def test_public_envelope_constructor_validates_complete_source_event() -> None:
    event = _events(1)[0]
    del event["total_us"]

    with pytest.raises(ValueError, match="total_us"):
        _direct_envelope(event=event)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"replay_run_id": "   "}, "replay_run_id"),
        ({"source_offset": True}, "source_offset"),
        ({"source_offset": 0.5}, "source_offset"),
        ({"source_offset": -1}, "source_offset"),
        ({"emission_index": False}, "emission_index"),
        ({"emission_index": 1.0}, "emission_index"),
        ({"duplicate_ordinal": True}, "duplicate_ordinal"),
        ({"duplicate_ordinal": -1}, "duplicate_ordinal"),
        ({"synthetic_event_time": "2026-08-23T12:00:00+00:00"}, "synthetic_event_time"),
        ({"synthetic_event_time": "2026-08-23T17:30:00+05:30"}, "synthetic_event_time"),
        ({"delivery_id": "0" * 64}, "delivery_id"),
    ],
)
def test_public_envelope_constructor_rejects_invalid_delivery_metadata(
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _direct_envelope(**changes)


def test_envelope_event_is_deeply_immutable_and_detached_from_caller() -> None:
    source = _events(1)[0]
    envelope = _direct_envelope(event=source)
    original_json = envelope.to_json_bytes()

    source_logits = source["fixed_logits"]
    assert isinstance(source_logits, list)
    source_logits[0] = 999_999
    envelope_logits = envelope.event["fixed_logits"]
    assert isinstance(envelope_logits, tuple)
    with pytest.raises(TypeError):
        envelope_logits[0] = 999_999  # type: ignore[index]

    assert envelope.to_json_bytes() == original_json


def test_resume_uses_canonical_offset_and_preserves_uninterrupted_time_and_identity() -> None:
    events = _events(5)
    uninterrupted = list(iter_replay_schedule(events, replay_run_id=RUN_ID, clock=_clock()))
    resumed = list(
        iter_replay_schedule(
            events,
            replay_run_id=RUN_ID,
            clock=_clock(),
            resume_offset=3,
        )
    )

    assert [item.source_offset for item in resumed] == [3, 4]
    assert resumed[0].synthetic_event_time == uninterrupted[3].synthetic_event_time
    assert resumed[0].delivery_id == uninterrupted[3].delivery_id
    assert resumed[0].source_event_id == uninterrupted[3].source_event_id
    assert [item.emission_index for item in resumed] == [3, 4]
    assert [item.to_json_bytes() for item in resumed] == [
        item.to_json_bytes() for item in uninterrupted[3:]
    ]


def test_duplicate_every_uses_one_based_positions_before_disorder() -> None:
    """Every kth source emits ordinal 0 then exactly one ordinal 1 duplicate."""
    deliveries = list(
        iter_replay_schedule(
            _events(5),
            replay_run_id=RUN_ID,
            clock=_clock(),
            duplicate_every=2,
        )
    )

    assert [item.source_offset for item in deliveries] == [0, 1, 1, 2, 3, 3, 4]
    assert [item.duplicate_ordinal for item in deliveries] == [0, 0, 1, 0, 0, 1, 0]
    assert [item.emission_index for item in deliveries] == list(range(7))
    first, duplicate = deliveries[1:3]
    assert first.event == duplicate.event
    assert first.source_event_id == duplicate.source_event_id
    assert first.synthetic_event_time == duplicate.synthetic_event_time
    assert first.delivery_id != duplicate.delivery_id

    repeated = list(
        iter_replay_schedule(
            _events(5),
            replay_run_id=RUN_ID,
            clock=_clock(),
            duplicate_every=2,
        )
    )
    assert [item.delivery_id for item in deliveries] == [item.delivery_id for item in repeated]


def test_resume_preserves_global_duplicate_enumeration() -> None:
    deliveries = list(
        iter_replay_schedule(
            _events(5),
            replay_run_id=RUN_ID,
            clock=_clock(),
            resume_offset=3,
            duplicate_every=2,
        )
    )

    assert [(item.source_offset, item.duplicate_ordinal) for item in deliveries] == [
        (3, 0),
        (3, 1),
        (4, 0),
    ]
    assert [item.emission_index for item in deliveries] == [4, 5, 6]
    assert deliveries[0].synthetic_event_time == "2026-08-23T12:00:00.750000Z"


def test_out_of_order_mode_reverses_each_bounded_window_without_retiming() -> None:
    deliveries = list(
        iter_replay_schedule(
            _events(7),
            replay_run_id=RUN_ID,
            clock=_clock(),
            out_of_order_window=3,
        )
    )

    assert [item.source_offset for item in deliveries] == [2, 1, 0, 5, 4, 3, 6]
    assert [item.emission_index for item in deliveries] == list(range(7))
    for item in deliveries:
        assert item.synthetic_event_time == DeliveryEnvelope.format_event_time(
            _clock().at_offset(item.source_offset)
        )
        assert item.event["row_id"] == item.source_offset


def test_resume_is_byte_identical_filter_of_global_duplicate_and_disorder_schedule() -> None:
    events = _events(6)
    uninterrupted = list(
        iter_replay_schedule(
            events,
            replay_run_id=RUN_ID,
            clock=_clock(),
            duplicate_every=2,
            out_of_order_window=3,
        )
    )
    resumed = list(
        iter_replay_schedule(
            events,
            replay_run_id=RUN_ID,
            clock=_clock(),
            resume_offset=1,
            duplicate_every=2,
            out_of_order_window=3,
        )
    )
    expected = [item for item in uninterrupted if item.source_offset >= 1]

    assert [(item.source_offset, item.duplicate_ordinal) for item in resumed] == [
        (1, 1),
        (1, 0),
        (3, 1),
        (3, 0),
        (2, 0),
        (5, 1),
        (5, 0),
        (4, 0),
    ]
    assert [item.to_json_bytes() for item in resumed] == [item.to_json_bytes() for item in expected]


def test_normal_and_out_of_order_schedules_are_lazy_and_bounded() -> None:
    normal_consumed = 0
    disorder_consumed = 0

    def normal_source() -> Iterator[dict[str, object]]:
        nonlocal normal_consumed
        for event in _events(10):
            normal_consumed += 1
            yield event

    def disorder_source() -> Iterator[dict[str, object]]:
        nonlocal disorder_consumed
        for event in _events(10):
            disorder_consumed += 1
            yield event

    normal = iter_replay_schedule(normal_source(), replay_run_id=RUN_ID, clock=_clock())
    disorder = iter_replay_schedule(
        disorder_source(),
        replay_run_id=RUN_ID,
        clock=_clock(),
        out_of_order_window=4,
    )

    assert normal_consumed == 0
    assert next(normal).source_offset == 0
    assert normal_consumed == 1
    assert disorder_consumed == 0
    assert next(disorder).source_offset == 3
    assert disorder_consumed == 4


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"schema_version": "wrong"}, "schema_version"),
        ({"source_event_id": "bad"}, "source_event_id"),
        ({"lineage_id": None}, "lineage_id"),
        ({"fixed_logits": [float("nan")]}, "JSON-compatible"),
    ],
)
def test_malformed_source_mappings_fail_closed(
    changes: dict[str, object],
    message: str,
) -> None:
    event = _events(1)[0]
    event.update(changes)

    with pytest.raises(ValueError, match=message):
        list(iter_replay_schedule([event], replay_run_id=RUN_ID, clock=_clock()))


def test_non_mapping_source_event_fails_closed() -> None:
    with pytest.raises(ValueError, match="mapping"):
        list(
            iter_replay_schedule(  # type: ignore[arg-type]
                [["not", "a", "mapping"]],
                replay_run_id=RUN_ID,
                clock=_clock(),
            )
        )


def test_malformed_source_mapping_before_resume_offset_still_fails_closed() -> None:
    malformed, valid = _events(2)
    del malformed["source_event_id"]

    with pytest.raises(ValueError, match="source_event_id"):
        list(
            iter_replay_schedule(
                [malformed, valid],
                replay_run_id=RUN_ID,
                clock=_clock(),
                resume_offset=1,
            )
        )


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"resume_offset": -1}, "resume_offset"),
        ({"duplicate_every": 0}, "duplicate_every"),
        ({"out_of_order_window": 0}, "out_of_order_window"),
    ],
)
def test_schedule_rejects_invalid_options(
    options: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        list(
            iter_replay_schedule(
                _events(1),
                replay_run_id=RUN_ID,
                clock=_clock(),
                **options,
            )
        )


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"resume_offset": True}, "resume_offset"),
        ({"resume_offset": 1.0}, "resume_offset"),
        ({"duplicate_every": False}, "duplicate_every"),
        ({"duplicate_every": 2.0}, "duplicate_every"),
        ({"out_of_order_window": True}, "out_of_order_window"),
        ({"out_of_order_window": 1.5}, "out_of_order_window"),
    ],
)
def test_schedule_rejects_non_exact_integer_options_before_consuming_source(
    options: dict[str, object],
    message: str,
) -> None:
    consumed = False

    def source() -> Iterator[dict[str, object]]:
        nonlocal consumed
        consumed = True
        yield from _events(1)

    with pytest.raises(ValueError, match=message):
        iter_replay_schedule(
            source(),
            replay_run_id=RUN_ID,
            clock=_clock(),
            **options,  # type: ignore[arg-type]
        )

    assert consumed is False
