"""Lazy deterministic replay scheduling and delivery envelopes."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from types import MappingProxyType
from typing import ClassVar, Self, cast

from tinyml_ids_telemetry.contracts.schema import validate_event_mapping
from tinyml_ids_telemetry.replay.clock import SyntheticClock


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _freeze_json(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _validated_source_event(
    source_event: object,
) -> tuple[dict[str, object], str, str]:
    event = validate_event_mapping(source_event)
    return event, cast(str, event["source_event_id"]), cast(str, event["lineage_id"])


def _delivery_id(
    replay_run_id: str,
    source_event_id: str,
    source_offset: int,
    duplicate_ordinal: int,
) -> str:
    identity = _canonical_json([replay_run_id, source_event_id, source_offset, duplicate_ordinal])
    return sha256(identity.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True, init=False)
class DeliveryEnvelope:
    """One immutable source occurrence plus deterministic replay metadata."""

    SCHEMA_VERSION: ClassVar[str] = "telemetry-replay-envelope-v1"

    replay_run_id: str
    delivery_id: str
    source_offset: int
    emission_index: int
    duplicate_ordinal: int
    synthetic_event_time: str
    _event: Mapping[str, object] = field(repr=False)

    def __init__(
        self,
        *,
        replay_run_id: str,
        delivery_id: str,
        source_offset: int,
        emission_index: int,
        duplicate_ordinal: int,
        synthetic_event_time: str,
        event: Mapping[str, object],
    ) -> None:
        validated_event = validate_event_mapping(event)
        self._initialize(
            replay_run_id=replay_run_id,
            delivery_id=delivery_id,
            source_offset=source_offset,
            emission_index=emission_index,
            duplicate_ordinal=duplicate_ordinal,
            synthetic_event_time=synthetic_event_time,
            event=validated_event,
        )

    @classmethod
    def _from_validated_event(
        cls,
        *,
        replay_run_id: str,
        delivery_id: str,
        source_offset: int,
        emission_index: int,
        duplicate_ordinal: int,
        synthetic_event_time: str,
        event: dict[str, object],
    ) -> Self:
        envelope = cls.__new__(cls)
        envelope._initialize(
            replay_run_id=replay_run_id,
            delivery_id=delivery_id,
            source_offset=source_offset,
            emission_index=emission_index,
            duplicate_ordinal=duplicate_ordinal,
            synthetic_event_time=synthetic_event_time,
            event=event,
        )
        return envelope

    def _initialize(
        self,
        *,
        replay_run_id: object,
        delivery_id: object,
        source_offset: object,
        emission_index: object,
        duplicate_ordinal: object,
        synthetic_event_time: object,
        event: dict[str, object],
    ) -> None:
        if not isinstance(replay_run_id, str) or not replay_run_id.strip():
            raise ValueError("replay_run_id must be a non-empty string")
        validated_source_offset = self._non_negative_exact_int("source_offset", source_offset)
        validated_emission_index = self._non_negative_exact_int("emission_index", emission_index)
        validated_duplicate_ordinal = self._non_negative_exact_int(
            "duplicate_ordinal", duplicate_ordinal
        )
        if not isinstance(synthetic_event_time, str):
            raise ValueError("synthetic_event_time must be a canonical UTC timestamp")
        try:
            parsed_event_time = datetime.fromisoformat(
                synthetic_event_time.removesuffix("Z") + "+00:00"
            )
        except ValueError as error:
            raise ValueError("synthetic_event_time must be a canonical UTC timestamp") from error
        if self.format_event_time(parsed_event_time) != synthetic_event_time:
            raise ValueError("synthetic_event_time must be a canonical UTC timestamp")
        expected_delivery_id = _delivery_id(
            replay_run_id,
            cast(str, event["source_event_id"]),
            validated_source_offset,
            validated_duplicate_ordinal,
        )
        if delivery_id != expected_delivery_id:
            raise ValueError("delivery_id disagrees with deterministic delivery identity")
        object.__setattr__(self, "replay_run_id", replay_run_id)
        object.__setattr__(self, "delivery_id", expected_delivery_id)
        object.__setattr__(self, "source_offset", validated_source_offset)
        object.__setattr__(self, "emission_index", validated_emission_index)
        object.__setattr__(self, "duplicate_ordinal", validated_duplicate_ordinal)
        object.__setattr__(self, "synthetic_event_time", synthetic_event_time)
        object.__setattr__(self, "_event", cast(Mapping[str, object], _freeze_json(event)))

    @staticmethod
    def _non_negative_exact_int(name: str, value: object) -> int:
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
        return value

    @property
    def event(self) -> Mapping[str, object]:
        """Return the deeply immutable original JSON mapping."""
        return self._event

    @property
    def source_event_id(self) -> str:
        """Return unchanged canonical source identity."""
        return cast(str, self._event["source_event_id"])

    @property
    def lineage_id(self) -> str:
        """Return unchanged canonical lineage identity."""
        return cast(str, self._event["lineage_id"])

    @staticmethod
    def format_event_time(value: datetime) -> str:
        """Format an aware time as an exact compact UTC timestamp."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("synthetic event time must be timezone-aware")
        formatted = value.astimezone(UTC).isoformat(timespec="microseconds")
        return formatted.replace(".000000+00:00", "Z").replace("+00:00", "Z")

    def to_dict(self) -> dict[str, object]:
        """Return the nested delivery contract as JSON-compatible data."""
        return {
            "schema_version": self.SCHEMA_VERSION,
            "replay_run_id": self.replay_run_id,
            "delivery_id": self.delivery_id,
            "source_offset": self.source_offset,
            "emission_index": self.emission_index,
            "duplicate_ordinal": self.duplicate_ordinal,
            "synthetic_event_time": self.synthetic_event_time,
            "event": _thaw_json(self._event),
        }

    def to_json_bytes(self) -> bytes:
        """Serialize the envelope as compact, sorted canonical UTF-8 JSON."""
        return _canonical_json(self.to_dict()).encode("ascii")


@dataclass(frozen=True, slots=True)
class _Occurrence:
    event: dict[str, object]
    source_event_id: str
    source_offset: int
    duplicate_ordinal: int
    synthetic_event_time: str
    delivery_id: str


def _iter_occurrences(
    source_events: Iterable[Mapping[str, object]],
    *,
    replay_run_id: str,
    clock: SyntheticClock,
    duplicate_every: int | None,
) -> Iterator[_Occurrence]:
    for source_offset, source_event in enumerate(source_events):
        event, source_event_id, _ = _validated_source_event(source_event)
        event_time = DeliveryEnvelope.format_event_time(clock.at_offset(source_offset))
        duplicate_ordinals = (
            (0, 1)
            if (duplicate_every is not None and (source_offset + 1) % duplicate_every == 0)
            else (0,)
        )
        for duplicate_ordinal in duplicate_ordinals:
            yield _Occurrence(
                event=event,
                source_event_id=source_event_id,
                source_offset=source_offset,
                duplicate_ordinal=duplicate_ordinal,
                synthetic_event_time=event_time,
                delivery_id=_delivery_id(
                    replay_run_id,
                    source_event_id,
                    source_offset,
                    duplicate_ordinal,
                ),
            )


def _iter_reordered(
    occurrences: Iterable[_Occurrence],
    window: int,
) -> Iterator[_Occurrence]:
    if window == 1:
        yield from occurrences
        return
    buffer: list[_Occurrence] = []
    for occurrence in occurrences:
        buffer.append(occurrence)
        if len(buffer) == window:
            yield from reversed(buffer)
            buffer.clear()
    yield from reversed(buffer)


def _iter_delivery_envelopes(
    occurrences: Iterable[_Occurrence],
    *,
    replay_run_id: str,
    out_of_order_window: int,
) -> Iterator[DeliveryEnvelope]:
    for emission_index, occurrence in enumerate(_iter_reordered(occurrences, out_of_order_window)):
        yield DeliveryEnvelope._from_validated_event(
            replay_run_id=replay_run_id,
            delivery_id=occurrence.delivery_id,
            source_offset=occurrence.source_offset,
            emission_index=emission_index,
            duplicate_ordinal=occurrence.duplicate_ordinal,
            synthetic_event_time=occurrence.synthetic_event_time,
            event=occurrence.event,
        )


def iter_replay_schedule(
    source_events: Iterable[Mapping[str, object]],
    *,
    replay_run_id: str,
    clock: SyntheticClock,
    resume_offset: int = 0,
    duplicate_every: int | None = None,
    out_of_order_window: int = 1,
) -> Iterator[DeliveryEnvelope]:
    """Yield a lazy deterministic at-least-once delivery schedule.

    ``duplicate_every=k`` emits one ordinal-1 duplicate after ordinal 0 for every
    source whose one-based position is divisible by ``k``. Duplication happens
    before deterministic reverse-window disorder. Resume filtering happens only
    after the full transformed schedule receives global emission indexes.
    """
    if not isinstance(replay_run_id, str) or not replay_run_id.strip():
        raise ValueError("replay_run_id must be non-empty")
    if type(resume_offset) is not int or resume_offset < 0:
        raise ValueError("resume_offset must be a non-negative integer")
    if duplicate_every is not None and (type(duplicate_every) is not int or duplicate_every <= 0):
        raise ValueError("duplicate_every must be a positive integer")
    if type(out_of_order_window) is not int or out_of_order_window <= 0:
        raise ValueError("out_of_order_window must be a positive integer")

    occurrences = _iter_occurrences(
        source_events,
        replay_run_id=replay_run_id,
        clock=clock,
        duplicate_every=duplicate_every,
    )
    envelopes = _iter_delivery_envelopes(
        occurrences,
        replay_run_id=replay_run_id,
        out_of_order_window=out_of_order_window,
    )
    return (envelope for envelope in envelopes if envelope.source_offset >= resume_offset)
