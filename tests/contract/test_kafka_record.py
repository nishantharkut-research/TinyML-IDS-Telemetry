from __future__ import annotations

import json
import tomllib
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.replay.clock import SyntheticClock
from tinyml_ids_telemetry.replay.kafka import (
    KafkaDeliveryError,
    KafkaSink,
    kafka_record,
)
from tinyml_ids_telemetry.replay.schedule import DeliveryEnvelope, iter_replay_schedule
from tinyml_ids_telemetry.replay.sink import SinkClosedError

PROJECT_ROOT = Path(__file__).parents[2]


def _envelope() -> DeliveryEnvelope:
    clock = SyntheticClock(
        start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        interval=timedelta(seconds=1),
    )
    return next(
        iter_replay_schedule(
            [make_event().to_dict()],
            replay_run_id="kafka-contract-run",
            clock=clock,
        )
    )


def _envelopes(count: int) -> list[DeliveryEnvelope]:
    clock = SyntheticClock(
        start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        interval=timedelta(seconds=1),
    )
    return list(
        iter_replay_schedule(
            [make_event(row_id=offset).to_dict() for offset in range(count)],
            replay_run_id="kafka-contract-run",
            clock=clock,
        )
    )


class FakeFuture:
    def __init__(self) -> None:
        self._callbacks: list[tuple[Callable[..., object], tuple[object, ...]]] = []
        self._errbacks: list[tuple[Callable[..., object], tuple[object, ...]]] = []

    def add_callback(self, callback: Callable[..., object], *args: object) -> FakeFuture:
        self._callbacks.append((callback, args))
        return self

    def add_errback(self, errback: Callable[..., object], *args: object) -> FakeFuture:
        self._errbacks.append((errback, args))
        return self

    def succeed(self) -> None:
        for callback, args in self._callbacks:
            callback(object(), *args)

    def fail(self, error: BaseException) -> None:
        for errback, args in self._errbacks:
            errback(error, *args)


class FakeProducer:
    def __init__(
        self,
        *,
        flush_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.sent: list[dict[str, object]] = []
        self.futures: list[FakeFuture] = []
        self.flush_error = flush_error
        self.close_error = close_error
        self.flush_count = 0
        self.close_count = 0

    def send(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
    ) -> FakeFuture:
        self.sent.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "headers": headers,
                "partition": partition,
                "timestamp_ms": timestamp_ms,
            }
        )
        future = FakeFuture()
        self.futures.append(future)
        return future

    def flush(self, timeout: float | None = None) -> None:
        self.flush_count += 1
        if self.flush_error is not None:
            raise self.flush_error

    def close(self, timeout: float | None = None) -> None:
        self.close_count += 1
        if self.close_error is not None:
            raise self.close_error


def test_kafka_record_uses_lineage_key_canonical_envelope_and_required_headers() -> None:
    envelope = _envelope()

    record = kafka_record(envelope)

    assert record.key == envelope.lineage_id.encode("utf-8")
    assert record.value == envelope.to_json_bytes()
    assert json.loads(record.value) == envelope.to_dict()
    assert record.headers == (
        ("envelope_schema", b"telemetry-replay-envelope-v1"),
        ("replay_run_id", b"kafka-contract-run"),
        ("source_event_id", envelope.source_event_id.encode("utf-8")),
        ("delivery_id", envelope.delivery_id.encode("utf-8")),
    )
    assert all(isinstance(value, bytes) for _, value in record.headers)


def test_kafka_sink_uses_injected_producer_and_closes_cleanly() -> None:
    envelope = _envelope()
    producer = FakeProducer()
    sink = KafkaSink(topic="telemetry", producer=producer)

    sink.write(envelope)
    producer.futures[0].succeed()
    sink.close()

    record = kafka_record(envelope)
    assert producer.sent == [
        {
            "topic": "telemetry",
            "value": record.value,
            "key": record.key,
            "headers": list(record.headers),
            "partition": None,
            "timestamp_ms": None,
        }
    ]
    assert producer.flush_count == 1
    assert producer.close_count == 1
    with pytest.raises(SinkClosedError, match="closed"):
        sink.write(envelope)


def test_kafka_sink_raises_collected_asynchronous_delivery_failures() -> None:
    producer = FakeProducer()
    sink = KafkaSink(topic="telemetry", producer=producer)
    envelope = _envelope()
    sink.write(envelope)
    producer.futures[0].fail(RuntimeError("broker rejected record"))

    with pytest.raises(KafkaDeliveryError, match="broker rejected record") as raised:
        sink.close()

    assert raised.value.delivery_ids == (envelope.delivery_id,)
    assert producer.flush_count == 1
    assert producer.close_count == 1


def test_kafka_sink_public_flush_surfaces_asynchronous_delivery_failures() -> None:
    producer = FakeProducer()
    sink = KafkaSink(topic="telemetry", producer=producer)
    envelope = _envelope()
    sink.write(envelope)
    producer.futures[0].fail(RuntimeError("broker rejected during flush"))

    with pytest.raises(KafkaDeliveryError, match="broker rejected during flush") as raised:
        sink.flush()

    assert raised.value.delivery_ids == (envelope.delivery_id,)
    assert producer.flush_count == 1


def test_kafka_sink_fails_fast_before_sending_after_async_failure() -> None:
    producer = FakeProducer()
    sink = KafkaSink(topic="telemetry", producer=producer)
    first, second = _envelopes(2)
    sink.write(first)
    producer.futures[0].fail(RuntimeError("asynchronous send failed"))

    with pytest.raises(KafkaDeliveryError) as raised:
        sink.write(second)

    assert len(producer.sent) == 1
    assert raised.value.failure_count == 1
    assert raised.value.delivery_ids == (first.delivery_id,)
    assert raised.value.omitted_count == 0
    with pytest.raises(KafkaDeliveryError):
        sink.close()


def test_kafka_sink_bounds_concurrent_async_failure_details_without_losing_count() -> None:
    from tinyml_ids_telemetry.replay.kafka import MAX_RETAINED_DELIVERY_FAILURES

    failure_count = MAX_RETAINED_DELIVERY_FAILURES + 17
    envelopes = _envelopes(failure_count)
    producer = FakeProducer()
    sink = KafkaSink(topic="telemetry", producer=producer)
    for envelope in envelopes:
        sink.write(envelope)

    def fail_delivery(index: int) -> None:
        producer.futures[index].fail(RuntimeError(f"callback failure {index}: " + "x" * 10_000))

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(fail_delivery, range(failure_count)))

    with pytest.raises(KafkaDeliveryError) as raised:
        sink.close()

    error = raised.value
    assert error.failure_count == failure_count
    assert len(error.delivery_ids) == MAX_RETAINED_DELIVERY_FAILURES
    assert set(error.delivery_ids) <= {envelope.delivery_id for envelope in envelopes}
    assert error.omitted_count == 17
    assert "17 additional failures omitted" in str(error)
    assert len(str(error)) < 12_000


def test_kafka_sink_public_flush_preserves_async_and_flush_failures() -> None:
    producer = FakeProducer(flush_error=RuntimeError("producer flush failed"))
    sink = KafkaSink(topic="telemetry", producer=producer)
    envelope = _envelope()
    sink.write(envelope)
    producer.futures[0].fail(RuntimeError("asynchronous send failed"))

    with pytest.raises(BaseExceptionGroup) as raised:
        sink.flush()

    assert [str(error) for error in raised.value.exceptions] == [
        f"Kafka delivery failures: {envelope.delivery_id}: asynchronous send failed",
        "producer flush failed",
    ]


def test_kafka_sink_closes_producer_when_flush_raises() -> None:
    producer = FakeProducer(flush_error=RuntimeError("flush failed"))
    sink = KafkaSink(topic="telemetry", producer=producer)

    with pytest.raises(RuntimeError, match="flush failed"):
        sink.close()

    assert producer.close_count == 1


def test_kafka_sink_surfaces_producer_close_failure() -> None:
    producer = FakeProducer(close_error=RuntimeError("producer close failed"))
    sink = KafkaSink(topic="telemetry", producer=producer)

    with pytest.raises(RuntimeError, match="producer close failed"):
        sink.close()

    assert producer.flush_count == 1
    assert producer.close_count == 1


def test_kafka_sink_preserves_async_flush_and_close_failures() -> None:
    producer = FakeProducer(
        flush_error=RuntimeError("producer flush failed"),
        close_error=RuntimeError("producer close failed"),
    )
    sink = KafkaSink(topic="telemetry", producer=producer)
    envelope = _envelope()
    sink.write(envelope)
    producer.futures[0].fail(RuntimeError("asynchronous send failed"))

    with pytest.raises(BaseExceptionGroup) as raised:
        sink.close()

    assert [str(error) for error in raised.value.exceptions] == [
        f"Kafka delivery failures: {envelope.delivery_id}: asynchronous send failed",
        "producer flush failed",
        "producer close failed",
    ]
    assert producer.flush_count == 1
    assert producer.close_count == 1


def test_kafka_optional_dependency_uses_current_primary_package() -> None:
    configuration: dict[str, Any] = tomllib.loads(
        (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )

    assert configuration["project"]["optional-dependencies"]["kafka"] == ["kafka-python>=3.0.10,<4"]
