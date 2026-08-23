"""Optional kafka-python delivery adapter for replay envelopes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Protocol, cast

from tinyml_ids_telemetry.replay.schedule import DeliveryEnvelope
from tinyml_ids_telemetry.replay.sink import SinkClosedError

MAX_RETAINED_DELIVERY_FAILURES = 32
_MAX_DELIVERY_FAILURE_MESSAGE_CHARS = 256


class ProducerFuture(Protocol):
    def add_callback(
        self,
        callback: Callable[..., object],
        *args: object,
    ) -> object: ...

    def add_errback(
        self,
        errback: Callable[..., object],
        *args: object,
    ) -> object: ...


class Producer(Protocol):
    def send(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
    ) -> ProducerFuture: ...

    def flush(self, timeout: float | None = None) -> None: ...

    def close(self, timeout: float | None = None) -> None: ...


@dataclass(frozen=True, slots=True)
class KafkaRecord:
    """Kafka wire fields for one delivery envelope."""

    key: bytes
    value: bytes
    headers: tuple[tuple[str, bytes], ...]


class KafkaDeliveryError(RuntimeError):
    """One or more asynchronous producer deliveries failed."""

    def __init__(
        self,
        failures: tuple[tuple[str, BaseException], ...],
        *,
        failure_count: int | None = None,
    ) -> None:
        self.failure_count = len(failures) if failure_count is None else failure_count
        if self.failure_count < len(failures):
            raise ValueError("failure_count cannot be smaller than retained failures")
        self.delivery_ids = tuple(delivery_id for delivery_id, _ in failures)
        self.omitted_count = self.failure_count - len(failures)
        details = "; ".join(
            f"{delivery_id}: {_bounded_failure_message(error)}" for delivery_id, error in failures
        )
        if self.omitted_count:
            details = f"{details}; {self.omitted_count} additional failures omitted"
        super().__init__(f"Kafka delivery failures: {details}")


def _bounded_failure_message(error: BaseException) -> str:
    message = str(error)
    if len(message) <= _MAX_DELIVERY_FAILURE_MESSAGE_CHARS:
        return message
    return f"{message[: _MAX_DELIVERY_FAILURE_MESSAGE_CHARS - 3]}..."


def kafka_record(envelope: DeliveryEnvelope) -> KafkaRecord:
    """Build the canonical key, payload, and lineage headers for Kafka."""
    return KafkaRecord(
        key=envelope.lineage_id.encode("utf-8"),
        value=envelope.to_json_bytes(),
        headers=(
            ("envelope_schema", DeliveryEnvelope.SCHEMA_VERSION.encode("utf-8")),
            ("replay_run_id", envelope.replay_run_id.encode("utf-8")),
            ("source_event_id", envelope.source_event_id.encode("utf-8")),
            ("delivery_id", envelope.delivery_id.encode("utf-8")),
        ),
    )


def _create_producer(bootstrap_servers: str) -> Producer:
    try:
        from kafka import KafkaProducer  # type: ignore[import-untyped]
    except ImportError as error:
        raise RuntimeError(
            "Kafka destination requires the kafka-python optional dependency"
        ) from error
    return cast(
        Producer,
        KafkaProducer(bootstrap_servers=bootstrap_servers, acks="all"),
    )


class KafkaSink:
    """At-least-once Kafka sink with asynchronous delivery failure tracking."""

    def __init__(
        self,
        *,
        topic: str,
        bootstrap_servers: str = "localhost:9092",
        producer: Producer | None = None,
    ) -> None:
        if not topic.strip():
            raise ValueError("Kafka topic must be non-empty")
        self.topic = topic
        self._producer = _create_producer(bootstrap_servers) if producer is None else producer
        self._failures: list[tuple[str, BaseException]] = []
        self._failure_count = 0
        self._failure_lock = Lock()
        self._closed = False

    def write(self, envelope: DeliveryEnvelope) -> None:
        if self._closed:
            raise SinkClosedError("Kafka sink is closed")
        delivery_error = self._collected_delivery_error()
        if delivery_error is not None:
            raise delivery_error
        record = kafka_record(envelope)
        future = self._producer.send(
            self.topic,
            value=record.value,
            key=record.key,
            headers=list(record.headers),
        )
        future.add_callback(self._delivery_succeeded, envelope.delivery_id)
        future.add_errback(self._delivery_failed, envelope.delivery_id)

    def flush(self) -> None:
        if self._closed:
            raise SinkClosedError("Kafka sink is closed")
        lifecycle_errors: list[BaseException] = []
        try:
            self._producer.flush()
        except BaseException as error:
            lifecycle_errors.append(error)
        self._raise_collected_failures("Kafka sink flush failures", lifecycle_errors)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        lifecycle_errors: list[BaseException] = []
        try:
            self._producer.flush()
        except BaseException as error:
            lifecycle_errors.append(error)
        try:
            self._producer.close()
        except BaseException as error:
            lifecycle_errors.append(error)

        self._raise_collected_failures("Kafka sink close failures", lifecycle_errors)

    @staticmethod
    def _delivery_succeeded(metadata: object, delivery_id: str) -> None:
        del metadata, delivery_id

    def _delivery_failed(self, error: BaseException, delivery_id: str) -> None:
        with self._failure_lock:
            self._failure_count += 1
            if len(self._failures) < MAX_RETAINED_DELIVERY_FAILURES:
                self._failures.append((delivery_id, error))

    def _collected_delivery_error(self) -> KafkaDeliveryError | None:
        with self._failure_lock:
            failure_count = self._failure_count
            failures = tuple(self._failures)
        return KafkaDeliveryError(failures, failure_count=failure_count) if failure_count else None

    def _raise_collected_failures(
        self,
        message: str,
        lifecycle_errors: list[BaseException],
    ) -> None:
        delivery_error = self._collected_delivery_error()
        errors: list[BaseException] = []
        if delivery_error is not None:
            errors.append(delivery_error)
        errors.extend(lifecycle_errors)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup(message, errors)
