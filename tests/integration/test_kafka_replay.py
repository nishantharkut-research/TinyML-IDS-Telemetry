from __future__ import annotations

import importlib.util
import json
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.replay.clock import SyntheticClock
from tinyml_ids_telemetry.replay.kafka import KafkaSink
from tinyml_ids_telemetry.replay.schedule import iter_replay_schedule

pytestmark = pytest.mark.integration


def test_replay_delivery_round_trips_through_kafka() -> None:
    if importlib.util.find_spec("kafka") is None:
        pytest.skip("Kafka integration requires the kafka-python optional dependency")
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        pytest.skip("Kafka broker unavailable: set KAFKA_BOOTSTRAP_SERVERS to run this test")

    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    topic = f"tinyml-ids-replay-{uuid4().hex}"
    clock = SyntheticClock(
        start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        interval=timedelta(seconds=1),
    )
    envelope = next(
        iter_replay_schedule(
            [make_event().to_dict()],
            replay_run_id="integration-run",
            clock=clock,
        )
    )
    try:
        sink = KafkaSink(topic=topic, bootstrap_servers=bootstrap_servers)
        sink.write(envelope)
        sink.close()
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=10_000,
        )
    except NoBrokersAvailable as error:
        pytest.skip(f"Kafka broker unavailable at {bootstrap_servers}: {error}")

    try:
        record = next(iter(consumer))
    finally:
        consumer.close()

    assert record.key == envelope.lineage_id.encode("utf-8")
    assert json.loads(record.value) == envelope.to_dict()
    assert dict(record.headers)["delivery_id"] == envelope.delivery_id.encode("utf-8")
