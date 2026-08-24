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


def test_explicit_invalid_kafka_integration_opt_in_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TINYML_IDS_REQUIRE_KAFKA_INTEGRATION", "0")

    with pytest.raises(pytest.fail.Exception, match="must equal 1"):
        _prerequisites()


def test_required_kafka_integration_fails_when_bootstrap_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TINYML_IDS_REQUIRE_KAFKA_INTEGRATION", "1")
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

    with pytest.raises(pytest.fail.Exception, match="KAFKA_BOOTSTRAP_SERVERS"):
        _prerequisites()


def test_replay_delivery_round_trips_through_kafka() -> None:
    bootstrap_servers, required = _prerequisites()

    from kafka import KafkaAdminClient, KafkaConsumer
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError

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
    admin = None
    consumer = None
    sink = None
    topic_created = False
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)],
            timeout_ms=30_000,
            wait_for_metadata=True,
        )
        topic_created = True
        sink = KafkaSink(topic=topic, bootstrap_servers=bootstrap_servers)
        sink.write(envelope)
        sink.close()
        sink = None
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=10_000,
        )
        record = next(iter(consumer))
        assert record.key == envelope.lineage_id.encode("utf-8")
        assert json.loads(record.value) == envelope.to_dict()
        assert dict(record.headers)["delivery_id"] == envelope.delivery_id.encode("utf-8")
    except KafkaError as error:
        _unavailable(f"Kafka broker unavailable at {bootstrap_servers}: {error}", required)
    finally:
        if sink is not None:
            sink.close()
        if consumer is not None:
            consumer.close()
        if admin is not None and topic_created:
            admin.delete_topics([topic], timeout_ms=30_000)
        if admin is not None:
            admin.close()


def _prerequisites() -> tuple[str, bool]:
    opt_in = os.environ.get("TINYML_IDS_REQUIRE_KAFKA_INTEGRATION")
    if opt_in is not None and opt_in != "1":
        pytest.fail("TINYML_IDS_REQUIRE_KAFKA_INTEGRATION must equal 1 when explicitly set")
    required = opt_in == "1"
    if importlib.util.find_spec("kafka") is None:
        _unavailable("Kafka integration requires the kafka-python optional dependency", required)
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap_servers:
        _unavailable(
            "Kafka broker unavailable: set KAFKA_BOOTSTRAP_SERVERS to run this test",
            required,
        )
    return bootstrap_servers, required


def _unavailable(message: str, required: bool) -> None:
    if required:
        pytest.fail(message)
    pytest.skip(message)
