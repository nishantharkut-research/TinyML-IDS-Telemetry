"""Opt-in Kafka 4.1.2 to Spark 4.1.3 Parquet integration."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration

_SAFE_TOPIC_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}")
_LOGGER = logging.getLogger(__name__)


def _prerequisites() -> tuple[str, str]:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    topic_prefix = os.environ.get("KAFKA_TOPIC", "").strip()
    if not bootstrap or not topic_prefix:
        pytest.skip("set KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC for local Kafka integration")
    if _SAFE_TOPIC_PREFIX.fullmatch(topic_prefix) is None:
        pytest.fail("KAFKA_TOPIC must be a safe Kafka topic prefix")
    if os.name == "nt" and shutil.which("winutils") is None:
        pytest.skip("local Spark Parquet writes on Windows require Hadoop winutils.exe")
    return bootstrap, topic_prefix


def _warn_cleanup(label: str, action: Callable[[], object]) -> None:
    try:
        action()
    except Exception as error:
        _LOGGER.warning("%s cleanup failed: %s", label, error, exc_info=True)


def test_one_partition_kafka_delivery_reaches_four_distinct_parquet_sinks(
    tmp_path: Path,
) -> None:
    bootstrap, topic_prefix = _prerequisites()
    try:
        from kafka import KafkaAdminClient, KafkaProducer  # type: ignore[import-untyped]
        from kafka.admin import NewTopic  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F  # noqa: N812
    except ImportError as error:
        pytest.skip(f"Kafka/Spark Python prerequisite unavailable: {error}")

    from jobs.spark.startup import (
        SPARK_KAFKA_CONNECTOR,
        KafkaStreamContract,
        StreamingPaths,
        start_streaming_job,
    )
    from tests.unit.jobs.conftest import envelope_mapping, telemetry_event

    identity = uuid4().hex
    topic = f"{topic_prefix}-task6-{identity}"
    replay_run_id = f"run-{identity}"
    admin = None
    spark = None
    queries = None
    producer = None
    topic_creation_attempted = False
    try:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap)
            topic_creation_attempted = True
            admin.create_topics(
                [NewTopic(name=topic, num_partitions=1, replication_factor=1)],
                timeout_ms=30_000,
                wait_for_metadata=True,
            )
        except Exception as error:
            pytest.skip(f"ephemeral Kafka topic setup unavailable: {error}")

        descriptions = admin.describe_topics([topic])
        assert len(descriptions) == 1
        assert descriptions[0]["topic"] == topic
        assert [partition["partition"] for partition in descriptions[0]["partitions"]] == [0]

        try:
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("tinyml-ids-telemetry-kafka-integration")
                .config("spark.ui.enabled", "false")
                .config("spark.jars.packages", SPARK_KAFKA_CONNECTOR)
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate()
            )
        except Exception as error:
            pytest.skip(f"Spark Kafka connector 4.1.3 unavailable: {error}")

        event = telemetry_event()
        envelope = envelope_mapping(event, replay_run_id=replay_run_id)
        delivery_id = str(envelope["delivery_id"])
        payload = json.dumps(
            envelope,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        headers = [
            ("envelope_schema", b"telemetry-replay-envelope-v1"),
            ("replay_run_id", replay_run_id.encode("ascii")),
            ("source_event_id", event.source_event_id.encode("ascii")),
            ("delivery_id", delivery_id.encode("ascii")),
        ]
        paths = StreamingPaths.build(
            tmp_path / "stream",
            snapshot_directory=tmp_path / "snapshot",
            query_version="integration-v1",
        )
        contract = KafkaStreamContract(
            bootstrap_servers=bootstrap,
            topic=topic,
            replay_interval_ms=250,
            out_of_order_window=1,
            watermark_delay_ms=1,
        )
        queries = start_streaming_job(spark, contract=contract, paths=paths)
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all")
        metadata = producer.send(
            topic,
            key=event.lineage_id.encode("ascii"),
            value=payload,
            headers=headers,
        ).get(timeout=30)
        producer.flush()
        assert metadata.topic == topic
        assert metadata.partition == 0
        assert metadata.offset >= 0

        for query in queries.all:
            query.processAllAvailable()

        bronze = spark.read.parquet(str(paths.bronze))
        bronze_rows = bronze.filter(
            (F.col("kafka_topic") == metadata.topic)
            & (F.col("kafka_partition") == metadata.partition)
            & (F.col("kafka_offset") == metadata.offset)
        ).collect()
        assert len(bronze_rows) == 1
        bronze_row = bronze_rows[0]
        assert bronze_row.original_payload_binary == payload
        assert bronze_row.kafka_key == event.lineage_id.encode("ascii")
        bronze_headers = {header.key: bytes(header.value) for header in bronze_row.kafka_headers}
        assert bronze_headers["delivery_id"] == delivery_id.encode("ascii")

        silver_rows = (
            spark.read.parquet(str(paths.silver_provisional))
            .filter(
                (F.col("replay_run_id") == replay_run_id)
                & (F.col("source_event_id") == event.source_event_id)
                & (F.col("delivery_id") == delivery_id)
                & (F.col("kafka_topic") == metadata.topic)
                & (F.col("kafka_partition") == metadata.partition)
                & (F.col("kafka_offset") == metadata.offset)
            )
            .collect()
        )
        assert len(silver_rows) == 1

        policy_rows = (
            spark.read.parquet(str(paths.policy_decisions))
            .filter(
                (F.col("replay_run_id") == replay_run_id)
                & (F.col("source_event_id") == event.source_event_id)
            )
            .collect()
        )
        assert len(policy_rows) == 2
        assert {row.policy_id for row in policy_rows} == {
            "retain-all-v1",
            "security-aware-static-v1",
        }

        quarantine_files = tuple(paths.quarantine.rglob("*.parquet"))
        if quarantine_files:
            quarantined_delivery_count = (
                spark.read.parquet(str(paths.quarantine))
                .filter(
                    (F.col("kafka_topic") == metadata.topic)
                    & (F.col("kafka_partition") == metadata.partition)
                    & (F.col("kafka_offset") == metadata.offset)
                )
                .count()
            )
            assert quarantined_delivery_count == 0
        else:
            assert paths.quarantine.is_dir()
    finally:
        if producer is not None:
            _warn_cleanup("Kafka producer", producer.close)
        if queries is not None:
            _warn_cleanup("Spark queries", queries.stop)
        if spark is not None:
            _warn_cleanup("Spark session", spark.stop)
        if admin is not None and topic_creation_attempted:
            _warn_cleanup(
                "ephemeral Kafka topic",
                lambda: admin.delete_topics([topic], timeout_ms=30_000),
            )
        if admin is not None:
            _warn_cleanup("Kafka admin client", admin.close)
