"""Contracts for pure Spark Bronze, Silver, deduplication, and policy transforms."""

from __future__ import annotations

import hashlib
import json
import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip(
    "pyspark",
    reason="Spark job tests require the optional .[spark] dependencies",
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from jobs.spark.common import (
    CONTROLLED_TOPIC_PARTITIONS,
    QUARANTINE_REASON_PRECEDENCE,
    SILVER_GUARANTEE,
    SparkContractError,
    validate_watermark_delay,
)
from jobs.spark.policies import build_policy_decisions
from jobs.spark.stream_telemetry import (
    bounded_deduplicate,
    bronze_from_kafka,
    split_silver_and_quarantine,
)
from tinyml_ids_telemetry.policies.security_aware import load_security_aware_policy

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_bronze_preserves_every_delivery_and_all_kafka_evidence(
    spark: SparkSession,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    envelope = envelope_factory()
    kafka = kafka_frame_factory(spark, [envelope, envelope])

    rows = bronze_from_kafka(kafka).orderBy("kafka_offset").collect()

    assert len(rows) == 2
    assert [row.kafka_offset for row in rows] == [0, 1]
    assert rows[0].kafka_topic == "telemetry"
    assert rows[0].kafka_partition == 0
    assert rows[0].kafka_key == bytes.fromhex("61" * 64)
    assert [header.key for header in rows[0].kafka_headers] == [
        "envelope_schema",
        "replay_run_id",
        "source_event_id",
        "delivery_id",
    ]
    assert rows[0].original_payload_binary == rows[1].original_payload_binary
    assert rows[0].original_payload_string == rows[0].original_payload_binary.decode("ascii")
    assert rows[0].kafka_timestamp is not None
    assert rows[0].kafka_timestamp_type == 0
    assert rows[0].ingestion_time is not None


def test_quarantine_reason_codes_and_precedence_are_frozen() -> None:
    assert QUARANTINE_REASON_PRECEDENCE == (
        "invalid_utf8",
        "malformed_json",
        "unsupported_schema_version",
        "missing_required_field",
        "unexpected_property",
        "type_mismatch",
        "invalid_kafka_contract",
        "invalid_delivery_identity",
        "invalid_event_value",
    )


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("malformed", "malformed_json"),
        ("unsupported-envelope", "unsupported_schema_version"),
        ("missing", "missing_required_field"),
        ("type", "type_mismatch"),
        ("unsupported-event", "unsupported_schema_version"),
        ("invalid-utf8", "invalid_utf8"),
        ("unexpected-envelope", "unexpected_property"),
        ("unexpected-event", "unexpected_property"),
        ("invalid-timestamp", "invalid_delivery_identity"),
        ("invalid-derived-event", "invalid_event_value"),
    ],
)
def test_quarantine_uses_one_deterministic_precedence_reason(
    spark: SparkSession,
    case: str,
    reason: str,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    envelope = envelope_factory()
    payload: object
    if case == "malformed":
        payload = "{"
    elif case == "invalid-utf8":
        payload = b"\xff"
    elif case == "unsupported-envelope":
        payload = {"schema_version": "unknown-envelope"}
    elif case == "missing":
        payload = {
            "schema_version": "telemetry-replay-envelope-v1",
            "replay_run_id": "run-1",
        }
    elif case == "type":
        payload = {**envelope, "source_offset": "0"}
    elif case == "unexpected-envelope":
        payload = {**envelope, "extra": 1}
    elif case == "unexpected-event":
        payload = {**envelope, "event": {**dict(envelope["event"]), "extra": 1}}
    elif case == "invalid-timestamp":
        payload = {**envelope, "synthetic_event_time": "2026-08-23T12:00:00+00:00"}
    elif case == "invalid-derived-event":
        payload = {
            **envelope,
            "event": {**dict(envelope["event"]), "is_predicted_attack": True},
        }
    else:
        event = dict(envelope["event"])
        event["schema_version"] = "unknown-event"
        payload = {**envelope, "event": event}
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [payload]))

    silver, quarantine = split_silver_and_quarantine(bronze)

    assert silver.count() == 0
    quarantined_rows = quarantine.collect()
    assert len(quarantined_rows) == 1
    quarantined = quarantined_rows[0]
    assert quarantined.quarantine_reason == reason
    assert quarantined.original_payload_binary is not None
    assert quarantined.kafka_topic == "telemetry"
    assert quarantined.kafka_partition == 0
    assert quarantined.kafka_offset == 0
    assert quarantined.kafka_timestamp_type == 0


@pytest.mark.parametrize("case", ["key", "missing-header", "wrong-header", "delivery-id"])
def test_invalid_kafka_or_delivery_identity_is_quarantined(
    spark: SparkSession,
    case: str,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    envelope = envelope_factory()
    key = b"wrong-lineage" if case == "key" else None
    headers = None
    if case == "missing-header":
        headers = [("envelope_schema", b"telemetry-replay-envelope-v1")]
    elif case == "wrong-header":
        headers = [
            ("envelope_schema", b"telemetry-replay-envelope-v1"),
            ("replay_run_id", b"different-run"),
            ("source_event_id", str(envelope["event"]["source_event_id"]).encode()),
            ("delivery_id", str(envelope["delivery_id"]).encode()),
        ]
    if case == "delivery-id":
        envelope = {**envelope, "delivery_id": "0" * 64}
    bronze = bronze_from_kafka(
        kafka_frame_factory(
            spark,
            [envelope],
            key_override=key,
            headers_override=headers,
        )
    )

    _, quarantine = split_silver_and_quarantine(bronze)
    row = quarantine.first()

    assert row.quarantine_reason == (
        "invalid_delivery_identity" if case == "delivery-id" else "invalid_kafka_contract"
    )


def test_valid_envelope_becomes_typed_silver_with_delivery_and_event_identity(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    event = event_factory()
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [envelope_factory(event)]))

    silver, quarantine = split_silver_and_quarantine(bronze)
    silver_rows = silver.collect()
    assert len(silver_rows) == 1
    row = silver_rows[0]

    assert quarantine.count() == 0
    assert row.replay_run_id == "run-1"
    assert row.source_event_id == event.source_event_id
    assert row.lineage_id == event.lineage_id
    event_time_text = (
        silver.select(F.date_format("synthetic_event_time", "yyyy-MM-dd'T'HH:mm:ss").alias("value"))
        .first()
        .value
    )
    assert event_time_text == "2026-08-23T12:00:00"
    assert row.fixed_logits == [0, 1, 2, 4000, 3]
    assert row.total_us == 162
    assert row.event_payload_sha256 is not None
    assert row.original_payload_binary is not None


def test_valid_utf8_binary_string_mismatch_cannot_reach_silver(
    spark: SparkSession,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [envelope_factory()])).withColumn(
        "original_payload_string",
        F.concat(F.col("original_payload_string"), F.lit(" ")),
    )

    silver, quarantine = split_silver_and_quarantine(bronze)

    assert silver.count() == 0
    row = quarantine.first()
    assert row.quarantine_reason == "invalid_kafka_contract"
    assert row.original_payload_string != row.original_payload_binary.decode("utf-8")


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    [
        ("malformed", "malformed_json"),
        ("type-mismatch", "type_mismatch"),
    ],
)
def test_payload_parse_reason_precedes_binary_string_mismatch(
    spark: SparkSession,
    case: str,
    expected_reason: str,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    canonical = envelope_factory()
    if case == "malformed":
        bronze = bronze_from_kafka(kafka_frame_factory(spark, [canonical])).withColumn(
            "original_payload_binary",
            F.lit(b"{"),
        )
    else:
        type_mismatch = {
            **canonical,
            "source_offset": "0",
        }
        canonical_string = json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
        )
        bronze = bronze_from_kafka(kafka_frame_factory(spark, [type_mismatch])).withColumn(
            "original_payload_string", F.lit(canonical_string)
        )

    silver, quarantine = split_silver_and_quarantine(bronze)

    assert silver.count() == 0
    assert quarantine.first().quarantine_reason == expected_reason


@pytest.mark.parametrize("invalid_element", [None, "not-an-integer"])
def test_fixed_logits_null_converted_element_is_type_mismatch(
    spark: SparkSession,
    invalid_element: object,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    envelope = envelope_factory()
    envelope["event"]["fixed_logits"] = [0, 1, invalid_element, 4000, 3]
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [envelope]))

    silver, quarantine = split_silver_and_quarantine(bronze)

    assert silver.count() == 0
    assert quarantine.first().quarantine_reason == "type_mismatch"


def test_empty_required_event_string_is_quarantined(
    spark: SparkSession,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    envelope = envelope_factory()
    envelope["event"]["status"] = ""
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [envelope]))

    silver, quarantine = split_silver_and_quarantine(bronze)

    assert silver.count() == 0
    assert quarantine.first().quarantine_reason == "invalid_event_value"


def test_negative_logit_frac_bits_is_quarantined_before_policy_derivation(
    spark: SparkSession,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    envelope = envelope_factory()
    event = dict(envelope["event"])
    event["logit_frac_bits"] = -1
    top_two = sorted(event["fixed_logits"], reverse=True)[:2]
    event["normalized_logit_margin"] = (top_two[0] - top_two[1]) / (2.0**-1)
    payload = {**envelope, "event": event}
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [payload]))

    silver, quarantine = split_silver_and_quarantine(bronze)

    assert silver.count() == 0
    assert quarantine.first().quarantine_reason == "invalid_event_value"
    assert build_policy_decisions(silver).count() == 0


@pytest.mark.parametrize(
    ("delay", "interval", "window"),
    [
        (0, 250, 1),
        (-1, 250, 1),
        (750, 250, 4),
        (751, 0, 4),
        (751, 250, 0),
        (True, 250, 4),
        (751, False, 4),
    ],
)
def test_watermark_delay_must_strictly_exceed_derived_replay_timestamp_skew(
    delay: object,
    interval: object,
    window: object,
) -> None:
    with pytest.raises(SparkContractError):
        validate_watermark_delay(delay, interval, window)


def test_streaming_scope_is_explicitly_one_partition_and_provisional() -> None:
    assert CONTROLLED_TOPIC_PARTITIONS == 1
    assert SILVER_GUARANTEE == (
        "provisional watermark-bounded deduplication for a controlled one-partition replay"
    )


def test_streaming_dedup_declares_event_time_watermark_and_bounded_identity(
    spark: SparkSession,
) -> None:
    source = (
        spark.readStream.format("rate")
        .load()
        .select(
            F.lit("run-1").alias("replay_run_id"),
            F.sha2(F.col("value").cast("string"), 256).alias("source_event_id"),
            F.col("timestamp").alias("synthetic_event_time"),
        )
    )

    deduplicated = bounded_deduplicate(
        source,
        watermark_delay_ms=751,
        replay_interval_ms=250,
        out_of_order_window=4,
    )
    logical_plan = deduplicated._jdf.queryExecution().logical().toString()

    assert deduplicated.isStreaming
    assert "EventTimeWatermark" in logical_plan
    assert "DeduplicateWithinWatermark" in logical_plan
    assert "replay_run_id" in logical_plan
    assert "source_event_id" in logical_plan


def test_streaming_dedup_drops_within_watermark_duplicate_and_late_replay(
    spark: SparkSession,
) -> None:
    source = (
        spark.readStream.format("rate-micro-batch")
        .option("rowsPerBatch", 1)
        .option("numPartitions", 1)
        .option("startTimestamp", 1_787_485_200_000)
        .option("advanceMillisPerBatch", 1_000)
        .load()
        .filter(F.col("value") < 5)
        .select(
            F.lit("run-1").alias("replay_run_id"),
            F.when(F.col("value").isin(0, 1, 4), F.lit("event-a"))
            .when(F.col("value") == 2, F.lit("event-b"))
            .otherwise(F.lit("event-c"))
            .alias("source_event_id"),
            F.timestamp_seconds(
                F.lit(1_787_485_200)
                + F.when(F.col("value") == 0, F.lit(10))
                .when(F.col("value") == 1, F.lit(11))
                .when(F.col("value") == 2, F.lit(20))
                .when(F.col("value") == 3, F.lit(30))
                .otherwise(F.lit(10))
            ).alias("synthetic_event_time"),
            "value",
        )
    )
    deduplicated = bounded_deduplicate(
        source,
        watermark_delay_ms=5_000,
        replay_interval_ms=1_000,
        out_of_order_window=1,
    )
    query_name = f"bounded_dedup_{uuid4().hex}"
    writer = (
        deduplicated.writeStream.format("memory")
        .queryName(query_name)
        .outputMode("append")
        .trigger(processingTime="100 milliseconds")
    )
    try:
        query = writer.start()
    except Exception as exc:
        missing_winutils = os.name == "nt" and "HADOOP_HOME and hadoop.home.dir are unset" in str(
            exc
        )
        if missing_winutils:
            pytest.skip("Spark Structured Streaming needs Hadoop winutils.exe on Windows")
        raise

    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            progress = query.lastProgress
            if progress is not None and int(progress["batchId"]) >= 4:
                break
            if query.exception() is not None:
                raise query.exception()  # type: ignore[misc]
            time.sleep(0.1)
        else:
            pytest.fail("rate-micro-batch did not complete five deterministic batches")

        rows = spark.table(query_name).orderBy("source_event_id").collect()
        assert [row.source_event_id for row in rows] == ["event-a", "event-b", "event-c"]
        assert [row.value for row in rows] == [0, 2, 3]
    finally:
        query.stop()
        spark.catalog.dropTempView(query_name)


def test_policy_decisions_match_python_policy_and_ignore_true_labels(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    policy = load_security_aware_policy(PROJECT_ROOT / "configs/retention-policies.toml")
    events = [
        event_factory(
            source_event_id="1" * 64, predicted_class_id=0, fixed_logits=(5000, 0, 1, 2, 3)
        ),
        event_factory(source_event_id="2" * 64, status="ERROR"),
        event_factory(source_event_id="3" * 64, attempts=2),
        event_factory(source_event_id="4" * 64, fixed_logits=(0, 1, 2, 3242, 3)),
        event_factory(source_event_id="5" * 64, fixed_logits=(0, 1, 2, 7000, 3)),
    ]
    bronze = bronze_from_kafka(
        kafka_frame_factory(spark, [envelope_factory(event) for event in events])
    )
    silver, quarantine = split_silver_and_quarantine(bronze)
    assert quarantine.count() == 0

    decisions = build_policy_decisions(silver)
    rows = {(row.source_event_id, row.policy_id): row for row in decisions.collect()}

    assert len(rows) == len(events) * 2
    for event in events:
        retain_all = rows[(event.source_event_id, "retain-all-v1")]
        assert retain_all.retained is True
        assert retain_all.reason == "retain_all"
        expected = policy.decide(event)
        actual = rows[(event.source_event_id, "security-aware-static-v1")]
        assert actual.retained is expected.retained
        assert actual.reason == expected.reason
        assert actual.inclusion_probability_numerator == 1
        assert actual.inclusion_probability_denominator == (
            10 if expected.inclusion_probability < 1 else 1
        )
        assert actual.inclusion_probability == Decimal(str(expected.inclusion_probability))
        assert actual.inverse_probability_weight == Decimal(
            str(expected.inverse_probability_weight)
        )

    mutated = silver.withColumn("true_class_id", F.lit(0)).withColumn(
        "true_class_name", F.lit("Blackhole")
    )
    original_online = {
        tuple(row)
        for row in decisions.filter(F.col("policy_id") == "security-aware-static-v1")
        .select(
            "replay_run_id",
            "source_event_id",
            "policy_id",
            "retained",
            "reason",
            "inclusion_probability_numerator",
            "inclusion_probability_denominator",
            "inclusion_probability",
            "inverse_probability_weight",
        )
        .collect()
    }
    mutated_online = {
        tuple(row)
        for row in build_policy_decisions(mutated)
        .filter(F.col("policy_id") == "security-aware-static-v1")
        .select(
            "replay_run_id",
            "source_event_id",
            "policy_id",
            "retained",
            "reason",
            "inclusion_probability_numerator",
            "inclusion_probability_denominator",
            "inclusion_probability",
            "inverse_probability_weight",
        )
        .collect()
    }
    assert mutated_online == original_online


def test_security_sampling_uses_unsigned_first_sha256_u64_modulo(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    policy_id = "security-aware-static-v1"
    source_id = next(
        f"{value:064x}"
        for value in range(1, 1000)
        if int.from_bytes(
            hashlib.sha256(f"{policy_id}|{value:064x}".encode("ascii")).digest()[:8],
            "big",
        )
        >= 2**63
    )
    event = event_factory(source_event_id=source_id, fixed_logits=(0, 1, 2, 7000, 3))
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [envelope_factory(event)]))
    silver, _ = split_silver_and_quarantine(bronze)

    row = build_policy_decisions(silver).filter(F.col("policy_id") == policy_id).first()
    expected_bucket = (
        int.from_bytes(
            hashlib.sha256(f"{policy_id}|{source_id}".encode("ascii")).digest()[:8],
            "big",
        )
        % 10
    )

    assert row.retained is (expected_bucket == 0)


def test_security_policy_uses_exact_model_specific_integer_margin(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    events = [
        event_factory(source_event_id="6" * 64, fixed_logits=(0, 1, 2, 3243, 3)),
        event_factory(source_event_id="7" * 64, fixed_logits=(0, 1, 2, 3244, 3)),
        event_factory(
            source_event_id="8" * 64,
            student_model="student_B",
            fixed_logits=(0, 1, 2, 2921, 3),
        ),
        event_factory(
            source_event_id="9" * 64,
            student_model="student_B",
            fixed_logits=(0, 1, 2, 2922, 3),
        ),
    ]
    bronze = bronze_from_kafka(
        kafka_frame_factory(spark, [envelope_factory(event) for event in events])
    )
    silver, _ = split_silver_and_quarantine(bronze)

    reasons = {
        row.source_event_id: row.reason
        for row in build_policy_decisions(silver)
        .filter(F.col("policy_id") == "security-aware-static-v1")
        .collect()
    }

    assert reasons["6" * 64] == "low_margin"
    assert reasons["7" * 64] in {"normal_sample", "normal_not_sampled"}
    assert reasons["8" * 64] == "low_margin"
    assert reasons["9" * 64] in {"normal_sample", "normal_not_sampled"}


@pytest.mark.parametrize(
    ("student_model", "logit_frac_bits", "expected_message"),
    [
        ("unsupported", 9, "unsupported student_model"),
        ("unsupported", 8, "unsupported student_model"),
        ("student_A", 8, "logit_frac_bits mismatch"),
    ],
)
def test_security_policy_validates_model_then_scale_before_attack_branch(
    spark: SparkSession,
    student_model: str,
    logit_frac_bits: int,
    expected_message: str,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    event = event_factory(
        predicted_class_id=0,
        fixed_logits=(5000, 0, 1, 2, 3),
    )
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [envelope_factory(event)]))
    silver, quarantine = split_silver_and_quarantine(bronze)
    assert quarantine.count() == 0
    invalid = silver.withColumn("student_model", F.lit(student_model)).withColumn(
        "logit_frac_bits", F.lit(logit_frac_bits)
    )

    with pytest.raises(Exception) as raised:
        build_policy_decisions(invalid).collect()

    assert expected_message in str(raised.value)
