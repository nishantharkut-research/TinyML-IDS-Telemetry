"""Exact Bronze-to-canonical-Gold proof and publication contracts."""

from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip(
    "pyspark",
    reason="Spark job tests require the optional .[spark] dependencies",
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

import jobs.spark.finalize_run as finalization_module
from jobs.spark.finalize_run import (
    GoldFinalizationError,
    finalize_run,
    prepare_exact_gold,
)
from jobs.spark.stream_telemetry import bronze_from_kafka
from tinyml_ids_telemetry.contracts.identity import lineage_id
from tinyml_ids_telemetry.replay.clock import SyntheticClock
from tinyml_ids_telemetry.replay.receipt import (
    build_completion_receipt,
    publish_completion_receipt,
)
from tinyml_ids_telemetry.replay.schedule import iter_replay_schedule


def _events(event_factory: Any) -> list[Any]:
    return [
        event_factory(source_event_id="1" * 64, row_id=0, source_row_index=7),
        event_factory(source_event_id="2" * 64, row_id=1, source_row_index=8),
    ]


def _published_receipt(
    root: Path,
    receipt: dict[str, object],
    *,
    name: str = "completion_receipt.json",
) -> Path:
    path = root / "receipts" / name
    publish_completion_receipt(path, receipt, protected_roots=())
    return path


def _four_events(event_factory: Any) -> list[Any]:
    return [
        event_factory(
            source_event_id=str(index + 1) * 64,
            row_id=index,
            source_row_index=7 + index,
        )
        for index in range(4)
    ]


def _schedule(
    events: list[Any],
    *,
    replay_run_id: str = "run-1",
    duplicate_every: int | None = None,
    out_of_order_window: int = 1,
) -> list[dict[str, object]]:
    return [
        item.to_dict()
        for item in iter_replay_schedule(
            [dict(event) if isinstance(event, dict) else event.to_dict() for event in events],
            replay_run_id=replay_run_id,
            clock=SyntheticClock(
                start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
                interval=timedelta(milliseconds=250),
            ),
            duplicate_every=duplicate_every,
            out_of_order_window=out_of_order_window,
        )
    ]


def _read_json_lines(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _write_json_lines(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def _reseal_snapshot(
    snapshot: Path,
    receipt: dict[str, object],
) -> dict[str, object]:
    manifest_path = snapshot / "snapshot_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["events_sha256"] = sha256((snapshot / "events.jsonl").read_bytes()).hexdigest()
    manifest["lineages_sha256"] = sha256((snapshot / "lineages.jsonl").read_bytes()).hexdigest()
    identity = {
        "schema_version": "telemetry-snapshot-v1",
        "sealed_report_sha256": manifest["sealed_report_sha256"],
        "events_sha256": manifest["events_sha256"],
        "lineages_sha256": manifest["lineages_sha256"],
        "event_count": manifest["event_count"],
    }
    manifest["snapshot_id"] = sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
    options = receipt["replay_options"]
    return build_completion_receipt(
        replay_run_id=str(receipt["replay_run_id"]),
        snapshot_id=str(manifest["snapshot_id"]),
        sealed_report_sha256=str(manifest["sealed_report_sha256"]),
        events_sha256=str(manifest["events_sha256"]),
        lineages_sha256=str(manifest["lineages_sha256"]),
        evaluation_sequence_sha256=str(manifest["evaluation_sequence_sha256"]),
        snapshot_event_count=int(manifest["event_count"]),
        snapshot_lineage_ids=tuple(manifest["lineage_ids"]),
        start_utc=str(options["start_utc"]),
        interval_ms=int(options["interval_ms"]),
        rate_per_second=options["rate_per_second"],
        resume_offset=int(options["resume_offset"]),
        duplicate_every=options["duplicate_every"],
        out_of_order_window=int(options["out_of_order_window"]),
        destination=str(receipt["destination"]),
        kafka_topic=str(receipt["kafka_topic"]),
        observed_delivery_count=int(receipt["observed_delivery_count"]),
    )


def test_gold_is_rebuilt_from_bronze_and_publishes_canonical_snapshot_rows(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(
        tmp_path,
        _events(event_factory),
        duplicate_every=2,
        out_of_order_window=2,
    )
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [*deliveries, deliveries[0]]))
    bronze_with_identical_repeat = bronze.unionByName(bronze.filter(F.col("kafka_offset") == 0))

    gold, proof = prepare_exact_gold(
        bronze_with_identical_repeat,
        snapshot_directory=snapshot,
        completion_receipt=receipt,
    )
    rows = gold.orderBy("source_event_id").collect()

    expected_ids = sorted(
        event["source_event_id"] for event in _read_json_lines(snapshot / "events.jsonl")
    )
    assert [row.source_event_id for row in rows] == expected_ids
    assert all(row.replay_run_id == "run-1" for row in rows)
    assert "kafka_offset" not in gold.columns
    assert "original_payload_binary" not in gold.columns
    assert proof.source_event_count == 2
    assert proof.delivery_count == 3
    assert proof.identical_bronze_repeats == 2
    assert proof.lineage_counts == ((receipt["snapshot"]["lineage_ids"][0], 2),)


@pytest.mark.parametrize(
    "case",
    [
        "malformed-event",
        "unexpected-event-property",
        "wrong-event-count",
        "malformed-lineage",
        "lineage-id-set",
        "lineage-event-metadata",
        "lineage-row-coverage",
    ],
)
def test_gold_strictly_validates_every_canonical_snapshot_row(
    spark: SparkSession,
    tmp_path: Path,
    case: str,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    events_path = snapshot / "events.jsonl"
    lineages_path = snapshot / "lineages.jsonl"
    if case == "malformed-event":
        events_path.write_text("{\n", encoding="utf-8")
    elif case == "unexpected-event-property":
        events = _read_json_lines(events_path)
        events[0]["unexpected"] = True
        _write_json_lines(events_path, events)
    elif case == "wrong-event-count":
        _write_json_lines(events_path, _read_json_lines(events_path)[:-1])
    elif case == "malformed-lineage":
        lineages_path.write_text("{\n", encoding="utf-8")
    else:
        lineages = _read_json_lines(lineages_path)
        lineage = lineages[0]
        if case == "lineage-id-set":
            lineage["wireless_protocol_id"] = "different-wireless-protocol"
            lineage["lineage_id"] = lineage_id(
                protocol_id=str(lineage["wireless_protocol_id"]),
                export_id=str(lineage["export_id"]),
                wireless_bundle_id=str(lineage["wireless_bundle_id"]),
                source_session_id=str(lineage["source_session_id"]),
                source_stage_id=str(lineage["source_stage_id"]),
            )
        elif case == "lineage-event-metadata":
            lineage["board"] = "arduino_r4_wifi"
        else:
            lineage["expected_rows"] = int(lineage["expected_rows"]) + 1
        _write_json_lines(lineages_path, lineages)
    resealed_receipt = _reseal_snapshot(snapshot, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))

    with pytest.raises(GoldFinalizationError, match="canonical snapshot"):
        prepare_exact_gold(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt=resealed_receipt,
        )


def test_gold_rejects_resealed_noncanonical_source_event_id(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, _ = snapshot_factory(tmp_path, _events(event_factory))
    events = _read_json_lines(snapshot / "events.jsonl")
    events[0]["source_event_id"] = "f" * 64
    _write_json_lines(snapshot / "events.jsonl", events)
    resealed_receipt = _reseal_snapshot(snapshot, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, _schedule(events)))

    with pytest.raises(GoldFinalizationError, match="source_event_id"):
        prepare_exact_gold(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt=resealed_receipt,
        )


def test_gold_selects_only_target_candidates_from_shared_bronze(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    events = _events(event_factory)
    snapshot, receipt, target_deliveries = snapshot_factory(tmp_path, events)
    other_deliveries = _schedule(events, replay_run_id="run-2")
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [*other_deliveries, *target_deliveries]))

    gold, proof = prepare_exact_gold(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt=receipt,
    )

    assert gold.select("replay_run_id").distinct().first().replay_run_id == "run-1"
    assert proof.delivery_count == len(target_deliveries)


def test_gold_includes_header_payload_run_disagreement_for_quarantine(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    first = deliveries[0]
    headers = [
        ("envelope_schema", b"telemetry-replay-envelope-v1"),
        ("replay_run_id", b"run-2"),
        ("source_event_id", str(first["event"]["source_event_id"]).encode("ascii")),
        ("delivery_id", str(first["delivery_id"]).encode("ascii")),
    ]
    mismatched = kafka_frame_factory(
        spark,
        [first],
        headers_override=headers,
    )
    valid_tail = kafka_frame_factory(spark, deliveries[1:]).withColumn(
        "offset", F.col("offset") + F.lit(1)
    )
    bronze = bronze_from_kafka(mismatched.unionByName(valid_tail))

    with pytest.raises(GoldFinalizationError, match="quarantine"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_accepts_completed_resume_with_valid_prefix_subset_and_full_tail(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    events = _four_events(event_factory)
    snapshot, receipt, tail = snapshot_factory(
        tmp_path,
        events,
        duplicate_every=2,
        out_of_order_window=2,
        resume_offset=2,
    )
    full = _schedule(
        _read_json_lines(snapshot / "events.jsonl"),
        duplicate_every=2,
        out_of_order_window=2,
    )
    prefix_subset = [
        item
        for item in full
        if int(item["source_offset"]) < 2 and int(item["duplicate_ordinal"]) == 0
    ]
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [*prefix_subset, *tail]))

    gold, proof = prepare_exact_gold(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt=receipt,
    )

    assert gold.count() == 4
    assert proof.delivery_count == len(prefix_subset) + len(tail)


def test_gold_schedule_reconstruction_preserves_non_millisecond_start(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    start_utc = datetime(2026, 8, 23, 0, 0, 0, 123456, tzinfo=UTC)
    snapshot, receipt, deliveries = snapshot_factory(
        tmp_path,
        _events(event_factory),
        start_utc=start_utc,
    )
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))

    gold, proof = prepare_exact_gold(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt=receipt,
    )

    assert gold.count() == 2
    assert proof.delivery_count == 2


def test_gold_rejects_resumed_run_with_missing_tail_delivery(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    events = _four_events(event_factory)
    snapshot, receipt, tail = snapshot_factory(tmp_path, events, resume_offset=2)
    prefix = [
        item
        for item in _schedule(_read_json_lines(snapshot / "events.jsonl"))
        if int(item["source_offset"]) < 2
    ]
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [*prefix, *tail[:-1]]))

    with pytest.raises(GoldFinalizationError, match="resume tail"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_rejects_resumed_run_with_unscheduled_prefix_delivery(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    events = _four_events(event_factory)
    snapshot, receipt, tail = snapshot_factory(tmp_path, events, resume_offset=2)
    prefix = [
        item
        for item in _schedule(_read_json_lines(snapshot / "events.jsonl"))
        if int(item["source_offset"]) < 2
    ]
    prefix[0] = {**prefix[0], "emission_index": 99}
    bronze = bronze_from_kafka(kafka_frame_factory(spark, [*prefix, *tail]))

    with pytest.raises(GoldFinalizationError, match="schedule"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_rejects_same_kafka_position_with_different_raw_payload(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    kafka = kafka_frame_factory(spark, deliveries)
    conflicting_delivery = json.loads(json.dumps(deliveries[0]))
    conflicting_delivery["event"]["status"] = "ERROR"
    conflicting = kafka_frame_factory(spark, [conflicting_delivery])
    bronze = bronze_from_kafka(kafka.unionByName(conflicting))

    with pytest.raises(GoldFinalizationError, match="Kafka position"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_rejects_other_run_record_at_target_kafka_position(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    events = _events(event_factory)
    snapshot, receipt, target_deliveries = snapshot_factory(tmp_path, events)
    target = kafka_frame_factory(spark, target_deliveries)
    other_run_same_position = kafka_frame_factory(
        spark,
        _schedule(events, replay_run_id="run-2")[:1],
    )
    bronze = bronze_from_kafka(target.unionByName(other_run_same_position))

    with pytest.raises(GoldFinalizationError, match="Kafka position"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_rejects_canonical_string_with_unrelated_binary_payload(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries)).withColumn(
        "original_payload_binary",
        F.lit(b'{"unrelated":true}'),
    )

    with pytest.raises(GoldFinalizationError, match="quarantine"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_discovers_target_from_binary_when_string_and_header_name_other_run(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    first = deliveries[0]
    event = first["event"]
    other_run_headers = [
        ("envelope_schema", b"telemetry-replay-envelope-v1"),
        ("replay_run_id", b"run-2"),
        ("source_event_id", str(event["source_event_id"]).encode("ascii")),
        ("delivery_id", str(first["delivery_id"]).encode("ascii")),
    ]
    other_run_string = json.dumps(
        {**first, "replay_run_id": "run-2"},
        sort_keys=True,
        separators=(",", ":"),
    )
    mismatched = bronze_from_kafka(
        kafka_frame_factory(
            spark,
            [first],
            headers_override=other_run_headers,
        )
    ).withColumn("original_payload_string", F.lit(other_run_string))
    remaining = bronze_from_kafka(kafka_frame_factory(spark, deliveries[1:])).withColumn(
        "kafka_offset", F.col("kafka_offset") + F.lit(1)
    )
    bronze = mismatched.unionByName(remaining)

    with pytest.raises(GoldFinalizationError, match="quarantine"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


@pytest.mark.parametrize(
    "case",
    [
        "null-topic",
        "wrong-topic",
        "null-partition",
        "wrong-partition",
        "null-offset",
        "negative-offset",
        "null-timestamp",
        "null-timestamp-type",
        "unsupported-timestamp-type",
    ],
)
def test_gold_rejects_invalid_target_kafka_metadata_before_position_logic(
    spark: SparkSession,
    tmp_path: Path,
    case: str,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    if case == "null-topic":
        bronze = bronze.withColumn("kafka_topic", F.lit(None).cast("string"))
    elif case == "wrong-topic":
        bronze = bronze.withColumn("kafka_topic", F.lit("other-topic"))
    elif case == "null-partition":
        bronze = bronze.withColumn("kafka_partition", F.lit(None).cast("int"))
    elif case == "wrong-partition":
        bronze = bronze.withColumn("kafka_partition", F.lit(1))
    elif case == "null-offset":
        bronze = bronze.withColumn("kafka_offset", F.lit(None).cast("long"))
    elif case == "negative-offset":
        bronze = bronze.withColumn("kafka_offset", F.lit(-1).cast("long"))
    elif case == "null-timestamp":
        bronze = bronze.withColumn("kafka_timestamp", F.lit(None).cast("timestamp"))
    elif case == "null-timestamp-type":
        bronze = bronze.withColumn("kafka_timestamp_type", F.lit(None).cast("int"))
    else:
        bronze = bronze.withColumn("kafka_timestamp_type", F.lit(2))

    with pytest.raises(GoldFinalizationError, match="Kafka metadata"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_accepts_kafka_log_append_timestamp_type(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries)).withColumn(
        "kafka_timestamp_type", F.lit(1)
    )

    gold, _ = prepare_exact_gold(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt=receipt,
    )

    assert gold.count() == len(_events(event_factory))


def test_gold_validates_invalid_duplicate_candidate_before_position_dedup(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    kafka = kafka_frame_factory(spark, deliveries)
    invalid_duplicate = (
        kafka.filter(F.col("offset") == 0)
        .withColumn("offset", F.lit(100).cast("long"))
        .withColumn("value", F.lit(b"{}"))
    )
    bronze = bronze_from_kafka(kafka.unionByName(invalid_duplicate))

    with pytest.raises(GoldFinalizationError, match="quarantine"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


@pytest.mark.parametrize(
    "field",
    ["key", "headers", "timestamp", "timestampType", "payload"],
)
def test_gold_rejects_any_conflicting_kafka_record_field_before_position_dedup(
    spark: SparkSession,
    tmp_path: Path,
    field: str,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    kafka = kafka_frame_factory(spark, deliveries)
    conflicting = kafka.filter(F.col("offset") == 0)
    if field == "key":
        conflicting = conflicting.withColumn("key", F.lit(b"different-lineage"))
    elif field == "headers":
        conflicting = conflicting.withColumn(
            "headers",
            F.transform(
                "headers",
                lambda item: F.struct(
                    item["key"].alias("key"),
                    F.when(item["key"] == "delivery_id", F.lit(b"0" * 64))
                    .otherwise(item["value"])
                    .alias("value"),
                ),
            ),
        )
    elif field == "timestamp":
        conflicting = conflicting.withColumn(
            "timestamp", F.col("timestamp") + F.expr("INTERVAL 1 SECOND")
        )
    elif field == "timestampType":
        conflicting = conflicting.withColumn("timestampType", F.col("timestampType") + 1)
    else:
        conflicting = conflicting.withColumn("value", F.lit(b"{}"))
    bronze = bronze_from_kafka(kafka.unionByName(conflicting))

    with pytest.raises(GoldFinalizationError, match=r"Kafka position|quarantine"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_rejects_same_delivery_or_source_identity_with_different_event(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
    envelope_factory: Any,
) -> None:
    events = _events(event_factory)
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, events)
    canonical_event = deliveries[0]["event"]
    changed = event_factory(
        source_event_id=str(canonical_event["source_event_id"]),
        lineage_id=str(canonical_event["lineage_id"]),
        row_id=0,
        source_row_index=7,
        fixed_logits=(0, 1, 2, 5000, 3),
    )
    conflicting_envelope = envelope_factory(changed)
    assert conflicting_envelope["delivery_id"] == deliveries[0]["delivery_id"]
    bronze = bronze_from_kafka(
        kafka_frame_factory(spark, [deliveries[0], conflicting_envelope, deliveries[1]])
    )

    with pytest.raises(GoldFinalizationError, match=r"delivery_id|source identity"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_gold_rejects_unscheduled_duplicate_ordinal(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    events = _events(event_factory)
    snapshot, receipt, _deliveries = snapshot_factory(tmp_path, events, duplicate_every=None)
    _, _, duplicated_deliveries = snapshot_factory(
        tmp_path / "other",
        events,
        duplicate_every=2,
    )
    bronze = bronze_from_kafka(kafka_frame_factory(spark, duplicated_deliveries))

    with pytest.raises(GoldFinalizationError, match="schedule"):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


@pytest.mark.parametrize("case", ["missing", "unexpected", "lineage", "quarantine"])
def test_gold_rejects_incomplete_or_noncanonical_bronze(
    spark: SparkSession,
    tmp_path: Path,
    case: str,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
    envelope_factory: Any,
) -> None:
    events = _events(event_factory)
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, events)
    if case == "missing":
        observed: list[object] = deliveries[:1]
    elif case == "unexpected":
        extra = event_factory(source_event_id="3" * 64, row_id=2, source_row_index=9)
        observed = [*deliveries, envelope_factory(extra)]
    elif case == "lineage":
        wrong = event_factory(
            source_event_id="1" * 64,
            lineage_id="b" * 64,
            row_id=0,
            source_row_index=7,
        )
        observed = [envelope_factory(wrong), deliveries[1]]
    else:
        observed = ["{", deliveries[1]]
    bronze = bronze_from_kafka(kafka_frame_factory(spark, observed))

    with pytest.raises(
        GoldFinalizationError,
        match=r"quarantine|schedule|canonical snapshot|resume tail",
    ):
        prepare_exact_gold(bronze, snapshot_directory=snapshot, completion_receipt=receipt)


def test_final_publication_requires_a_receipt_path(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))

    with pytest.raises(GoldFinalizationError, match="receipt path"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt,
            output_parent=tmp_path / "gold",
        )

    assert not (tmp_path / "gold").exists()


def test_final_publication_rejects_receipt_inside_snapshot_root(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    receipt_path = snapshot / "completion_receipt.json"
    publish_completion_receipt(receipt_path, receipt, protected_roots=())
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))

    with pytest.raises(GoldFinalizationError, match=r"receipt.*evidence root"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt_path,
            output_parent=tmp_path / "gold",
        )

    assert not (tmp_path / "gold").exists()


def test_finalized_manifest_binds_receipt_digest_without_local_path(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    gold, proof = prepare_exact_gold(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt=receipt,
    )
    contract = finalization_module._read_contract(snapshot, receipt)
    digest = "f" * 64

    manifest = finalization_module._manifest(
        contract,
        proof,
        gold,
        dict.fromkeys(finalization_module._OUTPUT_NAMES, 0),
        "e" * 64,
        ("retain-all-v1", "security-aware-static-v1"),
        completion_receipt_sha256=digest,
        spark_aggregates_sha256="d" * 64,
    )

    assert manifest["completion_receipt_sha256"] == digest
    assert "completion_receipt_path" not in manifest


def test_finalization_fails_closed_on_foreign_publication_claim(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    receipt_path = _published_receipt(tmp_path, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    output = tmp_path / "gold"
    output.mkdir()
    claim = output / ".replay_run_id=run-1.publication-claim"
    claim.mkdir()

    with pytest.raises(GoldFinalizationError, match="publication is in progress"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt_path,
            output_parent=output,
        )

    assert claim.is_dir()
    assert not (output / "replay_run_id=run-1").exists()


def test_foreign_publication_claim_never_replaces_competing_target(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    receipt_path = _published_receipt(tmp_path, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    output = tmp_path / "gold"
    target = output / "replay_run_id=run-1"
    target.mkdir(parents=True)
    sentinel = target / "owner.txt"
    sentinel.write_text("competing publisher", encoding="ascii")
    claim = output / ".replay_run_id=run-1.publication-claim"
    claim.mkdir()

    with pytest.raises(GoldFinalizationError, match="publication is in progress"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt_path,
            output_parent=output,
        )

    assert sentinel.read_text(encoding="ascii") == "competing publisher"
    assert claim.is_dir()


def test_finalized_parent_publication_is_atomic_no_clobber_and_manifest_idempotent(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    if os.name == "nt" and shutil.which("winutils") is None:
        pytest.skip("local Spark Parquet writes on Windows require Hadoop winutils.exe")
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    receipt_path = _published_receipt(tmp_path, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    output = tmp_path / "gold"

    first = finalize_run(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt_path=receipt_path,
        output_parent=output,
        protected_roots=(snapshot,),
    )
    first_manifest_bytes = first.manifest_path.read_bytes()
    second = finalize_run(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt_path=receipt_path,
        output_parent=output,
        protected_roots=(snapshot,),
    )

    assert first.published is True
    assert second.published is False
    assert second.manifest_path.read_bytes() == first_manifest_bytes
    dataset_names = {
        "canonical_gold",
        "policy_decisions",
        "execution_dimension_counts",
        "operational_metrics",
        "predictive_row_metrics",
        "retention_metrics",
        "retention_reason_counts",
    }
    assert all((first.run_directory / name / "_SUCCESS").is_file() for name in dataset_names)
    manifest = json.loads(first_manifest_bytes)
    assert manifest["schema_version"] == "telemetry-finalized-run-v1"
    assert manifest["snapshot_id"] == receipt["snapshot"]["snapshot_id"]
    assert manifest["events_sha256"] == receipt["snapshot"]["events_sha256"]
    assert manifest["completion_receipt_sha256"] == sha256(receipt_path.read_bytes()).hexdigest()
    aggregate_payload = (first.run_directory / "spark_aggregates.json").read_bytes()
    assert manifest["spark_aggregates_sha256"] == sha256(aggregate_payload).hexdigest()
    assert len(manifest["canonical_event_identity_sha256"]) == 64
    assert "completion_receipt_path" not in manifest
    assert "parquet_sha256" not in manifest
    assert set(manifest["outputs"]) == dataset_names
    assert manifest["policy_ids"] == ["retain-all-v1", "security-aware-static-v1"]
    policy_config = Path("configs/retention-policies.toml")
    assert manifest["policy_config_sha256"] == sha256(policy_config.read_bytes()).hexdigest()

    incompatible = dict(receipt)
    incompatible["schedule_identity_sha256"] = "0" * 64
    incompatible_path = tmp_path / "receipts" / "incompatible.json"
    incompatible_path.write_bytes(
        (json.dumps(incompatible, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
    )
    with pytest.raises(GoldFinalizationError):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=incompatible_path,
            output_parent=output,
            protected_roots=(snapshot,),
        )
    assert first.manifest_path.read_bytes() == first_manifest_bytes


def test_existing_finalized_run_rejects_deleted_canonical_parquet(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    if os.name == "nt" and shutil.which("winutils") is None:
        pytest.skip("local Spark Parquet writes on Windows require Hadoop winutils.exe")
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    receipt_path = _published_receipt(tmp_path, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    output = tmp_path / "gold"
    first = finalize_run(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt_path=receipt_path,
        output_parent=output,
        protected_roots=(snapshot,),
    )
    shutil.rmtree(first.run_directory / "canonical_gold")

    with pytest.raises(GoldFinalizationError, match="incompatible output"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt_path,
            output_parent=output,
            protected_roots=(snapshot,),
        )


def test_exact_relation_comparison_rejects_same_count_changed_value(
    spark: SparkSession,
) -> None:
    expected = spark.createDataFrame([(1, "alpha"), (2, "beta")], ["id", "value"])
    corrupted = spark.createDataFrame([(1, "alpha"), (2, "changed")], ["id", "value"])

    assert finalization_module._relations_match_exactly(expected, expected)
    assert not finalization_module._relations_match_exactly(corrupted, expected)


@pytest.mark.parametrize("difference", ["nullable", "contains_null", "metadata"])
def test_exact_relation_comparison_rejects_schema_only_difference(
    spark: SparkSession,
    difference: str,
) -> None:
    from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType

    expected_schema = StructType(
        [StructField("values", ArrayType(IntegerType(), False), False, {"contract": "v1"})]
    )
    if difference == "nullable":
        observed_field = StructField(
            "values", ArrayType(IntegerType(), False), True, {"contract": "v1"}
        )
    elif difference == "contains_null":
        observed_field = StructField(
            "values", ArrayType(IntegerType(), True), False, {"contract": "v1"}
        )
    else:
        observed_field = StructField(
            "values", ArrayType(IntegerType(), False), False, {"contract": "changed"}
        )
    observed_schema = StructType([observed_field])
    expected = spark.createDataFrame([([1, 2],)], expected_schema)
    observed = spark.createDataFrame([([1, 2],)], observed_schema)

    assert not finalization_module._relations_match_exactly(observed, expected)


def test_persisted_relation_comparison_accepts_spark_nullable_roundtrip_schema(
    spark: SparkSession,
) -> None:
    from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType

    expected_schema = StructType(
        [StructField("values", ArrayType(IntegerType(), False), False, {"contract": "v1"})]
    )
    expected = spark.createDataFrame([([1, 2],)], expected_schema)
    observed = spark.createDataFrame([([1, 2],)], expected_schema.toNullable())

    assert finalization_module._persisted_relations_match_exactly(observed, expected)


@pytest.mark.parametrize("difference", ["type", "metadata", "rows"])
def test_persisted_relation_comparison_rejects_logical_corruption(
    spark: SparkSession,
    difference: str,
) -> None:
    from pyspark.sql.types import ArrayType, IntegerType, LongType, StructField, StructType

    expected_schema = StructType(
        [StructField("values", ArrayType(IntegerType(), False), False, {"contract": "v1"})]
    )
    expected = spark.createDataFrame([([1, 2],)], expected_schema)
    if difference == "type":
        observed_schema = StructType(
            [StructField("values", ArrayType(LongType(), True), True, {"contract": "v1"})]
        )
        observed_rows = [([1, 2],)]
    elif difference == "metadata":
        observed_schema = StructType(
            [StructField("values", ArrayType(IntegerType(), True), True, {"contract": "v2"})]
        )
        observed_rows = [([1, 2],)]
    else:
        observed_schema = expected_schema.toNullable()
        observed_rows = [([1, 3],)]
    observed = spark.createDataFrame(observed_rows, observed_schema)

    assert not finalization_module._persisted_relations_match_exactly(observed, expected)


def test_existing_finalized_run_rejects_same_count_changed_policy_value(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    if os.name == "nt" and shutil.which("winutils") is None:
        pytest.skip("local Spark Parquet writes on Windows require Hadoop winutils.exe")
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    receipt_path = _published_receipt(tmp_path, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    output = tmp_path / "gold"
    first = finalize_run(
        bronze,
        snapshot_directory=snapshot,
        completion_receipt_path=receipt_path,
        output_parent=output,
        protected_roots=(snapshot,),
    )
    policy_path = first.run_directory / "policy_decisions"
    replacement = first.run_directory / ".corrupted-policy-decisions"
    corrupted = spark.read.parquet(str(policy_path)).withColumn(
        "reason",
        F.when(
            F.col("policy_id") == "retain-all-v1",
            F.lit("corrupted-retain-all"),
        ).otherwise(F.col("reason")),
    )
    corrupted.write.mode("errorifexists").parquet(str(replacement))
    shutil.rmtree(policy_path)
    os.rename(replacement, policy_path)

    with pytest.raises(GoldFinalizationError, match="incompatible output"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt_path,
            output_parent=output,
            protected_roots=(snapshot,),
        )


def test_finalization_rejects_incompatible_policy_config_before_publication(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    receipt_path = _published_receipt(tmp_path, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    invalid_config = tmp_path / "retention-policies.toml"
    invalid_config.write_text(
        Path("configs/retention-policies.toml")
        .read_text(encoding="utf-8")
        .replace('policy_id = "retain-all-v1"', 'policy_id = "changed"'),
        encoding="utf-8",
    )
    output = tmp_path / "gold"

    with pytest.raises(GoldFinalizationError, match="policy configuration"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt_path,
            output_parent=output,
            protected_roots=(snapshot,),
            policy_config_path=invalid_config,
        )
    assert not output.exists()


def test_finalization_rejects_output_inside_snapshot_evidence_root(
    spark: SparkSession,
    tmp_path: Path,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    snapshot, receipt, deliveries = snapshot_factory(tmp_path, _events(event_factory))
    receipt_path = _published_receipt(tmp_path, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))

    with pytest.raises(GoldFinalizationError, match="evidence root"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt_path,
            output_parent=snapshot / "gold",
        )


@pytest.mark.parametrize("environment_name", ["CUKD_XAI_ROOT", "TINYML_IDS_EVIDENCE_ROOT"])
def test_finalization_always_protects_environment_evidence_roots(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    environment_name: str,
    event_factory: Any,
    snapshot_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    snapshot, receipt, deliveries = snapshot_factory(source_root, _events(event_factory))
    receipt_path = _published_receipt(tmp_path, receipt)
    bronze = bronze_from_kafka(kafka_frame_factory(spark, deliveries))
    evidence_root = tmp_path / "evidence"
    monkeypatch.setenv(environment_name, str(evidence_root))
    output = evidence_root / "gold"

    with pytest.raises(GoldFinalizationError, match="evidence root"):
        finalize_run(
            bronze,
            snapshot_directory=snapshot,
            completion_receipt_path=receipt_path,
            output_parent=output,
        )
    assert not output.exists()
