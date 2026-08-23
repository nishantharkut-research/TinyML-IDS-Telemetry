"""Local Spark fixtures and canonical telemetry builders."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from tinyml_ids_telemetry.contracts.event import TelemetryEvent
from tinyml_ids_telemetry.contracts.identity import lineage_id, source_event_id
from tinyml_ids_telemetry.contracts.lineage import LineageRecord
from tinyml_ids_telemetry.replay.clock import SyntheticClock
from tinyml_ids_telemetry.replay.receipt import (
    build_completion_receipt,
    expected_delivery_count,
)
from tinyml_ids_telemetry.replay.schedule import (
    DeliveryEnvelope,
    iter_replay_schedule,
)


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    """Provide one isolated real local Spark session for job tests."""
    from pyspark.sql import SparkSession

    from jobs.spark.common import SparkContractError
    from jobs.spark.startup import require_java_17

    try:
        require_java_17()
    except SparkContractError as error:
        pytest.skip(str(error))
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tinyml-ids-telemetry-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def telemetry_event(
    *,
    source_event_id: str = "1" * 64,
    lineage_id: str = "a" * 64,
    student_model: str = "student_A",
    status: str = "OK",
    true_class_id: int = 3,
    predicted_class_id: int = 3,
    fixed_logits: tuple[int, ...] = (0, 1, 2, 4000, 3),
    attempts: int = 1,
    response_timeout_count: int = 0,
    ignored_datagram_count: int = 0,
    row_id: int = 0,
    source_row_index: int = 7,
) -> TelemetryEvent:
    """Build a valid event with overridable online-policy inputs."""
    return TelemetryEvent(
        source_event_id=source_event_id,
        lineage_id=lineage_id,
        source_session_id="session-1",
        source_stage_id="stage-1",
        row_id=row_id,
        source_row_index=source_row_index,
        board="esp32c3",
        student_model=student_model,
        status=status,
        true_class_id=true_class_id,
        predicted_class_id=predicted_class_id,
        fixed_predicted_class_id=predicted_class_id,
        fp32_predicted_class_id=predicted_class_id,
        class_names=("Blackhole", "Flooding", "Grayhole", "Normal", "TDMA"),
        normal_class_id=3,
        fixed_logits=fixed_logits,
        logit_frac_bits=9,
        preprocess_us=19,
        inference_us=143,
        total_us=162,
        attempts=attempts,
        response_timeout_count=response_timeout_count,
        ignored_datagram_count=ignored_datagram_count,
        ignored_wrong_endpoint_count=0,
        ignored_bad_length_count=0,
        ignored_non_ascii_count=0,
        ignored_malformed_count=0,
        ignored_bad_prefix_count=0,
        ignored_bad_identity_count=0,
        ignored_bad_attempt_count=0,
        ignored_bad_inner_hex_count=0,
        ignored_bad_crc_count=0,
        ignored_bad_inner_text_count=0,
        ignored_wrong_session_count=0,
        ignored_wrong_stage_count=0,
        ignored_wrong_transaction_count=0,
        ignored_wrong_attempt_count=0,
        successful_request_bytes=238,
        request_datagram_bytes_sent=238,
        response_bytes=196,
        host_observed_datagram_rtt_us=9580,
        transaction_elapsed_us=9580,
    )


def envelope_mapping(
    event: TelemetryEvent | Mapping[str, object] | None = None,
    *,
    replay_run_id: str = "run-1",
) -> dict[str, object]:
    """Build a deterministic replay envelope mapping."""
    event_mapping = (
        telemetry_event().to_dict()
        if event is None
        else event.to_dict()
        if isinstance(event, TelemetryEvent)
        else dict(event)
    )
    envelope: DeliveryEnvelope = next(
        iter_replay_schedule(
            [event_mapping],
            replay_run_id=replay_run_id,
            clock=SyntheticClock(
                start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
                interval=timedelta(seconds=1),
            ),
        )
    )
    return envelope.to_dict()


def kafka_frame(
    spark: Any,
    payloads: list[bytes | str | Mapping[str, object]],
    *,
    key_override: bytes | None = None,
    headers_override: list[tuple[str, bytes]] | None = None,
) -> Any:
    """Create a Kafka-shaped DataFrame with stable metadata."""
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    kafka_input_schema = StructType(
        [
            StructField("key", BinaryType(), True),
            StructField("value", BinaryType(), True),
            StructField("topic", StringType(), False),
            StructField("partition", IntegerType(), False),
            StructField("offset", LongType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("timestampType", IntegerType(), False),
            StructField(
                "headers",
                ArrayType(
                    StructType(
                        [
                            StructField("key", StringType(), False),
                            StructField("value", BinaryType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    rows: list[tuple[Any, ...]] = []
    for offset, payload in enumerate(payloads):
        mapping = payload if isinstance(payload, Mapping) else None
        if isinstance(payload, Mapping):
            value = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
        elif isinstance(payload, str):
            value = payload.encode("utf-8")
        else:
            value = payload
        event = mapping.get("event") if mapping is not None else None
        event_mapping = event if isinstance(event, Mapping) else {}
        key = key_override or str(event_mapping.get("lineage_id", "a" * 64)).encode("ascii")
        headers = headers_override or [
            (
                "envelope_schema",
                str(
                    mapping.get("schema_version", "telemetry-replay-envelope-v1")
                    if mapping is not None
                    else "telemetry-replay-envelope-v1"
                ).encode("utf-8"),
            ),
            (
                "replay_run_id",
                str(
                    mapping.get("replay_run_id", "run-1") if mapping is not None else "run-1"
                ).encode("utf-8"),
            ),
            (
                "source_event_id",
                str(event_mapping.get("source_event_id", "1" * 64)).encode("utf-8"),
            ),
            (
                "delivery_id",
                str(
                    mapping.get("delivery_id", "2" * 64) if mapping is not None else "2" * 64
                ).encode("utf-8"),
            ),
        ]
        rows.append(
            (
                key,
                value,
                "telemetry",
                0,
                offset,
                datetime(2026, 8, 23, 12, 0, offset, tzinfo=UTC),
                0,
                headers,
            )
        )
    return spark.createDataFrame(rows, kafka_input_schema)


@pytest.fixture
def event_factory() -> Any:
    return telemetry_event


@pytest.fixture
def envelope_factory() -> Any:
    return envelope_mapping


@pytest.fixture
def kafka_frame_factory() -> Any:
    return kafka_frame


def write_stream_json(path: Path, rows: list[Mapping[str, object]]) -> None:
    """Publish one input file atomically enough for a local file stream test."""
    path.mkdir(parents=True, exist_ok=True)
    target = path / f"batch-{len(tuple(path.glob('batch-*.json'))):04d}.json"
    target.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _fixture_lineage(events: list[TelemetryEvent], index: int) -> LineageRecord:
    first = events[0]

    def digest(name: str) -> str:
        return hashlib.sha256(f"{name}-{index}".encode("ascii")).hexdigest()

    wireless_protocol_id = f"fixture-wireless-{index}"
    export_id = digest("export")
    wireless_bundle_id = digest("bundle")
    identifier = lineage_id(
        protocol_id=wireless_protocol_id,
        export_id=export_id,
        wireless_bundle_id=wireless_bundle_id,
        source_session_id=first.source_session_id,
        source_stage_id=first.source_stage_id,
    )
    return LineageRecord(
        lineage_id=identifier,
        board=first.board,
        student_model=first.student_model,
        seed=42,
        experiment_protocol_id="fixture-experiment-v1",
        wireless_protocol_id=wireless_protocol_id,
        dataset_protocol_id="fixture-dataset-v1",
        dataset_sha256=digest("dataset"),
        test_split_sha256=digest("test-split"),
        scaler_sha256=digest("scaler"),
        export_id=export_id,
        wireless_bundle_id=wireless_bundle_id,
        model_file_sha256=digest("model-file"),
        model_artifact_sha256=digest("model-artifact"),
        firmware_binary_sha256=digest("firmware"),
        source_session_id=first.source_session_id,
        source_stage_id=first.source_stage_id,
        expected_rows=len(events),
        class_names=first.class_names,
        normal_class_id=first.normal_class_id,
        logit_frac_bits=first.logit_frac_bits,
        mcu_csv_sha256=digest("mcu-csv"),
        reference_csv_sha256=digest("reference-csv"),
        completion_manifest_sha256=digest("completion"),
        compile_record_sha256=digest("compile"),
        mcu_relative_path=f"fixture/{index}/mcu.csv",
        reference_relative_path=f"fixture/{index}/reference.csv",
    )


def snapshot_and_receipt(
    root: Path,
    events: list[TelemetryEvent],
    *,
    duplicate_every: int | None = None,
    out_of_order_window: int = 1,
    resume_offset: int = 0,
    start_utc: datetime | None = None,
) -> tuple[Path, dict[str, object], list[dict[str, object]]]:
    """Create one hash-consistent canonical snapshot and its completed replay receipt."""
    start_utc = start_utc or datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    snapshot = root / "snapshot"
    snapshot.mkdir(parents=True)
    grouped: dict[str, list[TelemetryEvent]] = {}
    for event in events:
        grouped.setdefault(event.lineage_id, []).append(event)
    lineage_groups = [
        (_fixture_lineage(group, index), group) for index, group in enumerate(grouped.values())
    ]
    lineage_groups.sort(key=lambda item: item[0].lineage_id)
    row_counts = {len(group) for _, group in lineage_groups}
    if len(row_counts) != 1:
        raise ValueError("snapshot fixture lineages must have equal row counts")
    event_mappings: list[dict[str, object]] = []
    for lineage, group in lineage_groups:
        for event in group:
            mapping = event.to_dict()
            mapping["lineage_id"] = lineage.lineage_id
            mapping["source_event_id"] = source_event_id(
                protocol_id=lineage.wireless_protocol_id,
                export_id=lineage.export_id,
                wireless_bundle_id=lineage.wireless_bundle_id,
                source_session_id=lineage.source_session_id,
                source_stage_id=lineage.source_stage_id,
                row_id=event.row_id,
            )
            event_mappings.append(mapping)
    event_bytes = b"".join(
        (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        for event in event_mappings
    )
    lineage_records = [lineage.to_dict() for lineage, _ in lineage_groups]
    lineage_ids = [lineage.lineage_id for lineage, _ in lineage_groups]
    lineage_bytes = b"".join(
        (json.dumps(lineage, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        for lineage in lineage_records
    )
    (snapshot / "events.jsonl").write_bytes(event_bytes)
    (snapshot / "lineages.jsonl").write_bytes(lineage_bytes)
    events_hash = hashlib.sha256(event_bytes).hexdigest()
    lineages_hash = hashlib.sha256(lineage_bytes).hexdigest()
    sealed_hash = "b" * 64
    identity = {
        "schema_version": "telemetry-snapshot-v1",
        "sealed_report_sha256": sealed_hash,
        "events_sha256": events_hash,
        "lineages_sha256": lineages_hash,
        "event_count": len(events),
    }
    snapshot_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    rows_per_lineage = row_counts.pop()
    evaluation_hashes: set[str] = set()
    for _, group in lineage_groups:
        evaluation_digest = hashlib.sha256()
        for event in group:
            evaluation_digest.update(
                f"{event.source_row_index}|{event.true_class_id}\n".encode("ascii")
            )
        evaluation_hashes.add(evaluation_digest.hexdigest())
    if len(evaluation_hashes) != 1:
        raise ValueError("snapshot fixture lineages must share one evaluation sequence")
    evaluation_hash = evaluation_hashes.pop()
    manifest = {
        "schema_version": "telemetry-snapshot-v1",
        "snapshot_id": snapshot_id,
        "created_at_utc": "2026-08-23T12:00:00Z",
        "sealed_report_sha256": sealed_hash,
        "event_count": len(events),
        "lineage_count": len(lineage_ids),
        "execution_lineage_count": len(lineage_ids),
        "rows_per_lineage": rows_per_lineage,
        "evaluation_sequence_sha256": evaluation_hash,
        "events_sha256": events_hash,
        "lineages_sha256": lineages_hash,
        "lineage_ids": lineage_ids,
        "source_interpretation": "fixture hardware executions over one shared test sequence",
        "event_time_boundary": "synthetic replay time only",
    }
    (snapshot / "snapshot_manifest.json").write_text(
        json.dumps(manifest, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    deliveries = list(
        iter_replay_schedule(
            event_mappings,
            replay_run_id="run-1",
            clock=SyntheticClock(
                start_utc=start_utc,
                interval=timedelta(milliseconds=250),
            ),
            duplicate_every=duplicate_every,
            out_of_order_window=out_of_order_window,
            resume_offset=resume_offset,
        )
    )
    receipt = build_completion_receipt(
        replay_run_id="run-1",
        snapshot_id=snapshot_id,
        sealed_report_sha256=sealed_hash,
        events_sha256=events_hash,
        lineages_sha256=lineages_hash,
        evaluation_sequence_sha256=evaluation_hash,
        snapshot_event_count=len(events),
        snapshot_lineage_ids=tuple(lineage_ids),
        start_utc=DeliveryEnvelope.format_event_time(start_utc),
        interval_ms=250,
        rate_per_second=None,
        resume_offset=resume_offset,
        duplicate_every=duplicate_every,
        out_of_order_window=out_of_order_window,
        destination="kafka",
        kafka_topic="telemetry",
        observed_delivery_count=expected_delivery_count(
            len(events), resume_offset, duplicate_every
        ),
    )
    return snapshot, receipt, [delivery.to_dict() for delivery in deliveries]


@pytest.fixture
def snapshot_factory() -> Any:
    return snapshot_and_receipt
