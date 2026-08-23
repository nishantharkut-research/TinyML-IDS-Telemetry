"""Replay a verified canonical snapshot to JSONL or Kafka."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO, cast

from tinyml_ids_telemetry.contracts.identity import source_event_id
from tinyml_ids_telemetry.contracts.lineage import LineageRecord
from tinyml_ids_telemetry.contracts.schema import (
    SchemaContractError,
    validate_event_mapping,
    validate_lineage_mapping,
    validate_snapshot_manifest_mapping,
)
from tinyml_ids_telemetry.evidence.catalog import ensure_output_outside_evidence_roots
from tinyml_ids_telemetry.evidence.hashes import sha256_handle
from tinyml_ids_telemetry.replay.clock import SyntheticClock
from tinyml_ids_telemetry.replay.pacing import DeliveryPacer
from tinyml_ids_telemetry.replay.schedule import DeliveryEnvelope, iter_replay_schedule
from tinyml_ids_telemetry.replay.sink import JsonlSink, ReplaySink


class ReplayCommandError(ValueError):
    """A snapshot or replay option violates the delivery contract."""


class ReplayCommandErrorGroup(ExceptionGroup):
    """Multiple replay and cleanup failures preserved as inspectable leaves."""


@dataclass(frozen=True, slots=True)
class _VerifiedSnapshot:
    events_handle: BinaryIO
    event_count: int


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _non_negative_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _utc_datetime(value: str) -> datetime:
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an ISO 8601 UTC timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("must be an explicit UTC timestamp")
    return parsed.astimezone(UTC)


def configure_replay_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("replay", help="Replay a verified canonical snapshot")
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-utc", type=_utc_datetime, required=True)
    parser.add_argument("--interval-ms", type=_positive_integer, required=True)
    parser.add_argument("--rate-per-second", type=_positive_integer)
    parser.add_argument("--resume-offset", type=_non_negative_integer, default=0)
    parser.add_argument("--duplicate-every", type=_positive_integer)
    parser.add_argument("--out-of-order-window", type=_positive_integer, default=1)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--jsonl", type=Path)
    destination.add_argument("--kafka-topic")
    parser.add_argument("--bootstrap-servers")
    parser.set_defaults(command_handler=run_replay_command)


@contextmanager
def _verified_snapshot(snapshot_directory: Path) -> Iterator[_VerifiedSnapshot]:
    manifest_path = snapshot_directory / "snapshot_manifest.json"
    events_path = snapshot_directory / "events.jsonl"
    lineages_path = snapshot_directory / "lineages.jsonl"
    if not manifest_path.is_file():
        raise ReplayCommandError(f"Missing snapshot manifest: {manifest_path}")
    try:
        manifest_value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReplayCommandError("snapshot_manifest.json must contain valid UTF-8 JSON") from error
    try:
        manifest = validate_snapshot_manifest_mapping(manifest_value)
    except SchemaContractError as error:
        raise ReplayCommandError(str(error)) from error

    if not events_path.is_file():
        raise ReplayCommandError(f"Missing snapshot events file: {events_path}")
    if not lineages_path.is_file():
        raise ReplayCommandError(f"Missing snapshot lineages file: {lineages_path}")

    lineages = _verify_lineages(lineages_path, manifest.lineages_sha256)
    observed_lineage_ids = tuple(record.lineage_id for record in lineages)
    if len(lineages) != manifest.lineage_count:
        raise ReplayCommandError(
            "snapshot lineage_count mismatch: "
            f"expected {manifest.lineage_count}, got {len(lineages)}"
        )
    if observed_lineage_ids != manifest.lineage_ids:
        raise ReplayCommandError("snapshot manifest lineage_ids do not match lineages.jsonl order")
    for record in lineages:
        if record.expected_rows != manifest.rows_per_lineage:
            raise ReplayCommandError(
                f"lineage {record.lineage_id} expected_rows does not match rows_per_lineage"
            )

    with events_path.open("rb") as events_handle:
        actual_events_hash = sha256_handle(events_handle)
        if actual_events_hash != manifest.events_sha256:
            raise ReplayCommandError(
                "events SHA-256 mismatch: "
                f"expected {manifest.events_sha256}, got {actual_events_hash}"
            )
        _verify_events(
            events_handle,
            manifest.event_count,
            lineages,
            manifest.evaluation_sequence_sha256,
        )
        events_handle.seek(0)
        yield _VerifiedSnapshot(
            events_handle=events_handle,
            event_count=manifest.event_count,
        )


def _verify_lineages(path: Path, expected_hash: str) -> tuple[LineageRecord, ...]:
    with path.open("rb") as handle:
        actual_hash = sha256_handle(handle)
        if actual_hash != expected_hash:
            raise ReplayCommandError(
                f"lineages SHA-256 mismatch: expected {expected_hash}, got {actual_hash}"
            )
        records: list[LineageRecord] = []
        identifiers: set[str] = set()
        for line_number, value in _iter_json_lines(handle, "lineages.jsonl"):
            try:
                record = validate_lineage_mapping(value)
            except SchemaContractError as error:
                raise ReplayCommandError(f"lineages.jsonl line {line_number}: {error}") from error
            if record.lineage_id in identifiers:
                raise ReplayCommandError(f"lineages.jsonl line {line_number}: duplicate lineage_id")
            identifiers.add(record.lineage_id)
            records.append(record)
    return tuple(records)


def _verify_events(
    handle: BinaryIO,
    expected_count: int,
    lineages: tuple[LineageRecord, ...],
    evaluation_sequence_sha256: str,
) -> None:
    records_by_id = {record.lineage_id: record for record in lineages}
    counts = dict.fromkeys(records_by_id, 0)
    evaluation_hashes = {record.lineage_id: sha256() for record in lineages}
    source_event_ids: set[str] = set()
    observed_count = 0
    canonical_lineage_index = 0
    expected_row_id = 0
    for line_number, value in _iter_json_lines(handle, "events.jsonl"):
        observed_count += 1
        try:
            event = validate_event_mapping(value)
        except SchemaContractError as error:
            raise ReplayCommandError(f"events.jsonl line {line_number}: {error}") from error
        lineage_id = cast(str, event["lineage_id"])
        lineage = records_by_id.get(lineage_id)
        if lineage is None:
            raise ReplayCommandError(
                f"events.jsonl line {line_number}: lineage_id is absent from lineages.jsonl"
            )
        if canonical_lineage_index >= len(lineages):
            raise ReplayCommandError(
                f"events.jsonl line {line_number}: event exceeds manifest lineage row counts"
            )
        canonical_lineage = lineages[canonical_lineage_index]
        if lineage_id != canonical_lineage.lineage_id:
            raise ReplayCommandError(
                f"events.jsonl line {line_number}: lineage_id violates manifest lineage order"
            )
        if event["row_id"] != expected_row_id:
            raise ReplayCommandError(
                f"events.jsonl line {line_number}: row_id violates canonical row_id order"
            )
        for event_field, lineage_value in (
            ("source_session_id", lineage.source_session_id),
            ("source_stage_id", lineage.source_stage_id),
            ("board", lineage.board),
            ("student_model", lineage.student_model),
        ):
            if event[event_field] != lineage_value:
                raise ReplayCommandError(
                    f"events.jsonl line {line_number}: {event_field} disagrees with lineage"
                )
        _verify_event_invariants(event, lineage, line_number)
        source_event_id = cast(str, event["source_event_id"])
        if source_event_id in source_event_ids:
            raise ReplayCommandError(f"events.jsonl line {line_number}: duplicate source_event_id")
        source_event_ids.add(source_event_id)
        counts[lineage_id] += 1
        evaluation_hashes[lineage_id].update(
            f"{event['source_row_index']}|{event['true_class_id']}\n".encode("ascii")
        )
        expected_row_id += 1
        if expected_row_id == canonical_lineage.expected_rows:
            canonical_lineage_index += 1
            expected_row_id = 0

    if observed_count != expected_count:
        raise ReplayCommandError(
            f"snapshot event_count mismatch: expected {expected_count}, got {observed_count}"
        )
    for lineage in lineages:
        observed = counts[lineage.lineage_id]
        if observed != lineage.expected_rows:
            raise ReplayCommandError(
                f"lineage {lineage.lineage_id} event count mismatch: "
                f"expected {lineage.expected_rows}, got {observed}"
            )
        if evaluation_hashes[lineage.lineage_id].hexdigest() != evaluation_sequence_sha256:
            raise ReplayCommandError(
                f"lineage {lineage.lineage_id} evaluation_sequence_sha256 "
                "disagrees with snapshot manifest"
            )


def _verify_event_invariants(
    event: dict[str, object],
    lineage: LineageRecord,
    line_number: int,
) -> None:
    row_id = cast(int, event["row_id"])
    expected_source_event_id = source_event_id(
        protocol_id=lineage.wireless_protocol_id,
        export_id=lineage.export_id,
        wireless_bundle_id=lineage.wireless_bundle_id,
        source_session_id=lineage.source_session_id,
        source_stage_id=lineage.source_stage_id,
        row_id=row_id,
    )
    if event["source_event_id"] != expected_source_event_id:
        raise ReplayCommandError(
            f"events.jsonl line {line_number}: source_event_id disagrees with lineage identity"
        )

    fixed_logits = cast(list[int], event["fixed_logits"])
    predicted_class_id = cast(int, event["predicted_class_id"])
    first_argmax = max(range(len(fixed_logits)), key=fixed_logits.__getitem__)
    if predicted_class_id != first_argmax:
        raise ReplayCommandError(
            f"events.jsonl line {line_number}: predicted_class_id must equal "
            "the first fixed-logit argmax"
        )
    if event["fixed_predicted_class_id"] != predicted_class_id:
        raise ReplayCommandError(
            f"events.jsonl line {line_number}: fixed_predicted_class_id "
            "must equal predicted_class_id"
        )

    true_class_id = cast(int, event["true_class_id"])
    for name_field, class_id in (
        ("true_class_name", true_class_id),
        ("predicted_class_name", predicted_class_id),
    ):
        if event[name_field] != lineage.class_names[class_id]:
            raise ReplayCommandError(
                f"events.jsonl line {line_number}: {name_field} disagrees with lineage class_names"
            )

    for event_field, lineage_value in (
        ("normal_class_id", lineage.normal_class_id),
        ("logit_frac_bits", lineage.logit_frac_bits),
    ):
        if event[event_field] != lineage_value:
            raise ReplayCommandError(
                f"events.jsonl line {line_number}: {event_field} disagrees with lineage"
            )

    is_predicted_attack = predicted_class_id != lineage.normal_class_id
    if event["is_predicted_attack"] is not is_predicted_attack:
        raise ReplayCommandError(
            f"events.jsonl line {line_number}: is_predicted_attack is not derived correctly"
        )

    top_two = sorted(fixed_logits, reverse=True)[:2]
    normalized_margin = (top_two[0] - top_two[1]) / (1 << lineage.logit_frac_bits)
    if event["normalized_logit_margin"] != normalized_margin:
        raise ReplayCommandError(
            f"events.jsonl line {line_number}: normalized_logit_margin is not derived exactly"
        )

    if event["total_us"] != cast(int, event["preprocess_us"]) + cast(int, event["inference_us"]):
        raise ReplayCommandError(
            f"events.jsonl line {line_number}: total_us must equal preprocess_us plus inference_us"
        )


def _iter_json_lines(handle: BinaryIO, filename: str) -> Iterator[tuple[int, object]]:
    handle.seek(0)
    for line_number, line in enumerate(handle, start=1):
        try:
            yield line_number, json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ReplayCommandError(
                f"{filename} line {line_number} must contain valid UTF-8 JSON"
            ) from error


def _iter_source_events(snapshot: _VerifiedSnapshot) -> Iterator[dict[str, object]]:
    for _, value in _iter_json_lines(snapshot.events_handle, "events.jsonl"):
        yield cast(dict[str, object], value)


def _resolve_jsonl_destination(output_path: Path, snapshot_directory: Path) -> Path:
    protected_roots = [snapshot_directory.expanduser().resolve()]
    for variable in ("CUKD_XAI_ROOT", "TINYML_IDS_EVIDENCE_ROOT"):
        value = os.environ.get(variable)
        if value is not None and value.strip():
            protected_roots.append(Path(value).expanduser().resolve())
    return ensure_output_outside_evidence_roots(output_path, tuple(protected_roots))


def _write_deliveries(
    sink: ReplaySink,
    deliveries: Iterable[DeliveryEnvelope],
) -> int:
    delivery_count = 0
    for envelope in deliveries:
        sink.write(envelope)
        delivery_count += 1
    return delivery_count


def _iter_paced_deliveries(
    deliveries: Iterable[DeliveryEnvelope],
    pacer: DeliveryPacer | None,
) -> Iterator[DeliveryEnvelope]:
    for delivery_index, envelope in enumerate(deliveries):
        if pacer is not None:
            pacer.wait_for_delivery(delivery_index)
        yield envelope


def _write_kafka(
    deliveries: Iterable[DeliveryEnvelope],
    *,
    topic: str,
    bootstrap_servers: str,
) -> int:
    from tinyml_ids_telemetry.replay.kafka import KafkaSink

    try:
        sink = KafkaSink(topic=topic, bootstrap_servers=bootstrap_servers)
        try:
            delivery_count = _write_deliveries(sink, deliveries)
        except BaseException as replay_error:
            try:
                sink.close()
            except BaseException as close_error:
                message = "Kafka replay failed and cleanup failed"
                if isinstance(replay_error, Exception) and isinstance(close_error, Exception):
                    raise ReplayCommandErrorGroup(
                        message,
                        [replay_error, close_error],
                    ) from replay_error
                raise BaseExceptionGroup(message, [replay_error, close_error]) from replay_error
            raise
        sink.close()
        return delivery_count
    except (ReplayCommandError, ReplayCommandErrorGroup):
        raise
    except ExceptionGroup as error:
        raise ReplayCommandErrorGroup("Kafka replay failed", [error]) from error
    except Exception as error:
        raise ReplayCommandError(f"Kafka replay failed: {error}") from error


def run_replay_command(arguments: argparse.Namespace) -> int:
    """Verify a snapshot, replay it, and print a compact completion summary."""
    if not arguments.run_id.strip():
        raise ReplayCommandError("--run-id must be non-empty")
    if arguments.jsonl is not None and arguments.bootstrap_servers is not None:
        raise ReplayCommandError(
            "--bootstrap-servers cannot be used with --jsonl; it is only valid for Kafka"
        )

    with _verified_snapshot(arguments.snapshot) as snapshot:
        try:
            interval = timedelta(milliseconds=arguments.interval_ms)
        except OverflowError as error:
            raise ReplayCommandError("--interval-ms is too large") from error
        clock = SyntheticClock(start_utc=arguments.start_utc, interval=interval)
        deliveries: Iterable[DeliveryEnvelope]
        if arguments.resume_offset >= snapshot.event_count:
            deliveries = ()
        else:
            try:
                clock.at_offset(snapshot.event_count - 1)
            except OverflowError as error:
                raise ReplayCommandError(
                    "synthetic replay timestamp exceeds datetime range"
                ) from error
            deliveries = iter_replay_schedule(
                _iter_source_events(snapshot),
                replay_run_id=arguments.run_id,
                clock=clock,
                resume_offset=arguments.resume_offset,
                duplicate_every=arguments.duplicate_every,
                out_of_order_window=arguments.out_of_order_window,
            )
        pacer = (
            None
            if arguments.rate_per_second is None
            else DeliveryPacer(rate_per_second=arguments.rate_per_second)
        )
        deliveries = _iter_paced_deliveries(deliveries, pacer)
        try:
            if arguments.jsonl is not None:
                jsonl_destination = _resolve_jsonl_destination(
                    arguments.jsonl,
                    arguments.snapshot,
                )
                with JsonlSink(jsonl_destination) as sink:
                    delivery_count = _write_deliveries(sink, deliveries)
                destination = "jsonl"
            else:
                delivery_count = _write_kafka(
                    deliveries,
                    topic=arguments.kafka_topic,
                    bootstrap_servers=arguments.bootstrap_servers or "localhost:9092",
                )
                destination = "kafka"
        except ReplayCommandError:
            raise
        except ValueError as error:
            raise ReplayCommandError(str(error)) from error

        summary: dict[str, object] = {
            "schema_version": "telemetry-replay-summary-v1",
            "status": "completed",
            "delivery_semantics": "at-least-once",
            "replay_run_id": arguments.run_id,
            "destination": destination,
            "rate_per_second": arguments.rate_per_second,
            "pacing_mode": ("unpaced" if arguments.rate_per_second is None else "controlled"),
            "resume_offset": arguments.resume_offset,
            "source_event_count": max(snapshot.event_count - arguments.resume_offset, 0),
            "delivery_count": delivery_count,
        }
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0
