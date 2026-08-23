"""Immutable deterministic completion receipts for replay runs."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import cast

from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceContractError,
    ensure_output_outside_evidence_roots,
)

_DIGEST = re.compile(r"[0-9a-f]{64}")
_TOP_LEVEL_KEYS = {
    "schema_version",
    "status",
    "replay_run_id",
    "snapshot",
    "replay_options",
    "destination",
    "kafka_topic",
    "expected_source_event_count",
    "expected_delivery_count",
    "observed_delivery_count",
    "max_replay_timestamp_skew_ms",
    "schedule_identity_sha256",
    "delivery_semantics",
    "completion_scope",
}
_SNAPSHOT_KEYS = {
    "snapshot_id",
    "sealed_report_sha256",
    "events_sha256",
    "lineages_sha256",
    "evaluation_sequence_sha256",
    "snapshot_event_count",
    "lineage_ids",
}
_OPTION_KEYS = {
    "start_utc",
    "interval_ms",
    "rate_per_second",
    "resume_offset",
    "duplicate_every",
    "out_of_order_window",
}
_COMPLETION_SCOPE = (
    "sink close completed after all sends were acknowledged; this receipt does not "
    "independently prove broker persistence"
)


class ReplayReceiptError(ValueError):
    """Raised when a replay completion receipt is invalid or incompatible."""


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def expected_delivery_count(
    snapshot_event_count: int,
    resume_offset: int,
    duplicate_every: int | None,
) -> int:
    """Return the exact delivery cardinality of the deterministic schedule."""
    for name, value in (
        ("snapshot_event_count", snapshot_event_count),
        ("resume_offset", resume_offset),
    ):
        if type(value) is not int or value < 0:
            raise ReplayReceiptError(f"{name} must be a non-negative integer")
    if duplicate_every is not None and (type(duplicate_every) is not int or duplicate_every <= 0):
        raise ReplayReceiptError("duplicate_every must be a positive integer or null")
    retained_sources = max(snapshot_event_count - resume_offset, 0)
    if duplicate_every is None or retained_sources == 0:
        return retained_sources
    duplicate_count = (
        snapshot_event_count // duplicate_every
        - min(resume_offset, snapshot_event_count) // duplicate_every
    )
    return retained_sources + duplicate_count


def _schedule_identity(
    replay_run_id: str,
    snapshot: Mapping[str, object],
    replay_options: Mapping[str, object],
    destination: str,
    kafka_topic: str | None,
) -> str:
    value = {
        "schema_version": "telemetry-replay-schedule-v1",
        "replay_run_id": replay_run_id,
        "snapshot": dict(snapshot),
        "replay_options": dict(replay_options),
        "delivery_config": {
            "destination": destination,
            "kafka_topic": kafka_topic,
        },
    }
    return sha256(_canonical_bytes(value)).hexdigest()


def build_completion_receipt(
    *,
    replay_run_id: str,
    snapshot_id: str,
    sealed_report_sha256: str,
    events_sha256: str,
    lineages_sha256: str,
    evaluation_sequence_sha256: str,
    snapshot_event_count: int,
    snapshot_lineage_ids: tuple[str, ...],
    start_utc: str,
    interval_ms: int,
    rate_per_second: int | None,
    resume_offset: int,
    duplicate_every: int | None,
    out_of_order_window: int,
    destination: str,
    kafka_topic: str | None,
    observed_delivery_count: int,
) -> dict[str, object]:
    """Build and fully validate one completed immutable replay receipt."""
    snapshot = {
        "snapshot_id": snapshot_id,
        "sealed_report_sha256": sealed_report_sha256,
        "events_sha256": events_sha256,
        "lineages_sha256": lineages_sha256,
        "evaluation_sequence_sha256": evaluation_sequence_sha256,
        "snapshot_event_count": snapshot_event_count,
        "lineage_ids": sorted(snapshot_lineage_ids),
    }
    replay_options = {
        "start_utc": start_utc,
        "interval_ms": interval_ms,
        "rate_per_second": rate_per_second,
        "resume_offset": resume_offset,
        "duplicate_every": duplicate_every,
        "out_of_order_window": out_of_order_window,
    }
    expected_sources = max(snapshot_event_count - resume_offset, 0)
    expected_deliveries = expected_delivery_count(
        snapshot_event_count,
        resume_offset,
        duplicate_every,
    )
    receipt: dict[str, object] = {
        "schema_version": "telemetry-replay-completion-v1",
        "status": "completed",
        "replay_run_id": replay_run_id,
        "snapshot": snapshot,
        "replay_options": replay_options,
        "destination": destination,
        "kafka_topic": kafka_topic,
        "expected_source_event_count": expected_sources,
        "expected_delivery_count": expected_deliveries,
        "observed_delivery_count": observed_delivery_count,
        "max_replay_timestamp_skew_ms": (out_of_order_window - 1) * interval_ms,
        "schedule_identity_sha256": _schedule_identity(
            replay_run_id,
            snapshot,
            replay_options,
            destination,
            kafka_topic,
        ),
        "delivery_semantics": "at-least-once",
        "completion_scope": _COMPLETION_SCOPE,
    }
    validate_completion_receipt(receipt)
    return receipt


def _require_mapping(value: object, name: str, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ReplayReceiptError(f"{name} must contain exactly the required fields")
    return dict(cast(Mapping[str, object], value))


def _require_digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ReplayReceiptError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _require_non_negative_int(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ReplayReceiptError(f"{name} must be a non-negative integer")
    return value


def validate_completion_receipt(value: object) -> dict[str, object]:
    """Validate strict fields, cardinalities, and deterministic schedule identity."""
    receipt = _require_mapping(value, "completion receipt", _TOP_LEVEL_KEYS)
    if receipt["schema_version"] != "telemetry-replay-completion-v1":
        raise ReplayReceiptError("unsupported completion receipt schema_version")
    if receipt["status"] != "completed":
        raise ReplayReceiptError("completion receipt status must be completed")
    run_id = receipt["replay_run_id"]
    if not isinstance(run_id, str) or not run_id.strip():
        raise ReplayReceiptError("replay_run_id must be a non-empty string")
    snapshot = _require_mapping(receipt["snapshot"], "snapshot", _SNAPSHOT_KEYS)
    for name in (
        "snapshot_id",
        "sealed_report_sha256",
        "events_sha256",
        "lineages_sha256",
        "evaluation_sequence_sha256",
    ):
        _require_digest(snapshot[name], f"snapshot.{name}")
    snapshot_count = _require_non_negative_int(
        snapshot["snapshot_event_count"], "snapshot.snapshot_event_count"
    )
    lineage_ids = snapshot["lineage_ids"]
    if not isinstance(lineage_ids, list) or not lineage_ids:
        raise ReplayReceiptError("snapshot.lineage_ids must be a non-empty list")
    if len(set(lineage_ids)) != len(lineage_ids):
        raise ReplayReceiptError("snapshot.lineage_ids must be unique")
    if lineage_ids != sorted(lineage_ids):
        raise ReplayReceiptError("snapshot.lineage_ids must be sorted")
    for lineage_id in lineage_ids:
        _require_digest(lineage_id, "snapshot.lineage_ids item")

    options = _require_mapping(receipt["replay_options"], "replay_options", _OPTION_KEYS)
    start_utc = options["start_utc"]
    if not isinstance(start_utc, str):
        raise ReplayReceiptError("replay_options.start_utc must be canonical UTC")
    try:
        parsed_start = datetime.fromisoformat(start_utc.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise ReplayReceiptError("replay_options.start_utc must be canonical UTC") from error
    if parsed_start.tzinfo is None or parsed_start.utcoffset() != UTC.utcoffset(parsed_start):
        raise ReplayReceiptError("replay_options.start_utc must be canonical UTC")
    canonical_start = (
        parsed_start.astimezone(UTC)
        .isoformat(timespec="microseconds")
        .replace(".000000+00:00", "Z")
        .replace("+00:00", "Z")
    )
    if start_utc != canonical_start:
        raise ReplayReceiptError("replay_options.start_utc must be canonical UTC")
    interval_ms = _require_non_negative_int(options["interval_ms"], "interval_ms")
    if interval_ms == 0:
        raise ReplayReceiptError("interval_ms must be positive")
    resume_offset = _require_non_negative_int(options["resume_offset"], "resume_offset")
    out_of_order_window = _require_non_negative_int(
        options["out_of_order_window"], "out_of_order_window"
    )
    if out_of_order_window == 0:
        raise ReplayReceiptError("out_of_order_window must be positive")
    duplicate_every = options["duplicate_every"]
    rate_per_second = options["rate_per_second"]
    for optional_name, optional_value in (
        ("duplicate_every", duplicate_every),
        ("rate_per_second", rate_per_second),
    ):
        if optional_value is not None and (type(optional_value) is not int or optional_value <= 0):
            raise ReplayReceiptError(f"{optional_name} must be a positive integer or null")

    destination = receipt["destination"]
    topic = receipt["kafka_topic"]
    if destination not in {"jsonl", "kafka"}:
        raise ReplayReceiptError("destination must be jsonl or kafka")
    if destination == "kafka" and (not isinstance(topic, str) or not topic):
        raise ReplayReceiptError("kafka_topic is required for Kafka replay")
    if destination == "jsonl" and topic is not None:
        raise ReplayReceiptError("kafka_topic must be null for JSONL replay")

    receipt_expected_sources = _require_non_negative_int(
        receipt["expected_source_event_count"], "expected_source_event_count"
    )
    receipt_expected_deliveries = _require_non_negative_int(
        receipt["expected_delivery_count"], "expected_delivery_count"
    )
    observed_deliveries = _require_non_negative_int(
        receipt["observed_delivery_count"], "observed_delivery_count"
    )
    maximum_skew_ms = _require_non_negative_int(
        receipt["max_replay_timestamp_skew_ms"], "max_replay_timestamp_skew_ms"
    )
    expected_sources = max(snapshot_count - resume_offset, 0)
    if receipt_expected_sources != expected_sources:
        raise ReplayReceiptError("expected_source_event_count disagrees with replay options")
    expected_deliveries = expected_delivery_count(
        snapshot_count,
        resume_offset,
        cast(int | None, duplicate_every),
    )
    if receipt_expected_deliveries != expected_deliveries:
        raise ReplayReceiptError("expected_delivery_count disagrees with replay options")
    if observed_deliveries != expected_deliveries:
        raise ReplayReceiptError("observed_delivery_count disagrees with expected delivery count")
    expected_skew = (out_of_order_window - 1) * interval_ms
    if maximum_skew_ms != expected_skew:
        raise ReplayReceiptError("max_replay_timestamp_skew_ms disagrees with replay options")
    expected_identity = _schedule_identity(
        run_id,
        snapshot,
        options,
        destination,
        cast(str | None, topic),
    )
    if receipt["schedule_identity_sha256"] != expected_identity:
        raise ReplayReceiptError("schedule_identity_sha256 disagrees with receipt content")
    if receipt["delivery_semantics"] != "at-least-once":
        raise ReplayReceiptError("delivery_semantics must be at-least-once")
    if receipt["completion_scope"] != _COMPLETION_SCOPE:
        raise ReplayReceiptError("completion_scope does not match the receipt contract")
    return receipt


def load_completion_receipt(path: Path) -> tuple[dict[str, object], str]:
    """Load one canonical, validated completion receipt and its byte digest."""
    if not path.is_file():
        raise ReplayReceiptError("completion receipt path must be a regular file")
    try:
        payload = path.read_bytes()
        value = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReplayReceiptError("completion receipt must be valid UTF-8 JSON") from error
    validated = validate_completion_receipt(value)
    if payload != _canonical_bytes(validated):
        raise ReplayReceiptError("completion receipt bytes are not canonical")
    return validated, sha256(payload).hexdigest()


def publish_completion_receipt(
    destination: Path,
    receipt: Mapping[str, object],
    *,
    protected_roots: tuple[Path, ...],
) -> bool:
    """Publish without overwrite; return false only for an identical existing receipt."""
    validated = validate_completion_receipt(receipt)
    try:
        target = ensure_output_outside_evidence_roots(destination, protected_roots)
    except EvidenceContractError as error:
        raise ReplayReceiptError("receipt destination is inside an evidence root") from error
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_bytes(validated)
    if target.exists():
        if target.is_file() and target.read_bytes() == payload:
            return False
        raise ReplayReceiptError("completion receipt destination contains incompatible content")

    temporary_name: str | None = None
    try:
        with NamedTemporaryFile(
            mode="wb",
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
            delete=False,
        ) as handle:
            temporary_name = handle.name
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_name, target)
        except FileExistsError:
            if target.is_file() and target.read_bytes() == payload:
                return False
            raise ReplayReceiptError(
                "completion receipt destination contains incompatible content"
            ) from None
        return True
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)
