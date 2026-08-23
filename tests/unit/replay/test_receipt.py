"""Immutable replay-run completion receipt contracts."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

import tinyml_ids_telemetry.replay.receipt as receipt_module
from tinyml_ids_telemetry.replay.receipt import (
    ReplayReceiptError,
    build_completion_receipt,
    expected_delivery_count,
    publish_completion_receipt,
    validate_completion_receipt,
)


def _receipt(*, duplicate_every: int | None = 2) -> dict[str, object]:
    return build_completion_receipt(
        replay_run_id="run-1",
        snapshot_id="a" * 64,
        sealed_report_sha256="b" * 64,
        events_sha256="c" * 64,
        lineages_sha256="d" * 64,
        evaluation_sequence_sha256="e" * 64,
        snapshot_event_count=10,
        snapshot_lineage_ids=("f" * 64, "1" * 64),
        start_utc="2026-08-23T12:00:00Z",
        interval_ms=250,
        rate_per_second=None,
        resume_offset=3,
        duplicate_every=duplicate_every,
        out_of_order_window=4,
        destination="kafka",
        kafka_topic="telemetry",
        observed_delivery_count=expected_delivery_count(10, 3, duplicate_every),
    )


def _single_event_receipt() -> dict[str, object]:
    return build_completion_receipt(
        replay_run_id="run-single",
        snapshot_id="a" * 64,
        sealed_report_sha256="b" * 64,
        events_sha256="c" * 64,
        lineages_sha256="d" * 64,
        evaluation_sequence_sha256="e" * 64,
        snapshot_event_count=1,
        snapshot_lineage_ids=("f" * 64,),
        start_utc="2026-08-23T12:00:00Z",
        interval_ms=250,
        rate_per_second=None,
        resume_offset=0,
        duplicate_every=None,
        out_of_order_window=1,
        destination="kafka",
        kafka_topic="telemetry",
        observed_delivery_count=1,
    )


@pytest.mark.parametrize(
    ("event_count", "resume", "duplicate_every", "expected"),
    [
        (10, 0, None, 10),
        (10, 3, None, 7),
        (10, 3, 2, 11),
        (10, 10, 2, 0),
        (10, 20, 2, 0),
    ],
)
def test_expected_delivery_count_matches_resume_and_one_based_duplicate_schedule(
    event_count: int,
    resume: int,
    duplicate_every: int | None,
    expected: int,
) -> None:
    assert expected_delivery_count(event_count, resume, duplicate_every) == expected


def test_receipt_freezes_source_hashes_options_counts_and_schedule_identity() -> None:
    receipt = _receipt()

    assert receipt["schema_version"] == "telemetry-replay-completion-v1"
    assert receipt["status"] == "completed"
    assert receipt["replay_run_id"] == "run-1"
    assert receipt["snapshot"] == {
        "evaluation_sequence_sha256": "e" * 64,
        "events_sha256": "c" * 64,
        "lineage_ids": ["1" * 64, "f" * 64],
        "lineages_sha256": "d" * 64,
        "sealed_report_sha256": "b" * 64,
        "snapshot_event_count": 10,
        "snapshot_id": "a" * 64,
    }
    assert receipt["replay_options"] == {
        "duplicate_every": 2,
        "interval_ms": 250,
        "out_of_order_window": 4,
        "rate_per_second": None,
        "resume_offset": 3,
        "start_utc": "2026-08-23T12:00:00Z",
    }
    assert receipt["expected_source_event_count"] == 7
    assert receipt["expected_delivery_count"] == 11
    assert receipt["observed_delivery_count"] == 11
    assert receipt["max_replay_timestamp_skew_ms"] == 750
    assert receipt["schedule_identity_sha256"] == _receipt()["schedule_identity_sha256"]
    assert receipt["completion_scope"] == (
        "sink close completed after all sends were acknowledged; this receipt does not "
        "independently prove broker persistence"
    )
    assert validate_completion_receipt(receipt) == receipt


def test_receipt_rejects_count_or_schedule_identity_tampering() -> None:
    receipt = _receipt()
    receipt["observed_delivery_count"] = 10
    with pytest.raises(ReplayReceiptError, match="observed_delivery_count"):
        validate_completion_receipt(receipt)


@pytest.mark.parametrize(
    ("field", "boolean_value"),
    [
        ("expected_source_event_count", True),
        ("expected_delivery_count", True),
        ("observed_delivery_count", True),
        ("max_replay_timestamp_skew_ms", False),
    ],
)
def test_receipt_rejects_boolean_count_and_skew_values(
    field: str,
    boolean_value: bool,
) -> None:
    receipt = _single_event_receipt()
    receipt[field] = boolean_value

    with pytest.raises(ReplayReceiptError, match=field):
        validate_completion_receipt(receipt)


def test_receipt_rejects_noncanonical_start_time() -> None:
    with pytest.raises(ReplayReceiptError, match="canonical UTC"):
        build_completion_receipt(
            replay_run_id="run-1",
            snapshot_id="a" * 64,
            sealed_report_sha256="b" * 64,
            events_sha256="c" * 64,
            lineages_sha256="d" * 64,
            evaluation_sequence_sha256="e" * 64,
            snapshot_event_count=1,
            snapshot_lineage_ids=("f" * 64,),
            start_utc="2026-08-23T12:00:00.000000Z",
            interval_ms=250,
            rate_per_second=None,
            resume_offset=0,
            duplicate_every=None,
            out_of_order_window=1,
            destination="kafka",
            kafka_topic="telemetry",
            observed_delivery_count=1,
        )

    receipt = _receipt()
    receipt["schedule_identity_sha256"] = "0" * 64
    with pytest.raises(ReplayReceiptError, match="schedule_identity_sha256"):
        validate_completion_receipt(receipt)

    receipt = _receipt()
    receipt["kafka_topic"] = "other-topic"
    with pytest.raises(ReplayReceiptError, match="schedule_identity_sha256"):
        validate_completion_receipt(receipt)


def test_receipt_publication_is_no_clobber_and_exactly_idempotent(tmp_path: Path) -> None:
    target = tmp_path / "receipts" / "run-1.json"
    receipt = _receipt()

    assert publish_completion_receipt(target, receipt, protected_roots=()) is True
    first_bytes = target.read_bytes()
    assert publish_completion_receipt(target, receipt, protected_roots=()) is False
    assert target.read_bytes() == first_bytes

    incompatible = _receipt(duplicate_every=None)
    with pytest.raises(ReplayReceiptError, match="incompatible"):
        publish_completion_receipt(target, incompatible, protected_roots=())
    assert target.read_bytes() == first_bytes


def test_receipt_publication_rejects_evidence_root(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    evidence.mkdir()

    with pytest.raises(ReplayReceiptError, match="evidence root"):
        publish_completion_receipt(
            evidence / "receipt.json",
            _receipt(),
            protected_roots=(evidence,),
        )


def test_published_receipt_is_canonical_utf8_json(tmp_path: Path) -> None:
    target = tmp_path / "receipt.json"
    receipt = _receipt()

    publish_completion_receipt(target, receipt, protected_roots=())

    assert target.read_bytes() == (
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("ascii")


def test_receipt_loader_requires_canonical_regular_file(tmp_path: Path) -> None:
    receipt = _receipt()
    canonical = tmp_path / "canonical.json"
    publish_completion_receipt(canonical, receipt, protected_roots=())

    loaded, digest = receipt_module.load_completion_receipt(canonical)

    assert loaded == receipt
    assert digest == sha256(canonical.read_bytes()).hexdigest()


def test_receipt_loader_rejects_noncanonical_json_bytes(tmp_path: Path) -> None:
    target = tmp_path / "receipt.json"
    target.write_text(json.dumps(_receipt(), indent=2), encoding="ascii")

    with pytest.raises(ReplayReceiptError, match="canonical"):
        receipt_module.load_completion_receipt(target)


def test_receipt_loader_rejects_non_file_path(tmp_path: Path) -> None:
    target = tmp_path / "receipt.json"
    target.mkdir()

    with pytest.raises(ReplayReceiptError, match="regular file"):
        receipt_module.load_completion_receipt(target)
