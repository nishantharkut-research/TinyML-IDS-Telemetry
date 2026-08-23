from __future__ import annotations

import builtins
import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.cli import replay as replay_cli
from tinyml_ids_telemetry.cli.main import main
from tinyml_ids_telemetry.contracts.identity import lineage_id, source_event_id
from tinyml_ids_telemetry.contracts.schema import validate_snapshot_manifest_mapping
from tinyml_ids_telemetry.replay import kafka as kafka_module


def _canonical_line(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        + b"\n"
    )


def _lineage(expected_rows: int) -> dict[str, object]:
    export_id = "d" * 64
    wireless_bundle_id = "e" * 64
    wireless_protocol_id = "fixture-wireless-v1"
    source_session_id = "fixture-session"
    source_stage_id = "fixture-stage"
    identifier = lineage_id(
        protocol_id=wireless_protocol_id,
        export_id=export_id,
        wireless_bundle_id=wireless_bundle_id,
        source_session_id=source_session_id,
        source_stage_id=source_stage_id,
    )
    return {
        "schema_version": "telemetry-lineage-v1",
        "lineage_id": identifier,
        "board": "fixture-board",
        "student_model": "student_A",
        "seed": 42,
        "experiment_protocol_id": "fixture-experiment-v1",
        "wireless_protocol_id": wireless_protocol_id,
        "dataset_protocol_id": "fixture-dataset-v1",
        "dataset_sha256": "1" * 64,
        "test_split_sha256": "2" * 64,
        "scaler_sha256": "3" * 64,
        "export_id": export_id,
        "wireless_bundle_id": wireless_bundle_id,
        "model_file_sha256": "4" * 64,
        "model_artifact_sha256": "5" * 64,
        "firmware_binary_sha256": "6" * 64,
        "source_session_id": source_session_id,
        "source_stage_id": source_stage_id,
        "expected_rows": expected_rows,
        "class_names": ["Blackhole", "Flooding", "Grayhole", "Normal", "TDMA"],
        "normal_class_id": 3,
        "logit_frac_bits": 9,
        "mcu_csv_sha256": "7" * 64,
        "reference_csv_sha256": "8" * 64,
        "completion_manifest_sha256": "9" * 64,
        "compile_record_sha256": "a" * 64,
        "mcu_relative_path": "results/full_mcu.csv",
        "reference_relative_path": "results/full_reference.csv",
    }


def _snapshot_id(manifest: dict[str, object]) -> str:
    identity = {
        "schema_version": "telemetry-snapshot-v1",
        "sealed_report_sha256": manifest["sealed_report_sha256"],
        "events_sha256": manifest["events_sha256"],
        "lineages_sha256": manifest["lineages_sha256"],
        "event_count": manifest["event_count"],
    }
    return sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _read_manifest(snapshot: Path) -> dict[str, object]:
    return json.loads((snapshot / "snapshot_manifest.json").read_text(encoding="utf-8"))


def _write_manifest(snapshot: Path, manifest: dict[str, object]) -> None:
    (snapshot / "snapshot_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _rewrite_events(snapshot: Path, events: list[dict[str, object]]) -> None:
    event_bytes = b"".join(_canonical_line(event) for event in events)
    (snapshot / "events.jsonl").write_bytes(event_bytes)
    manifest = _read_manifest(snapshot)
    manifest["events_sha256"] = sha256(event_bytes).hexdigest()
    manifest["snapshot_id"] = _snapshot_id(manifest)
    _write_manifest(snapshot, manifest)


def _snapshot(
    root: Path,
    *,
    events: list[object] | None = None,
) -> Path:
    snapshot = root / "snapshot"
    snapshot.mkdir()
    source_events = (
        [make_event(row_id=offset).to_dict() for offset in range(4)] if events is None else events
    )
    lineage = _lineage(len(source_events))
    for event in source_events:
        if isinstance(event, dict):
            if "lineage_id" in event:
                event["lineage_id"] = lineage["lineage_id"]
            event["source_session_id"] = lineage["source_session_id"]
            event["source_stage_id"] = lineage["source_stage_id"]
            event["board"] = lineage["board"]
            event["student_model"] = lineage["student_model"]
            event["source_event_id"] = source_event_id(
                protocol_id=str(lineage["wireless_protocol_id"]),
                export_id=str(lineage["export_id"]),
                wireless_bundle_id=str(lineage["wireless_bundle_id"]),
                source_session_id=str(lineage["source_session_id"]),
                source_stage_id=str(lineage["source_stage_id"]),
                row_id=int(event["row_id"]),
            )
    event_bytes = b"".join(_canonical_line(event) for event in source_events)
    (snapshot / "events.jsonl").write_bytes(event_bytes)
    lineage_bytes = _canonical_line(lineage)
    (snapshot / "lineages.jsonl").write_bytes(lineage_bytes)
    event_count = len(source_events)
    manifest = {
        "schema_version": "telemetry-snapshot-v1",
        "snapshot_id": "0" * 64,
        "created_at_utc": "2026-08-23T12:00:00Z",
        "sealed_report_sha256": "b" * 64,
        "event_count": event_count,
        "lineage_count": 1,
        "execution_lineage_count": 1,
        "rows_per_lineage": event_count,
        "evaluation_sequence_sha256": sha256(
            b"".join(
                f"{event['source_row_index']}|{event['true_class_id']}\n".encode("ascii")
                for event in source_events
                if isinstance(event, dict)
            )
        ).hexdigest(),
        "events_sha256": sha256(event_bytes).hexdigest(),
        "lineages_sha256": sha256(lineage_bytes).hexdigest(),
        "lineage_ids": [lineage["lineage_id"]],
        "source_interpretation": (
            f"1 board-model executions over one shared {event_count}-record test sequence; "
            "event_count is not an independent predictive-sample count."
        ),
        "event_time_boundary": (
            "The source snapshot has no natural event timestamp; replay time is added only "
            "by the delivery layer."
        ),
    }
    manifest["snapshot_id"] = _snapshot_id(manifest)
    _write_manifest(snapshot, manifest)
    return snapshot


def _arguments(snapshot: Path, output: Path) -> list[str]:
    return [
        "replay",
        "--snapshot",
        str(snapshot),
        "--run-id",
        "cli-replay-run",
        "--start-utc",
        "2026-08-23T12:00:00Z",
        "--interval-ms",
        "250",
        "--jsonl",
        str(output),
    ]


def _exception_leaf_messages(error: BaseException) -> list[str]:
    if isinstance(error, BaseExceptionGroup):
        return [
            message for nested in error.exceptions for message in _exception_leaf_messages(nested)
        ]
    return [str(error)]


class _WriteAndCloseFailingKafkaSink:
    close_attempts = 0

    def __init__(self, *, topic: str, bootstrap_servers: str) -> None:
        del topic, bootstrap_servers

    def write(self, envelope: object) -> None:
        del envelope
        raise RuntimeError("synchronous write failed")

    def close(self) -> None:
        type(self).close_attempts += 1
        raise ExceptionGroup(
            "Kafka sink close failures",
            [
                RuntimeError("asynchronous delivery failed"),
                RuntimeError("producer flush failed"),
                RuntimeError("producer close failed"),
            ],
        )


class _InterruptingKafkaSink:
    close_attempts = 0
    close_error: BaseException | None = None

    def __init__(self, *, topic: str, bootstrap_servers: str) -> None:
        del topic, bootstrap_servers

    def write(self, envelope: object) -> None:
        del envelope
        raise KeyboardInterrupt("stop replay")

    def close(self) -> None:
        type(self).close_attempts += 1
        if self.close_error is not None:
            raise self.close_error


class _CloseOnlyFailingKafkaSink:
    write_attempts = 0
    close_attempts = 0

    def __init__(self, *, topic: str, bootstrap_servers: str) -> None:
        del topic, bootstrap_servers

    def write(self, envelope: object) -> None:
        del envelope
        type(self).write_attempts += 1

    def close(self) -> None:
        type(self).close_attempts += 1
        raise ExceptionGroup(
            "Kafka sink close failures",
            [
                RuntimeError("asynchronous delivery failed"),
                RuntimeError("producer flush failed"),
                RuntimeError("producer close failed"),
            ],
        )


def test_snapshot_fixture_uses_canonical_event_identity_and_evaluation_hash(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(tmp_path)
    lineage = json.loads((snapshot / "lineages.jsonl").read_text(encoding="utf-8"))
    events = [json.loads(line) for line in (snapshot / "events.jsonl").read_text().splitlines()]
    manifest = _read_manifest(snapshot)

    assert [event["source_event_id"] for event in events] == [
        source_event_id(
            protocol_id=lineage["wireless_protocol_id"],
            export_id=lineage["export_id"],
            wireless_bundle_id=lineage["wireless_bundle_id"],
            source_session_id=lineage["source_session_id"],
            source_stage_id=lineage["source_stage_id"],
            row_id=event["row_id"],
        )
        for event in events
    ]
    evaluation_sequence = "".join(
        f"{event['source_row_index']}|{event['true_class_id']}\n" for event in events
    ).encode("ascii")
    assert manifest["evaluation_sequence_sha256"] == sha256(evaluation_sequence).hexdigest()
    assert (
        validate_snapshot_manifest_mapping(manifest).evaluation_sequence_sha256
        == manifest["evaluation_sequence_sha256"]
    )


def test_kafka_cleanup_aggregate_preserves_write_delivery_flush_and_close_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _WriteAndCloseFailingKafkaSink.close_attempts = 0
    monkeypatch.setattr(kafka_module, "KafkaSink", _WriteAndCloseFailingKafkaSink)

    with pytest.raises(ExceptionGroup) as raised:
        replay_cli._write_kafka(
            [object()],  # type: ignore[list-item]
            topic="telemetry",
            bootstrap_servers="broker.example:9092",
        )

    assert _exception_leaf_messages(raised.value) == [
        "synchronous write failed",
        "asynchronous delivery failed",
        "producer flush failed",
        "producer close failed",
    ]
    assert _WriteAndCloseFailingKafkaSink.close_attempts == 1


def test_kafka_cleanup_runs_for_keyboard_interrupt_and_preserves_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _InterruptingKafkaSink.close_attempts = 0
    _InterruptingKafkaSink.close_error = None
    monkeypatch.setattr(kafka_module, "KafkaSink", _InterruptingKafkaSink)

    with pytest.raises(KeyboardInterrupt, match="stop replay"):
        replay_cli._write_kafka(
            [object()],  # type: ignore[list-item]
            topic="telemetry",
            bootstrap_servers="broker.example:9092",
        )

    assert _InterruptingKafkaSink.close_attempts == 1


def test_kafka_cleanup_preserves_interrupt_and_close_failure_in_base_exception_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _InterruptingKafkaSink.close_attempts = 0
    _InterruptingKafkaSink.close_error = RuntimeError("producer cleanup failed")
    monkeypatch.setattr(kafka_module, "KafkaSink", _InterruptingKafkaSink)

    with pytest.raises(BaseExceptionGroup) as raised:
        replay_cli._write_kafka(
            [object()],  # type: ignore[list-item]
            topic="telemetry",
            bootstrap_servers="broker.example:9092",
        )

    assert type(raised.value) is BaseExceptionGroup
    assert _exception_leaf_messages(raised.value) == ["stop replay", "producer cleanup failed"]
    assert _InterruptingKafkaSink.close_attempts == 1


def test_kafka_close_only_aggregate_preserves_inspectable_exception_leaves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _CloseOnlyFailingKafkaSink.write_attempts = 0
    _CloseOnlyFailingKafkaSink.close_attempts = 0
    monkeypatch.setattr(kafka_module, "KafkaSink", _CloseOnlyFailingKafkaSink)

    with pytest.raises(replay_cli.ReplayCommandErrorGroup) as raised:
        replay_cli._write_kafka(
            [object(), object()],  # type: ignore[list-item]
            topic="telemetry",
            bootstrap_servers="broker.example:9092",
        )

    assert _exception_leaf_messages(raised.value) == [
        "asynchronous delivery failed",
        "producer flush failed",
        "producer close failed",
    ]
    assert _CloseOnlyFailingKafkaSink.write_attempts == 2
    assert _CloseOnlyFailingKafkaSink.close_attempts == 1


def test_main_keeps_compact_exit_behavior_for_kafka_cleanup_aggregate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path)
    _WriteAndCloseFailingKafkaSink.close_attempts = 0
    monkeypatch.setattr(kafka_module, "KafkaSink", _WriteAndCloseFailingKafkaSink)
    arguments = [
        *_arguments(snapshot, tmp_path / "unused.jsonl")[:-2],
        "--kafka-topic",
        "telemetry",
    ]

    exit_code = main(arguments)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err.startswith("error: Kafka replay failed")
    assert _WriteAndCloseFailingKafkaSink.close_attempts == 1


def test_jsonl_dry_run_never_imports_kafka_and_prints_compact_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path)
    output = tmp_path / "replay.jsonl"
    original_import = builtins.__import__

    def reject_kafka_import(
        name: str,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        if name == "kafka" or name.startswith("kafka."):
            raise AssertionError("JSONL replay imported Kafka")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", reject_kafka_import)

    exit_code = main(
        [
            *_arguments(snapshot, output),
            "--resume-offset",
            "1",
            "--duplicate-every",
            "2",
            "--out-of-order-window",
            "2",
        ]
    )

    assert exit_code == 0
    deliveries = [json.loads(line) for line in output.read_text().splitlines()]
    assert [item["source_offset"] for item in deliveries] == [1, 2, 1, 3, 3]
    assert [item["duplicate_ordinal"] for item in deliveries] == [0, 0, 1, 1, 0]
    assert [item["emission_index"] for item in deliveries] == [0, 2, 3, 4, 5]
    summary = {
        "delivery_count": 5,
        "delivery_semantics": "at-least-once",
        "destination": "jsonl",
        "pacing_mode": "unpaced",
        "rate_per_second": None,
        "replay_run_id": "cli-replay-run",
        "resume_offset": 1,
        "schema_version": "telemetry-replay-summary-v1",
        "source_event_count": 3,
        "status": "completed",
    }
    assert (
        capsys.readouterr().out
        == json.dumps(
            summary,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )


def test_controlled_rate_paces_every_delivery_and_reports_cli_mode(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path)
    output = tmp_path / "replay.jsonl"
    paced_indexes: list[int] = []

    class RecordingPacer:
        def __init__(self, *, rate_per_second: int) -> None:
            assert rate_per_second == 20

        def wait_for_delivery(self, delivery_index: int) -> None:
            paced_indexes.append(delivery_index)

    monkeypatch.setattr(replay_cli, "DeliveryPacer", RecordingPacer, raising=False)

    exit_code = main(
        [
            *_arguments(snapshot, output),
            "--rate-per-second",
            "20",
            "--duplicate-every",
            "2",
        ]
    )

    assert exit_code == 0
    assert paced_indexes == list(range(6))
    summary = json.loads(capsys.readouterr().out)
    assert summary["delivery_count"] == 6
    assert summary["rate_per_second"] == 20
    assert summary["pacing_mode"] == "controlled"


def test_replay_verifies_events_hash_before_creating_destination(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    output = tmp_path / "replay.jsonl"
    with (snapshot / "events.jsonl").open("ab") as handle:
        handle.write(b"{}\n")

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert "events SHA-256 mismatch" in captured.err
    assert not output.exists()


@pytest.mark.parametrize(
    "protected_root_source",
    ["snapshot", "CUKD_XAI_ROOT", "TINYML_IDS_EVIDENCE_ROOT"],
)
def test_jsonl_destination_rejects_effective_paths_under_evidence_roots_before_creation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    protected_root_source: str,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    snapshot = _snapshot(workspace)
    monkeypatch.delenv("CUKD_XAI_ROOT", raising=False)
    monkeypatch.delenv("TINYML_IDS_EVIDENCE_ROOT", raising=False)
    if protected_root_source == "snapshot":
        protected_root = snapshot
    else:
        protected_root = tmp_path / protected_root_source.lower()
        protected_root.mkdir()
        monkeypatch.setenv(protected_root_source, str(protected_root))
    output_parent = protected_root / "generated"
    output = output_parent / ".." / "generated" / "replay.jsonl"

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert "outside every evidence root" in captured.err
    assert not output_parent.exists()
    assert not output.exists()


def test_replay_requires_complete_manifest_before_creating_destination(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    manifest = _read_manifest(snapshot)
    del manifest["lineages_sha256"]
    _write_manifest(snapshot, manifest)
    output = tmp_path / "replay.jsonl"

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "lineages_sha256" in captured.err
    assert not output.exists()


def test_replay_validates_exact_event_count_before_inspecting_destination(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    manifest = _read_manifest(snapshot)
    manifest["event_count"] = 5
    manifest["snapshot_id"] = _snapshot_id(manifest)
    _write_manifest(snapshot, manifest)
    output = tmp_path / "replay.jsonl"
    output.write_text("competing-output\n", encoding="utf-8")

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "event_count" in captured.err
    assert "already exists" not in captured.err
    assert output.read_text(encoding="utf-8") == "competing-output\n"


def test_replay_verifies_lineages_hash_before_creating_destination(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    with (snapshot / "lineages.jsonl").open("ab") as handle:
        handle.write(b"{}\n")
    output = tmp_path / "replay.jsonl"

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "lineages SHA-256 mismatch" in captured.err
    assert not output.exists()


def test_replay_validates_lineage_content_and_identity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    lineage = json.loads((snapshot / "lineages.jsonl").read_text(encoding="utf-8"))
    lineage["lineage_id"] = "f" * 64
    lineage_bytes = _canonical_line(lineage)
    (snapshot / "lineages.jsonl").write_bytes(lineage_bytes)
    manifest = _read_manifest(snapshot)
    manifest["lineages_sha256"] = sha256(lineage_bytes).hexdigest()
    manifest["lineage_ids"] = [lineage["lineage_id"]]
    manifest["snapshot_id"] = _snapshot_id(manifest)
    _write_manifest(snapshot, manifest)
    output = tmp_path / "replay.jsonl"

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "lineage_id" in captured.err
    assert not output.exists()


def test_replay_validates_every_event_before_kafka_dispatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed_events = [make_event(row_id=offset).to_dict() for offset in range(2)]
    del malformed_events[-1]["total_us"]
    snapshot = _snapshot(tmp_path, events=malformed_events)
    kafka_called = False

    def record_kafka_call(
        deliveries: object,
        *,
        topic: str,
        bootstrap_servers: str,
    ) -> int:
        nonlocal kafka_called
        del deliveries, topic, bootstrap_servers
        kafka_called = True
        return 0

    monkeypatch.setattr(replay_cli, "_write_kafka", record_kafka_call)
    arguments = [
        *_arguments(snapshot, tmp_path / "unused.jsonl")[:-2],
        "--kafka-topic",
        "telemetry",
    ]

    exit_code = main(arguments)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "total_us" in captured.err
    assert kafka_called is False


@pytest.mark.parametrize(
    ("field", "invalid_value", "message"),
    [
        ("source_event_id", "f" * 64, "source_event_id"),
        ("predicted_class_id", 0, "first fixed-logit argmax"),
        ("fixed_predicted_class_id", 0, "fixed_predicted_class_id"),
        ("true_class_name", "Flooding", "true_class_name"),
        ("predicted_class_name", "Flooding", "predicted_class_name"),
        ("normal_class_id", 0, "normal_class_id"),
        ("logit_frac_bits", 8, "logit_frac_bits"),
        ("is_predicted_attack", True, "is_predicted_attack"),
        ("normalized_logit_margin", 10.0000000001, "normalized_logit_margin"),
        ("total_us", 31, "total_us"),
    ],
)
def test_replay_rejects_inconsistent_event_invariant_before_kafka_dispatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    invalid_value: object,
    message: str,
) -> None:
    snapshot = _snapshot(tmp_path)
    events = [json.loads(line) for line in (snapshot / "events.jsonl").read_text().splitlines()]
    events[0][field] = invalid_value
    _rewrite_events(snapshot, events)
    kafka_called = False

    def record_kafka_call(
        deliveries: object,
        *,
        topic: str,
        bootstrap_servers: str,
    ) -> int:
        nonlocal kafka_called
        del deliveries, topic, bootstrap_servers
        kafka_called = True
        return 0

    monkeypatch.setattr(replay_cli, "_write_kafka", record_kafka_call)
    arguments = [
        *_arguments(snapshot, tmp_path / "unused.jsonl")[:-2],
        "--kafka-topic",
        "telemetry",
    ]

    exit_code = main(arguments)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert message in captured.err
    assert kafka_called is False


def test_replay_rejects_reversed_canonical_event_order_before_opening_sink(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path)
    events = [json.loads(line) for line in (snapshot / "events.jsonl").read_text().splitlines()]
    _rewrite_events(snapshot, list(reversed(events)))
    output = tmp_path / "replay.jsonl"

    def reject_sink_open(path: Path) -> None:
        del path
        raise AssertionError("JSONL sink opened before preflight completed")

    monkeypatch.setattr(replay_cli, "JsonlSink", reject_sink_open)

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "canonical row_id order" in captured.err
    assert not output.exists()


def test_replay_rejects_fake_evaluation_sequence_hash_before_opening_sink(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path)
    manifest = _read_manifest(snapshot)
    manifest["evaluation_sequence_sha256"] = "f" * 64
    _write_manifest(snapshot, manifest)
    output = tmp_path / "replay.jsonl"

    def reject_sink_open(path: Path) -> None:
        del path
        raise AssertionError("JSONL sink opened before preflight completed")

    monkeypatch.setattr(replay_cli, "JsonlSink", reject_sink_open)

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "evaluation_sequence_sha256" in captured.err
    assert not output.exists()


def test_replay_rejects_malformed_source_mapping_without_partial_jsonl(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    malformed = make_event().to_dict()
    del malformed["lineage_id"]
    snapshot = _snapshot(tmp_path, events=[malformed])
    output = tmp_path / "replay.jsonl"

    exit_code = main(_arguments(snapshot, output))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert "lineage_id" in captured.err
    assert not output.exists()
    assert list(tmp_path.iterdir()) == [snapshot]


def test_jsonl_destination_rejects_kafka_bootstrap_configuration(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    output = tmp_path / "replay.jsonl"

    exit_code = main([*_arguments(snapshot, output), "--bootstrap-servers", "broker.example:9092"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "--bootstrap-servers" in captured.err
    assert "--jsonl" in captured.err
    assert not output.exists()


def test_replay_rejects_whitespace_only_run_id_before_opening_sink(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path)
    output = tmp_path / "replay.jsonl"
    arguments = _arguments(snapshot, output)
    arguments[arguments.index("--run-id") + 1] = " \t "
    sink_opened = False

    def reject_sink_open(path: Path) -> None:
        nonlocal sink_opened
        del path
        sink_opened = True
        raise AssertionError("JSONL sink opened before run ID preflight")

    monkeypatch.setattr(replay_cli, "JsonlSink", reject_sink_open)

    exit_code = main(arguments)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "error: --run-id must be non-empty\n"
    assert sink_opened is False
    assert not output.exists()


@pytest.mark.parametrize(
    ("destination_arguments", "message"),
    [
        ([], "required"),
        (["--jsonl", "output.jsonl", "--kafka-topic", "telemetry"], "not allowed"),
    ],
)
def test_replay_requires_exactly_one_destination(
    tmp_path: Path,
    destination_arguments: list[str],
    message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    arguments = _arguments(snapshot, tmp_path / "unused.jsonl")[:-2]

    with pytest.raises(SystemExit) as raised:
        main([*arguments, *destination_arguments])

    assert raised.value.code == 2
    captured = capsys.readouterr()
    assert "--jsonl" in captured.err
    assert "--kafka-topic" in captured.err
    assert message in captured.err


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--resume-offset", "-1"),
        ("--duplicate-every", "0"),
        ("--out-of-order-window", "0"),
        ("--interval-ms", "0"),
        ("--rate-per-second", "0"),
    ],
)
def test_replay_rejects_invalid_numeric_options(
    tmp_path: Path,
    option: str,
    value: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    arguments = _arguments(snapshot, tmp_path / "unused.jsonl")
    option_index = arguments.index(option) if option in arguments else -1
    if option_index >= 0:
        arguments[option_index + 1] = value
    else:
        arguments.extend([option, value])

    with pytest.raises(SystemExit) as raised:
        main(arguments)

    assert raised.value.code == 2
    captured = capsys.readouterr()
    assert option in captured.err
    assert "must be" in captured.err


def test_replay_reports_interval_overflow_as_compact_command_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    output = tmp_path / "replay.jsonl"
    arguments = _arguments(snapshot, output)
    arguments[arguments.index("--interval-ms") + 1] = str(10**1000)

    exit_code = main(arguments)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "error: --interval-ms is too large\n"
    assert not output.exists()


@pytest.mark.parametrize("destination", ["jsonl", "kafka"])
def test_replay_preflights_maximum_synthetic_timestamp_before_destination(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    destination: str,
) -> None:
    snapshot = _snapshot(tmp_path)
    output = tmp_path / "replay.jsonl"
    arguments = _arguments(snapshot, output)
    arguments[arguments.index("--start-utc") + 1] = "9999-12-30T00:00:00Z"
    arguments[arguments.index("--interval-ms") + 1] = "86400000"
    if destination == "kafka":
        arguments[-2:] = ["--kafka-topic", "telemetry"]
    jsonl_opened = False
    kafka_dispatched = False

    def reject_jsonl_open(path: Path) -> None:
        nonlocal jsonl_opened
        del path
        jsonl_opened = True
        raise AssertionError("JSONL sink opened before timestamp preflight")

    def reject_kafka_dispatch(
        deliveries: object,
        *,
        topic: str,
        bootstrap_servers: str,
    ) -> int:
        nonlocal kafka_dispatched
        del deliveries, topic, bootstrap_servers
        kafka_dispatched = True
        raise AssertionError("Kafka dispatched before timestamp preflight")

    monkeypatch.setattr(replay_cli, "JsonlSink", reject_jsonl_open)
    monkeypatch.setattr(replay_cli, "_write_kafka", reject_kafka_dispatch)

    exit_code = main(arguments)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "error: synthetic replay timestamp exceeds datetime range\n"
    assert jsonl_opened is False
    assert kafka_dispatched is False
    assert not output.exists()


@pytest.mark.parametrize("resume_offset", [4, 9])
def test_empty_range_resume_skips_timestamp_preflight_and_event_scheduling(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    resume_offset: int,
) -> None:
    snapshot = _snapshot(tmp_path)
    output = tmp_path / "replay.jsonl"
    arguments = [*_arguments(snapshot, output), "--resume-offset", str(resume_offset)]
    arguments[arguments.index("--start-utc") + 1] = "9999-12-30T00:00:00Z"
    arguments[arguments.index("--interval-ms") + 1] = "86400000"

    def reject_schedule(*args: object, **kwargs: object) -> object:
        del args, kwargs
        pytest.fail("empty-range resume constructed an event schedule")

    monkeypatch.setattr(replay_cli, "iter_replay_schedule", reject_schedule)

    exit_code = main(arguments)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""
    summary = json.loads(captured.out)
    assert summary["source_event_count"] == 0
    assert summary["delivery_count"] == 0
    assert output.read_bytes() == b""


def test_replay_requires_explicit_utc_start(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot(tmp_path)
    arguments = _arguments(snapshot, tmp_path / "unused.jsonl")
    arguments[arguments.index("--start-utc") + 1] = "2026-08-23T12:00:00"

    with pytest.raises(SystemExit) as raised:
        main(arguments)

    assert raised.value.code == 2
    captured = capsys.readouterr()
    assert "--start-utc" in captured.err
    assert "UTC" in captured.err
