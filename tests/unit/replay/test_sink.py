from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.replay import sink as sink_module
from tinyml_ids_telemetry.replay.clock import SyntheticClock
from tinyml_ids_telemetry.replay.schedule import DeliveryEnvelope, iter_replay_schedule
from tinyml_ids_telemetry.replay.sink import JsonlSink, MemorySink, ReplaySink, SinkClosedError


def _deliveries(count: int = 2) -> list[DeliveryEnvelope]:
    clock = SyntheticClock(
        start_utc=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        interval=timedelta(seconds=1),
    )
    events = [make_event(row_id=offset).to_dict() for offset in range(count)]
    return list(iter_replay_schedule(events, replay_run_id="sink-test", clock=clock))


def _as_sink(sink: ReplaySink) -> ReplaySink:
    return sink


def test_memory_sink_captures_deliveries_and_rejects_writes_after_close() -> None:
    sink = MemorySink()
    first, second = _deliveries()

    _as_sink(sink).write(first)
    sink.write(second)
    sink.close()

    assert sink.deliveries == (first, second)
    with pytest.raises(SinkClosedError, match="closed"):
        sink.write(first)


def test_jsonl_sink_publishes_canonical_lines_atomically_on_close(tmp_path: Path) -> None:
    output = tmp_path / "replay" / "deliveries.jsonl"
    first, second = _deliveries()
    sink = JsonlSink(output)

    sink.write(first)
    sink.write(second)

    assert not output.exists()
    sink.close()
    assert output.read_bytes() == first.to_json_bytes() + b"\n" + second.to_json_bytes() + b"\n"
    assert list(output.parent.iterdir()) == [output]
    with pytest.raises(SinkClosedError, match="closed"):
        sink.write(first)


def test_jsonl_sink_aborts_without_partial_output_or_temporary_files(tmp_path: Path) -> None:
    output = tmp_path / "deliveries.jsonl"

    with pytest.raises(RuntimeError, match="stop replay"), JsonlSink(output) as sink:
        sink.write(_deliveries(1)[0])
        raise RuntimeError("stop replay")

    assert not output.exists()
    assert list(tmp_path.iterdir()) == []


def test_jsonl_sink_rejects_existing_destination_without_changing_it(tmp_path: Path) -> None:
    output = tmp_path / "deliveries.jsonl"
    output.write_text("existing\n", encoding="utf-8")

    with pytest.raises(FileExistsError, match="already exists"):
        JsonlSink(output)

    assert output.read_text(encoding="utf-8") == "existing\n"


def test_jsonl_sink_never_clobbers_destination_created_during_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "deliveries.jsonl"
    sink = JsonlSink(output)
    sink.write(_deliveries(1)[0])
    original_link = sink_module.os.link

    def create_competing_destination(source: Path, destination: Path) -> None:
        output.write_text("competing\n", encoding="utf-8")
        original_link(source, destination)

    monkeypatch.setattr(sink_module.os, "link", create_competing_destination)

    with pytest.raises(FileExistsError, match="already exists"):
        sink.close()

    assert output.read_text(encoding="utf-8") == "competing\n"
    assert list(tmp_path.iterdir()) == [output]
