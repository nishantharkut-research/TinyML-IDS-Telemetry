from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from jobs.spark.run_pipeline import PipelineRequest, build_replay_command, execute_pipeline


class _Query:
    def __init__(self) -> None:
        self.processed = 0

    def processAllAvailable(self) -> None:  # noqa: N802
        self.processed += 1


class _Queries:
    def __init__(self) -> None:
        self.all = (_Query(), _Query(), _Query(), _Query())
        self.stopped = 0

    def stop(self) -> None:
        self.stopped += 1


class _Spark:
    def __init__(self) -> None:
        self.stopped = 0
        self.read = SimpleNamespace(parquet=lambda path: ("bronze", path))

    def stop(self) -> None:
        self.stopped += 1


def _request(tmp_path: Path) -> PipelineRequest:
    artifact_root = tmp_path / "artifacts"
    snapshot = artifact_root / "canonical-snapshot-v1"
    snapshot.mkdir(parents=True)
    return PipelineRequest(
        artifact_root=artifact_root,
        snapshot_directory=snapshot,
        run_id="course-normal-001",
        start_utc="2026-08-24T00:00:00Z",
        interval_ms=10,
        rate_per_second=1_000,
        resume_offset=0,
        duplicate_every=5,
        out_of_order_window=4,
        watermark_delay_ms=31,
        bootstrap_servers="kafka:19092",
        topic="ids.telemetry.raw",
        query_version="course-v1",
    )


def test_pipeline_request_builds_one_replay_and_versioned_output_contract(tmp_path: Path) -> None:
    request = _request(tmp_path)
    python = Path("python3")

    command = build_replay_command(request, python_executable=python)

    assert command[:5] == (
        str(python),
        "-m",
        "tinyml_ids_telemetry.cli.main",
        "replay",
        "--snapshot",
    )
    assert command[5] == str(request.snapshot_directory)
    assert command[-4:] == (
        "--kafka-topic",
        "ids.telemetry.raw",
        "--bootstrap-servers",
        "kafka:19092",
    )
    assert request.completion_receipt_path.parent == request.run_root
    assert request.streaming_paths.bronze == request.run_root / "stream" / "bronze"
    assert request.finalized_output_parent == request.artifact_root / "finalized"


def test_execute_pipeline_drains_every_query_and_finalizes_exact_bronze(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from jobs.spark import run_pipeline as module

    request = _request(tmp_path)
    spark = _Spark()
    queries = _Queries()
    replay_commands: list[tuple[str, ...]] = []
    finalized: list[object] = []

    monkeypatch.setattr(module, "_create_spark_session", lambda: spark)
    monkeypatch.setattr(module, "start_streaming_job", lambda *args, **kwargs: queries)
    monkeypatch.setattr(module, "_require_matching_receipt", lambda *args, **kwargs: None)

    def replay(command: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        replay_commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(module.subprocess, "run", replay)

    def finalize(bronze: object, **kwargs: object) -> SimpleNamespace:
        finalized.append((bronze, kwargs))
        return SimpleNamespace(
            run_directory=request.finalized_output_parent / f"replay_run_id={request.run_id}",
            manifest_path=request.finalized_output_parent
            / f"replay_run_id={request.run_id}"
            / "finalized_run_manifest.json",
            published=True,
        )

    monkeypatch.setattr(module, "finalize_run", finalize)

    result = execute_pipeline(request, python_executable=Path(sys.executable))

    assert len(replay_commands) == 1
    assert [query.processed for query in queries.all] == [1, 1, 1, 1]
    assert len(finalized) == 1
    assert finalized[0][0] == ("bronze", str(request.streaming_paths.bronze))
    assert result["replay_run_id"] == request.run_id
    assert result["published"] is True
    assert queries.stopped == 1
    assert spark.stopped == 1


def test_execute_pipeline_stops_streaming_after_replay_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from jobs.spark import run_pipeline as module

    request = _request(tmp_path)
    spark = _Spark()
    queries = _Queries()
    monkeypatch.setattr(module, "_create_spark_session", lambda: spark)
    monkeypatch.setattr(module, "start_streaming_job", lambda *args, **kwargs: queries)

    def fail(*args: object, **kwargs: object) -> None:
        raise subprocess.CalledProcessError(2, "replay")

    monkeypatch.setattr(module.subprocess, "run", fail)

    with pytest.raises(subprocess.CalledProcessError):
        execute_pipeline(request)

    assert queries.stopped == 1
    assert spark.stopped == 1
