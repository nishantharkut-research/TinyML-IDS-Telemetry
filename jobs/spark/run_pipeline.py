"""One-run Spark streaming, deterministic Kafka replay, and Gold finalization."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import perf_counter
from typing import cast

from pyspark.sql import SparkSession

from jobs.spark.finalize_run import finalize_run
from jobs.spark.startup import (
    KafkaStreamContract,
    StreamingPaths,
    StreamingQueries,
    start_streaming_job,
)
from tinyml_ids_telemetry.replay.receipt import load_completion_receipt
from tinyml_ids_telemetry.replay.schedule import DeliveryEnvelope

_SAFE_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SAFE_TOPIC = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}")
_DEFAULT_POLICY_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "retention-policies.toml"


class PipelineRunError(ValueError):
    """A production replay-to-finalization request is unsafe or inconsistent."""


def _integer(value: object, field: str, *, minimum: int) -> int:
    if type(value) is not int or value < minimum:
        raise PipelineRunError(f"{field} must be an integer greater than or equal to {minimum}")
    return value


def _canonical_utc(value: str) -> str:
    from datetime import datetime

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise PipelineRunError("start_utc must be an ISO 8601 UTC timestamp") from error
    try:
        canonical = DeliveryEnvelope.format_event_time(parsed)
    except ValueError as error:
        raise PipelineRunError("start_utc must include an explicit UTC offset") from error
    if canonical != value:
        raise PipelineRunError("start_utc must use canonical UTC form")
    return canonical


@dataclass(frozen=True, slots=True)
class PipelineRequest:
    """Complete immutable contract for one replay and finalized Spark publication."""

    artifact_root: Path
    snapshot_directory: Path
    run_id: str
    start_utc: str
    interval_ms: int
    rate_per_second: int | None
    resume_offset: int
    duplicate_every: int | None
    out_of_order_window: int
    watermark_delay_ms: int
    bootstrap_servers: str
    topic: str
    query_version: str
    policy_config_path: Path = _DEFAULT_POLICY_CONFIG

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifact_root", self.artifact_root.expanduser().resolve())
        object.__setattr__(
            self,
            "snapshot_directory",
            self.snapshot_directory.expanduser().resolve(),
        )
        object.__setattr__(
            self,
            "policy_config_path",
            self.policy_config_path.expanduser().resolve(),
        )
        if not self.snapshot_directory.is_dir():
            raise PipelineRunError("snapshot_directory must be an existing directory")
        if not self.policy_config_path.is_file():
            raise PipelineRunError("policy_config_path must be an existing file")
        if _SAFE_IDENTIFIER.fullmatch(self.run_id) is None:
            raise PipelineRunError("run_id must be a safe identifier")
        if _SAFE_IDENTIFIER.fullmatch(self.query_version) is None:
            raise PipelineRunError("query_version must be a safe identifier")
        if _SAFE_TOPIC.fullmatch(self.topic) is None:
            raise PipelineRunError("topic must be a safe Kafka topic")
        if not self.bootstrap_servers.strip():
            raise PipelineRunError("bootstrap_servers must be nonempty")
        object.__setattr__(self, "start_utc", _canonical_utc(self.start_utc))
        _integer(self.interval_ms, "interval_ms", minimum=1)
        _integer(self.resume_offset, "resume_offset", minimum=0)
        _integer(self.out_of_order_window, "out_of_order_window", minimum=1)
        _integer(self.watermark_delay_ms, "watermark_delay_ms", minimum=1)
        if self.rate_per_second is not None:
            _integer(self.rate_per_second, "rate_per_second", minimum=1)
        if self.duplicate_every is not None:
            _integer(self.duplicate_every, "duplicate_every", minimum=1)
        KafkaStreamContract(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic,
            replay_interval_ms=self.interval_ms,
            out_of_order_window=self.out_of_order_window,
            watermark_delay_ms=self.watermark_delay_ms,
        )
        self.streaming_paths.validate()

    @property
    def run_root(self) -> Path:
        return self.artifact_root / "runs" / self.run_id

    @property
    def completion_receipt_path(self) -> Path:
        return self.run_root / "completion_receipt.json"

    @property
    def execution_report_path(self) -> Path:
        return self.run_root / "pipeline_execution.json"

    @property
    def streaming_paths(self) -> StreamingPaths:
        return StreamingPaths.build(
            self.run_root / "stream",
            snapshot_directory=self.snapshot_directory,
            query_version=self.query_version,
        )

    @property
    def finalized_output_parent(self) -> Path:
        return self.artifact_root / "finalized"


def build_replay_command(
    request: PipelineRequest,
    *,
    python_executable: Path,
) -> tuple[str, ...]:
    """Build the exact deterministic replay subprocess command."""
    command = [
        str(python_executable),
        "-m",
        "tinyml_ids_telemetry.cli.main",
        "replay",
        "--snapshot",
        str(request.snapshot_directory),
        "--run-id",
        request.run_id,
        "--start-utc",
        request.start_utc,
        "--interval-ms",
        str(request.interval_ms),
        "--resume-offset",
        str(request.resume_offset),
        "--out-of-order-window",
        str(request.out_of_order_window),
        "--receipt",
        str(request.completion_receipt_path),
    ]
    if request.rate_per_second is not None:
        command.extend(("--rate-per-second", str(request.rate_per_second)))
    if request.duplicate_every is not None:
        command.extend(("--duplicate-every", str(request.duplicate_every)))
    command.extend(
        (
            "--kafka-topic",
            request.topic,
            "--bootstrap-servers",
            request.bootstrap_servers,
        )
    )
    return tuple(command)


def _require_matching_receipt(request: PipelineRequest) -> Mapping[str, object]:
    receipt, _ = load_completion_receipt(request.completion_receipt_path)
    raw_options = receipt["replay_options"]
    if not isinstance(raw_options, Mapping):
        raise PipelineRunError("completion receipt replay_options must be an object")
    options = cast(Mapping[str, object], raw_options)
    expected_options = {
        "start_utc": request.start_utc,
        "interval_ms": request.interval_ms,
        "rate_per_second": request.rate_per_second,
        "resume_offset": request.resume_offset,
        "duplicate_every": request.duplicate_every,
        "out_of_order_window": request.out_of_order_window,
    }
    if (
        receipt["replay_run_id"] != request.run_id
        or receipt["destination"] != "kafka"
        or receipt["kafka_topic"] != request.topic
        or dict(options) != expected_options
    ):
        raise PipelineRunError("completion receipt does not match the pipeline request")
    return receipt


def _create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("tinyml-ids-telemetry-course-run")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _request_contract(request: PipelineRequest) -> dict[str, object]:
    return {
        "start_utc": request.start_utc,
        "interval_ms": request.interval_ms,
        "rate_per_second": request.rate_per_second,
        "resume_offset": request.resume_offset,
        "duplicate_every": request.duplicate_every,
        "out_of_order_window": request.out_of_order_window,
        "watermark_delay_ms": request.watermark_delay_ms,
        "bootstrap_servers": request.bootstrap_servers,
        "topic": request.topic,
        "query_version": request.query_version,
    }


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def _tree_bytes(path: Path) -> int:
    if not path.is_dir():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _load_finalized_counts(manifest_path: Path, run_id: str) -> tuple[int, int]:
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PipelineRunError("finalized run manifest is invalid") from error
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != "telemetry-finalized-run-v1"
        or value.get("replay_run_id") != run_id
    ):
        raise PipelineRunError("finalized run manifest does not match this run")
    source_count = _integer(value.get("source_event_count"), "source_event_count", minimum=1)
    delivery_count = _integer(value.get("delivery_count"), "delivery_count", minimum=1)
    if delivery_count < source_count:
        raise PipelineRunError("finalized delivery_count is smaller than source_event_count")
    return source_count, delivery_count


def _publish_execution_report(path: Path, report: Mapping[str, object]) -> None:
    payload = _canonical_bytes(report)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = NamedTemporaryFile(  # noqa: SIM115 - atomic publication owns the handle.
        mode="w+b",
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    )
    temporary_path = Path(temporary.name)
    try:
        temporary.write(payload)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary.close()
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise PipelineRunError(
                    "pipeline execution report conflicts with this run"
                ) from None
    finally:
        temporary.close()
        temporary_path.unlink(missing_ok=True)


def _load_execution_report(
    request: PipelineRequest,
    receipt: Mapping[str, object],
) -> dict[str, object]:
    try:
        payload = request.execution_report_path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PipelineRunError("pipeline execution report is invalid") from error
    if not isinstance(value, dict) or payload != _canonical_bytes(value):
        raise PipelineRunError("pipeline execution report is not canonical")
    snapshot = receipt.get("snapshot")
    if not isinstance(snapshot, Mapping):
        raise PipelineRunError("completion receipt snapshot must be an object")
    if (
        value.get("schema_version") != "telemetry-pipeline-execution-v1"
        or value.get("replay_run_id") != request.run_id
        or value.get("request") != _request_contract(request)
        or value.get("schedule_identity_sha256") != receipt.get("schedule_identity_sha256")
        or value.get("snapshot_id") != snapshot.get("snapshot_id")
    ):
        raise PipelineRunError("pipeline execution report does not match this run")
    finalized_directory = request.finalized_output_parent / f"replay_run_id={request.run_id}"
    finalized_manifest = finalized_directory / "finalized_run_manifest.json"
    if value.get("run_directory") != str(finalized_directory) or value.get("manifest_path") != str(
        finalized_manifest
    ):
        raise PipelineRunError("pipeline execution report has unexpected finalized paths")
    finalized_sources, finalized_deliveries = _load_finalized_counts(
        finalized_manifest,
        request.run_id,
    )
    raw_counts = value.get("counts")
    if not isinstance(raw_counts, Mapping):
        raise PipelineRunError("pipeline execution report counts must be an object")
    if (
        raw_counts.get("finalized_source_event_count") != finalized_sources
        or raw_counts.get("finalized_delivery_count") != finalized_deliveries
    ):
        raise PipelineRunError("pipeline execution report counts disagree with finalization")
    raw_storage = value.get("storage_bytes")
    if not isinstance(raw_storage, Mapping):
        raise PipelineRunError("pipeline execution report storage_bytes must be an object")
    observed_storage = {
        "finalized/canonical_gold": _tree_bytes(finalized_directory / "canonical_gold"),
        "finalized/retained_security_aware": _tree_bytes(
            finalized_directory / "retained_security_aware"
        ),
    }
    if any(raw_storage.get(name) != size for name, size in observed_storage.items()):
        raise PipelineRunError("pipeline execution report finalized output bytes changed")
    return cast(dict[str, object], value)


def execute_pipeline(
    request: PipelineRequest,
    *,
    python_executable: Path = Path(sys.executable),
) -> dict[str, object]:
    """Execute one replay, drain streaming queries, and publish exact Gold."""
    request.run_root.mkdir(parents=True, exist_ok=True)
    if request.execution_report_path.exists():
        return _load_execution_report(request, _require_matching_receipt(request))

    started_at = datetime.now(UTC)
    total_started = perf_counter()
    spark_started = perf_counter()
    spark = _create_spark_session()
    phases = {"spark_startup": perf_counter() - spark_started}
    queries: StreamingQueries | None = None
    queries_stopped = False
    replay_executed = False
    receipt: Mapping[str, object] | None = None
    result: object | None = None
    paths = request.streaming_paths
    try:
        contract = KafkaStreamContract(
            bootstrap_servers=request.bootstrap_servers,
            topic=request.topic,
            replay_interval_ms=request.interval_ms,
            out_of_order_window=request.out_of_order_window,
            watermark_delay_ms=request.watermark_delay_ms,
        )
        query_start = perf_counter()
        queries = start_streaming_job(spark, contract=contract, paths=paths)
        phases["query_startup"] = perf_counter() - query_start
        replay_start = perf_counter()
        if not request.completion_receipt_path.exists():
            replay_executed = True
            subprocess.run(
                build_replay_command(request, python_executable=python_executable),
                check=True,
            )
        phases["replay"] = perf_counter() - replay_start
        receipt = _require_matching_receipt(request)
        drain_start = perf_counter()
        for query in queries.all:
            query.processAllAvailable()
        phases["stream_drain"] = perf_counter() - drain_start
        query_stop = perf_counter()
        queries.stop()
        phases["query_stop"] = perf_counter() - query_stop
        queries_stopped = True

        bronze = spark.read.parquet(str(paths.bronze))
        finalization_start = perf_counter()
        result = finalize_run(
            bronze,
            snapshot_directory=request.snapshot_directory,
            completion_receipt_path=request.completion_receipt_path,
            output_parent=request.finalized_output_parent,
            policy_config_path=request.policy_config_path,
        )
        phases["finalization"] = perf_counter() - finalization_start
    finally:
        cleanup_start = perf_counter()
        try:
            if queries is not None and not queries_stopped:
                queries.stop()
        finally:
            spark.stop()
        phases["spark_cleanup"] = perf_counter() - cleanup_start

    if receipt is None or result is None:
        raise AssertionError("successful pipeline execution must produce receipt and finalization")
    snapshot = receipt.get("snapshot")
    if not isinstance(snapshot, Mapping):
        raise PipelineRunError("completion receipt snapshot must be an object")
    total_seconds = perf_counter() - total_started
    phases["total"] = total_seconds
    snapshot_sources = _integer(
        snapshot.get("snapshot_event_count"),
        "snapshot.snapshot_event_count",
        minimum=1,
    )
    scheduled_sources = _integer(
        receipt.get("expected_source_event_count"),
        "expected_source_event_count",
        minimum=0,
    )
    scheduled_deliveries = _integer(
        receipt.get("expected_delivery_count"),
        "expected_delivery_count",
        minimum=0,
    )
    acknowledged_deliveries = _integer(
        receipt.get("observed_delivery_count"),
        "observed_delivery_count",
        minimum=0,
    )
    finalized_sources, finalized_deliveries = _load_finalized_counts(
        result.manifest_path,
        request.run_id,
    )
    if (
        finalized_sources != snapshot_sources
        or scheduled_sources > snapshot_sources
        or scheduled_deliveries < scheduled_sources
        or acknowledged_deliveries != scheduled_deliveries
    ):
        raise PipelineRunError("replay and finalized counts are inconsistent")
    counts = {
        "snapshot_source_event_count": snapshot_sources,
        "scheduled_source_event_count": scheduled_sources,
        "scheduled_delivery_count": scheduled_deliveries,
        "acknowledged_delivery_count": acknowledged_deliveries,
        "finalized_source_event_count": finalized_sources,
        "finalized_delivery_count": finalized_deliveries,
    }
    replay_seconds = phases["replay"]
    report: dict[str, object] = {
        "schema_version": "telemetry-pipeline-execution-v1",
        "replay_run_id": request.run_id,
        "snapshot_id": snapshot.get("snapshot_id"),
        "schedule_identity_sha256": receipt.get("schedule_identity_sha256"),
        "started_at_utc": started_at.isoformat().replace("+00:00", "Z"),
        "completed_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "request": _request_contract(request),
        "replay_executed": replay_executed,
        "published": result.published,
        "run_directory": str(result.run_directory),
        "manifest_path": str(result.manifest_path),
        "counts": counts,
        "phase_seconds": {name: round(value, 9) for name, value in sorted(phases.items())},
        "rates_per_second": {
            "acknowledged_replay_delivery_rate": (
                round(acknowledged_deliveries / replay_seconds, 6)
                if replay_executed and replay_seconds > 0
                else None
            ),
            "finalized_source_event_rate": round(finalized_sources / total_seconds, 6),
        },
        "storage_bytes": {
            "stream/bronze": _tree_bytes(paths.bronze),
            "stream/silver_provisional": _tree_bytes(paths.silver_provisional),
            "stream/quarantine": _tree_bytes(paths.quarantine),
            "stream/policy_decisions": _tree_bytes(paths.policy_decisions),
            "finalized/total": _tree_bytes(result.run_directory),
            "finalized/canonical_gold": _tree_bytes(result.run_directory / "canonical_gold"),
            "finalized/retained_security_aware": _tree_bytes(
                result.run_directory / "retained_security_aware"
            ),
        },
    }
    _publish_execution_report(request.execution_report_path, report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic Kafka replay through Spark and exact Gold finalization"
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--interval-ms", type=int, required=True)
    parser.add_argument("--rate-per-second", type=int)
    parser.add_argument("--resume-offset", type=int, default=0)
    parser.add_argument("--duplicate-every", type=int)
    parser.add_argument("--out-of-order-window", type=int, default=1)
    parser.add_argument("--watermark-delay-ms", type=int, required=True)
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="ids.telemetry.raw")
    parser.add_argument("--query-version", required=True)
    parser.add_argument("--policy-config", type=Path, default=_DEFAULT_POLICY_CONFIG)
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    """Command-line entrypoint for one fail-closed course pipeline run."""
    values = _parser().parse_args(arguments)
    request = PipelineRequest(
        artifact_root=values.artifact_root,
        snapshot_directory=values.snapshot,
        run_id=values.run_id,
        start_utc=values.start_utc,
        interval_ms=values.interval_ms,
        rate_per_second=values.rate_per_second,
        resume_offset=values.resume_offset,
        duplicate_every=values.duplicate_every,
        out_of_order_window=values.out_of_order_window,
        watermark_delay_ms=values.watermark_delay_ms,
        bootstrap_servers=values.bootstrap_servers,
        topic=values.topic,
        query_version=values.query_version,
        policy_config_path=values.policy_config,
    )
    print(json.dumps(execute_pipeline(request), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
