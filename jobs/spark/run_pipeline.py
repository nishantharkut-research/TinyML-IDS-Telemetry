"""One-run Spark streaming, deterministic Kafka replay, and Gold finalization."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
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


def execute_pipeline(
    request: PipelineRequest,
    *,
    python_executable: Path = Path(sys.executable),
) -> dict[str, object]:
    """Execute one replay, drain streaming queries, and publish exact Gold."""
    request.run_root.mkdir(parents=True, exist_ok=True)
    spark = _create_spark_session()
    queries: StreamingQueries | None = None
    queries_stopped = False
    try:
        contract = KafkaStreamContract(
            bootstrap_servers=request.bootstrap_servers,
            topic=request.topic,
            replay_interval_ms=request.interval_ms,
            out_of_order_window=request.out_of_order_window,
            watermark_delay_ms=request.watermark_delay_ms,
        )
        paths = request.streaming_paths
        queries = start_streaming_job(spark, contract=contract, paths=paths)
        if not request.completion_receipt_path.exists():
            subprocess.run(
                build_replay_command(request, python_executable=python_executable),
                check=True,
            )
        _require_matching_receipt(request)
        for query in queries.all:
            query.processAllAvailable()
        queries.stop()
        queries_stopped = True

        bronze = spark.read.parquet(str(paths.bronze))
        result = finalize_run(
            bronze,
            snapshot_directory=request.snapshot_directory,
            completion_receipt_path=request.completion_receipt_path,
            output_parent=request.finalized_output_parent,
            policy_config_path=request.policy_config_path,
        )
        return {
            "schema_version": "tinyml-ids-pipeline-result-v1",
            "replay_run_id": request.run_id,
            "run_directory": str(result.run_directory),
            "manifest_path": str(result.manifest_path),
            "published": result.published,
        }
    finally:
        if queries is not None and not queries_stopped:
            queries.stop()
        spark.stop()


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
