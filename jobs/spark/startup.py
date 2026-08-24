"""Validated Spark 4.1.3 Kafka streaming startup."""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from jobs.spark.common import (
    CONTROLLED_TOPIC_PARTITIONS,
    SparkContractError,
    validate_watermark_delay,
)
from jobs.spark.policies import build_policy_decisions
from jobs.spark.stream_telemetry import (
    bounded_deduplicate,
    bronze_from_kafka,
    split_silver_and_quarantine,
)

SPARK_KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"
_QUERY_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")


def parse_java_major_version(version_output: str) -> int:
    """Extract the Java major version from standard java -version output."""
    match = re.search(r'version\s+"(?P<version>[0-9]+)(?:\.(?P<minor>[0-9]+))?', version_output)
    if match is None:
        raise SparkContractError("unable to parse java -version output")
    major = int(match.group("version"))
    if major == 1:
        minor = match.group("minor")
        if minor is None:
            raise SparkContractError("legacy Java version is missing its major component")
        return int(minor)
    return major


def require_java_17() -> int:
    """Fail before Spark startup unless the active Java runtime is version 17+."""
    try:
        process = subprocess.run(
            ["java", "-version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SparkContractError("Java 17 or newer is required on PATH") from error
    output = f"{process.stdout}\n{process.stderr}"
    if process.returncode != 0:
        raise SparkContractError("java -version failed")
    major = parse_java_major_version(output)
    if major < 17:
        raise SparkContractError(f"Java 17 or newer is required; found Java {major}")
    return major


@dataclass(frozen=True, slots=True)
class KafkaStreamContract:
    """Controlled one-partition replay and derived watermark contract."""

    bootstrap_servers: str
    topic: str
    replay_interval_ms: int
    out_of_order_window: int
    watermark_delay_ms: int

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise SparkContractError("bootstrap_servers must be non-empty")
        if not self.topic.strip():
            raise SparkContractError("topic must be non-empty")
        validate_watermark_delay(
            self.watermark_delay_ms,
            self.replay_interval_ms,
            self.out_of_order_window,
        )

    @property
    def watermark_delay(self) -> str:
        return validate_watermark_delay(
            self.watermark_delay_ms,
            self.replay_interval_ms,
            self.out_of_order_window,
        )


def kafka_source_options(contract: KafkaStreamContract) -> dict[str, str]:
    """Return the complete fail-closed Kafka reader options."""
    return {
        "kafka.bootstrap.servers": contract.bootstrap_servers,
        "subscribe": contract.topic,
        "startingOffsets": "earliest",
        "includeHeaders": "true",
        "failOnDataLoss": "true",
    }


PartitionInspector = Callable[[str, str], int]


def _partition_count_from_descriptions(descriptions: object, topic: str) -> int:
    if not isinstance(descriptions, list) or len(descriptions) != 1:
        raise SparkContractError(f"Kafka topic {topic!r} returned invalid metadata")
    description = descriptions[0]
    if (
        not isinstance(description, Mapping)
        or description.get("error_code") != 0
        or description.get("name") != topic
    ):
        raise SparkContractError(f"Kafka topic {topic!r} returned invalid metadata")
    partitions = description.get("partitions")
    if not isinstance(partitions, list) or not partitions:
        raise SparkContractError(f"Kafka topic {topic!r} returned invalid metadata")
    indices: list[int] = []
    for partition in partitions:
        if not isinstance(partition, Mapping):
            raise SparkContractError(f"Kafka topic {topic!r} returned invalid metadata")
        index = partition.get("partition_index")
        if type(index) is not int:
            raise SparkContractError(f"Kafka topic {topic!r} returned invalid metadata")
        indices.append(index)
    if sorted(indices) != list(range(len(partitions))):
        raise SparkContractError(f"Kafka topic {topic!r} returned invalid metadata")
    return len(partitions)


def _kafka_partition_count(bootstrap_servers: str, topic: str) -> int:
    try:
        from kafka import KafkaAdminClient  # type: ignore[import-untyped]
    except ImportError as error:
        raise SparkContractError(
            "Kafka topic validation requires the optional .[kafka] dependencies"
        ) from error
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            descriptions = admin.describe_topics([topic])
        finally:
            admin.close()
    except Exception as error:
        raise SparkContractError(f"unable to inspect Kafka topic {topic!r}: {error}") from error
    return _partition_count_from_descriptions(descriptions, topic)


def validate_kafka_topic(
    contract: KafkaStreamContract,
    *,
    partition_inspector: PartitionInspector | None = None,
) -> int:
    """Inspect the real topic and enforce the controlled one-partition scope."""
    inspector = _kafka_partition_count if partition_inspector is None else partition_inspector
    count = inspector(contract.bootstrap_servers, contract.topic)
    if type(count) is not int or count != CONTROLLED_TOPIC_PARTITIONS:
        raise SparkContractError("streaming correctness requires an actual one-partition topic")
    return count


def validate_spark_kafka_connector(spark: SparkSession) -> str:
    """Require exactly one matching Spark Kafka connector at the pinned version."""
    configured = spark.sparkContext.getConf().get("spark.jars.packages", "")
    packages = [package.strip() for package in configured.split(",") if package.strip()]
    connector_identity = SPARK_KAFKA_CONNECTOR.rsplit(":", 1)[0]
    matching = [package for package in packages if package.rsplit(":", 1)[0] == connector_identity]
    if matching != [SPARK_KAFKA_CONNECTOR]:
        raise SparkContractError(
            f"Spark Kafka connector must contain exactly {SPARK_KAFKA_CONNECTOR}"
        )
    return SPARK_KAFKA_CONNECTOR


@dataclass(frozen=True, slots=True)
class StreamingPaths:
    """Distinct outputs and versioned checkpoints for four streaming queries."""

    bronze: Path
    silver_provisional: Path
    quarantine: Path
    policy_decisions: Path
    bronze_checkpoint: Path
    silver_checkpoint: Path
    quarantine_checkpoint: Path
    policy_checkpoint: Path
    query_version: str
    snapshot_directory: Path
    protected_roots: tuple[Path, ...]

    def __post_init__(self) -> None:
        for name in (
            "bronze",
            "silver_provisional",
            "quarantine",
            "policy_decisions",
            "bronze_checkpoint",
            "silver_checkpoint",
            "quarantine_checkpoint",
            "policy_checkpoint",
            "snapshot_directory",
        ):
            path = Path(getattr(self, name)).expanduser().resolve()
            object.__setattr__(self, name, path)
        protected = tuple(
            dict.fromkeys(Path(root).expanduser().resolve() for root in self.protected_roots)
        )
        object.__setattr__(self, "protected_roots", protected)
        self.validate()

    def validate(self) -> StreamingPaths:
        if _QUERY_VERSION.fullmatch(self.query_version) is None:
            raise SparkContractError("query_version is not a safe checkpoint identifier")
        all_paths = (*self.output_paths, *self.checkpoint_paths)
        if len(set(all_paths)) != len(all_paths):
            raise SparkContractError("all streaming outputs and checkpoints must be distinct")
        for index, path in enumerate(all_paths):
            for other in all_paths[index + 1 :]:
                if path.is_relative_to(other) or other.is_relative_to(path):
                    raise SparkContractError("streaming outputs and checkpoints must not overlap")
        if any(path.parent.name != self.query_version for path in self.checkpoint_paths):
            raise SparkContractError(
                "every checkpoint path must be directly versioned by query_version"
            )
        protected = [self.snapshot_directory, *self.protected_roots]
        for variable in ("TINYML_IDS_EVIDENCE_ROOT", "CUKD_XAI_ROOT"):
            value = os.environ.get(variable)
            if value is not None and value.strip():
                protected.append(Path(value).expanduser().resolve())
        for path in all_paths:
            for evidence_root in tuple(dict.fromkeys(protected)):
                if (
                    path == evidence_root
                    or path.is_relative_to(evidence_root)
                    or evidence_root.is_relative_to(path)
                ):
                    raise SparkContractError(
                        "streaming output and checkpoints must remain outside every evidence root"
                    )
        return self

    @classmethod
    def build(
        cls,
        root: Path,
        *,
        snapshot_directory: Path,
        query_version: str,
        protected_roots: tuple[Path, ...] = (),
        environment: Mapping[str, str] | None = None,
    ) -> StreamingPaths:
        base = root.expanduser().resolve()
        checkpoints = base / "checkpoints" / query_version
        injected_roots = list(protected_roots)
        for variable in ("TINYML_IDS_EVIDENCE_ROOT", "CUKD_XAI_ROOT"):
            value = (environment or {}).get(variable)
            if value is not None and value.strip():
                injected_roots.append(Path(value))
        return cls(
            bronze=base / "bronze",
            silver_provisional=base / "silver_provisional",
            quarantine=base / "quarantine",
            policy_decisions=base / "policy_decisions",
            bronze_checkpoint=checkpoints / "bronze",
            silver_checkpoint=checkpoints / "silver_provisional",
            quarantine_checkpoint=checkpoints / "quarantine",
            policy_checkpoint=checkpoints / "policy_decisions",
            query_version=query_version,
            snapshot_directory=snapshot_directory,
            protected_roots=tuple(injected_roots),
        )

    @property
    def output_paths(self) -> tuple[Path, ...]:
        return (self.bronze, self.silver_provisional, self.quarantine, self.policy_decisions)

    @property
    def checkpoint_paths(self) -> tuple[Path, ...]:
        return (
            self.bronze_checkpoint,
            self.silver_checkpoint,
            self.quarantine_checkpoint,
            self.policy_checkpoint,
        )


@dataclass(frozen=True, slots=True)
class StreamingQueries:
    """The four independently checkpointed streaming sinks."""

    bronze: StreamingQuery
    silver_provisional: StreamingQuery
    quarantine: StreamingQuery
    policy_decisions: StreamingQuery

    def stop(self) -> None:
        for query in self.all:
            if query.isActive:
                query.stop()

    @property
    def all(self) -> tuple[StreamingQuery, ...]:
        return (
            self.bronze,
            self.silver_provisional,
            self.quarantine,
            self.policy_decisions,
        )


def _start_parquet(
    frame: DataFrame,
    *,
    output: Path,
    checkpoint: Path,
    query_name: str,
) -> StreamingQuery:
    return (
        frame.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output))
        .option("checkpointLocation", str(checkpoint))
        .queryName(query_name)
        .start()
    )


def start_streaming_job(
    spark: SparkSession,
    *,
    contract: KafkaStreamContract,
    paths: StreamingPaths,
    partition_inspector: PartitionInspector | None = None,
) -> StreamingQueries:
    """Start append-only Bronze and separate provisional derived Parquet sinks."""
    paths.validate()
    require_java_17()
    if spark.version != "4.1.3":
        raise SparkContractError(f"Spark 4.1.3 is required; found {spark.version}")
    validate_spark_kafka_connector(spark)
    validate_kafka_topic(contract, partition_inspector=partition_inspector)
    kafka = spark.readStream.format("kafka").options(**kafka_source_options(contract)).load()
    bronze = bronze_from_kafka(kafka)
    silver, quarantine = split_silver_and_quarantine(bronze)
    bounded_silver = bounded_deduplicate(
        silver,
        watermark_delay_ms=contract.watermark_delay_ms,
        replay_interval_ms=contract.replay_interval_ms,
        out_of_order_window=contract.out_of_order_window,
    )
    policy_decisions = build_policy_decisions(bounded_silver)
    specifications = (
        (bronze, paths.bronze, paths.bronze_checkpoint, "bronze"),
        (
            bounded_silver,
            paths.silver_provisional,
            paths.silver_checkpoint,
            "silver-provisional",
        ),
        (quarantine, paths.quarantine, paths.quarantine_checkpoint, "quarantine"),
        (
            policy_decisions,
            paths.policy_decisions,
            paths.policy_checkpoint,
            "policy-decisions",
        ),
    )
    started: list[StreamingQuery] = []
    try:
        for frame, output, checkpoint, name in specifications:
            started.append(
                _start_parquet(
                    frame,
                    output=output,
                    checkpoint=checkpoint,
                    query_name=f"{name}-{paths.query_version}",
                )
            )
    except Exception:
        for query in started:
            query.stop()
        raise
    return StreamingQueries(*started)
