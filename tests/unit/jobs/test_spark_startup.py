"""Spark runtime, Kafka source, and streaming output contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip(
    "pyspark",
    reason="Spark job tests require the optional .[spark] dependencies",
)

from jobs.spark import startup as startup_module
from jobs.spark.common import SparkContractError
from jobs.spark.startup import (
    SPARK_KAFKA_CONNECTOR,
    KafkaStreamContract,
    StreamingPaths,
    kafka_source_options,
    parse_java_major_version,
    require_java_17,
    validate_kafka_topic,
)


def test_java_17_preflight_uses_the_installed_runtime() -> None:
    try:
        major = require_java_17()
    except SparkContractError as error:
        pytest.skip(str(error))
    assert major >= 17


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('openjdk version "17.0.10" 2024-01-16', 17),
        ('java version "21.0.2" 2024-01-16 LTS', 21),
        ('java version "1.8.0_401"', 8),
    ],
)
def test_java_version_parser_is_explicit(text: str, expected: int) -> None:
    assert parse_java_major_version(text) == expected


def test_kafka_source_contract_is_controlled_one_partition_and_fail_closed() -> None:
    contract = KafkaStreamContract(
        bootstrap_servers="127.0.0.1:9092",
        topic="telemetry",
        replay_interval_ms=250,
        out_of_order_window=4,
        watermark_delay_ms=751,
    )

    assert SPARK_KAFKA_CONNECTOR == "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"
    assert kafka_source_options(contract) == {
        "kafka.bootstrap.servers": "127.0.0.1:9092",
        "subscribe": "telemetry",
        "startingOffsets": "earliest",
        "includeHeaders": "true",
        "failOnDataLoss": "true",
    }
    assert contract.watermark_delay == "751 milliseconds"


@pytest.mark.parametrize(
    "changes",
    [
        {"watermark_delay_ms": 750},
        {"bootstrap_servers": ""},
        {"topic": ""},
    ],
)
def test_kafka_source_contract_rejects_unsafe_streaming_scope(
    changes: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "bootstrap_servers": "127.0.0.1:9092",
        "topic": "telemetry",
        "replay_interval_ms": 250,
        "out_of_order_window": 4,
        "watermark_delay_ms": 751,
    }
    values.update(changes)
    with pytest.raises(SparkContractError):
        KafkaStreamContract(**values)  # type: ignore[arg-type]


def test_actual_kafka_topic_partition_count_is_checked_through_injected_inspector() -> None:
    contract = KafkaStreamContract(
        bootstrap_servers="127.0.0.1:9092",
        topic="telemetry",
        replay_interval_ms=250,
        out_of_order_window=4,
        watermark_delay_ms=751,
    )
    calls: list[tuple[str, str]] = []

    def one_partition(bootstrap_servers: str, topic: str) -> int:
        calls.append((bootstrap_servers, topic))
        return 1

    assert validate_kafka_topic(contract, partition_inspector=one_partition) == 1
    assert calls == [("127.0.0.1:9092", "telemetry")]

    with pytest.raises(SparkContractError, match="one-partition"):
        validate_kafka_topic(contract, partition_inspector=lambda _bootstrap, _topic: 2)


def test_kafka_admin_metadata_uses_the_pinned_client_shape() -> None:
    descriptions = [
        {
            "error_code": 0,
            "name": "telemetry",
            "partitions": [{"partition_index": 0}],
        }
    ]

    assert startup_module._partition_count_from_descriptions(descriptions, "telemetry") == 1


@pytest.mark.parametrize(
    "descriptions",
    [
        [{"error_code": 0, "topic": "telemetry", "partitions": [{"partition_index": 0}]}],
        [{"error_code": 3, "name": "telemetry", "partitions": []}],
        [{"error_code": 0, "name": "other", "partitions": [{"partition_index": 0}]}],
        [
            {
                "error_code": 0,
                "name": "telemetry",
                "partitions": [{"partition_index": 1}],
            }
        ],
    ],
)
def test_kafka_admin_metadata_fails_closed(descriptions: list[dict[str, object]]) -> None:
    with pytest.raises(SparkContractError, match="invalid metadata"):
        startup_module._partition_count_from_descriptions(descriptions, "telemetry")


def test_streaming_paths_are_explicit_distinct_and_query_versioned(tmp_path: Path) -> None:
    paths = StreamingPaths.build(
        tmp_path,
        snapshot_directory=tmp_path / "snapshot",
        query_version="task6-v1",
    )

    assert set(paths.output_paths) == {
        tmp_path.resolve() / "bronze",
        tmp_path.resolve() / "silver_provisional",
        tmp_path.resolve() / "quarantine",
        tmp_path.resolve() / "policy_decisions",
    }
    assert len(set(paths.checkpoint_paths)) == 4
    assert all("task6-v1" in path.parts for path in paths.checkpoint_paths)
    assert set(paths.output_paths).isdisjoint(paths.checkpoint_paths)


@pytest.mark.parametrize("version", ["", "../v1", "v1/other", "spaces are unsafe"])
def test_streaming_paths_reject_unsafe_checkpoint_version(
    tmp_path: Path,
    version: str,
) -> None:
    with pytest.raises(SparkContractError):
        StreamingPaths.build(
            tmp_path,
            snapshot_directory=tmp_path / "snapshot",
            query_version=version,
        )


@pytest.mark.parametrize("source", ["explicit", "TINYML_IDS_EVIDENCE_ROOT", "CUKD_XAI_ROOT"])
def test_streaming_paths_reject_outputs_or_checkpoints_below_protected_roots(
    tmp_path: Path,
    source: str,
) -> None:
    protected = tmp_path / "evidence"
    stream_root = protected / "generated-stream"
    explicit = (protected,) if source == "explicit" else ()
    environment = {} if source == "explicit" else {source: str(protected)}

    with pytest.raises(SparkContractError, match="evidence root"):
        StreamingPaths.build(
            stream_root,
            snapshot_directory=tmp_path / "snapshot",
            query_version="task6-v1",
            protected_roots=explicit,
            environment=environment,
        )


def test_streaming_paths_always_protect_required_snapshot_directory(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot"

    with pytest.raises(SparkContractError, match="evidence root"):
        StreamingPaths.build(
            snapshot / "generated-stream",
            snapshot_directory=snapshot,
            query_version="task6-v1",
        )


@pytest.mark.parametrize("variable", ["CUKD_XAI_ROOT", "TINYML_IDS_EVIDENCE_ROOT"])
def test_streaming_paths_test_environment_cannot_replace_actual_environment_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
) -> None:
    actual_root = tmp_path / "actual-evidence"
    injected_root = tmp_path / "injected-evidence"
    monkeypatch.setenv(variable, str(actual_root))

    with pytest.raises(SparkContractError, match="evidence root"):
        StreamingPaths.build(
            actual_root / "generated-stream",
            snapshot_directory=tmp_path / "snapshot",
            query_version="task6-v1",
            environment={variable: str(injected_root)},
        )


def _direct_paths(
    root: Path,
    *,
    snapshot_directory: Path,
    protected_roots: tuple[Path, ...] = (),
    overlap_outputs: bool = False,
    version_checkpoints: bool = True,
) -> StreamingPaths:
    version = "task6-v1"
    checkpoint_root = root / "checkpoints"
    if version_checkpoints:
        checkpoint_root /= version
    bronze = root / "bronze"
    return StreamingPaths(
        bronze=bronze,
        silver_provisional=bronze if overlap_outputs else root / "silver_provisional",
        quarantine=root / "quarantine",
        policy_decisions=root / "policy_decisions",
        bronze_checkpoint=checkpoint_root / "bronze",
        silver_checkpoint=checkpoint_root / "silver_provisional",
        quarantine_checkpoint=checkpoint_root / "quarantine",
        policy_checkpoint=checkpoint_root / "policy_decisions",
        query_version=version,
        snapshot_directory=snapshot_directory,
        protected_roots=protected_roots,
    )


def test_direct_streaming_paths_reject_evidence_root_bypass(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"

    with pytest.raises(SparkContractError, match="evidence root"):
        _direct_paths(
            evidence / "stream",
            snapshot_directory=tmp_path / "snapshot",
            protected_roots=(evidence,),
        )


def test_direct_streaming_paths_reject_overlapping_outputs(tmp_path: Path) -> None:
    with pytest.raises(SparkContractError, match="distinct"):
        _direct_paths(
            tmp_path / "stream",
            snapshot_directory=tmp_path / "snapshot",
            overlap_outputs=True,
        )


def test_streaming_paths_reject_nested_output_paths(tmp_path: Path) -> None:
    root = tmp_path / "stream"
    checkpoints = root / "checkpoints" / "task6-v1"

    with pytest.raises(SparkContractError, match="overlap"):
        StreamingPaths(
            bronze=root / "bronze",
            silver_provisional=root / "bronze" / "silver",
            quarantine=root / "quarantine",
            policy_decisions=root / "policy_decisions",
            bronze_checkpoint=checkpoints / "bronze",
            silver_checkpoint=checkpoints / "silver_provisional",
            quarantine_checkpoint=checkpoints / "quarantine",
            policy_checkpoint=checkpoints / "policy_decisions",
            query_version="task6-v1",
            snapshot_directory=tmp_path / "snapshot",
            protected_roots=(),
        )


def test_streaming_paths_reject_checkpoint_nested_below_output(tmp_path: Path) -> None:
    root = tmp_path / "stream"
    bronze = root / "bronze"
    checkpoints = root / "checkpoints" / "task6-v1"

    with pytest.raises(SparkContractError, match="overlap"):
        StreamingPaths(
            bronze=bronze,
            silver_provisional=root / "silver_provisional",
            quarantine=root / "quarantine",
            policy_decisions=root / "policy_decisions",
            bronze_checkpoint=bronze / "task6-v1" / "checkpoint",
            silver_checkpoint=checkpoints / "silver_provisional",
            quarantine_checkpoint=checkpoints / "quarantine",
            policy_checkpoint=checkpoints / "policy_decisions",
            query_version="task6-v1",
            snapshot_directory=tmp_path / "snapshot",
            protected_roots=(),
        )


def test_streaming_paths_reject_output_ancestor_of_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "stream"
    bronze = root / "bronze"
    checkpoints = root / "checkpoints" / "task6-v1"

    with pytest.raises(SparkContractError, match="evidence root"):
        StreamingPaths(
            bronze=bronze,
            silver_provisional=root / "silver_provisional",
            quarantine=root / "quarantine",
            policy_decisions=root / "policy_decisions",
            bronze_checkpoint=checkpoints / "bronze",
            silver_checkpoint=checkpoints / "silver_provisional",
            quarantine_checkpoint=checkpoints / "quarantine",
            policy_checkpoint=checkpoints / "policy_decisions",
            query_version="task6-v1",
            snapshot_directory=bronze / "snapshot",
            protected_roots=(),
        )


def test_direct_streaming_paths_reject_unversioned_checkpoint(tmp_path: Path) -> None:
    with pytest.raises(SparkContractError, match="query_version"):
        _direct_paths(
            tmp_path / "stream",
            snapshot_directory=tmp_path / "snapshot",
            version_checkpoints=False,
        )


def test_direct_streaming_paths_reject_incidental_version_in_output_root(tmp_path: Path) -> None:
    with pytest.raises(SparkContractError, match="query_version"):
        _direct_paths(
            tmp_path / "task6-v1" / "stream",
            snapshot_directory=tmp_path / "snapshot",
            version_checkpoints=False,
        )


def test_streaming_startup_revalidates_paths_before_runtime_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream_root = tmp_path / "stream"
    monkeypatch.delenv("TINYML_IDS_EVIDENCE_ROOT", raising=False)
    monkeypatch.delenv("CUKD_XAI_ROOT", raising=False)
    paths = StreamingPaths.build(
        stream_root,
        snapshot_directory=tmp_path / "snapshot",
        query_version="task6-v1",
    )
    monkeypatch.setenv("TINYML_IDS_EVIDENCE_ROOT", str(stream_root))

    def unexpected_java_check() -> int:
        raise AssertionError("Java preflight must not run before path revalidation")

    monkeypatch.setattr(startup_module, "require_java_17", unexpected_java_check)
    contract = KafkaStreamContract(
        bootstrap_servers="127.0.0.1:9092",
        topic="telemetry",
        replay_interval_ms=250,
        out_of_order_window=1,
        watermark_delay_ms=1,
    )

    with pytest.raises(SparkContractError, match="evidence root"):
        startup_module.start_streaming_job(
            _NoReadSpark(SPARK_KAFKA_CONNECTOR),  # type: ignore[arg-type]
            contract=contract,
            paths=paths,
            partition_inspector=lambda _bootstrap, _topic: 1,
        )


class _FakeSparkConf:
    def __init__(self, packages: str) -> None:
        self._packages = packages

    def get(self, name: str, default: str = "") -> str:
        assert name == "spark.jars.packages"
        return self._packages or default


class _FakeSparkContext:
    def __init__(self, packages: str) -> None:
        self._conf = _FakeSparkConf(packages)

    def getConf(self) -> _FakeSparkConf:  # noqa: N802
        return self._conf


class _FakeSpark:
    def __init__(self, packages: str) -> None:
        self.sparkContext = _FakeSparkContext(packages)


class _NoReadSpark(_FakeSpark):
    version = "4.1.3"

    @property
    def readStream(self) -> None:  # noqa: N802
        raise AssertionError("readStream must not be accessed before connector preflight")


@pytest.mark.parametrize(
    "packages",
    [
        "",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
        f"{SPARK_KAFKA_CONNECTOR},{SPARK_KAFKA_CONNECTOR}",
    ],
)
def test_spark_kafka_connector_preflight_rejects_absent_mismatched_or_duplicate(
    packages: str,
) -> None:
    with pytest.raises(SparkContractError, match="Kafka connector"):
        startup_module.validate_spark_kafka_connector(_FakeSpark(packages))  # type: ignore[arg-type]


def test_spark_kafka_connector_preflight_accepts_exact_coordinate_among_packages() -> None:
    packages = f"com.example:other:1.0,{SPARK_KAFKA_CONNECTOR}"

    assert (
        startup_module.validate_spark_kafka_connector(  # type: ignore[arg-type]
            _FakeSpark(packages)
        )
        == SPARK_KAFKA_CONNECTOR
    )


def test_streaming_startup_rejects_missing_connector_before_read_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(startup_module, "require_java_17", lambda: 17)
    contract = KafkaStreamContract(
        bootstrap_servers="127.0.0.1:9092",
        topic="telemetry",
        replay_interval_ms=250,
        out_of_order_window=1,
        watermark_delay_ms=1,
    )
    paths = StreamingPaths.build(
        tmp_path / "stream",
        snapshot_directory=tmp_path / "snapshot",
        query_version="task6-v1",
    )

    with pytest.raises(SparkContractError, match="Kafka connector"):
        startup_module.start_streaming_job(
            _NoReadSpark(""),  # type: ignore[arg-type]
            contract=contract,
            paths=paths,
            partition_inspector=lambda _bootstrap, _topic: 1,
        )
