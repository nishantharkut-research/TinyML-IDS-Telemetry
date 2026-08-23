from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from hashlib import sha256
from pathlib import Path

import pytest

from jobs.hadoop.oracle_manifest import validate_hadoop_oracle_manifest_mapping
from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.contracts.schema import validate_snapshot_manifest_mapping

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).parents[2]
MAPPER = PROJECT_ROOT / "jobs" / "hadoop" / "mapper.py"
REDUCER = PROJECT_ROOT / "jobs" / "hadoop" / "reducer.py"
POLICY_CONFIG = PROJECT_ROOT / "configs" / "retention-policies.toml"
RUNNER = PROJECT_ROOT / "scripts" / "run-hadoop-oracle.sh"
_SAFE_HDFS_ROOT = re.compile(r"/[A-Za-z0-9._/-]+")


def test_explicit_invalid_hadoop_integration_opt_in_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TINYML_IDS_RUN_HADOOP_INTEGRATION", "0")

    try:
        with pytest.raises(pytest.fail.Exception, match="must equal 1"):
            _prerequisites()
    except pytest.skip.Exception:
        pytest.fail("an explicit integration opt-in value must not skip")


def _prerequisites() -> tuple[str, Path, Path, str]:
    opt_in = os.environ.get("TINYML_IDS_RUN_HADOOP_INTEGRATION")
    if opt_in is None:
        pytest.skip(
            "set TINYML_IDS_RUN_HADOOP_INTEGRATION=1 to opt in to Hadoop Streaming integration"
        )
    if opt_in != "1":
        pytest.fail("TINYML_IDS_RUN_HADOOP_INTEGRATION must equal 1 when explicitly set")
    hadoop_home_value = os.environ.get("HADOOP_HOME", "").strip()
    hdfs_root = os.environ.get("TINYML_IDS_HDFS_TEST_ROOT", "").rstrip("/")
    bash = shutil.which("bash")
    if not hadoop_home_value or not hdfs_root or bash is None:
        pytest.fail("Hadoop integration requires bash, HADOOP_HOME, and TINYML_IDS_HDFS_TEST_ROOT")
    hadoop_home = Path(hadoop_home_value)
    hadoop = hadoop_home / "bin" / "hadoop"
    hdfs = hadoop_home / "bin" / "hdfs"
    streaming_jar = (
        hadoop_home / "share" / "hadoop" / "tools" / "lib" / "hadoop-streaming-3.5.0.jar"
    )
    missing = [path for path in (hadoop, hdfs, streaming_jar) if not path.is_file()]
    if missing:
        pytest.fail(f"Hadoop 3.5.0 prerequisite is absent: {missing[0]}")
    if _SAFE_HDFS_ROOT.fullmatch(hdfs_root) is None or hdfs_root == "/":
        pytest.fail("TINYML_IDS_HDFS_TEST_ROOT must be a safe absolute HDFS test directory")
    return bash, hadoop_home, hdfs, hdfs_root


def _run(command: list[str], *, environment: dict[str, str] | None = None) -> bytes:
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
        capture_output=True,
    )
    return completed.stdout


def _fixture_bytes() -> bytes:
    events = [
        make_event(
            row_id=0,
            predicted_class_id=0,
            true_class_id=0,
        ).to_dict(),
        make_event(
            row_id=1,
            predicted_class_id=3,
            true_class_id=3,
        ).to_dict(),
    ]
    return b"".join((_canonical(event) + "\n").encode("ascii") for event in events)


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _snapshot_manifest(events: bytes) -> dict[str, object]:
    events_sha256 = sha256(events).hexdigest()
    identity = {
        "schema_version": "telemetry-snapshot-v1",
        "sealed_report_sha256": "7" * 64,
        "events_sha256": events_sha256,
        "lineages_sha256": "8" * 64,
        "event_count": 2,
    }
    manifest = {
        "schema_version": "telemetry-snapshot-v1",
        "snapshot_id": sha256(_canonical(identity).encode("ascii")).hexdigest(),
        "created_at_utc": "2026-08-23T00:00:00Z",
        "sealed_report_sha256": "7" * 64,
        "event_count": 2,
        "lineage_count": 1,
        "execution_lineage_count": 1,
        "rows_per_lineage": 2,
        "evaluation_sequence_sha256": "9" * 64,
        "events_sha256": events_sha256,
        "lineages_sha256": "8" * 64,
        "lineage_ids": ["b" * 64],
        "source_interpretation": "fixture",
        "event_time_boundary": "synthetic replay time",
    }
    validate_snapshot_manifest_mapping(manifest)
    return manifest


def _local_oracle(fixture: Path) -> bytes:
    with fixture.open("rb") as source:
        mapped = subprocess.run(
            [
                sys.executable,
                str(MAPPER),
                "--policy-config",
                str(POLICY_CONFIG),
            ],
            cwd=PROJECT_ROOT,
            stdin=source,
            check=True,
            capture_output=True,
        ).stdout
    sorted_mapper_output = b"\n".join(sorted(mapped.splitlines())) + b"\n"
    return subprocess.run(
        [sys.executable, str(REDUCER)],
        cwd=PROJECT_ROOT,
        input=sorted_mapper_output,
        check=True,
        capture_output=True,
    ).stdout


def test_official_hadoop_streaming_matches_the_local_oracle(tmp_path: Path) -> None:
    bash, hadoop_home, hdfs, configured_root = _prerequisites()
    unique_root = f"{configured_root}/tinyml-ids-{uuid.uuid4().hex}"
    hdfs_input = f"{unique_root}/input"
    hdfs_output = f"{unique_root}/output"
    fixture = tmp_path / "events.jsonl"
    fixture.write_bytes(_fixture_bytes())
    snapshot_manifest = tmp_path / "snapshot_manifest.json"
    snapshot_manifest.write_text(
        json.dumps(_snapshot_manifest(fixture.read_bytes()), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    expected = _local_oracle(fixture)
    environment = dict(os.environ)
    environment["HADOOP_HOME"] = str(hadoop_home)

    try:
        _run([str(hdfs), "dfs", "-mkdir", "-p", hdfs_input], environment=environment)
        _run(
            [str(hdfs), "dfs", "-put", str(fixture), f"{hdfs_input}/events.jsonl"],
            environment=environment,
        )
        _run(
            [
                str(hdfs),
                "dfs",
                "-put",
                str(snapshot_manifest),
                f"{hdfs_input}/snapshot_manifest.json",
            ],
            environment=environment,
        )

        runner_output = _run(
            [bash, str(RUNNER), hdfs_input, hdfs_output],
            environment=environment,
        )
        assert runner_output.splitlines() == [
            f"{hdfs_output}/metrics/part-r-00000".encode("ascii"),
            f"{hdfs_output}/hadoop_oracle_manifest.json".encode("ascii"),
        ]
        observed = _run(
            [str(hdfs), "dfs", "-cat", f"{hdfs_output}/metrics/part-r-00000"],
            environment=environment,
        )
        manifest_payload = _run(
            [
                str(hdfs),
                "dfs",
                "-cat",
                f"{hdfs_output}/hadoop_oracle_manifest.json",
            ],
            environment=environment,
        )

        assert observed == expected
        manifest = json.loads(manifest_payload)
        validated = validate_hadoop_oracle_manifest_mapping(manifest)
        assert validated.metrics_sha256 == sha256(observed).hexdigest()
        assert validated.metrics_row_count == len(observed.splitlines())
        assert validated.snapshot_event_count == 2

        repeated = subprocess.run(
            [bash, str(RUNNER), hdfs_input, hdfs_output],
            cwd=PROJECT_ROOT,
            env=environment,
            check=False,
            capture_output=True,
        )
        assert repeated.returncode == 73
        assert repeated.stdout == b""
        assert b"refusing to replace existing HDFS output" in repeated.stderr
        assert (
            _run(
                [str(hdfs), "dfs", "-cat", f"{hdfs_output}/metrics/part-r-00000"],
                environment=environment,
            )
            == observed
        )
    finally:
        primary_failed = sys.exc_info()[0] is not None
        try:
            cleanup = subprocess.run(
                [str(hdfs), "dfs", "-rm", "-r", "-f", unique_root],
                cwd=PROJECT_ROOT,
                env=environment,
                check=False,
                capture_output=True,
            )
        except OSError as error:
            if not primary_failed:
                pytest.fail(f"HDFS integration cleanup failed: {error}")
        else:
            if cleanup.returncode != 0 and not primary_failed:
                pytest.fail(
                    f"HDFS integration cleanup failed with status {cleanup.returncode}: "
                    f"{cleanup.stderr.decode(errors='replace')}"
                )
