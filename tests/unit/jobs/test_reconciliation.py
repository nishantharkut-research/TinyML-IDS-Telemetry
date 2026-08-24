from __future__ import annotations

import io
import json
import os
import shutil
import site
import subprocess
import sys
from collections.abc import Iterable, Mapping
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from jobs.hadoop.mapper import run as run_mapper
from jobs.hadoop.oracle_manifest import (
    OracleManifestError,
    validate_finalized_run_manifest_mapping,
    verify_snapshot_events,
)
from jobs.hadoop.reconcile import (
    ReconciliationError,
    _number,
    _spark_metrics,
    canonical_report,
    main,
    publish_report,
)
from jobs.hadoop.reconcile import (
    reconcile as _core_reconcile,
)
from jobs.hadoop.reducer import run as run_reducer
from tests.helpers.event_factory import make_event
from tinyml_ids_telemetry.contracts.schema import ValidatedSnapshotManifest

PROJECT_ROOT = Path(__file__).parents[3]
RUNNER = PROJECT_ROOT / "scripts" / "run-hadoop-oracle.sh"
POLICY_CONFIG = PROJECT_ROOT / "configs" / "retention-policies.toml"
IGNORED_SPECIFIC_SUMS = {
    "ignored_wrong_endpoint_count_sum": 1,
    "ignored_bad_length_count_sum": 2,
    "ignored_non_ascii_count_sum": 3,
    "ignored_malformed_count_sum": 4,
    "ignored_bad_prefix_count_sum": 5,
    "ignored_bad_identity_count_sum": 6,
    "ignored_bad_attempt_count_sum": 7,
    "ignored_bad_inner_hex_count_sum": 8,
    "ignored_bad_crc_count_sum": 9,
    "ignored_bad_inner_text_count_sum": 10,
    "ignored_wrong_session_count_sum": 11,
    "ignored_wrong_stage_count_sum": 12,
    "ignored_wrong_transaction_count_sum": 13,
    "ignored_wrong_attempt_count_sum": 14,
}


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _metric(
    metric: str,
    value: int | float,
    dimensions: Mapping[str, object] | None = None,
    *,
    unit: str = "hardware_execution",
) -> str:
    return _canonical(
        {
            "dimensions": dict(dimensions or {}),
            "metric": metric,
            "statistical_unit": unit,
            "value": value,
        }
    )


def _spark_aggregates() -> dict[str, list[dict[str, object]]]:
    return {
        "execution_dimension_counts": [
            {
                "replay_run_id": "run-1",
                "dimension_kind": "board",
                "board": "esp32c3",
                "student_model": None,
                "class_id": None,
                "class_name": None,
                "statistical_unit": "hardware_execution",
                "hardware_execution_count": 1,
            },
            {
                "replay_run_id": "run-1",
                "dimension_kind": "student_model",
                "board": None,
                "student_model": "student_A",
                "class_id": None,
                "class_name": None,
                "statistical_unit": "hardware_execution",
                "hardware_execution_count": 1,
            },
            {
                "replay_run_id": "run-1",
                "dimension_kind": "true_class",
                "board": None,
                "student_model": None,
                "class_id": 0,
                "class_name": "Blackhole",
                "statistical_unit": "hardware_execution",
                "hardware_execution_count": 1,
            },
            {
                "replay_run_id": "run-1",
                "dimension_kind": "predicted_class",
                "board": None,
                "student_model": None,
                "class_id": 0,
                "class_name": "Blackhole",
                "statistical_unit": "hardware_execution",
                "hardware_execution_count": 1,
            },
            {
                "replay_run_id": "run-1",
                "dimension_kind": "board_student_model",
                "board": "esp32c3",
                "student_model": "student_A",
                "class_id": None,
                "class_name": None,
                "statistical_unit": "hardware_execution",
                "hardware_execution_count": 1,
            },
        ],
        "operational_metrics": [
            {
                "replay_run_id": "run-1",
                "lineage_id": "b" * 64,
                "board": "esp32c3",
                "student_model": "student_A",
                "statistical_unit": "hardware_execution",
                "hardware_execution_count": 1,
                "successful_execution_count": 1,
                "predicted_attack_execution_count": 1,
                "retry_event_count": 1,
                "retry_count": 1,
                "response_timeout_count_sum": 1,
                "ignored_datagram_count_sum": 2,
                **IGNORED_SPECIFIC_SUMS,
                "preprocess_us_sum": 10,
                "inference_us_sum": 20,
                "total_us_sum": 30,
                "request_datagram_bytes_sent_sum": 200,
                "response_bytes_sum": 160,
                "host_observed_datagram_rtt_us_sum": 1_000,
                "transaction_elapsed_us_sum": 1_000,
            }
        ],
        "predictive_row_metrics": [
            {
                "replay_run_id": "run-1",
                "metric": "hardware_execution_count",
                "statistical_unit": "hardware_execution",
                "value": 1,
            },
            {
                "replay_run_id": "run-1",
                "metric": "execution_lineage_count",
                "statistical_unit": "execution_lineage",
                "value": 1,
            },
            {
                "replay_run_id": "run-1",
                "metric": "unique_predictive_row_count",
                "statistical_unit": "predictive_source_row",
                "value": 1,
            },
        ],
        "retention_metrics": [
            {
                "replay_run_id": "run-1",
                "policy_id": "retain-all-v1",
                "statistical_unit": "hardware_execution",
                "hardware_execution_count": 1,
                "retained_hardware_execution_count": 1,
                "predicted_attack_execution_count": 1,
                "retained_predicted_attack_execution_count": 1,
                "actual_attack_execution_count": 1,
                "retained_actual_attack_execution_count": 1,
                "inclusion_probability_numerator_sum": 1,
                "inclusion_probability_denominator_sum": 1,
            },
            {
                "replay_run_id": "run-1",
                "policy_id": "security-aware-static-v1",
                "statistical_unit": "hardware_execution",
                "hardware_execution_count": 1,
                "retained_hardware_execution_count": 1,
                "predicted_attack_execution_count": 1,
                "retained_predicted_attack_execution_count": 1,
                "actual_attack_execution_count": 1,
                "retained_actual_attack_execution_count": 1,
                "inclusion_probability_numerator_sum": 1,
                "inclusion_probability_denominator_sum": 1,
            },
        ],
        "retention_reason_counts": [
            {
                "replay_run_id": "run-1",
                "policy_id": "retain-all-v1",
                "reason": "retain_all",
                "retained": True,
                "statistical_unit": "hardware_execution",
                "decision_count": 1,
            },
            {
                "replay_run_id": "run-1",
                "policy_id": "security-aware-static-v1",
                "reason": "predicted_attack",
                "retained": True,
                "statistical_unit": "hardware_execution",
                "decision_count": 1,
            },
        ],
    }


def _hadoop_rows() -> list[str]:
    source_id = "a" * 64
    lineage_id = "b" * 64
    payload = "c" * 64
    lineage = {
        "lineage_id": lineage_id,
        "board": "esp32c3",
        "student_model": "student_A",
    }
    rows = [
        _metric(
            "source_event_occurrence_count",
            1,
            {
                "source_event_id": source_id,
                "lineage_id": lineage_id,
                "row_id": 0,
                "source_row_index": 7,
                "payload_sha256": payload,
            },
            unit="canonical_input_record",
        ),
        _metric(
            "lineage_row_occurrence_count",
            1,
            {
                "lineage_id": lineage_id,
                "row_id": 0,
                "source_event_id": source_id,
                "payload_sha256": payload,
            },
            unit="canonical_input_record",
        ),
        _metric(
            "predictive_source_row_occurrence_count",
            1,
            {
                "source_row_index": 7,
                "lineage_id": lineage_id,
                "source_event_id": source_id,
                "payload_sha256": payload,
            },
            unit="predictive_source_row",
        ),
        _metric("hardware_execution_count", 1),
        _metric("hardware_execution_count", 1, {"board": "esp32c3"}),
        _metric("hardware_execution_count", 1, {"student_model": "student_A"}),
        _metric(
            "hardware_execution_count",
            1,
            {"true_class_id": 0, "true_class_name": "Blackhole"},
        ),
        _metric(
            "hardware_execution_count",
            1,
            {"predicted_class_id": 0, "predicted_class_name": "Blackhole"},
        ),
        _metric(
            "hardware_execution_count",
            1,
            {"board": "esp32c3", "student_model": "student_A"},
        ),
    ]
    operational = {
        "hardware_execution_count": 1,
        "successful_execution_count": 1,
        "predicted_attack_execution_count": 1,
        "retry_event_count": 1,
        "retry_count": 1,
        "response_timeout_count_sum": 1,
        "ignored_datagram_count_sum": 2,
        **IGNORED_SPECIFIC_SUMS,
        "preprocess_us_sum": 10,
        "inference_us_sum": 20,
        "total_us_sum": 30,
        "request_datagram_bytes_sent_sum": 200,
        "response_bytes_sum": 160,
        "host_observed_datagram_rtt_us_sum": 1_000,
        "transaction_elapsed_us_sum": 1_000,
    }
    retention = {
        "hardware_execution_count": 1,
        "retained_hardware_execution_count": 1,
        "predicted_attack_execution_count": 1,
        "retained_predicted_attack_execution_count": 1,
        "actual_attack_execution_count": 1,
        "retained_actual_attack_execution_count": 1,
        "inclusion_probability_numerator_sum": 1,
        "inclusion_probability_denominator_sum": 1,
    }
    rows.extend(_metric(metric, value, lineage) for metric, value in operational.items())
    for policy_id, reason in (
        ("retain-all-v1", "retain_all"),
        ("security-aware-static-v1", "predicted_attack"),
    ):
        policy = {"policy_id": policy_id}
        rows.extend(_metric(metric, value, policy) for metric, value in retention.items())
        rows.append(
            _metric(
                "decision_count",
                1,
                {"policy_id": policy_id, "reason": reason, "retained": True},
            )
        )
    return sorted(rows, key=_mapper_key_for_reducer_row)


def _actual_reducer_rows() -> list[str]:
    event = make_event(
        board="esp32c3",
        predicted_class_id=0,
        true_class_id=0,
        attempts=2,
        response_timeout_count=1,
        ignored_datagram_count=2,
    ).to_dict()
    event.update(
        source_event_id="a" * 64,
        lineage_id="b" * 64,
        source_row_index=7,
    )
    event.update(
        {field.removesuffix("_sum"): value for field, value in IGNORED_SPECIFIC_SUMS.items()}
    )
    mapped = io.StringIO()
    run_mapper(io.StringIO(_canonical(event) + "\n"), mapped, POLICY_CONFIG)
    reduced = io.StringIO()
    mapped_lines = sorted(mapped.getvalue().splitlines())
    run_reducer(io.StringIO("\n".join(mapped_lines) + "\n"), reduced)
    return reduced.getvalue().splitlines()


def _mapper_key_for_reducer_row(raw: str) -> str:
    row = json.loads(raw)
    dimensions = row["dimensions"]
    metric = row["metric"]
    names: tuple[str, ...]
    if metric == "source_event_occurrence_count":
        names = (
            "source_event_id",
            "lineage_id",
            "row_id",
            "source_row_index",
            "payload_sha256",
        )
    elif metric == "lineage_row_occurrence_count":
        names = ("lineage_id", "row_id", "source_event_id", "payload_sha256")
    elif metric == "predictive_source_row_occurrence_count":
        names = ("source_row_index", "lineage_id", "source_event_id", "payload_sha256")
    elif set(dimensions) == {"lineage_id", "board", "student_model"}:
        names = ("lineage_id", "board", "student_model")
    elif set(dimensions) == {"policy_id", "reason", "retained"}:
        names = ("policy_id", "reason", "retained")
    elif set(dimensions) == {"policy_id"}:
        names = ("policy_id",)
    else:
        names = tuple(sorted(dimensions))
    return _canonical(
        {
            "dimensions": [[name, dimensions[name]] for name in names],
            "metric": metric,
            "statistical_unit": row["statistical_unit"],
        }
    )


def _differences(report: Mapping[str, object]) -> list[dict[str, object]]:
    return cast(list[dict[str, object]], report["differences"])


def _summary(report: Mapping[str, object]) -> dict[str, int]:
    return cast(dict[str, int], report["summary"])


def _metrics_payload(rows: list[str]) -> bytes:
    return b"".join(raw.encode("utf-8") + b"\t\n" for raw in rows)


def _spark_payload(aggregates: Mapping[str, object]) -> bytes:
    return (_canonical(aggregates) + "\n").encode("ascii")


def _canonical_event_identity(rows: list[str]) -> str:
    source: dict[str, tuple[str, int, int, str]] = {}
    for raw in rows:
        try:
            row = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        dimensions = row["dimensions"]
        if not isinstance(dimensions, dict):
            continue
        if row.get("metric") == "source_event_occurrence_count" and set(dimensions) == {
            "source_event_id",
            "lineage_id",
            "row_id",
            "source_row_index",
            "payload_sha256",
        }:
            source[dimensions["source_event_id"]] = (
                dimensions["lineage_id"],
                dimensions["row_id"],
                dimensions["source_row_index"],
                dimensions["payload_sha256"],
            )
    digest = sha256()
    for source_event_id in sorted(source):
        lineage_id, row_id, source_row_index, payload_sha256 = source[source_event_id]
        digest.update(
            f"{source_event_id}|{lineage_id}|{row_id}|{source_row_index}|{payload_sha256}\n".encode(
                "ascii"
            )
        )
    return digest.hexdigest()


def _finalized_run_manifest(
    *,
    source_event_count: int = 1,
    delivery_count: int | None = None,
    event_identity_sha256: str | None = None,
    spark_aggregates: Mapping[str, object] | None = None,
) -> dict[str, object]:
    aggregates = _spark_aggregates() if spark_aggregates is None else spark_aggregates
    return {
        "schema_version": "telemetry-finalized-run-v1",
        "replay_run_id": "run-1",
        "snapshot_id": "1" * 64,
        "events_sha256": "6" * 64,
        "schedule_identity_sha256": "2" * 64,
        "completion_receipt_sha256": "3" * 64,
        "source_event_count": source_event_count,
        "delivery_count": source_event_count if delivery_count is None else delivery_count,
        "lineage_counts": [{"lineage_id": "b" * 64, "hardware_execution_rows": source_event_count}],
        "canonical_gold_identity_sha256": "4" * 64,
        "canonical_event_identity_sha256": (
            _canonical_event_identity(_hadoop_rows())
            if event_identity_sha256 is None
            else event_identity_sha256
        ),
        "spark_aggregates_sha256": sha256(_spark_payload(aggregates)).hexdigest(),
        "policy_config_sha256": "5" * 64,
        "policy_ids": ["retain-all-v1", "security-aware-static-v1"],
        "outputs": {"canonical_gold": {"row_count": 1}},
        "statistical_unit": "hardware executions",
        "publication_contract": "canonical snapshot rows",
    }


def _source_occurrence_count(rows: list[str]) -> int:
    count = 0
    for raw in rows:
        try:
            row = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and row.get("metric") == "source_event_occurrence_count":
            count += 1
    return max(count, 1)


def _hadoop_oracle_manifest(rows: list[str]) -> dict[str, object]:
    payload = _metrics_payload(rows)
    return {
        "schema_version": "hadoop-oracle-manifest-v1",
        "snapshot_id": "1" * 64,
        "events_sha256": "6" * 64,
        "snapshot_event_count": _source_occurrence_count(rows),
        "policy_config_sha256": "5" * 64,
        "hadoop_version": "3.5.0",
        "reducer_count": 1,
        "input_events_hdfs_path": "/fixtures/canonical/events.jsonl",
        "output_metrics_hdfs_path": "/results/oracle/part-r-00000",
        "metrics_sha256": sha256(payload).hexdigest(),
        "metrics_row_count": len(rows),
        "canonical_event_identity_sha256": _canonical_event_identity(rows),
    }


def reconcile(
    spark_aggregates: Mapping[str, list[dict[str, object]]],
    hadoop_metric_lines: Iterable[str],
    *,
    percentile_tolerances: Mapping[str, int | float] | None = None,
) -> dict[str, object]:
    rows = list(hadoop_metric_lines)
    source_event_count = _source_occurrence_count(rows)
    payload = _metrics_payload(rows)
    return _core_reconcile(
        spark_aggregates,
        rows,
        finalized_run_manifest=_finalized_run_manifest(
            source_event_count=source_event_count,
            event_identity_sha256=_canonical_event_identity(rows),
            spark_aggregates=spark_aggregates,
        ),
        hadoop_oracle_manifest=_hadoop_oracle_manifest(rows),
        hadoop_metrics_sha256=sha256(payload).hexdigest(),
        spark_aggregates_sha256=sha256(_spark_payload(spark_aggregates)).hexdigest(),
        percentile_tolerances=percentile_tolerances,
    )


def _cli_inputs(
    tmp_path: Path,
    rows: list[str],
) -> tuple[Path, Path]:
    spark_path = tmp_path / "spark-aggregates.json"
    hadoop_path = tmp_path / "hadoop-metrics.jsonl"
    spark_path.write_bytes(_spark_payload(_spark_aggregates()))
    hadoop_path.write_bytes(_metrics_payload(rows))
    source_event_count = _source_occurrence_count(rows)
    (tmp_path / "finalized-run-manifest.json").write_bytes(
        (_canonical(_finalized_run_manifest(source_event_count=source_event_count)) + "\n").encode(
            "ascii"
        )
    )
    (tmp_path / "hadoop-run-manifest.json").write_bytes(
        (_canonical(_hadoop_oracle_manifest(rows)) + "\n").encode("ascii")
    )
    return spark_path, hadoop_path


def _cli_arguments(spark_path: Path, hadoop_path: Path) -> list[str]:
    return [
        "--spark-aggregates",
        str(spark_path),
        "--hadoop-metrics",
        str(hadoop_path),
        "--finalized-run-manifest",
        str(spark_path.parent / "finalized-run-manifest.json"),
        "--hadoop-run-manifest",
        str(spark_path.parent / "hadoop-run-manifest.json"),
    ]


def _bash_executable() -> str:
    candidates = [
        os.environ.get("BASH"),
        r"C:\Program Files\Git\bin\bash.exe" if os.name == "nt" else None,
        shutil.which("bash"),
    ]
    for candidate in candidates:
        if not candidate or not Path(candidate).is_file():
            continue
        if os.name == "nt" and "git" not in str(Path(candidate)).lower():
            continue
        completed = subprocess.run(
            [candidate, "--version"],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode == 0 and "GNU bash" in completed.stdout:
            return candidate
    pytest.skip("runner execution contract requires GNU Bash")


def _bash_path(path: Path) -> str:
    resolved = path.resolve()
    if os.name != "nt":
        return str(resolved)
    drive = resolved.drive.removesuffix(":").lower()
    relative = resolved.as_posix()[3:]
    return f"/{drive}/{relative}"


def _snapshot_manifest(events: bytes) -> dict[str, object]:
    events_sha256 = sha256(events).hexdigest()
    identity = {
        "schema_version": "telemetry-snapshot-v1",
        "sealed_report_sha256": "7" * 64,
        "events_sha256": events_sha256,
        "lineages_sha256": "8" * 64,
        "event_count": 1,
    }
    return {
        "schema_version": "telemetry-snapshot-v1",
        "snapshot_id": sha256(_canonical(identity).encode("ascii")).hexdigest(),
        "created_at_utc": "2026-08-23T00:00:00Z",
        "sealed_report_sha256": "7" * 64,
        "event_count": 1,
        "lineage_count": 1,
        "execution_lineage_count": 1,
        "rows_per_lineage": 1,
        "evaluation_sequence_sha256": "9" * 64,
        "events_sha256": events_sha256,
        "lineages_sha256": "8" * 64,
        "lineage_ids": ["b" * 64],
        "source_interpretation": "fixture",
        "event_time_boundary": "synthetic replay time",
    }


def _run_fake_runner(
    tmp_path: Path,
    *,
    snapshot_get_status: int = 0,
    events_cat_status: int = 0,
    hadoop_job_status: int = 0,
    manifest_put_status: int = 0,
    final_rename_status: int = 0,
    publication_lock_status: int = 0,
    preexisting_output: bool = False,
    isolated_gateway_python: bool = False,
    task_python: str = "python3",
) -> tuple[subprocess.CompletedProcess[str], Path, Path, Path]:
    hadoop_home = tmp_path / "hadoop-3.5.0"
    binary_directory = hadoop_home / "bin"
    jar = hadoop_home / "share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar"
    binary_directory.mkdir(parents=True)
    jar.parent.mkdir(parents=True)
    jar.write_bytes(b"fake exact-name streaming jar")
    capture = tmp_path / "hadoop-argv.txt"
    hdfs_capture = tmp_path / "hdfs-argv.txt"
    published_manifest = tmp_path / "published-hadoop-manifest.json"
    events = tmp_path / "events.jsonl"
    snapshot_manifest = tmp_path / "snapshot-manifest.json"
    metrics = tmp_path / "part-00000"
    events.write_bytes(b"{}\n")
    snapshot_manifest.write_bytes(
        (
            json.dumps(_snapshot_manifest(events.read_bytes()), indent=2, sort_keys=True) + "\n"
        ).encode("ascii")
    )
    metrics.write_bytes(_metrics_payload(_hadoop_rows()))
    hadoop = binary_directory / "hadoop"
    hdfs = binary_directory / "hdfs"
    hadoop.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
if [[ ${1:-} == version ]]; then
    printf 'Hadoop 3.5.0\n'
    exit 0
fi
if [[ ${1:-} == jar ]]; then
    printf '%s\n' "$@" > "$FAKE_HADOOP_ARGV"
    printf 'packageJobJar: fake Hadoop staging output\n'
    exit "$FAKE_HADOOP_JOB_STATUS"
fi
exit 99
""",
        encoding="utf-8",
        newline="\n",
    )
    hdfs.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_HDFS_ARGV"
if [[ ${1:-} != dfs ]]; then
    exit 98
fi
if [[ ${2:-} == -get ]]; then
    if [[ ${3##*/} == snapshot_manifest.json ]]; then
        if [[ $FAKE_HDFS_SNAPSHOT_GET_STATUS -ne 0 ]]; then
            exit "$FAKE_HDFS_SNAPSHOT_GET_STATUS"
        fi
        cp "$FAKE_SNAPSHOT_MANIFEST" "$4"
    else
        if [[ $FAKE_HDFS_EVENTS_CAT_STATUS -ne 0 ]]; then
            exit "$FAKE_HDFS_EVENTS_CAT_STATUS"
        fi
        cp "$FAKE_EVENTS" "$4"
    fi
    exit 0
fi
if [[ ${2:-} == -mkdir ]]; then
    exit 0
fi
if [[ ${2:-} == -test && ${3:-} == -e ]]; then
    if [[ $FAKE_HDFS_PREEXISTING_OUTPUT == 1 ]]; then
        exit 0
    fi
    exit 1
fi
if [[ ${2:-} == -cat && ${3##*/} == events.jsonl ]]; then
    cat "$FAKE_EVENTS"
    exit "$FAKE_HDFS_EVENTS_CAT_STATUS"
fi
if [[ ${2:-} == -cat && ${3##*/} == part-00000 ]]; then
    cat "$FAKE_METRICS"
    exit 0
fi
if [[ ${2:-} == -put ]]; then
    if [[ ${4:-} == *.publication-lock ]]; then
        exit "$FAKE_HDFS_PUBLICATION_LOCK_STATUS"
    fi
    if [[ ${3##*/} == hadoop_oracle_manifest.json ]]; then
        if [[ $FAKE_HDFS_MANIFEST_PUT_STATUS -ne 0 ]]; then
            exit "$FAKE_HDFS_MANIFEST_PUT_STATUS"
        fi
        cp "$3" "$FAKE_PUBLISHED_MANIFEST"
    fi
    exit 0
fi
if [[ ${2:-} == -mv ]]; then
    exit "$FAKE_HDFS_FINAL_RENAME_STATUS"
fi
if [[ ${2:-} == -rm ]]; then
    exit 0
fi
exit 97
""",
        encoding="utf-8",
        newline="\n",
    )
    hadoop.chmod(0o755)
    hdfs.chmod(0o755)
    python_bin = Path(sys.executable)
    environment = dict(os.environ)
    environment.update(
        HADOOP_HOME=_bash_path(hadoop_home),
        HADOOP_TASK_PYTHON=task_python,
        PYTHON_BIN=_bash_path(python_bin),
        FAKE_HADOOP_ARGV=_bash_path(capture),
        FAKE_HADOOP_JOB_STATUS=str(hadoop_job_status),
        FAKE_HDFS_ARGV=_bash_path(hdfs_capture),
        FAKE_HDFS_SNAPSHOT_GET_STATUS=str(snapshot_get_status),
        FAKE_HDFS_EVENTS_CAT_STATUS=str(events_cat_status),
        FAKE_HDFS_MANIFEST_PUT_STATUS=str(manifest_put_status),
        FAKE_HDFS_FINAL_RENAME_STATUS=str(final_rename_status),
        FAKE_HDFS_PUBLICATION_LOCK_STATUS=str(publication_lock_status),
        FAKE_HDFS_PREEXISTING_OUTPUT="1" if preexisting_output else "0",
        FAKE_SNAPSHOT_MANIFEST=_bash_path(snapshot_manifest),
        FAKE_EVENTS=_bash_path(events),
        FAKE_METRICS=_bash_path(metrics),
        FAKE_PUBLISHED_MANIFEST=_bash_path(published_manifest),
    )
    if isolated_gateway_python:
        bootstrap = tmp_path / "gateway-python-bootstrap.py"
        bootstrap.write_text(
            """import os
import runpy
import sys

sys.path.append(os.environ["FAKE_SITE_PACKAGES"])
script = sys.argv[1]
sys.argv = sys.argv[1:]
runpy.run_path(script, run_name="__main__")
""",
            encoding="utf-8",
            newline="\n",
        )
        wrapper = tmp_path / "gateway-python"
        wrapper.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
exec "$FAKE_REAL_PYTHON" -S "$FAKE_PYTHON_BOOTSTRAP" "$@"
""",
            encoding="utf-8",
            newline="\n",
        )
        wrapper.chmod(0o755)
        environment.update(
            PYTHONPATH="",
            PYTHON_BIN=_bash_path(wrapper),
            FAKE_REAL_PYTHON=_bash_path(python_bin),
            FAKE_PYTHON_BOOTSTRAP=_bash_path(bootstrap),
            FAKE_SITE_PACKAGES=str(
                next(
                    Path(path)
                    for path in site.getsitepackages()
                    if (Path(path) / "jsonschema").is_dir()
                )
            ),
        )
    completed = subprocess.run(
        [
            _bash_executable(),
            _bash_path(RUNNER),
            "/fixtures/canonical",
            "/results/oracle",
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed, capture, published_manifest, hdfs_capture


def _replace_metric(rows: list[str], metric: str, value: int) -> list[str]:
    replaced: list[str] = []
    changed = False
    for raw in rows:
        row = json.loads(raw)
        if not changed and row["metric"] == metric and row["dimensions"].get("lineage_id"):
            row["value"] = value
            raw = _canonical(row)
            changed = True
        replaced.append(raw)
    assert changed
    return replaced


def test_source_identity_mutation_after_manifest_generation_is_a_digest_mismatch() -> None:
    rows = _hadoop_rows()
    hadoop_manifest = _hadoop_oracle_manifest(rows)
    mutated = [json.loads(raw) for raw in rows]
    occurrence = next(row for row in mutated if row["metric"] == "source_event_occurrence_count")
    occurrence["dimensions"]["source_event_id"] = "d" * 64
    mutated_rows = sorted((_canonical(row) for row in mutated), key=_mapper_key_for_reducer_row)
    actual_metrics_sha256 = sha256(_metrics_payload(mutated_rows)).hexdigest()

    report = _core_reconcile(
        _spark_aggregates(),
        mutated_rows,
        finalized_run_manifest=_finalized_run_manifest(),
        hadoop_oracle_manifest=hadoop_manifest,
        hadoop_metrics_sha256=actual_metrics_sha256,
        spark_aggregates_sha256=sha256(_spark_payload(_spark_aggregates())).hexdigest(),
    )

    assert report["matched"] is False
    assert {
        "actual_value": actual_metrics_sha256,
        "expected_value": hadoop_manifest["metrics_sha256"],
        "field": "metrics_sha256",
        "kind": "provenance_mismatch",
    } in _differences(report)


def test_reconciliation_rejects_inconsistent_occurrence_identity_mappings() -> None:
    rows = [json.loads(raw) for raw in _hadoop_rows()]
    for row in rows:
        if row["metric"] in {
            "lineage_row_occurrence_count",
            "predictive_source_row_occurrence_count",
        }:
            row["dimensions"]["source_event_id"] = "d" * 64
    mutated = sorted((_canonical(row) for row in rows), key=_mapper_key_for_reducer_row)
    payload = _metrics_payload(mutated)
    hadoop_manifest = _hadoop_oracle_manifest(mutated)

    report = _core_reconcile(
        _spark_aggregates(),
        mutated,
        finalized_run_manifest=_finalized_run_manifest(),
        hadoop_oracle_manifest=hadoop_manifest,
        hadoop_metrics_sha256=sha256(payload).hexdigest(),
        spark_aggregates_sha256=sha256(_spark_payload(_spark_aggregates())).hexdigest(),
    )

    assert report["matched"] is False
    assert any(
        difference["kind"] == "canonical_event_identity_mismatch"
        for difference in _differences(report)
    )


def test_reconciliation_binds_hadoop_metrics_to_finalized_canonical_events() -> None:
    rows = [json.loads(raw) for raw in _hadoop_rows()]
    for row in rows:
        if row["metric"].endswith("_occurrence_count"):
            row["dimensions"]["payload_sha256"] = "d" * 64
    mutated = sorted((_canonical(row) for row in rows), key=_mapper_key_for_reducer_row)
    payload = _metrics_payload(mutated)

    report = _core_reconcile(
        _spark_aggregates(),
        mutated,
        finalized_run_manifest=_finalized_run_manifest(),
        hadoop_oracle_manifest=_hadoop_oracle_manifest(mutated),
        hadoop_metrics_sha256=sha256(payload).hexdigest(),
        spark_aggregates_sha256=sha256(_spark_payload(_spark_aggregates())).hexdigest(),
    )

    assert report["matched"] is False
    assert any(
        difference["field"] == "canonical_event_identity_sha256"
        and difference["kind"] == "provenance_mismatch"
        for difference in _differences(report)
    )


def test_reconciliation_binds_spark_aggregates_to_finalized_publication() -> None:
    spark = _spark_aggregates()
    spark["operational_metrics"][0]["total_us_sum"] = 31
    rows = _hadoop_rows()
    metrics_payload = _metrics_payload(rows)

    report = _core_reconcile(
        spark,
        rows,
        finalized_run_manifest=_finalized_run_manifest(),
        hadoop_oracle_manifest=_hadoop_oracle_manifest(rows),
        hadoop_metrics_sha256=sha256(metrics_payload).hexdigest(),
        spark_aggregates_sha256=sha256(_spark_payload(spark)).hexdigest(),
    )

    assert report["matched"] is False
    assert any(
        difference["field"] == "spark_aggregates_sha256"
        and difference["kind"] == "provenance_mismatch"
        for difference in _differences(report)
    )


def test_finalized_manifest_accepts_a_valid_resumed_run_with_fewer_deliveries() -> None:
    manifest = _finalized_run_manifest(source_event_count=3, delivery_count=0)

    validated = validate_finalized_run_manifest_mapping(manifest)

    assert validated.source_event_count == 3


def test_changed_hadoop_events_digest_is_a_publishable_provenance_mismatch() -> None:
    rows = _hadoop_rows()
    hadoop_manifest = _hadoop_oracle_manifest(rows)
    hadoop_manifest["events_sha256"] = "7" * 64

    report = _core_reconcile(
        _spark_aggregates(),
        rows,
        finalized_run_manifest=_finalized_run_manifest(),
        hadoop_oracle_manifest=hadoop_manifest,
        hadoop_metrics_sha256=sha256(_metrics_payload(rows)).hexdigest(),
        spark_aggregates_sha256=sha256(_spark_payload(_spark_aggregates())).hexdigest(),
    )

    assert {
        "actual_value": "7" * 64,
        "expected_value": "6" * 64,
        "field": "events_sha256",
        "kind": "provenance_mismatch",
    } in _differences(report)


def test_exact_reconciliation_preserves_all_statistical_units() -> None:
    rows = _hadoop_rows()
    report = reconcile(_spark_aggregates(), rows)

    assert report["differences"] == []
    assert report["matched"] is True
    assert report["schema_version"] == "hadoop-spark-reconciliation-v1"
    assert report["summary"] == {
        "duplicate_count": 0,
        "execution_lineage_count": 1,
        "hardware_execution_count": 1,
        "total_canonical_input_records": 1,
        "unique_canonical_records": 1,
        "unique_predictive_row_count": 1,
    }
    assert cast(dict[str, object], report["provenance"])["hadoop_oracle_manifest"] == {
        key: value
        for key, value in _hadoop_oracle_manifest(rows).items()
        if key != "schema_version"
    }


def test_mapper_reducer_reconciliation_detects_mutation_of_every_exact_metric() -> None:
    rows = _actual_reducer_rows()

    assert reconcile(_spark_aggregates(), rows)["matched"] is True
    mutated_count = 0
    for index, raw in enumerate(rows):
        row = json.loads(raw)
        if row["metric"].endswith("_occurrence_count"):
            continue
        mutated_count += 1
        row["value"] += 1
        mutated = [*rows]
        mutated[index] = _canonical(row)

        report = reconcile(_spark_aggregates(), mutated)

        assert report["matched"] is False, row
        assert _differences(report), row

    assert mutated_count == len(rows) - 3


def test_reports_missing_and_extra_metric_identities() -> None:
    rows = [
        row
        for row in _hadoop_rows()
        if not (
            json.loads(row)["metric"] == "total_us_sum"
            and "lineage_id" in json.loads(row)["dimensions"]
        )
    ]
    rows.append(
        _metric(
            "total_us_sum",
            30,
            {
                "lineage_id": "d" * 64,
                "board": "esp32c3",
                "student_model": "student_A",
            },
        )
    )

    report = reconcile(_spark_aggregates(), sorted(rows, key=_mapper_key_for_reducer_row))

    kinds = {difference["kind"] for difference in _differences(report)}
    assert kinds == {"extra_hadoop_metric", "missing_hadoop_metric"}
    assert all("metric_key" in difference for difference in _differences(report))
    assert report["matched"] is False


def test_reports_exact_field_value_mismatch_with_metric_key() -> None:
    report = reconcile(_spark_aggregates(), _replace_metric(_hadoop_rows(), "total_us_sum", 31))

    assert report["differences"] == [
        {
            "field": "total_us_sum",
            "hadoop_value": 31,
            "kind": "value_mismatch",
            "metric_key": {
                "dimensions": {
                    "board": "esp32c3",
                    "lineage_id": "b" * 64,
                    "student_model": "student_A",
                },
                "metric": "total_us_sum",
                "statistical_unit": "hardware_execution",
            },
            "spark_value": 30,
        }
    ]


def test_count_and_sum_fields_use_exact_integer_equality() -> None:
    spark = _spark_aggregates()
    spark["operational_metrics"][0]["total_us_sum"] = 30.0

    with pytest.raises(ReconciliationError, match="total_us_sum must be a non-negative integer"):
        reconcile(spark, _hadoop_rows())


@pytest.mark.parametrize("metric", IGNORED_SPECIFIC_SUMS)
def test_reconciliation_compares_every_specific_ignored_counter_sum(metric: str) -> None:
    report = reconcile(
        _spark_aggregates(),
        _replace_metric(_hadoop_rows(), metric, IGNORED_SPECIFIC_SUMS[metric] + 1),
    )

    assert _differences(report)[0]["kind"] == "value_mismatch"
    assert _differences(report)[0]["field"] == metric


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (
            '{"dimensions":{},"metric":"retry_count","metric":"retry_count",'
            '"statistical_unit":"hardware_execution","value":1}',
            "duplicate JSON property",
        ),
        (
            '{"dimensions": {},"metric":"retry_count",'
            '"statistical_unit":"hardware_execution","value":1}',
            "canonical compact JSON",
        ),
        (_canonical({"dimensions": {}, "metric": "retry_count", "value": 1}), "exactly"),
        (_metric("retry_count", True), "non-negative integer"),
        (
            _canonical(
                {
                    "dimensions": [],
                    "metric": "retry_count",
                    "statistical_unit": "hardware_execution",
                    "value": 1,
                }
            ),
            "dimensions must be an object",
        ),
    ],
)
def test_rejects_invalid_hadoop_json_schema_and_types(raw: str, message: str) -> None:
    with pytest.raises(ReconciliationError, match=message):
        reconcile(_spark_aggregates(), [raw])


def test_rejects_duplicate_hadoop_metric_identity() -> None:
    rows = _hadoop_rows()
    duplicate = rows[0]

    with pytest.raises(ReconciliationError, match="duplicate Hadoop metric identity"):
        reconcile(_spark_aggregates(), sorted([*rows, duplicate], key=_mapper_key_for_reducer_row))


def test_occurrence_rows_derive_counts_without_conflating_predictive_rows() -> None:
    rows: list[str] = []
    for index, lineage_id in enumerate(("1" * 64, "2" * 64, "3" * 64, "4" * 64)):
        source_id = f"{index + 10:064x}"
        payload = f"{index + 20:064x}"
        rows.extend(
            [
                _metric(
                    "source_event_occurrence_count",
                    1,
                    {
                        "source_event_id": source_id,
                        "lineage_id": lineage_id,
                        "row_id": 0,
                        "source_row_index": 7,
                        "payload_sha256": payload,
                    },
                    unit="canonical_input_record",
                ),
                _metric(
                    "lineage_row_occurrence_count",
                    1,
                    {
                        "lineage_id": lineage_id,
                        "row_id": 0,
                        "source_event_id": source_id,
                        "payload_sha256": payload,
                    },
                    unit="canonical_input_record",
                ),
                _metric(
                    "predictive_source_row_occurrence_count",
                    1,
                    {
                        "source_row_index": 7,
                        "lineage_id": lineage_id,
                        "source_event_id": source_id,
                        "payload_sha256": payload,
                    },
                    unit="predictive_source_row",
                ),
            ]
        )
    rows.append(_metric("hardware_execution_count", 4))
    spark = _spark_aggregates()
    predictive_values = {
        "hardware_execution_count": 4,
        "execution_lineage_count": 4,
        "unique_predictive_row_count": 1,
    }
    for row in spark["predictive_row_metrics"]:
        row["value"] = predictive_values[row["metric"]]
    spark["operational_metrics"] = []
    spark["execution_dimension_counts"] = []
    spark["retention_metrics"] = []
    spark["retention_reason_counts"] = []

    report = reconcile(spark, sorted(rows, key=_mapper_key_for_reducer_row))

    assert report["summary"] == {
        "duplicate_count": 0,
        "execution_lineage_count": 4,
        "hardware_execution_count": 4,
        "total_canonical_input_records": 4,
        "unique_canonical_records": 4,
        "unique_predictive_row_count": 1,
    }
    assert report["matched"] is True


def test_occurrence_and_hardware_cardinalities_must_match_unique_input_records() -> None:
    rows = _hadoop_rows()
    rows.append(
        _metric(
            "source_event_occurrence_count",
            1,
            {
                "source_event_id": "d" * 64,
                "lineage_id": "f" * 64,
                "row_id": 1,
                "source_row_index": 8,
                "payload_sha256": "e" * 64,
            },
            unit="canonical_input_record",
        )
    )

    report = reconcile(
        _spark_aggregates(),
        sorted(rows, key=_mapper_key_for_reducer_row),
    )

    assert _differences(report) == [
        {
            "actual": 1,
            "expected": 2,
            "field": "hardware_execution_count",
            "kind": "internal_cardinality_mismatch",
            "reference_field": "unique_canonical_records",
        },
        {
            "actual": 1,
            "expected": 2,
            "field": "lineage_row_occurrence_count",
            "kind": "internal_cardinality_mismatch",
            "reference_field": "unique_canonical_records",
        },
        {
            "actual": 1,
            "expected": 2,
            "field": "predictive_source_row_occurrence_count",
            "kind": "internal_cardinality_mismatch",
            "reference_field": "unique_canonical_records",
        },
        {
            "kind": "canonical_event_identity_mismatch",
            "source_event_id": "d" * 64,
        },
    ]
    assert report["matched"] is False


@pytest.mark.parametrize(
    ("dimension_kind", "changes"),
    [
        ("board", {"student_model": "student_A"}),
        ("student_model", {"board": "esp32c3"}),
        ("true_class", {"class_id": None}),
        ("predicted_class", {"class_name": None}),
        ("board_student_model", {"class_id": 0}),
    ],
)
def test_execution_dimension_counts_enforce_exact_nullable_shape(
    dimension_kind: str,
    changes: dict[str, object],
) -> None:
    spark = _spark_aggregates()
    row = next(
        row
        for row in spark["execution_dimension_counts"]
        if row["dimension_kind"] == dimension_kind
    )
    row.update(changes)

    with pytest.raises(ReconciliationError, match=rf"{dimension_kind}.*dimension shape"):
        reconcile(spark, _hadoop_rows())


def test_execution_dimension_counts_reject_unknown_dimension_kind() -> None:
    spark = _spark_aggregates()
    spark["execution_dimension_counts"][0]["dimension_kind"] = "lineage"

    with pytest.raises(ReconciliationError, match="unsupported dimension_kind"):
        reconcile(spark, _hadoop_rows())


@pytest.mark.parametrize("dimension_kind", [True, []])
def test_execution_dimension_counts_require_string_dimension_kind(
    dimension_kind: object,
) -> None:
    spark = _spark_aggregates()
    spark["execution_dimension_counts"][0]["dimension_kind"] = dimension_kind

    with pytest.raises(ReconciliationError, match="dimension_kind must be a non-empty string"):
        reconcile(spark, _hadoop_rows())


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("class_id", True, "class_id must be an integer between 0 and 4"),
        ("class_name", "Normal", "class_name is inconsistent with class_id"),
    ],
)
def test_execution_dimension_counts_validate_class_identity(
    field: str,
    value: object,
    message: str,
) -> None:
    spark = _spark_aggregates()
    true_class = next(
        row for row in spark["execution_dimension_counts"] if row["dimension_kind"] == "true_class"
    )
    true_class[field] = value

    with pytest.raises(ReconciliationError, match=message):
        reconcile(spark, _hadoop_rows())


def test_source_occurrence_value_must_equal_exactly_one() -> None:
    rows = _hadoop_rows()
    for index, raw in enumerate(rows):
        row = json.loads(raw)
        if row["metric"] == "source_event_occurrence_count":
            row["value"] = 2
            rows[index] = _canonical(row)
            break

    with pytest.raises(
        ReconciliationError,
        match="source_event_occurrence_count value must equal exactly one",
    ):
        reconcile(_spark_aggregates(), rows)


@pytest.mark.parametrize(
    ("metric", "invalid_value"),
    [
        ("lineage_row_occurrence_count", 0),
        ("lineage_row_occurrence_count", 2),
        ("predictive_source_row_occurrence_count", 0),
        ("predictive_source_row_occurrence_count", 2),
    ],
)
def test_lineage_and_predictive_occurrence_values_must_equal_exactly_one(
    metric: str,
    invalid_value: int,
) -> None:
    rows = _hadoop_rows()
    for index, raw in enumerate(rows):
        row = json.loads(raw)
        if row["metric"] == metric:
            row["value"] = invalid_value
            rows[index] = _canonical(row)
            break

    with pytest.raises(
        ReconciliationError,
        match=rf"{metric} value must equal exactly one",
    ):
        reconcile(_spark_aggregates(), rows)


def test_represented_mean_must_equal_the_exact_derived_sum_over_count() -> None:
    spark = _spark_aggregates()
    spark["operational_metrics"][0]["total_us_mean"] = 30.0

    assert reconcile(spark, _hadoop_rows())["matched"] is True

    spark["operational_metrics"][0]["total_us_mean"] = 29.9
    with pytest.raises(ReconciliationError, match="total_us_mean must be derived"):
        reconcile(spark, _hadoop_rows())


def test_represented_mean_accepts_the_correctly_rounded_python_quotient() -> None:
    spark = _spark_aggregates()
    row = spark["operational_metrics"][0]
    row["hardware_execution_count"] = 3
    row["total_us_sum"] = 1
    row["total_us_mean"] = 1 / 3

    _spark_metrics(spark)

    row["total_us_mean"] = 0.33333333333333337
    with pytest.raises(ReconciliationError, match="total_us_mean must be derived"):
        _spark_metrics(spark)


def test_numeric_validation_does_not_apply_float_finiteness_to_integers() -> None:
    assert _number(10**400, "large exact count") == 10**400


def _with_percentile(
    *, spark_value: float, hadoop_value: float
) -> tuple[dict[str, list[dict[str, object]]], list[str]]:
    spark = _spark_aggregates()
    spark["operational_metrics"][0]["inference_us_percentile_95"] = spark_value
    rows = _hadoop_rows()
    rows.append(
        _metric(
            "inference_us_percentile_95",
            hadoop_value,
            {
                "lineage_id": "b" * 64,
                "board": "esp32c3",
                "student_model": "student_A",
            },
        )
    )
    return spark, sorted(rows, key=_mapper_key_for_reducer_row)


def test_percentile_comparison_requires_an_explicit_per_field_tolerance() -> None:
    spark, rows = _with_percentile(spark_value=20.0, hadoop_value=20.1)

    with pytest.raises(ReconciliationError, match="explicit absolute tolerance"):
        reconcile(spark, rows)


def test_hadoop_only_percentile_also_requires_an_explicit_field_tolerance() -> None:
    rows = _hadoop_rows()
    rows.append(
        _metric(
            "inference_us_percentile_95",
            20.1,
            {
                "lineage_id": "b" * 64,
                "board": "esp32c3",
                "student_model": "student_A",
            },
        )
    )

    with pytest.raises(
        ReconciliationError,
        match="inference_us_percentile_95 requires an explicit absolute tolerance",
    ):
        reconcile(_spark_aggregates(), sorted(rows, key=_mapper_key_for_reducer_row))


def test_hadoop_only_percentile_is_reported_as_an_extra_metric() -> None:
    dimensions = {
        "lineage_id": "b" * 64,
        "board": "esp32c3",
        "student_model": "student_A",
    }
    rows = _hadoop_rows()
    rows.append(_metric("inference_us_percentile_95", 20.1, dimensions))

    report = reconcile(
        _spark_aggregates(),
        sorted(rows, key=_mapper_key_for_reducer_row),
        percentile_tolerances={"inference_us_percentile_95": 0.1},
    )

    assert _differences(report) == [
        {
            "kind": "extra_hadoop_metric",
            "metric_key": {
                "dimensions": dimensions,
                "metric": "inference_us_percentile_95",
                "statistical_unit": "hardware_execution",
            },
        }
    ]
    assert report["matched"] is False


@pytest.mark.parametrize(
    ("dimensions", "unit"),
    [
        ({}, "hardware_execution"),
        (
            {
                "lineage_id": "b" * 64,
                "board": "esp32c3",
                "student_model": "student_A",
            },
            "predictive_source_row",
        ),
    ],
)
def test_hadoop_percentile_rejects_dimensions_or_unit_outside_spark_schemas(
    dimensions: dict[str, object],
    unit: str,
) -> None:
    rows = _hadoop_rows()
    rows.append(_metric("latency_percentile_95", 20.1, dimensions, unit=unit))

    with pytest.raises(
        ReconciliationError,
        match="percentile dimensions and statistical_unit do not match a Spark aggregate schema",
    ):
        reconcile(
            _spark_aggregates(),
            sorted(rows, key=_mapper_key_for_reducer_row),
            percentile_tolerances={"latency_percentile_95": 0.1},
        )


@pytest.mark.parametrize(
    "dimensions",
    [
        {"lineage_id": 7, "board": "esp32c3", "student_model": "student_A"},
        {"policy_id": "retain-all-v1", "reason": "retain_all", "retained": 1},
    ],
)
def test_hadoop_percentile_rejects_invalid_dimension_value_types(
    dimensions: dict[str, object],
) -> None:
    rows = _hadoop_rows()
    rows.append(_metric("latency_percentile_95", 20.1, dimensions))

    with pytest.raises(ReconciliationError, match="Hadoop percentile dimension"):
        reconcile(
            _spark_aggregates(),
            sorted(rows, key=_mapper_key_for_reducer_row),
            percentile_tolerances={"latency_percentile_95": 0.1},
        )


def test_spark_only_percentile_is_reported_as_missing_from_hadoop() -> None:
    spark = _spark_aggregates()
    spark["operational_metrics"][0]["inference_us_percentile_95"] = 20.0

    report = reconcile(
        spark,
        _hadoop_rows(),
        percentile_tolerances={"inference_us_percentile_95": 0.1},
    )

    assert _differences(report) == [
        {
            "kind": "missing_hadoop_metric",
            "metric_key": {
                "dimensions": {
                    "lineage_id": "b" * 64,
                    "board": "esp32c3",
                    "student_model": "student_A",
                },
                "metric": "inference_us_percentile_95",
                "statistical_unit": "hardware_execution",
            },
        }
    ]


def test_percentile_values_reject_boolean_numbers_on_both_sides() -> None:
    spark = _spark_aggregates()
    spark["operational_metrics"][0]["inference_us_percentile_95"] = True
    with pytest.raises(ReconciliationError, match="must be a finite number"):
        reconcile(
            spark,
            _hadoop_rows(),
            percentile_tolerances={"inference_us_percentile_95": 0.1},
        )

    rows = _hadoop_rows()
    rows.append(
        _metric(
            "inference_us_percentile_95",
            True,
            {
                "lineage_id": "b" * 64,
                "board": "esp32c3",
                "student_model": "student_A",
            },
        )
    )
    with pytest.raises(ReconciliationError, match="must be a finite number"):
        reconcile(
            _spark_aggregates(),
            sorted(rows, key=_mapper_key_for_reducer_row),
            percentile_tolerances={"inference_us_percentile_95": 0.1},
        )


def test_percentile_comparison_passes_and_fails_against_its_field_tolerance() -> None:
    spark, rows = _with_percentile(spark_value=20.0, hadoop_value=20.1)

    passed = reconcile(
        spark,
        rows,
        percentile_tolerances={"inference_us_percentile_95": 0.1},
    )
    failed = reconcile(
        spark,
        rows,
        percentile_tolerances={"inference_us_percentile_95": 0.09},
    )

    assert passed["matched"] is True
    assert failed["matched"] is False
    assert _differences(failed)[0]["kind"] == "percentile_tolerance_exceeded"
    assert _differences(failed)[0]["absolute_tolerance"] == 0.09


def test_report_serialization_is_deterministic_canonical_json() -> None:
    report = reconcile(_spark_aggregates(), _hadoop_rows())

    first = canonical_report(report)
    second = canonical_report(report)

    assert first == second
    assert first.endswith("\n")
    assert first.strip() == _canonical(json.loads(first))


def test_report_publication_is_atomic_no_clobber_and_protects_evidence_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = reconcile(_spark_aggregates(), _hadoop_rows())
    output = tmp_path / "reports" / "reconciliation.json"

    publish_report(output, report, environment={})

    original = output.read_bytes()
    assert original == canonical_report(report).encode("utf-8")
    assert not list(output.parent.glob(f".{output.name}.*.tmp"))
    with pytest.raises(FileExistsError):
        publish_report(output, report, environment={})
    assert output.read_bytes() == original

    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()
    monkeypatch.setenv("CUKD_XAI_ROOT", str(evidence_root))
    with pytest.raises(ReconciliationError, match="outside every evidence root"):
        publish_report(evidence_root / "report.json", report, environment=os.environ)
    assert not (evidence_root / "report.json").exists()


@pytest.mark.parametrize("variable", ["CUKD_XAI_ROOT", "TINYML_IDS_EVIDENCE_ROOT"])
def test_report_publication_trims_environment_evidence_roots(
    tmp_path: Path,
    variable: str,
) -> None:
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    with pytest.raises(ReconciliationError, match="outside every evidence root"):
        publish_report(
            evidence_root / "report.json",
            reconcile(_spark_aggregates(), _hadoop_rows()),
            environment={variable: f"  {evidence_root}  "},
        )


def test_cli_publishes_canonical_mismatch_report_and_returns_one(tmp_path: Path) -> None:
    spark_path, hadoop_path = _cli_inputs(
        tmp_path,
        _replace_metric(_hadoop_rows(), "total_us_sum", 31),
    )
    output = tmp_path / "reconciliation.json"

    status = main(
        [
            *_cli_arguments(spark_path, hadoop_path),
            "--output",
            str(output),
        ]
    )

    assert status == 1
    payload = output.read_text(encoding="utf-8")
    report = json.loads(payload)
    assert report["matched"] is False
    assert payload == _canonical(report) + "\n"


def test_cli_publishes_well_formed_provenance_mismatch_and_returns_one(
    tmp_path: Path,
) -> None:
    spark_path, hadoop_path = _cli_inputs(tmp_path, _hadoop_rows())
    finalized_path = tmp_path / "finalized-run-manifest.json"
    finalized = _finalized_run_manifest()
    finalized["snapshot_id"] = "a" * 64
    finalized_path.write_bytes((_canonical(finalized) + "\n").encode("ascii"))
    output = tmp_path / "reconciliation.json"

    status = main([*_cli_arguments(spark_path, hadoop_path), "--output", str(output)])

    assert status == 1
    report = json.loads(output.read_bytes())
    assert {
        "actual_value": "1" * 64,
        "expected_value": "a" * 64,
        "field": "snapshot_id",
        "kind": "provenance_mismatch",
    } in report["differences"]


def test_cli_rejects_noncanonical_hadoop_manifest_as_a_contract_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spark_path, hadoop_path = _cli_inputs(tmp_path, _hadoop_rows())
    manifest_path = tmp_path / "hadoop-run-manifest.json"
    manifest = _hadoop_oracle_manifest(_hadoop_rows())
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    output = tmp_path / "reconciliation.json"

    status = main([*_cli_arguments(spark_path, hadoop_path), "--output", str(output)])

    assert status == 2
    assert "canonical compact sorted JSON" in capsys.readouterr().err
    assert not output.exists()


def test_cli_mismatch_report_does_not_clobber_existing_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spark_path, hadoop_path = _cli_inputs(
        tmp_path,
        _replace_metric(_hadoop_rows(), "total_us_sum", 31),
    )
    output = tmp_path / "reconciliation.json"
    output.write_text("existing\n", encoding="utf-8")

    status = main(
        [
            *_cli_arguments(spark_path, hadoop_path),
            "--output",
            str(output),
        ]
    )

    assert status == 2
    assert "reconciliation report already exists" in capsys.readouterr().err
    assert output.read_text(encoding="utf-8") == "existing\n"
    assert not list(tmp_path.glob(f".{output.name}.*.tmp"))


def test_cli_mismatch_report_rejects_output_inside_evidence_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spark_path, hadoop_path = _cli_inputs(
        tmp_path,
        _replace_metric(_hadoop_rows(), "total_us_sum", 31),
    )
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()
    output = evidence_root / "reconciliation.json"
    monkeypatch.setenv("TINYML_IDS_EVIDENCE_ROOT", str(evidence_root))

    status = main(
        [
            *_cli_arguments(spark_path, hadoop_path),
            "--output",
            str(output),
        ]
    )

    assert status == 2
    assert "outside every evidence root" in capsys.readouterr().err
    assert not output.exists()
    assert not list(evidence_root.glob(f".{output.name}.*.tmp"))


def test_cli_parse_failure_never_publishes_partial_report(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spark_path, hadoop_path = _cli_inputs(tmp_path, _hadoop_rows())
    hadoop_path.write_text("not-json\n", encoding="utf-8")
    output = tmp_path / "reconciliation.json"

    status = main(
        [
            *_cli_arguments(spark_path, hadoop_path),
            "--output",
            str(output),
        ]
    )

    assert status == 2
    assert "must contain strict JSON" in capsys.readouterr().err
    assert not output.exists()
    assert not list(tmp_path.glob(f".{output.name}.*.tmp"))


def test_cli_input_io_failure_never_publishes_partial_report(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spark_path, _ = _cli_inputs(tmp_path, _hadoop_rows())
    output = tmp_path / "reconciliation.json"

    status = main(
        [
            *_cli_arguments(spark_path, tmp_path / "missing-hadoop.jsonl"),
            "--output",
            str(output),
        ]
    )

    assert status == 2
    assert "missing-hadoop.jsonl" in capsys.readouterr().err
    assert not output.exists()
    assert not list(tmp_path.glob(f".{output.name}.*.tmp"))


@pytest.mark.parametrize("serialization", ["whitespace", "pretty", "unsorted"])
def test_cli_rejects_noncanonical_spark_aggregate_json(
    tmp_path: Path,
    serialization: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spark_path, hadoop_path = _cli_inputs(tmp_path, _hadoop_rows())
    aggregates = _spark_aggregates()
    canonical = _canonical(aggregates)
    if serialization == "whitespace":
        raw = f" {canonical}"
    elif serialization == "pretty":
        raw = json.dumps(aggregates, indent=2, sort_keys=True)
    else:
        raw = json.dumps(aggregates, sort_keys=False, separators=(",", ":"))
        assert raw != canonical
    spark_path.write_text(raw, encoding="utf-8")
    output = tmp_path / "reconciliation.json"

    status = main(
        [
            *_cli_arguments(spark_path, hadoop_path),
            "--output",
            str(output),
        ]
    )

    assert status == 2
    assert "canonical compact sorted JSON" in capsys.readouterr().err
    assert not output.exists()


def test_runner_launches_exact_jar_with_files_and_exactly_one_reducer(tmp_path: Path) -> None:
    completed, capture, published_manifest, hdfs_capture = _run_fake_runner(tmp_path)

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == [
        "/results/oracle/metrics/part-00000",
        "/results/oracle/hadoop_oracle_manifest.json",
    ]
    arguments = capture.read_text(encoding="utf-8").splitlines()
    assert arguments[0] == "jar"
    assert arguments[1].endswith("/hadoop-streaming-3.5.0.jar")
    reduce_properties = [
        argument for argument in arguments if argument.startswith("mapreduce.job.reduces=")
    ]
    assert reduce_properties == ["mapreduce.job.reduces=1"]
    assert arguments.count("-numReduceTasks") == 1
    reducer_option = arguments.index("-numReduceTasks")
    assert arguments[reducer_option + 1] == "1"
    files_option = arguments.index("-files")
    shipped = {value.rsplit("/", 1)[-1] for value in arguments[files_option + 1].split(",")}
    assert shipped == {"mapper.py", "reducer.py", "retention-policies.toml"}
    assert "hadoop-streaming-*" not in RUNNER.read_text(encoding="utf-8")
    manifest = json.loads(published_manifest.read_bytes())
    assert set(manifest) == {
        "schema_version",
        "snapshot_id",
        "events_sha256",
        "snapshot_event_count",
        "policy_config_sha256",
        "hadoop_version",
        "reducer_count",
        "input_events_hdfs_path",
        "output_metrics_hdfs_path",
        "metrics_sha256",
        "metrics_row_count",
        "canonical_event_identity_sha256",
    }
    assert manifest["hadoop_version"] == "3.5.0"
    assert manifest["reducer_count"] == 1
    assert manifest["input_events_hdfs_path"] == "/results/oracle/input/events.jsonl"
    assert manifest["output_metrics_hdfs_path"] == "/results/oracle/metrics/part-00000"
    assert manifest["metrics_sha256"] == sha256(_metrics_payload(_hadoop_rows())).hexdigest()
    assert manifest["metrics_row_count"] == len(_hadoop_rows())
    assert manifest["canonical_event_identity_sha256"] == _canonical_event_identity(_hadoop_rows())
    assert manifest["policy_config_sha256"] == sha256(POLICY_CONFIG.read_bytes()).hexdigest()
    assert "replay_run_id" not in manifest
    hdfs_commands = hdfs_capture.read_text(encoding="utf-8").splitlines()
    operations = [command.split()[1] for command in hdfs_commands]
    assert operations.count("-get") == 2
    assert "-mkdir" in operations
    assert operations.count("-put") == 3
    assert "-test" in operations
    assert "-mv" in operations
    assert operations[-2:] == ["-rm", "-rm"]
    input_option = arguments.index("-input")
    output_option = arguments.index("-output")
    assert ".staging-" in arguments[input_option + 1]
    assert arguments[input_option + 1].endswith("/input/events.jsonl")
    assert ".staging-" in arguments[output_option + 1]
    assert arguments[output_option + 1].endswith("/metrics")


def test_runner_gateway_helper_imports_core_package_from_checkout_src(tmp_path: Path) -> None:
    completed, _, published_manifest, _ = _run_fake_runner(
        tmp_path,
        isolated_gateway_python=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert published_manifest.is_file()


def test_runner_uses_explicit_python_for_distributed_mapper_and_reducer(tmp_path: Path) -> None:
    completed, capture, _, _ = _run_fake_runner(
        tmp_path,
        task_python="/opt/tinyml/bin/python3",
    )

    assert completed.returncode == 0, completed.stderr
    arguments = capture.read_text(encoding="utf-8").splitlines()
    mapper_option = arguments.index("-mapper")
    reducer_option = arguments.index("-reducer")
    assert arguments[mapper_option + 1] == (
        "/opt/tinyml/bin/python3 mapper.py --policy-config retention-policies.toml"
    )
    assert arguments[reducer_option + 1] == "/opt/tinyml/bin/python3 reducer.py"


def test_snapshot_event_stream_requires_the_exact_manifest_event_count() -> None:
    payload = b"{}\n"
    manifest = ValidatedSnapshotManifest(
        snapshot_id="1" * 64,
        sealed_report_sha256="2" * 64,
        event_count=2,
        lineage_count=1,
        rows_per_lineage=2,
        evaluation_sequence_sha256="3" * 64,
        events_sha256=sha256(payload).hexdigest(),
        lineages_sha256="4" * 64,
        lineage_ids=("5" * 64,),
    )

    with pytest.raises(OracleManifestError, match="event count mismatch"):
        verify_snapshot_events(manifest, io.BytesIO(payload))


def test_runner_propagates_hadoop_file_output_no_clobber_failure(tmp_path: Path) -> None:
    completed, capture, _, hdfs_capture = _run_fake_runner(tmp_path, final_rename_status=17)

    assert completed.returncode == 17
    assert capture.exists()
    commands = hdfs_capture.read_text(encoding="utf-8").splitlines()
    assert any(command.startswith("dfs -mv ") for command in commands)
    assert any(command.startswith("dfs -rm -r -f ") for command in commands)
    assert commands[-1] == "dfs -rm -f /results/oracle.publication-lock"


def test_runner_manifest_upload_failure_never_publishes_staging_root(tmp_path: Path) -> None:
    completed, capture, published_manifest, hdfs_capture = _run_fake_runner(
        tmp_path,
        manifest_put_status=23,
    )

    assert completed.returncode == 23
    assert capture.exists()
    assert not published_manifest.exists()
    commands = hdfs_capture.read_text(encoding="utf-8").splitlines()
    assert not any(command.startswith("dfs -mv ") for command in commands)
    assert any(command.startswith("dfs -rm -r -f ") for command in commands)
    assert commands[-1] == "dfs -rm -f /results/oracle.publication-lock"


def test_runner_refuses_to_publish_into_an_existing_final_directory(tmp_path: Path) -> None:
    completed, capture, published_manifest, hdfs_capture = _run_fake_runner(
        tmp_path,
        preexisting_output=True,
    )

    assert completed.returncode == 73
    assert "refusing to replace existing HDFS output" in completed.stderr
    assert not capture.exists()
    assert not published_manifest.exists()
    commands = hdfs_capture.read_text(encoding="utf-8").splitlines()
    assert any(command.startswith("dfs -test -e /results/oracle") for command in commands)
    assert not any(command.startswith("dfs -mv ") for command in commands)


def test_runner_fails_closed_when_publication_lock_cannot_be_acquired(tmp_path: Path) -> None:
    completed, capture, published_manifest, hdfs_capture = _run_fake_runner(
        tmp_path,
        publication_lock_status=17,
    )

    assert completed.returncode == 17
    assert "could not acquire HDFS publication lock" in completed.stderr
    assert not capture.exists()
    assert not published_manifest.exists()
    commands = hdfs_capture.read_text(encoding="utf-8").splitlines()
    assert not any(command.startswith("dfs -mv ") for command in commands)


def test_runner_manifest_get_failure_prevents_job_launch(tmp_path: Path) -> None:
    completed, capture, published_manifest, _ = _run_fake_runner(tmp_path, snapshot_get_status=7)

    assert completed.returncode == 7
    assert not capture.exists()
    assert not published_manifest.exists()


def test_runner_event_stream_failure_is_not_hidden_by_the_validation_pipe(tmp_path: Path) -> None:
    completed, capture, published_manifest, _ = _run_fake_runner(tmp_path, events_cat_status=19)

    assert completed.returncode == 19
    assert not capture.exists()
    assert not published_manifest.exists()
