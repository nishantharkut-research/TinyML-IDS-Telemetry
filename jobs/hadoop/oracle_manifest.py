"""Validate immutable Hadoop provenance and build its compact oracle manifest."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, cast

from tinyml_ids_telemetry.contracts.schema import (
    SchemaContractError,
    ValidatedSnapshotManifest,
    validate_snapshot_manifest_mapping,
)

_SHA256 = re.compile(r"[0-9a-f]{64}")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_HADOOP_FIELDS = frozenset(
    {
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
)
_FINALIZED_FIELDS = frozenset(
    {
        "schema_version",
        "replay_run_id",
        "snapshot_id",
        "events_sha256",
        "schedule_identity_sha256",
        "completion_receipt_sha256",
        "source_event_count",
        "delivery_count",
        "lineage_counts",
        "canonical_gold_identity_sha256",
        "canonical_event_identity_sha256",
        "spark_aggregates_sha256",
        "policy_config_sha256",
        "policy_ids",
        "outputs",
        "statistical_unit",
        "publication_contract",
    }
)


class OracleManifestError(ValueError):
    """A snapshot, finalized run, or Hadoop oracle manifest is invalid."""


@dataclass(frozen=True, slots=True)
class ValidatedHadoopOracleManifest:
    snapshot_id: str
    events_sha256: str
    snapshot_event_count: int
    policy_config_sha256: str
    hadoop_version: str
    reducer_count: int
    input_events_hdfs_path: str
    output_metrics_hdfs_path: str
    metrics_sha256: str
    metrics_row_count: int
    canonical_event_identity_sha256: str

    def compact_provenance(self) -> dict[str, object]:
        return {
            "events_sha256": self.events_sha256,
            "hadoop_version": self.hadoop_version,
            "input_events_hdfs_path": self.input_events_hdfs_path,
            "metrics_row_count": self.metrics_row_count,
            "metrics_sha256": self.metrics_sha256,
            "canonical_event_identity_sha256": self.canonical_event_identity_sha256,
            "output_metrics_hdfs_path": self.output_metrics_hdfs_path,
            "policy_config_sha256": self.policy_config_sha256,
            "reducer_count": self.reducer_count,
            "snapshot_event_count": self.snapshot_event_count,
            "snapshot_id": self.snapshot_id,
        }


@dataclass(frozen=True, slots=True)
class ValidatedFinalizedRunManifest:
    replay_run_id: str
    snapshot_id: str
    events_sha256: str
    source_event_count: int
    policy_config_sha256: str
    canonical_event_identity_sha256: str
    spark_aggregates_sha256: str

    def compact_provenance(self) -> dict[str, object]:
        return {
            "events_sha256": self.events_sha256,
            "policy_config_sha256": self.policy_config_sha256,
            "canonical_event_identity_sha256": self.canonical_event_identity_sha256,
            "spark_aggregates_sha256": self.spark_aggregates_sha256,
            "replay_run_id": self.replay_run_id,
            "snapshot_id": self.snapshot_id,
            "source_event_count": self.source_event_count,
        }


def _detached_mapping(value: object, context: str) -> dict[str, object]:
    try:
        detached = json.loads(
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
    except (TypeError, ValueError) as error:
        raise OracleManifestError(f"{context} must be JSON-compatible") from error
    if not isinstance(detached, dict):
        raise OracleManifestError(f"{context} must be a JSON object mapping")
    return cast(dict[str, object], detached)


def _exact_fields(value: Mapping[str, object], fields: frozenset[str], context: str) -> None:
    missing = sorted(fields - value.keys())
    if missing:
        raise OracleManifestError(f"{context} is missing required field {missing[0]}")
    extra = sorted(value.keys() - fields)
    if extra:
        raise OracleManifestError(f"{context} contains unexpected field {extra[0]}")


def _digest(value: object, context: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise OracleManifestError(f"{context} must be a lowercase SHA-256 digest")
    return value


def _integer(value: object, context: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise OracleManifestError(f"{context} must be an integer >= {minimum}")
    return value


def _string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise OracleManifestError(f"{context} must be a non-empty trimmed string")
    return value


def _hdfs_path(value: object, context: str) -> str:
    path = _string(value, context)
    if not path.startswith("/") or path == "/" or "\n" in path or "\r" in path:
        raise OracleManifestError(f"{context} must be an absolute HDFS path below root")
    return path


def validate_hadoop_oracle_manifest_mapping(
    value: object,
) -> ValidatedHadoopOracleManifest:
    """Validate the exact compact Hadoop oracle manifest v1 contract."""
    manifest = _detached_mapping(value, "Hadoop oracle manifest")
    _exact_fields(manifest, _HADOOP_FIELDS, "Hadoop oracle manifest")
    if manifest["schema_version"] != "hadoop-oracle-manifest-v1":
        raise OracleManifestError(
            "Hadoop oracle manifest schema_version must be hadoop-oracle-manifest-v1"
        )
    hadoop_version = _string(manifest["hadoop_version"], "Hadoop oracle manifest hadoop_version")
    if hadoop_version != "3.5.0":
        raise OracleManifestError("Hadoop oracle manifest hadoop_version must equal 3.5.0")
    reducer_count = _integer(
        manifest["reducer_count"], "Hadoop oracle manifest reducer_count", minimum=1
    )
    if reducer_count != 1:
        raise OracleManifestError("Hadoop oracle manifest reducer_count must equal one")
    return ValidatedHadoopOracleManifest(
        snapshot_id=_digest(manifest["snapshot_id"], "Hadoop oracle manifest snapshot_id"),
        events_sha256=_digest(manifest["events_sha256"], "Hadoop oracle manifest events_sha256"),
        snapshot_event_count=_integer(
            manifest["snapshot_event_count"],
            "Hadoop oracle manifest snapshot_event_count",
            minimum=1,
        ),
        policy_config_sha256=_digest(
            manifest["policy_config_sha256"],
            "Hadoop oracle manifest policy_config_sha256",
        ),
        hadoop_version=hadoop_version,
        reducer_count=reducer_count,
        input_events_hdfs_path=_hdfs_path(
            manifest["input_events_hdfs_path"],
            "Hadoop oracle manifest input_events_hdfs_path",
        ),
        output_metrics_hdfs_path=_hdfs_path(
            manifest["output_metrics_hdfs_path"],
            "Hadoop oracle manifest output_metrics_hdfs_path",
        ),
        metrics_sha256=_digest(manifest["metrics_sha256"], "Hadoop oracle manifest metrics_sha256"),
        metrics_row_count=_integer(
            manifest["metrics_row_count"],
            "Hadoop oracle manifest metrics_row_count",
            minimum=1,
        ),
        canonical_event_identity_sha256=_digest(
            manifest["canonical_event_identity_sha256"],
            "Hadoop oracle manifest canonical_event_identity_sha256",
        ),
    )


def validate_finalized_run_manifest_mapping(
    value: object,
) -> ValidatedFinalizedRunManifest:
    """Validate a finalized-run v1 mapping and return reconciliation provenance."""
    manifest = _detached_mapping(value, "finalized run manifest")
    _exact_fields(manifest, _FINALIZED_FIELDS, "finalized run manifest")
    if manifest["schema_version"] != "telemetry-finalized-run-v1":
        raise OracleManifestError(
            "finalized run manifest schema_version must be telemetry-finalized-run-v1"
        )
    replay_run_id = _string(manifest["replay_run_id"], "finalized run manifest replay_run_id")
    if _RUN_ID.fullmatch(replay_run_id) is None:
        raise OracleManifestError("finalized run manifest replay_run_id is invalid")
    source_event_count = _integer(
        manifest["source_event_count"], "finalized run manifest source_event_count", minimum=1
    )
    _integer(manifest["delivery_count"], "finalized run manifest delivery_count")
    lineage_counts = manifest["lineage_counts"]
    if not isinstance(lineage_counts, list) or not lineage_counts:
        raise OracleManifestError("finalized run manifest lineage_counts must be non-empty")
    lineage_ids: set[str] = set()
    lineage_total = 0
    for index, item in enumerate(lineage_counts):
        row = _detached_mapping(item, f"finalized run manifest lineage_counts[{index}]")
        _exact_fields(
            row,
            frozenset({"lineage_id", "hardware_execution_rows"}),
            f"finalized run manifest lineage_counts[{index}]",
        )
        lineage_id = _digest(
            row["lineage_id"], f"finalized run manifest lineage_counts[{index}] lineage_id"
        )
        if lineage_id in lineage_ids:
            raise OracleManifestError("finalized run manifest lineage_ids must be unique")
        lineage_ids.add(lineage_id)
        lineage_total += _integer(
            row["hardware_execution_rows"],
            f"finalized run manifest lineage_counts[{index}] hardware_execution_rows",
            minimum=1,
        )
    if lineage_total != source_event_count:
        raise OracleManifestError(
            "finalized run manifest lineage counts must sum to source_event_count"
        )
    policy_ids = manifest["policy_ids"]
    if (
        not isinstance(policy_ids, list)
        or not policy_ids
        or any(not isinstance(item, str) or not item for item in policy_ids)
        or len(set(policy_ids)) != len(policy_ids)
    ):
        raise OracleManifestError("finalized run manifest policy_ids must be unique strings")
    outputs = manifest["outputs"]
    if not isinstance(outputs, dict) or not outputs:
        raise OracleManifestError("finalized run manifest outputs must be a non-empty object")
    for name, item in outputs.items():
        _string(name, "finalized run manifest output name")
        output = _detached_mapping(item, f"finalized run manifest output {name}")
        _exact_fields(output, frozenset({"row_count"}), f"finalized run manifest output {name}")
        _integer(output["row_count"], f"finalized run manifest output {name} row_count")
    _digest(
        manifest["schedule_identity_sha256"],
        "finalized run manifest schedule_identity_sha256",
    )
    _digest(
        manifest["completion_receipt_sha256"],
        "finalized run manifest completion_receipt_sha256",
    )
    _digest(
        manifest["canonical_gold_identity_sha256"],
        "finalized run manifest canonical_gold_identity_sha256",
    )
    canonical_event_identity_sha256 = _digest(
        manifest["canonical_event_identity_sha256"],
        "finalized run manifest canonical_event_identity_sha256",
    )
    spark_aggregates_sha256 = _digest(
        manifest["spark_aggregates_sha256"],
        "finalized run manifest spark_aggregates_sha256",
    )
    _string(manifest["statistical_unit"], "finalized run manifest statistical_unit")
    _string(manifest["publication_contract"], "finalized run manifest publication_contract")
    return ValidatedFinalizedRunManifest(
        replay_run_id=replay_run_id,
        snapshot_id=_digest(manifest["snapshot_id"], "finalized run manifest snapshot_id"),
        events_sha256=_digest(manifest["events_sha256"], "finalized run manifest events_sha256"),
        source_event_count=source_event_count,
        policy_config_sha256=_digest(
            manifest["policy_config_sha256"],
            "finalized run manifest policy_config_sha256",
        ),
        canonical_event_identity_sha256=canonical_event_identity_sha256,
        spark_aggregates_sha256=spark_aggregates_sha256,
    )


def _stream_digest_and_rows(source: BinaryIO, context: str) -> tuple[str, int]:
    digest = sha256()
    row_count = 0
    last_byte = b""
    while chunk := source.read(1024 * 1024):
        digest.update(chunk)
        row_count += chunk.count(b"\n")
        last_byte = chunk[-1:]
    if last_byte != b"\n":
        raise OracleManifestError(f"{context} must end with a newline")
    return digest.hexdigest(), row_count


def _stream_metrics_evidence(source: BinaryIO) -> tuple[str, int, str, int]:
    metrics_digest = sha256()
    identity_digest = sha256()
    metrics_row_count = 0
    identity_row_count = 0
    previous_source_event_id: str | None = None
    required_dimensions = frozenset(
        {"source_event_id", "lineage_id", "row_id", "source_row_index", "payload_sha256"}
    )
    for line_number, raw_line in enumerate(source, start=1):
        if not raw_line.endswith(b"\n"):
            raise OracleManifestError("Hadoop reducer metrics must end with a newline")
        metrics_digest.update(raw_line)
        metrics_row_count += 1
        try:
            row = json.loads(
                raw_line,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            )
        except (UnicodeError, json.JSONDecodeError, OracleManifestError) as error:
            raise OracleManifestError(
                f"Hadoop reducer metric line {line_number} is invalid JSON: {error}"
            ) from error
        if not isinstance(row, dict) or row.get("metric") != "source_event_occurrence_count":
            continue
        if raw_line != canonical_manifest(row):
            raise OracleManifestError(
                f"Hadoop reducer metric line {line_number} must be canonical compact JSON"
            )
        _exact_fields(
            row,
            frozenset({"dimensions", "metric", "statistical_unit", "value"}),
            f"Hadoop reducer metric line {line_number}",
        )
        if row["statistical_unit"] != "canonical_input_record" or row["value"] != 1:
            raise OracleManifestError(
                f"Hadoop reducer metric line {line_number} has an invalid source occurrence"
            )
        dimensions = _detached_mapping(
            row["dimensions"], f"Hadoop reducer metric line {line_number} dimensions"
        )
        _exact_fields(
            dimensions,
            required_dimensions,
            f"Hadoop reducer metric line {line_number} dimensions",
        )
        source_event_id = _digest(
            dimensions["source_event_id"],
            f"Hadoop reducer metric line {line_number} source_event_id",
        )
        lineage_id = _digest(
            dimensions["lineage_id"],
            f"Hadoop reducer metric line {line_number} lineage_id",
        )
        payload_sha256 = _digest(
            dimensions["payload_sha256"],
            f"Hadoop reducer metric line {line_number} payload_sha256",
        )
        row_id = _integer(dimensions["row_id"], f"Hadoop reducer metric line {line_number} row_id")
        source_row_index = _integer(
            dimensions["source_row_index"],
            f"Hadoop reducer metric line {line_number} source_row_index",
        )
        if previous_source_event_id is not None and source_event_id <= previous_source_event_id:
            raise OracleManifestError(
                "Hadoop source occurrence rows must have unique ascending source_event_id values"
            )
        previous_source_event_id = source_event_id
        identity_digest.update(
            f"{source_event_id}|{lineage_id}|{row_id}|{source_row_index}|{payload_sha256}\n".encode(
                "ascii"
            )
        )
        identity_row_count += 1
    if metrics_row_count == 0:
        raise OracleManifestError("Hadoop reducer metrics must contain at least one row")
    if identity_row_count == 0:
        raise OracleManifestError("Hadoop reducer metrics are missing source occurrence rows")
    return (
        metrics_digest.hexdigest(),
        metrics_row_count,
        identity_digest.hexdigest(),
        identity_row_count,
    )


def load_snapshot_manifest(path: Path) -> ValidatedSnapshotManifest:
    """Read strict JSON and validate the canonical snapshot manifest mapping."""
    try:
        with path.open("r", encoding="utf-8") as source:
            value = json.load(
                source, object_pairs_hook=_unique_object, parse_constant=_reject_constant
            )
    except (OSError, UnicodeError, json.JSONDecodeError, OracleManifestError) as error:
        raise OracleManifestError(f"invalid snapshot manifest JSON: {error}") from error
    try:
        return validate_snapshot_manifest_mapping(value)
    except SchemaContractError as error:
        raise OracleManifestError(f"invalid snapshot manifest: {error}") from error


def verify_snapshot_events(manifest: ValidatedSnapshotManifest, source: BinaryIO) -> None:
    """Stream exact snapshot bytes and verify their digest and event count."""
    digest, event_count = _stream_digest_and_rows(source, "snapshot events")
    if digest != manifest.events_sha256:
        raise OracleManifestError(
            f"snapshot events SHA-256 mismatch: expected {manifest.events_sha256}, got {digest}"
        )
    if event_count != manifest.event_count:
        raise OracleManifestError(
            f"snapshot event count mismatch: expected {manifest.event_count}, got {event_count}"
        )


def build_hadoop_oracle_manifest(
    manifest: ValidatedSnapshotManifest,
    metrics_source: BinaryIO,
    *,
    policy_config_sha256: str,
    input_events_hdfs_path: str,
    output_metrics_hdfs_path: str,
) -> dict[str, object]:
    """Stream reducer bytes once and return the canonical compact v1 manifest mapping."""
    (
        metrics_sha256,
        metrics_row_count,
        canonical_event_identity_sha256,
        identity_row_count,
    ) = _stream_metrics_evidence(metrics_source)
    if identity_row_count != manifest.event_count:
        raise OracleManifestError(
            "Hadoop source occurrence row count does not match the snapshot event count"
        )
    value = {
        "schema_version": "hadoop-oracle-manifest-v1",
        "snapshot_id": manifest.snapshot_id,
        "events_sha256": manifest.events_sha256,
        "snapshot_event_count": manifest.event_count,
        "policy_config_sha256": _digest(policy_config_sha256, "policy_config_sha256"),
        "hadoop_version": "3.5.0",
        "reducer_count": 1,
        "input_events_hdfs_path": _hdfs_path(input_events_hdfs_path, "input_events_hdfs_path"),
        "output_metrics_hdfs_path": _hdfs_path(
            output_metrics_hdfs_path, "output_metrics_hdfs_path"
        ),
        "metrics_sha256": metrics_sha256,
        "metrics_row_count": metrics_row_count,
        "canonical_event_identity_sha256": canonical_event_identity_sha256,
    }
    validate_hadoop_oracle_manifest_mapping(value)
    return value


def canonical_manifest(value: Mapping[str, object]) -> bytes:
    """Serialize one compact sorted manifest with a terminal newline."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise OracleManifestError(f"duplicate JSON property: {name}")
        result[name] = value
    return result


def _reject_constant(value: str) -> object:
    raise OracleManifestError(f"non-finite JSON number is not permitted: {value}")


def main(argv: Sequence[str] | None = None) -> int:
    """Verify snapshot input or build a Hadoop oracle manifest from standard input."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify = subparsers.add_parser("verify-snapshot")
    verify.add_argument("--snapshot-manifest", required=True, type=Path)
    build = subparsers.add_parser("build")
    build.add_argument("--snapshot-manifest", required=True, type=Path)
    build.add_argument("--policy-config", required=True, type=Path)
    build.add_argument("--input-events-hdfs-path", required=True)
    build.add_argument("--output-metrics-hdfs-path", required=True)
    args = parser.parse_args(argv)
    try:
        snapshot = load_snapshot_manifest(args.snapshot_manifest)
        if args.command == "verify-snapshot":
            verify_snapshot_events(snapshot, sys.stdin.buffer)
        else:
            policy_digest = sha256(args.policy_config.read_bytes()).hexdigest()
            manifest = build_hadoop_oracle_manifest(
                snapshot,
                sys.stdin.buffer,
                policy_config_sha256=policy_digest,
                input_events_hdfs_path=args.input_events_hdfs_path,
                output_metrics_hdfs_path=args.output_metrics_hdfs_path,
            )
            sys.stdout.buffer.write(canonical_manifest(manifest))
        return 0
    except (OSError, OracleManifestError) as error:
        print(f"Hadoop oracle manifest failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
