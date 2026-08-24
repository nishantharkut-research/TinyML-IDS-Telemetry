"""Fail-closed access to finalized Spark aggregate publications."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import cast

_RUN_DIRECTORY = re.compile(r"replay_run_id=([A-Za-z0-9][A-Za-z0-9._-]{0,127})")
_OUTPUTS = frozenset(
    {
        "canonical_gold",
        "execution_dimension_counts",
        "operational_metrics",
        "policy_decisions",
        "predictive_row_metrics",
        "retention_metrics",
        "retention_reason_counts",
    }
)
_AGGREGATES = _OUTPUTS - {"canonical_gold", "policy_decisions"}


class FinalizedRunError(ValueError):
    """A dashboard input is incomplete, inconsistent, or not finalized."""


@dataclass(frozen=True)
class FinalizedRun:
    """Validated read-only aggregate view of one replay run."""

    run_directory: Path
    replay_run_id: str
    source_event_count: int
    delivery_count: int
    aggregates: Mapping[str, tuple[Mapping[str, object], ...]]


def _read_json_object(path: Path, label: str) -> tuple[dict[str, object], bytes]:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise FinalizedRunError(f"unable to read {label}: {path}") from error
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FinalizedRunError(f"{label} is not valid JSON") from error
    if not isinstance(value, dict):
        raise FinalizedRunError(f"{label} must be a JSON object")
    return cast(dict[str, object], value), payload


def _nonnegative_int(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        raise FinalizedRunError(f"finalized manifest {field} must be a nonnegative integer")
    return value


def load_finalized_run(run_directory: Path) -> FinalizedRun:
    """Load aggregate tables only after validating the finalized publication contract."""
    run = run_directory.expanduser().resolve()
    match = _RUN_DIRECTORY.fullmatch(run.name)
    if not run.is_dir() or match is None:
        raise FinalizedRunError("run directory must be named replay_run_id=<safe-id>")

    manifest, _ = _read_json_object(run / "finalized_run_manifest.json", "finalized manifest")
    if manifest.get("schema_version") != "telemetry-finalized-run-v1":
        raise FinalizedRunError("finalized manifest has an unsupported schema_version")
    replay_run_id = manifest.get("replay_run_id")
    if replay_run_id != match.group(1):
        raise FinalizedRunError("finalized manifest replay_run_id does not match its directory")

    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict) or set(outputs) != _OUTPUTS:
        raise FinalizedRunError("finalized manifest must declare the exact output set")
    for output in sorted(_OUTPUTS):
        if not (run / output / "_SUCCESS").is_file():
            raise FinalizedRunError(f"{output} is missing its _SUCCESS marker")

    aggregate_document, aggregate_payload = _read_json_object(
        run / "spark_aggregates.json", "Spark aggregate document"
    )
    expected_hash = manifest.get("spark_aggregates_sha256")
    if not isinstance(expected_hash, str) or sha256(aggregate_payload).hexdigest() != expected_hash:
        raise FinalizedRunError("Spark aggregate document SHA-256 does not match the manifest")
    if set(aggregate_document) != _AGGREGATES:
        raise FinalizedRunError("Spark aggregate document must contain the exact aggregate tables")

    tables: dict[str, tuple[Mapping[str, object], ...]] = {}
    for table_name in sorted(_AGGREGATES):
        raw_rows = aggregate_document[table_name]
        if not isinstance(raw_rows, list) or any(not isinstance(row, dict) for row in raw_rows):
            raise FinalizedRunError(f"aggregate table {table_name} must contain JSON objects")
        rows = cast(list[dict[str, object]], raw_rows)
        if any(row.get("replay_run_id") != replay_run_id for row in rows):
            raise FinalizedRunError(
                f"aggregate table {table_name} contains a foreign replay_run_id"
            )
        tables[table_name] = tuple(MappingProxyType(dict(row)) for row in rows)

    source_event_count = _nonnegative_int(manifest.get("source_event_count"), "source_event_count")
    delivery_count = _nonnegative_int(manifest.get("delivery_count"), "delivery_count")
    if source_event_count == 0 or delivery_count < source_event_count:
        raise FinalizedRunError("finalized manifest has inconsistent source and delivery counts")

    return FinalizedRun(
        run_directory=run,
        replay_run_id=cast(str, replay_run_id),
        source_event_count=source_event_count,
        delivery_count=delivery_count,
        aggregates=MappingProxyType(tables),
    )
