"""Validation against packaged canonical JSON Schema resources."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from hashlib import sha256
from importlib import resources
from typing import cast

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

from tinyml_ids_telemetry.contracts.lineage import LineageRecord

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "snapshot_id",
        "created_at_utc",
        "sealed_report_sha256",
        "event_count",
        "lineage_count",
        "execution_lineage_count",
        "rows_per_lineage",
        "evaluation_sequence_sha256",
        "events_sha256",
        "lineages_sha256",
        "lineage_ids",
        "source_interpretation",
        "event_time_boundary",
    }
)
_LINEAGE_FIELDS = frozenset(
    {
        "schema_version",
        "lineage_id",
        "board",
        "student_model",
        "seed",
        "experiment_protocol_id",
        "wireless_protocol_id",
        "dataset_protocol_id",
        "dataset_sha256",
        "test_split_sha256",
        "scaler_sha256",
        "export_id",
        "wireless_bundle_id",
        "model_file_sha256",
        "model_artifact_sha256",
        "firmware_binary_sha256",
        "source_session_id",
        "source_stage_id",
        "expected_rows",
        "class_names",
        "normal_class_id",
        "logit_frac_bits",
        "mcu_csv_sha256",
        "reference_csv_sha256",
        "completion_manifest_sha256",
        "compile_record_sha256",
        "mcu_relative_path",
        "reference_relative_path",
    }
)


class SchemaContractError(ValueError):
    """A mapping does not satisfy its canonical packaged schema."""


@dataclass(frozen=True, slots=True)
class ValidatedSnapshotManifest:
    """Manifest fields needed to verify the immutable snapshot payload."""

    event_count: int
    lineage_count: int
    rows_per_lineage: int
    evaluation_sequence_sha256: str
    events_sha256: str
    lineages_sha256: str
    lineage_ids: tuple[str, ...]


@lru_cache(maxsize=1)
def _event_validator() -> Draft202012Validator:
    schema_resource = resources.files("tinyml_ids_telemetry.schemas").joinpath(
        "telemetry-event-v1.schema.json"
    )
    schema_value = json.loads(schema_resource.read_text(encoding="utf-8"))
    if not isinstance(schema_value, dict):
        raise RuntimeError("packaged telemetry event schema must be a JSON object")
    schema = cast(dict[str, object], schema_value)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def validate_event_mapping(value: object) -> dict[str, object]:
    """Return a detached canonical event mapping or fail closed."""
    event = _detached_json_object(value, "telemetry event")
    errors = sorted(
        _event_validator().iter_errors(event),
        key=lambda error: (
            tuple(str(component) for component in error.absolute_path),
            error.message,
        ),
    )
    if errors:
        error = errors[0]
        path = "$" + "".join(f"[{component!r}]" for component in error.absolute_path)
        raise SchemaContractError(f"invalid telemetry-event-v1 at {path}: {error.message}")
    return event


def validate_snapshot_manifest_mapping(value: object) -> ValidatedSnapshotManifest:
    """Validate the exact canonical snapshot manifest shape and invariants."""
    manifest = _detached_json_object(value, "snapshot manifest")
    _require_exact_fields(manifest, _MANIFEST_FIELDS, "snapshot manifest")
    if manifest["schema_version"] != "telemetry-snapshot-v1":
        raise SchemaContractError("snapshot manifest schema_version must be telemetry-snapshot-v1")

    snapshot_id = _required_digest(manifest, "snapshot_id", "snapshot manifest")
    sealed_report_sha256 = _required_digest(manifest, "sealed_report_sha256", "snapshot manifest")
    events_sha256 = _required_digest(manifest, "events_sha256", "snapshot manifest")
    lineages_sha256 = _required_digest(manifest, "lineages_sha256", "snapshot manifest")
    evaluation_sequence_sha256 = _required_digest(
        manifest, "evaluation_sequence_sha256", "snapshot manifest"
    )
    event_count = _required_integer(manifest, "event_count", "snapshot manifest", minimum=0)
    lineage_count = _required_integer(manifest, "lineage_count", "snapshot manifest", minimum=1)
    execution_lineage_count = _required_integer(
        manifest, "execution_lineage_count", "snapshot manifest", minimum=1
    )
    rows_per_lineage = _required_integer(
        manifest, "rows_per_lineage", "snapshot manifest", minimum=1
    )
    if execution_lineage_count != lineage_count:
        raise SchemaContractError(
            "snapshot manifest execution_lineage_count must equal lineage_count"
        )
    if event_count != lineage_count * rows_per_lineage:
        raise SchemaContractError(
            "snapshot manifest event_count must equal lineage_count times rows_per_lineage"
        )

    created_at_utc = _required_string(manifest, "created_at_utc", "snapshot manifest")
    _validate_canonical_utc(created_at_utc)
    _required_string(manifest, "source_interpretation", "snapshot manifest")
    _required_string(manifest, "event_time_boundary", "snapshot manifest")

    lineage_ids_value = manifest["lineage_ids"]
    if not isinstance(lineage_ids_value, list):
        raise SchemaContractError("snapshot manifest lineage_ids must be an array")
    lineage_ids = tuple(
        _digest_value(identifier, f"snapshot manifest lineage_ids[{index}]")
        for index, identifier in enumerate(lineage_ids_value)
    )
    if len(lineage_ids) != lineage_count:
        raise SchemaContractError("snapshot manifest lineage_ids count must equal lineage_count")
    if len(set(lineage_ids)) != len(lineage_ids):
        raise SchemaContractError("snapshot manifest lineage_ids must be unique")
    if lineage_ids != tuple(sorted(lineage_ids)):
        raise SchemaContractError("snapshot manifest lineage_ids must be sorted")

    identity = {
        "schema_version": "telemetry-snapshot-v1",
        "sealed_report_sha256": sealed_report_sha256,
        "events_sha256": events_sha256,
        "lineages_sha256": lineages_sha256,
        "event_count": event_count,
    }
    computed_snapshot_id = sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    if snapshot_id != computed_snapshot_id:
        raise SchemaContractError(
            "snapshot manifest snapshot_id disagrees with its canonical identity fields"
        )

    return ValidatedSnapshotManifest(
        event_count=event_count,
        lineage_count=lineage_count,
        rows_per_lineage=rows_per_lineage,
        evaluation_sequence_sha256=evaluation_sequence_sha256,
        events_sha256=events_sha256,
        lineages_sha256=lineages_sha256,
        lineage_ids=lineage_ids,
    )


def validate_lineage_mapping(value: object) -> LineageRecord:
    """Validate one full canonical lineage mapping and its deterministic ID."""
    lineage = _detached_json_object(value, "telemetry lineage")
    _require_exact_fields(lineage, _LINEAGE_FIELDS, "telemetry lineage")
    if lineage["schema_version"] != LineageRecord.SCHEMA_VERSION:
        raise SchemaContractError("telemetry lineage schema_version must be telemetry-lineage-v1")

    class_names_value = lineage["class_names"]
    if not isinstance(class_names_value, list) or not all(
        isinstance(class_name, str) and class_name for class_name in class_names_value
    ):
        raise SchemaContractError("telemetry lineage class_names must be non-empty strings")

    try:
        return LineageRecord(
            lineage_id=_required_string(lineage, "lineage_id", "telemetry lineage"),
            board=_required_string(lineage, "board", "telemetry lineage"),
            student_model=_required_string(lineage, "student_model", "telemetry lineage"),
            seed=_required_integer(lineage, "seed", "telemetry lineage", minimum=0),
            experiment_protocol_id=_required_string(
                lineage, "experiment_protocol_id", "telemetry lineage"
            ),
            wireless_protocol_id=_required_string(
                lineage, "wireless_protocol_id", "telemetry lineage"
            ),
            dataset_protocol_id=_required_string(
                lineage, "dataset_protocol_id", "telemetry lineage"
            ),
            dataset_sha256=_required_string(lineage, "dataset_sha256", "telemetry lineage"),
            test_split_sha256=_required_string(lineage, "test_split_sha256", "telemetry lineage"),
            scaler_sha256=_required_string(lineage, "scaler_sha256", "telemetry lineage"),
            export_id=_required_string(lineage, "export_id", "telemetry lineage"),
            wireless_bundle_id=_required_string(lineage, "wireless_bundle_id", "telemetry lineage"),
            model_file_sha256=_required_string(lineage, "model_file_sha256", "telemetry lineage"),
            model_artifact_sha256=_required_string(
                lineage, "model_artifact_sha256", "telemetry lineage"
            ),
            firmware_binary_sha256=_required_string(
                lineage, "firmware_binary_sha256", "telemetry lineage"
            ),
            source_session_id=_required_string(lineage, "source_session_id", "telemetry lineage"),
            source_stage_id=_required_string(lineage, "source_stage_id", "telemetry lineage"),
            expected_rows=_required_integer(
                lineage, "expected_rows", "telemetry lineage", minimum=1
            ),
            class_names=tuple(cast(list[str], class_names_value)),
            normal_class_id=_required_integer(
                lineage, "normal_class_id", "telemetry lineage", minimum=0
            ),
            logit_frac_bits=_required_integer(
                lineage, "logit_frac_bits", "telemetry lineage", minimum=0
            ),
            mcu_csv_sha256=_required_string(lineage, "mcu_csv_sha256", "telemetry lineage"),
            reference_csv_sha256=_required_string(
                lineage, "reference_csv_sha256", "telemetry lineage"
            ),
            completion_manifest_sha256=_required_string(
                lineage, "completion_manifest_sha256", "telemetry lineage"
            ),
            compile_record_sha256=_required_string(
                lineage, "compile_record_sha256", "telemetry lineage"
            ),
            mcu_relative_path=_required_string(lineage, "mcu_relative_path", "telemetry lineage"),
            reference_relative_path=_required_string(
                lineage, "reference_relative_path", "telemetry lineage"
            ),
        )
    except ValueError as error:
        raise SchemaContractError(f"invalid telemetry-lineage-v1: {error}") from error


def _detached_json_object(value: object, contract: str) -> dict[str, object]:
    try:
        detached_value = json.loads(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as error:
        raise SchemaContractError(f"{contract} must be JSON-compatible") from error
    if not isinstance(detached_value, dict):
        raise SchemaContractError(f"{contract} must be a JSON object mapping")
    return cast(dict[str, object], detached_value)


def _require_exact_fields(
    value: dict[str, object], expected: frozenset[str], contract: str
) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    if missing:
        raise SchemaContractError(f"{contract} is missing required field {missing[0]}")
    extra = sorted(actual - expected)
    if extra:
        raise SchemaContractError(f"{contract} contains unexpected field {extra[0]}")


def _required_string(value: dict[str, object], field: str, contract: str) -> str:
    item = value[field]
    if not isinstance(item, str) or not item:
        raise SchemaContractError(f"{contract} {field} must be a non-empty string")
    return item


def _required_integer(value: dict[str, object], field: str, contract: str, *, minimum: int) -> int:
    item = value[field]
    if type(item) is not int or item < minimum:
        raise SchemaContractError(
            f"{contract} {field} must be an integer greater than or equal to {minimum}"
        )
    return item


def _digest_value(value: object, context: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise SchemaContractError(f"{context} must be a lowercase SHA-256 digest")
    return value


def _required_digest(value: dict[str, object], field: str, contract: str) -> str:
    return _digest_value(value[field], f"{contract} {field}")


def _validate_canonical_utc(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00" if value.endswith("Z") else value)
    except ValueError as error:
        raise SchemaContractError(
            "snapshot manifest created_at_utc must be a canonical UTC timestamp"
        ) from error
    canonical = parsed.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    if (
        parsed.utcoffset() is None
        or parsed.utcoffset() != UTC.utcoffset(parsed)
        or value != canonical
    ):
        raise SchemaContractError(
            "snapshot manifest created_at_utc must be a canonical UTC timestamp"
        )
