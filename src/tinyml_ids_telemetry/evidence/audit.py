"""Fail-closed audit of sealed CuKD-XAI wireless evidence."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import cast

from tinyml_ids_telemetry.contracts.identity import lineage_id
from tinyml_ids_telemetry.contracts.lineage import LineageRecord
from tinyml_ids_telemetry.evidence.adapter import iter_lineage_events
from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceCatalog,
    EvidenceContractError,
    LineageSpec,
    ModelSpec,
)
from tinyml_ids_telemetry.evidence.hashes import sha256_file

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class EvidenceAudit:
    sealed_report_sha256: str
    total_expected_events: int
    validated_event_count: int
    evaluation_sequence_sha256: str
    lineages: tuple[LineageRecord, ...]


def _load_json(path: Path, context: str) -> dict[str, object]:
    if not path.is_file():
        raise EvidenceContractError(f"Missing {context}: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise EvidenceContractError(f"Invalid JSON for {context}: {path}") from error
    if not isinstance(value, dict):
        raise EvidenceContractError(f"{context} must be a JSON object")
    return cast(dict[str, object], value)


def _load_verified_json(
    path: Path,
    expected_sha256: str,
    context: str,
) -> dict[str, object]:
    if not path.is_file():
        raise EvidenceContractError(f"Missing {context}: {path}")
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise EvidenceContractError(f"Cannot read {context}: {path}") from error
    if sha256(payload).hexdigest() != expected_sha256:
        raise EvidenceContractError(f"{context} SHA-256 mismatch")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EvidenceContractError(f"Invalid JSON for {context}: {path}") from error
    if not isinstance(value, dict):
        raise EvidenceContractError(f"{context} must be a JSON object")
    return cast(dict[str, object], value)


def _object(container: dict[str, object], key: str, context: str) -> dict[str, object]:
    value = container.get(key)
    if not isinstance(value, dict):
        raise EvidenceContractError(f"{context}.{key} must be an object")
    return cast(dict[str, object], value)


def _array(container: dict[str, object], key: str, context: str) -> list[object]:
    value = container.get(key)
    if not isinstance(value, list):
        raise EvidenceContractError(f"{context}.{key} must be an array")
    return cast(list[object], value)


def _string(container: dict[str, object], key: str, context: str) -> str:
    value = container.get(key)
    if not isinstance(value, str) or not value:
        raise EvidenceContractError(f"{context}.{key} must be a non-empty string")
    return value


def _integer(container: dict[str, object], key: str, context: str) -> int:
    value = container.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise EvidenceContractError(f"{context}.{key} must be an integer")
    return value


def _digest(container: dict[str, object], key: str, context: str) -> str:
    value = _string(container, key, context)
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise EvidenceContractError(f"{context}.{key} must be a SHA-256 digest")
    return value


def _expect_equal(actual: object, expected: object, context: str) -> None:
    if actual != expected:
        raise EvidenceContractError(f"{context} mismatch: expected {expected!r}, got {actual!r}")


def _verify_file_hash(path: Path, expected: str, context: str) -> str:
    if not path.is_file():
        raise EvidenceContractError(f"Missing {context}: {path}")
    actual = sha256_file(path)
    if actual != expected:
        raise EvidenceContractError(f"{context} SHA-256 mismatch")
    return actual


def _resolve_portable_artifact(
    compile_record_path: Path,
    evidence_root: Path,
    artifact: dict[str, object],
    context: str,
) -> tuple[Path, str]:
    raw_path = _string(artifact, "path", context)
    if "\\" in raw_path:
        raise EvidenceContractError(f"{context}.path must use POSIX separators")
    portable = PurePosixPath(raw_path)
    if portable.is_absolute() or ".." in portable.parts:
        raise EvidenceContractError(f"{context}.path leaves the compile directory")
    resolved = (compile_record_path.parent / Path(*portable.parts)).resolve()
    if not resolved.is_relative_to(evidence_root):
        raise EvidenceContractError(f"{context}.path leaves the evidence root")
    expected_hash = _digest(artifact, "sha256", context)
    _verify_file_hash(resolved, expected_hash, context)
    return resolved, expected_hash


def _manifest_file_hash(manifest: dict[str, object], file_name: str, context: str) -> str:
    matches: list[str] = []
    for index, item in enumerate(_array(manifest, "files", context)):
        if not isinstance(item, dict):
            raise EvidenceContractError(f"{context}.files[{index}] must be an object")
        record = cast(dict[str, object], item)
        if _string(record, "path", f"{context}.files[{index}]") == file_name:
            matches.append(_digest(record, "sha256", f"{context}.files[{index}]"))
    if len(matches) != 1:
        raise EvidenceContractError(f"{context} must bind exactly one {file_name}")
    return matches[0]


def _class_mapping(metadata: dict[str, object], context: str) -> tuple[tuple[str, ...], int]:
    raw_names = _array(metadata, "class_names", context)
    if len(raw_names) != 5 or not all(isinstance(name, str) and name for name in raw_names):
        raise EvidenceContractError(f"{context}.class_names must contain five names")
    class_names = tuple(cast(str, name) for name in raw_names)
    if len(set(class_names)) != len(class_names):
        raise EvidenceContractError(f"{context}.class_names must be unique")
    try:
        normal_class_id = class_names.index("Normal")
    except ValueError as error:
        raise EvidenceContractError(f"{context}.class_names must contain Normal") from error
    return class_names, normal_class_id


def _final_logit_frac(strict_report: dict[str, object], context: str) -> int:
    calibration = _object(strict_report, "fixed_point_calibration", context)
    layers = _array(calibration, "layers", f"{context}.fixed_point_calibration")
    if not layers or not isinstance(layers[-1], dict):
        raise EvidenceContractError(f"{context} must define a final output_frac")
    output_frac = _integer(
        cast(dict[str, object], layers[-1]),
        "output_frac",
        f"{context}.fixed_point_calibration.layers[-1]",
    )
    if output_frac < 0:
        raise EvidenceContractError(f"{context} final output_frac must be non-negative")
    return output_frac


def _audit_lineage(
    catalog: EvidenceCatalog,
    spec: LineageSpec,
    model: ModelSpec,
    sealed_entry: dict[str, object],
    expected_rows: int,
) -> LineageRecord:
    context = f"lineage[{spec.label}]"
    completion_hash = _digest(sealed_entry, "completion_manifest_sha256", context)
    compile_hash = _digest(sealed_entry, "compile_sha256", context)
    completion = _load_verified_json(
        spec.completion_manifest,
        completion_hash,
        f"{context}.completion_manifest",
    )
    compile_record = _load_verified_json(
        spec.compile_record,
        compile_hash,
        f"{context}.compile_record",
    )

    _expect_equal(completion.get("status"), "complete", f"{context}.completion.status")
    _expect_equal(compile_record.get("status"), "passed", f"{context}.compile.status")
    _expect_equal(completion.get("board"), spec.board, f"{context}.completion.board")
    _expect_equal(compile_record.get("board"), spec.board, f"{context}.compile.board")
    _expect_equal(completion.get("student"), spec.student_model, f"{context}.completion.student")
    _expect_equal(compile_record.get("student"), spec.student_model, f"{context}.compile.student")

    wireless_protocol_id = _string(
        completion,
        "wireless_application_protocol_id",
        f"{context}.completion",
    )
    _expect_equal(
        compile_record.get("protocol_id"),
        wireless_protocol_id,
        f"{context}.compile.protocol_id",
    )
    export_id = _digest(completion, "export_id", f"{context}.completion")
    bundle_id = _digest(completion, "wireless_bundle_id", f"{context}.completion")
    _expect_equal(compile_record.get("export_id"), export_id, f"{context}.compile.export_id")
    _expect_equal(
        compile_record.get("wireless_bundle_id"),
        bundle_id,
        f"{context}.compile.wireless_bundle_id",
    )
    source_session_id = _string(completion, "session_id", f"{context}.completion")
    for key, expected in (
        ("session_id", source_session_id),
        ("export_id", export_id),
        ("wireless_bundle_id", bundle_id),
    ):
        _expect_equal(sealed_entry.get(key), expected, f"{context}.sealed.{key}")

    stages = _object(completion, "stage_evidence", f"{context}.completion")
    full_stage = _object(stages, "full_56301", f"{context}.completion.stage_evidence")
    _expect_equal(_integer(full_stage, "rows", context), expected_rows, f"{context}.rows")
    _expect_equal(
        completion.get("full_stage_inferences"),
        expected_rows,
        f"{context}.full_stage_inferences",
    )
    source_stage_id = _string(full_stage, "stage_id", f"{context}.full_stage")
    mcu_hash = _verify_file_hash(
        spec.mcu_csv,
        _digest(full_stage, "mcu_csv_sha256", f"{context}.full_stage"),
        f"{context}.mcu_csv",
    )

    completion_file_hashes: set[str] = set()
    for index, item in enumerate(_array(completion, "files", f"{context}.completion")):
        file_context = f"{context}.completion.files[{index}]"
        if not isinstance(item, dict):
            raise EvidenceContractError(f"{file_context} must be an object")
        record = cast(dict[str, object], item)
        raw_path = _string(record, "path", file_context)
        if "\\" in raw_path:
            raise EvidenceContractError(f"{file_context}.path must use POSIX separators")
        portable_path = PurePosixPath(raw_path)
        if portable_path.is_absolute() or ".." in portable_path.parts:
            raise EvidenceContractError(f"{file_context}.path is unsafe")
        source_path = (spec.completion_manifest.parent / Path(*portable_path.parts)).resolve()
        if not source_path.is_relative_to(catalog.evidence_root):
            raise EvidenceContractError(f"{file_context}.path leaves the evidence root")
        expected_hash = _digest(record, "sha256", file_context)
        _verify_file_hash(source_path, expected_hash, file_context)
        completion_file_hashes.add(expected_hash)
    if mcu_hash not in completion_file_hashes:
        raise EvidenceContractError(f"{context}.completion.files does not bind the MCU CSV")
    metrics_hash = full_stage.get("metrics_json_sha256")
    if metrics_hash is not None:
        if not isinstance(metrics_hash, str) or _SHA256_PATTERN.fullmatch(metrics_hash) is None:
            raise EvidenceContractError(f"{context}.full_stage.metrics_json_sha256 is invalid")
        if metrics_hash not in completion_file_hashes:
            raise EvidenceContractError(f"{context}.completion.files does not bind full metrics")
        if "metrics_sha256" in sealed_entry:
            _expect_equal(
                sealed_entry.get("metrics_sha256"),
                metrics_hash,
                f"{context}.sealed.metrics_sha256",
            )

    portable = _object(compile_record, "portable_artifacts", f"{context}.compile")
    verified_portable: dict[str, tuple[Path, str]] = {}
    for key, item in portable.items():
        if not isinstance(item, dict):
            raise EvidenceContractError(
                f"{context}.compile.portable_artifacts.{key} must be an object"
            )
        verified_portable[key] = _resolve_portable_artifact(
            spec.compile_record,
            catalog.evidence_root,
            cast(dict[str, object], item),
            f"{context}.compile.portable_artifacts.{key}",
        )
    required_portable = {
        "firmware_binary",
        "strict_export_report",
        "strict_export_manifest",
        "hil_reference_predictions",
    }
    missing_portable = required_portable.difference(verified_portable)
    if missing_portable:
        raise EvidenceContractError(
            f"{context}.compile.portable_artifacts missing {sorted(missing_portable)}"
        )
    reference_path, reference_hash = verified_portable["hil_reference_predictions"]
    strict_report_path, strict_report_hash = verified_portable["strict_export_report"]
    strict_manifest_path, strict_manifest_hash = verified_portable["strict_export_manifest"]
    _, firmware_hash = verified_portable["firmware_binary"]
    _expect_equal(
        compile_record.get("firmware_binary_sha256"),
        firmware_hash,
        f"{context}.compile.firmware_binary_sha256",
    )
    strict_report = _load_verified_json(
        strict_report_path,
        strict_report_hash,
        f"{context}.strict_export_report",
    )
    strict_manifest = _load_verified_json(
        strict_manifest_path,
        strict_manifest_hash,
        f"{context}.strict_export_manifest",
    )
    _expect_equal(
        strict_manifest.get("status"),
        "passed",
        f"{context}.strict_export_manifest.status",
    )
    _expect_equal(
        strict_manifest.get("export_id"),
        export_id,
        f"{context}.strict_export_manifest.export_id",
    )
    _expect_equal(
        strict_manifest.get("student"),
        spec.student_model,
        f"{context}.strict_export_manifest.student",
    )
    _expect_equal(
        _manifest_file_hash(
            strict_manifest,
            "strict_export_report.json",
            f"{context}.strict_export_manifest",
        ),
        sha256_file(strict_report_path),
        f"{context}.strict_export_manifest.strict_export_report",
    )
    _expect_equal(
        _manifest_file_hash(
            strict_manifest,
            "hil_reference_predictions.csv",
            f"{context}.strict_export_manifest",
        ),
        reference_hash,
        f"{context}.strict_export_manifest.hil_reference_predictions",
    )
    preprocess_expected_hash = _manifest_file_hash(
        strict_manifest,
        "preprocess_metadata.json",
        f"{context}.strict_export_manifest",
    )
    _verify_file_hash(
        model.preprocess_metadata,
        preprocess_expected_hash,
        f"{context}.preprocess_metadata",
    )
    metadata = _load_verified_json(
        model.preprocess_metadata,
        preprocess_expected_hash,
        f"{context}.preprocess_metadata",
    )

    _expect_equal(strict_report.get("status"), "passed", f"{context}.strict.status")
    _expect_equal(strict_report.get("export_id"), export_id, f"{context}.strict.export_id")
    provenance = _object(strict_report, "provenance", f"{context}.strict")
    _expect_equal(provenance.get("student"), spec.student_model, f"{context}.provenance.student")
    dataset_protocol_id = _string(provenance, "protocol_id", f"{context}.provenance")
    _expect_equal(
        strict_manifest.get("protocol_id"),
        dataset_protocol_id,
        f"{context}.strict_export_manifest.protocol_id",
    )
    _expect_equal(
        metadata.get("protocol_id"),
        dataset_protocol_id,
        f"{context}.metadata.protocol_id",
    )
    dataset_sha256 = _digest(provenance, "dataset_sha256", f"{context}.provenance")
    scaler_sha256 = _digest(provenance, "scaler_sha256", f"{context}.provenance")
    _expect_equal(
        metadata.get("dataset_sha256"),
        dataset_sha256,
        f"{context}.metadata.dataset_sha256",
    )
    _expect_equal(metadata.get("scaler_sha256"), scaler_sha256, f"{context}.metadata.scaler_sha256")
    split_hashes = _object(provenance, "split_hashes", f"{context}.provenance")
    test_split_sha256 = _digest(split_hashes, "test", f"{context}.provenance.split_hashes")
    metadata_splits = _object(metadata, "split_hashes", f"{context}.metadata")
    _expect_equal(
        metadata_splits.get("test"),
        test_split_sha256,
        f"{context}.metadata.split_hashes.test",
    )
    split_sizes = _object(metadata, "split_sizes", f"{context}.metadata")
    _expect_equal(split_sizes.get("test"), expected_rows, f"{context}.metadata.split_sizes.test")
    _expect_equal(
        metadata.get("scaler_fit_partition"),
        "train only",
        f"{context}.metadata.scaler_fit_partition",
    )

    class_names, normal_class_id = _class_mapping(metadata, f"{context}.metadata")
    logit_frac_bits = _final_logit_frac(strict_report, f"{context}.strict_export_report")
    seed = _integer(provenance, "seed", f"{context}.provenance")
    model_file_sha256 = _digest(provenance, "model_file_sha256", f"{context}.provenance")
    model_artifact_sha256 = _digest(
        provenance,
        "model_artifact_sha256",
        f"{context}.provenance",
    )

    lineage_identifier = lineage_id(
        protocol_id=wireless_protocol_id,
        export_id=export_id,
        wireless_bundle_id=bundle_id,
        source_session_id=source_session_id,
        source_stage_id=source_stage_id,
    )
    return LineageRecord(
        lineage_id=lineage_identifier,
        board=spec.board,
        student_model=spec.student_model,
        seed=seed,
        experiment_protocol_id=_string(completion, "protocol_id", f"{context}.completion"),
        wireless_protocol_id=wireless_protocol_id,
        dataset_protocol_id=dataset_protocol_id,
        dataset_sha256=dataset_sha256,
        test_split_sha256=test_split_sha256,
        scaler_sha256=scaler_sha256,
        export_id=export_id,
        wireless_bundle_id=bundle_id,
        model_file_sha256=model_file_sha256,
        model_artifact_sha256=model_artifact_sha256,
        firmware_binary_sha256=firmware_hash,
        source_session_id=source_session_id,
        source_stage_id=source_stage_id,
        expected_rows=expected_rows,
        class_names=class_names,
        normal_class_id=normal_class_id,
        logit_frac_bits=logit_frac_bits,
        mcu_csv_sha256=mcu_hash,
        reference_csv_sha256=reference_hash,
        completion_manifest_sha256=completion_hash,
        compile_record_sha256=compile_hash,
        mcu_relative_path=spec.mcu_csv.relative_to(catalog.evidence_root).as_posix(),
        reference_relative_path=reference_path.relative_to(catalog.evidence_root).as_posix(),
    )


def audit_catalog(catalog: EvidenceCatalog) -> EvidenceAudit:
    """Verify the sealed report and construct trusted lineage records."""
    report = _load_verified_json(
        catalog.sealed_report,
        catalog.sealed_report_sha256,
        "sealed_report",
    )
    _expect_equal(report.get("status"), "passed", "sealed_report.status")
    expected_rows = _integer(
        report,
        "full_test_rows_per_board_model_pair",
        "sealed_report",
    )
    if expected_rows < 1:
        raise EvidenceContractError("sealed_report row count must be positive")
    entries = _object(report, "input_evidence", "sealed_report")
    expected_labels = {spec.label for spec in catalog.lineages}
    if set(entries) != expected_labels:
        raise EvidenceContractError("sealed_report input_evidence labels do not match the catalog")

    lineages = tuple(
        _audit_lineage(
            catalog,
            spec,
            catalog.models[spec.student_model],
            _object(entries, spec.label, "sealed_report.input_evidence"),
            expected_rows,
        )
        for spec in catalog.lineages
    )
    total_expected_events = sum(lineage.expected_rows for lineage in lineages)
    _expect_equal(
        report.get("full_test_board_model_predictions"),
        total_expected_events,
        "sealed_report.full_test_board_model_predictions",
    )
    shared_fields = (
        "dataset_protocol_id",
        "dataset_sha256",
        "test_split_sha256",
        "scaler_sha256",
        "expected_rows",
        "class_names",
        "normal_class_id",
    )
    first_lineage = lineages[0]
    for lineage in lineages[1:]:
        for field_name in shared_fields:
            _expect_equal(
                getattr(lineage, field_name),
                getattr(first_lineage, field_name),
                f"shared_evaluation.{field_name}",
            )

    source_event_ids: set[str] = set()
    evaluation_sequence_hash: str | None = None
    validated_event_count = 0
    for lineage in lineages:
        sequence_digest = sha256()
        lineage_count = 0
        for event in iter_lineage_events(catalog.evidence_root, lineage):
            if event.status != "OK":
                raise EvidenceContractError(
                    f"lineage[{lineage.lineage_id}] row[{event.row_id}] status is not OK"
                )
            if event.source_event_id in source_event_ids:
                raise EvidenceContractError("source_event_id must be globally unique")
            source_event_ids.add(event.source_event_id)
            sequence_digest.update(
                f"{event.source_row_index}|{event.true_class_id}\n".encode("ascii")
            )
            lineage_count += 1
        if lineage_count != lineage.expected_rows:
            raise EvidenceContractError("validated lineage row count mismatch")
        current_sequence_hash = sequence_digest.hexdigest()
        if evaluation_sequence_hash is None:
            evaluation_sequence_hash = current_sequence_hash
        elif current_sequence_hash != evaluation_sequence_hash:
            raise EvidenceContractError(
                "lineages do not share the same ordered evaluation records and labels"
            )
        validated_event_count += lineage_count

    if validated_event_count != total_expected_events or evaluation_sequence_hash is None:
        raise EvidenceContractError("validated event count does not match the sealed report")
    return EvidenceAudit(
        sealed_report_sha256=catalog.sealed_report_sha256,
        total_expected_events=total_expected_events,
        validated_event_count=validated_event_count,
        evaluation_sequence_sha256=evaluation_sequence_hash,
        lineages=lineages,
    )
