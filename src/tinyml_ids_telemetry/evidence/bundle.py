"""Minimal, hash-preserving import of verified source evidence."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import cast

from tinyml_ids_telemetry.evidence.audit import audit_catalog
from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceCatalog,
    EvidenceContractError,
    ensure_output_outside_evidence_roots,
)
from tinyml_ids_telemetry.evidence.hashes import sha256_size_file


@dataclass(frozen=True, slots=True)
class BundledArtifact:
    relative_path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class EvidenceBundleManifest:
    schema_version: str
    bundle_id: str
    sealed_report_sha256: str
    artifact_count: int
    execution_lineage_count: int
    rows_per_lineage: int
    evaluation_sequence_sha256: str
    artifacts: tuple[BundledArtifact, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "bundle_id": self.bundle_id,
            "sealed_report_sha256": self.sealed_report_sha256,
            "artifact_count": self.artifact_count,
            "execution_lineage_count": self.execution_lineage_count,
            "rows_per_lineage": self.rows_per_lineage,
            "evaluation_sequence_sha256": self.evaluation_sequence_sha256,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "source_interpretation": (
                f"{self.execution_lineage_count} board-model executions over one shared "
                f"{self.rows_per_lineage}-record test sequence; executions are not "
                "independent predictive samples."
            ),
            "artifact_scope": (
                "Reduced analytics evidence package: all completion-listed session files and "
                "all compile-record portable artifacts are included; full firmware source "
                "trees are outside this analytics boundary."
            ),
            "publication_boundary": (
                "Companion CuKD-XAI research evidence; add the formal article citation only "
                "after a stable bibliographic record exists."
            ),
        }


def _load_json_object(path: Path, context: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise EvidenceContractError(f"Invalid JSON for {context}: {path}") from error
    if not isinstance(value, dict):
        raise EvidenceContractError(f"{context} must be a JSON object")
    return cast(dict[str, object], value)


def _portable_source_path(
    root: Path,
    compile_record: Path,
    portable_artifacts: dict[str, object],
    key: str,
) -> Path:
    item = portable_artifacts.get(key)
    if not isinstance(item, dict):
        raise EvidenceContractError(f"compile_record.portable_artifacts.{key} is missing")
    raw_path = item.get("path")
    if not isinstance(raw_path, str) or not raw_path or "\\" in raw_path:
        raise EvidenceContractError(f"compile_record.portable_artifacts.{key}.path is invalid")
    portable = PurePosixPath(raw_path)
    if portable.is_absolute() or ".." in portable.parts:
        raise EvidenceContractError(f"compile_record.portable_artifacts.{key}.path is unsafe")
    path = (compile_record.parent / Path(*portable.parts)).resolve()
    if not path.is_relative_to(root):
        raise EvidenceContractError(f"compile_record.portable_artifacts.{key}.path leaves root")
    return path


def _completion_source_path(root: Path, completion_manifest: Path, raw_path: str) -> Path:
    if not raw_path or "\\" in raw_path:
        raise EvidenceContractError("completion_manifest.files.path is invalid")
    portable = PurePosixPath(raw_path)
    if portable.is_absolute() or ".." in portable.parts:
        raise EvidenceContractError("completion_manifest.files.path is unsafe")
    path = (completion_manifest.parent / Path(*portable.parts)).resolve()
    if not path.is_relative_to(root):
        raise EvidenceContractError("completion_manifest.files.path leaves root")
    return path


def _required_source_paths(catalog: EvidenceCatalog) -> tuple[Path, ...]:
    root = catalog.evidence_root
    paths: set[Path] = {catalog.sealed_report}
    paths.update(model.preprocess_metadata for model in catalog.models.values())
    for lineage in catalog.lineages:
        paths.update((lineage.completion_manifest, lineage.compile_record, lineage.mcu_csv))
        completion = _load_json_object(lineage.completion_manifest, "completion_manifest")
        completion_files = completion.get("files")
        if not isinstance(completion_files, list):
            raise EvidenceContractError("completion_manifest.files must be an array")
        for item in completion_files:
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                raise EvidenceContractError("completion_manifest.files entry is invalid")
            paths.add(
                _completion_source_path(
                    root,
                    lineage.completion_manifest,
                    cast(str, item["path"]),
                )
            )
        compile_record = _load_json_object(lineage.compile_record, "compile_record")
        portable = compile_record.get("portable_artifacts")
        if not isinstance(portable, dict):
            raise EvidenceContractError("compile_record.portable_artifacts must be an object")
        portable_artifacts = cast(dict[str, object], portable)
        paths.update(
            _portable_source_path(root, lineage.compile_record, portable_artifacts, key)
            for key in portable_artifacts
        )
    return tuple(sorted(paths, key=lambda path: path.relative_to(root).as_posix()))


def _bundle_id(
    sealed_report_sha256: str,
    execution_lineage_count: int,
    rows_per_lineage: int,
    evaluation_sequence_sha256: str,
    artifacts: tuple[BundledArtifact, ...],
) -> str:
    payload = json.dumps(
        {
            "schema_version": "evidence-bundle-v1",
            "sealed_report_sha256": sealed_report_sha256,
            "execution_lineage_count": execution_lineage_count,
            "rows_per_lineage": rows_per_lineage,
            "evaluation_sequence_sha256": evaluation_sequence_sha256,
            "artifacts": [artifact.to_dict() for artifact in artifacts],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return sha256(payload.encode("ascii")).hexdigest()


def _copy_verified(
    source: Path,
    destination: Path,
    expected_sha256: str,
    expected_size: int,
) -> None:
    digest = sha256()
    size = 0
    with source.open("rb") as source_handle, destination.open("wb") as destination_handle:
        for block in iter(lambda: source_handle.read(1024 * 1024), b""):
            digest.update(block)
            size += len(block)
            destination_handle.write(block)
    if digest.hexdigest() != expected_sha256 or size != expected_size:
        raise EvidenceContractError(f"Source changed while copying: {source}")


def _artifact_from_dict(value: object, context: str) -> BundledArtifact:
    if not isinstance(value, dict):
        raise EvidenceContractError(f"{context} must be an object")
    record = cast(dict[str, object], value)
    relative_path = record.get("relative_path")
    digest = record.get("sha256")
    size_bytes = record.get("size_bytes")
    if not isinstance(relative_path, str) or not relative_path:
        raise EvidenceContractError(f"{context}.relative_path is invalid")
    path = PurePosixPath(relative_path)
    if path.is_absolute() or ".." in path.parts or "\\" in relative_path:
        raise EvidenceContractError(f"{context}.relative_path is unsafe")
    if not isinstance(digest, str) or len(digest) != 64:
        raise EvidenceContractError(f"{context}.sha256 is invalid")
    if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
        raise EvidenceContractError(f"{context}.size_bytes is invalid")
    return BundledArtifact(relative_path, digest, size_bytes)


def verify_evidence_bundle(catalog: EvidenceCatalog) -> EvidenceBundleManifest:
    """Verify a bundle manifest, every copied file, and its deterministic identity."""
    manifest_path = catalog.evidence_root / "evidence_bundle_manifest.json"
    if not manifest_path.is_file():
        raise EvidenceContractError(f"Missing evidence_bundle_manifest: {manifest_path}")
    manifest_data = _load_json_object(manifest_path, "evidence_bundle_manifest")
    if manifest_data.get("schema_version") != "evidence-bundle-v1":
        raise EvidenceContractError("Unsupported evidence bundle schema")
    raw_artifacts = manifest_data.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise EvidenceContractError("evidence_bundle_manifest.artifacts must be an array")
    artifacts = tuple(
        _artifact_from_dict(value, f"evidence_bundle_manifest.artifacts[{index}]")
        for index, value in enumerate(raw_artifacts)
    )
    if tuple(sorted(artifacts, key=lambda item: item.relative_path)) != artifacts:
        raise EvidenceContractError("evidence bundle artifacts must be sorted")
    if len({artifact.relative_path for artifact in artifacts}) != len(artifacts):
        raise EvidenceContractError("evidence bundle artifact paths must be unique")
    for artifact in artifacts:
        unresolved = catalog.evidence_root / Path(*PurePosixPath(artifact.relative_path).parts)
        try:
            path = unresolved.resolve(strict=True)
        except OSError as error:
            raise EvidenceContractError(
                f"Missing or changed bundle artifact: {artifact.relative_path}"
            ) from error
        if not path.is_relative_to(catalog.evidence_root.resolve()):
            raise EvidenceContractError(
                f"bundle artifact leaves the evidence bundle root: {artifact.relative_path}"
            )
        if not path.is_file():
            raise EvidenceContractError(
                f"Missing or changed bundle artifact: {artifact.relative_path}"
            )
        actual_sha256, actual_size = sha256_size_file(path)
        if actual_size != artifact.size_bytes or actual_sha256 != artifact.sha256:
            raise EvidenceContractError(
                f"bundle artifact SHA-256 mismatch: {artifact.relative_path}"
            )
    sealed_report_sha256 = manifest_data.get("sealed_report_sha256")
    if sealed_report_sha256 != catalog.sealed_report_sha256:
        raise EvidenceContractError("evidence bundle sealed-report trust anchor mismatch")
    artifact_count = manifest_data.get("artifact_count")
    lineage_count = manifest_data.get("execution_lineage_count")
    rows_per_lineage = manifest_data.get("rows_per_lineage")
    evaluation_hash = manifest_data.get("evaluation_sequence_sha256")
    if artifact_count != len(artifacts):
        raise EvidenceContractError("evidence bundle artifact count mismatch")
    if not isinstance(lineage_count, int) or lineage_count < 1:
        raise EvidenceContractError("evidence bundle lineage count is invalid")
    if not isinstance(rows_per_lineage, int) or rows_per_lineage < 1:
        raise EvidenceContractError("evidence bundle row count is invalid")
    if not isinstance(evaluation_hash, str) or len(evaluation_hash) != 64:
        raise EvidenceContractError("evidence bundle evaluation hash is invalid")
    bundle_id = manifest_data.get("bundle_id")
    if bundle_id != _bundle_id(
        catalog.sealed_report_sha256,
        lineage_count,
        rows_per_lineage,
        evaluation_hash,
        artifacts,
    ):
        raise EvidenceContractError("evidence bundle ID mismatch")
    if catalog.evidence_bundle_id is not None and bundle_id != catalog.evidence_bundle_id:
        raise EvidenceContractError("evidence bundle external trust anchor mismatch")
    return EvidenceBundleManifest(
        schema_version="evidence-bundle-v1",
        bundle_id=bundle_id,
        sealed_report_sha256=catalog.sealed_report_sha256,
        artifact_count=len(artifacts),
        execution_lineage_count=lineage_count,
        rows_per_lineage=rows_per_lineage,
        evaluation_sequence_sha256=evaluation_hash,
        artifacts=artifacts,
    )


def create_evidence_bundle(
    catalog: EvidenceCatalog,
    output_directory: Path,
) -> EvidenceBundleManifest:
    """Audit and atomically copy the minimal evidence set outside the source root."""
    output = ensure_output_outside_evidence_roots(output_directory, catalog.protected_roots)
    if output.exists():
        raise EvidenceContractError(f"Evidence bundle destination already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)

    audit = audit_catalog(catalog)
    source_paths = _required_source_paths(catalog)
    artifacts_list: list[BundledArtifact] = []
    for path in source_paths:
        digest, size = sha256_size_file(path)
        artifacts_list.append(
            BundledArtifact(
                relative_path=path.relative_to(catalog.evidence_root).as_posix(),
                sha256=digest,
                size_bytes=size,
            )
        )
    artifacts = tuple(artifacts_list)
    manifest = EvidenceBundleManifest(
        schema_version="evidence-bundle-v1",
        bundle_id=_bundle_id(
            audit.sealed_report_sha256,
            len(audit.lineages),
            audit.lineages[0].expected_rows,
            audit.evaluation_sequence_sha256,
            artifacts,
        ),
        sealed_report_sha256=audit.sealed_report_sha256,
        artifact_count=len(artifacts),
        execution_lineage_count=len(audit.lineages),
        rows_per_lineage=audit.lineages[0].expected_rows,
        evaluation_sequence_sha256=audit.evaluation_sequence_sha256,
        artifacts=artifacts,
    )
    if (
        catalog.evidence_bundle_id is not None
        and manifest.bundle_id != catalog.evidence_bundle_id
    ):
        raise EvidenceContractError("evidence bundle external trust anchor mismatch")

    with TemporaryDirectory(prefix=".evidence-bundle-", dir=output.parent) as temporary:
        staging = Path(temporary)
        for source, artifact in zip(source_paths, artifacts, strict=True):
            destination = staging / Path(*PurePosixPath(artifact.relative_path).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            _copy_verified(source, destination, artifact.sha256, artifact.size_bytes)
        staged_catalog = replace(
            catalog,
            evidence_root=staging,
            sealed_report=staging / catalog.sealed_report.relative_to(catalog.evidence_root),
            protected_roots=(staging, *catalog.protected_roots),
            models={
                name: replace(
                    model,
                    preprocess_metadata=(
                        staging
                        / model.preprocess_metadata.relative_to(catalog.evidence_root)
                    ),
                )
                for name, model in catalog.models.items()
            },
            lineages=tuple(
                replace(
                    lineage,
                    completion_manifest=(
                        staging
                        / lineage.completion_manifest.relative_to(catalog.evidence_root)
                    ),
                    compile_record=(
                        staging / lineage.compile_record.relative_to(catalog.evidence_root)
                    ),
                    mcu_csv=staging / lineage.mcu_csv.relative_to(catalog.evidence_root),
                )
                for lineage in catalog.lineages
            ),
        )
        staged_audit = audit_catalog(staged_catalog)
        if staged_audit.evaluation_sequence_sha256 != audit.evaluation_sequence_sha256:
            raise EvidenceContractError("Staged evidence bundle audit disagrees with source audit")
        (staging / "evidence_bundle_manifest.json").write_text(
            json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(staging, output)
    return manifest
