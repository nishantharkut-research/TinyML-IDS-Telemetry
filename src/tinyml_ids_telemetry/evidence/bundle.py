"""Minimal, hash-preserving import of verified source evidence."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import cast

from tinyml_ids_telemetry.evidence.audit import audit_catalog
from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceCatalog,
    EvidenceContractError,
    ensure_output_outside_evidence_root,
)
from tinyml_ids_telemetry.evidence.hashes import sha256_file

_PORTABLE_ARTIFACT_KEYS = (
    "firmware_binary",
    "strict_export_report",
    "strict_export_manifest",
    "hil_reference_predictions",
)


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
    artifacts: tuple[BundledArtifact, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "bundle_id": self.bundle_id,
            "sealed_report_sha256": self.sealed_report_sha256,
            "artifact_count": self.artifact_count,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "source_interpretation": (
                "Four board-model executions over one shared 56,301-record test set; "
                "executions are not independent predictive samples."
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


def _required_source_paths(catalog: EvidenceCatalog) -> tuple[Path, ...]:
    root = catalog.evidence_root
    paths: set[Path] = {catalog.sealed_report}
    paths.update(model.preprocess_metadata for model in catalog.models.values())
    for lineage in catalog.lineages:
        paths.update((lineage.completion_manifest, lineage.compile_record, lineage.mcu_csv))
        compile_record = _load_json_object(lineage.compile_record, "compile_record")
        portable = compile_record.get("portable_artifacts")
        if not isinstance(portable, dict):
            raise EvidenceContractError("compile_record.portable_artifacts must be an object")
        portable_artifacts = cast(dict[str, object], portable)
        paths.update(
            _portable_source_path(root, lineage.compile_record, portable_artifacts, key)
            for key in _PORTABLE_ARTIFACT_KEYS
        )
    return tuple(sorted(paths, key=lambda path: path.relative_to(root).as_posix()))


def _bundle_id(sealed_report_sha256: str, artifacts: tuple[BundledArtifact, ...]) -> str:
    payload = json.dumps(
        {
            "schema_version": "evidence-bundle-v1",
            "sealed_report_sha256": sealed_report_sha256,
            "artifacts": [artifact.to_dict() for artifact in artifacts],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return sha256(payload.encode("ascii")).hexdigest()


def create_evidence_bundle(
    catalog: EvidenceCatalog,
    output_directory: Path,
) -> EvidenceBundleManifest:
    """Audit and atomically copy the minimal evidence set outside the source root."""
    output = ensure_output_outside_evidence_root(output_directory, catalog.evidence_root)
    if output.exists():
        raise EvidenceContractError(f"Evidence bundle destination already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)

    audit = audit_catalog(catalog)
    source_paths = _required_source_paths(catalog)
    artifacts = tuple(
        BundledArtifact(
            relative_path=path.relative_to(catalog.evidence_root).as_posix(),
            sha256=sha256_file(path),
            size_bytes=path.stat().st_size,
        )
        for path in source_paths
    )
    manifest = EvidenceBundleManifest(
        schema_version="evidence-bundle-v1",
        bundle_id=_bundle_id(audit.sealed_report_sha256, artifacts),
        sealed_report_sha256=audit.sealed_report_sha256,
        artifact_count=len(artifacts),
        artifacts=artifacts,
    )

    with TemporaryDirectory(prefix=".evidence-bundle-", dir=output.parent) as temporary:
        staging = Path(temporary)
        for source, artifact in zip(source_paths, artifacts, strict=True):
            destination = staging / Path(*PurePosixPath(artifact.relative_path).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            if sha256_file(destination) != artifact.sha256:
                raise EvidenceContractError(
                    f"Copied artifact SHA-256 mismatch: {artifact.relative_path}"
                )
        (staging / "evidence_bundle_manifest.json").write_text(
            json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(staging, output)
    return manifest
