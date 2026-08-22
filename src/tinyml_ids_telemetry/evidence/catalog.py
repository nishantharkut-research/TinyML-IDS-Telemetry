"""Portable, traversal-safe evidence catalog loading."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import cast


class EvidenceContractError(ValueError):
    """Raised when evidence configuration or content violates its contract."""


@dataclass(frozen=True, slots=True)
class ModelSpec:
    name: str
    preprocess_metadata: Path


@dataclass(frozen=True, slots=True)
class LineageSpec:
    label: str
    board: str
    student_model: str
    completion_manifest: Path
    compile_record: Path
    mcu_csv: Path


@dataclass(frozen=True, slots=True)
class EvidenceCatalog:
    evidence_root: Path
    sealed_report: Path
    models: Mapping[str, ModelSpec]
    lineages: tuple[LineageSpec, ...]


def _require_string(record: Mapping[str, object], key: str, context: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise EvidenceContractError(f"{context}.{key} must be a non-empty string")
    return value


def _resolve_relative(root: Path, raw_path: str, context: str) -> Path:
    if "\\" in raw_path:
        raise EvidenceContractError(f"{context} must use a repository-relative POSIX path")
    portable = PurePosixPath(raw_path)
    if portable.is_absolute() or ".." in portable.parts:
        raise EvidenceContractError(f"{context} must stay inside the evidence root")
    resolved = (root / Path(*portable.parts)).resolve()
    if not resolved.is_relative_to(root):
        raise EvidenceContractError(f"{context} must stay inside the evidence root")
    return resolved


def _mapping_list(data: Mapping[str, object], key: str) -> list[Mapping[str, object]]:
    value = data.get(key)
    if not isinstance(value, list) or not value:
        raise EvidenceContractError(f"{key} must be a non-empty array of tables")
    records: list[Mapping[str, object]] = []
    for index, record in enumerate(value):
        if not isinstance(record, dict):
            raise EvidenceContractError(f"{key}[{index}] must be a table")
        records.append(cast(Mapping[str, object], record))
    return records


def load_catalog(
    catalog_path: Path,
    *,
    evidence_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
) -> EvidenceCatalog:
    """Load a catalog while resolving every artifact below one read-only root."""
    env = os.environ if environment is None else environment
    root_value = (
        evidence_root
        if evidence_root is not None
        else env.get("TINYML_IDS_EVIDENCE_ROOT") or env.get("CUKD_XAI_ROOT")
    )
    if root_value is None or str(root_value).strip() == "":
        raise EvidenceContractError(
            "TINYML_IDS_EVIDENCE_ROOT or CUKD_XAI_ROOT must identify the evidence root"
        )
    root = Path(root_value).expanduser().resolve()
    if not root.is_dir():
        raise EvidenceContractError(f"Evidence root is not a directory: {root}")

    with catalog_path.open("rb") as handle:
        data = cast(dict[str, object], tomllib.load(handle))
    if data.get("version") != 1:
        raise EvidenceContractError("Unsupported evidence catalog version")

    sealed_report = _resolve_relative(
        root,
        _require_string(data, "sealed_report", "catalog"),
        "sealed_report",
    )

    models: dict[str, ModelSpec] = {}
    for index, record in enumerate(_mapping_list(data, "models")):
        context = f"models[{index}]"
        name = _require_string(record, "name", context)
        if name in models:
            raise EvidenceContractError(f"Duplicate model name: {name}")
        models[name] = ModelSpec(
            name=name,
            preprocess_metadata=_resolve_relative(
                root,
                _require_string(record, "preprocess_metadata", context),
                f"{context}.preprocess_metadata",
            ),
        )

    lineages: list[LineageSpec] = []
    labels: set[str] = set()
    for index, record in enumerate(_mapping_list(data, "lineages")):
        context = f"lineages[{index}]"
        label = _require_string(record, "label", context)
        student_model = _require_string(record, "student_model", context)
        if label in labels:
            raise EvidenceContractError(f"Duplicate lineage label: {label}")
        if student_model not in models:
            raise EvidenceContractError(f"{context}.student_model references an unknown model")
        labels.add(label)
        lineages.append(
            LineageSpec(
                label=label,
                board=_require_string(record, "board", context),
                student_model=student_model,
                completion_manifest=_resolve_relative(
                    root,
                    _require_string(record, "completion_manifest", context),
                    f"{context}.completion_manifest",
                ),
                compile_record=_resolve_relative(
                    root,
                    _require_string(record, "compile_record", context),
                    f"{context}.compile_record",
                ),
                mcu_csv=_resolve_relative(
                    root,
                    _require_string(record, "mcu_csv", context),
                    f"{context}.mcu_csv",
                ),
            )
        )

    return EvidenceCatalog(
        evidence_root=root,
        sealed_report=sealed_report,
        models=models,
        lineages=tuple(lineages),
    )


def ensure_output_outside_evidence_root(output_path: Path, evidence_root: Path) -> Path:
    """Resolve an output path and reject writes to the evidence repository."""
    root = evidence_root.resolve()
    output = output_path.expanduser().resolve()
    if output == root or output.is_relative_to(root):
        raise EvidenceContractError("Generated output must be outside the evidence root")
    return output
