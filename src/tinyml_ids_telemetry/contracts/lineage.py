"""Validated lineage dimension for source telemetry."""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from pathlib import PurePosixPath
from typing import ClassVar

from tinyml_ids_telemetry.contracts.identity import lineage_id

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class LineageRecord:
    """Provenance shared by every event in one board-model-stage lineage."""

    SCHEMA_VERSION: ClassVar[str] = "telemetry-lineage-v1"

    lineage_id: str
    board: str
    student_model: str
    seed: int
    experiment_protocol_id: str
    wireless_protocol_id: str
    dataset_protocol_id: str
    dataset_sha256: str
    test_split_sha256: str
    scaler_sha256: str
    export_id: str
    wireless_bundle_id: str
    model_file_sha256: str
    model_artifact_sha256: str
    firmware_binary_sha256: str
    source_session_id: str
    source_stage_id: str
    expected_rows: int
    class_names: tuple[str, ...]
    normal_class_id: int
    logit_frac_bits: int
    mcu_csv_sha256: str
    reference_csv_sha256: str
    completion_manifest_sha256: str
    compile_record_sha256: str
    mcu_relative_path: str
    reference_relative_path: str

    def __post_init__(self) -> None:
        for name in (
            "board",
            "student_model",
            "experiment_protocol_id",
            "wireless_protocol_id",
            "dataset_protocol_id",
            "source_session_id",
            "source_stage_id",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} must be non-empty")

        for name in (
            "dataset_sha256",
            "test_split_sha256",
            "scaler_sha256",
            "export_id",
            "wireless_bundle_id",
            "model_file_sha256",
            "model_artifact_sha256",
            "firmware_binary_sha256",
            "mcu_csv_sha256",
            "reference_csv_sha256",
            "completion_manifest_sha256",
            "compile_record_sha256",
        ):
            if _SHA256_PATTERN.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")

        if self.lineage_id != self.computed_lineage_id:
            raise ValueError("lineage_id disagrees with its canonical identity fields")
        if self.seed < 0:
            raise ValueError("seed must be non-negative")
        if self.expected_rows < 1:
            raise ValueError("expected_rows must be positive")
        if len(self.class_names) != 5 or len(set(self.class_names)) != 5:
            raise ValueError("class_names must contain five unique classes")
        if not 0 <= self.normal_class_id < len(self.class_names):
            raise ValueError("normal_class_id is outside the class mapping")
        if self.class_names[self.normal_class_id].casefold() != "normal":
            raise ValueError("normal_class_id does not identify the Normal class")
        if self.logit_frac_bits < 0:
            raise ValueError("logit_frac_bits must be non-negative")

        self._validate_relative_path("mcu_relative_path", self.mcu_relative_path)
        self._validate_relative_path("reference_relative_path", self.reference_relative_path)

    @property
    def computed_lineage_id(self) -> str:
        return lineage_id(
            protocol_id=self.wireless_protocol_id,
            export_id=self.export_id,
            wireless_bundle_id=self.wireless_bundle_id,
            source_session_id=self.source_session_id,
            source_stage_id=self.source_stage_id,
        )

    @staticmethod
    def _validate_relative_path(name: str, value: str) -> None:
        path = PurePosixPath(value)
        if not value or path.is_absolute() or ".." in path.parts:
            raise ValueError(f"{name} must be a repository-relative POSIX path")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            field.name: getattr(self, field.name) for field in fields(self)
        }
        payload["schema_version"] = self.SCHEMA_VERSION
        payload["class_names"] = list(self.class_names)
        return payload
