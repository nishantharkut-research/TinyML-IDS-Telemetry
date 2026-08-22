"""Atomic, content-addressed canonical evidence snapshots."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TextIO

from tinyml_ids_telemetry.evidence.adapter import iter_lineage_events
from tinyml_ids_telemetry.evidence.audit import audit_catalog
from tinyml_ids_telemetry.evidence.bundle import verify_evidence_bundle
from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceCatalog,
    EvidenceContractError,
    ensure_output_outside_evidence_roots,
)
from tinyml_ids_telemetry.evidence.hashes import sha256_file


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    schema_version: str
    snapshot_id: str
    created_at_utc: str
    sealed_report_sha256: str
    event_count: int
    lineage_count: int
    rows_per_lineage: int
    evaluation_sequence_sha256: str
    events_sha256: str
    lineages_sha256: str
    lineage_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "created_at_utc": self.created_at_utc,
            "sealed_report_sha256": self.sealed_report_sha256,
            "event_count": self.event_count,
            "lineage_count": self.lineage_count,
            "execution_lineage_count": self.lineage_count,
            "rows_per_lineage": self.rows_per_lineage,
            "evaluation_sequence_sha256": self.evaluation_sequence_sha256,
            "events_sha256": self.events_sha256,
            "lineages_sha256": self.lineages_sha256,
            "lineage_ids": list(self.lineage_ids),
            "source_interpretation": (
                f"{self.lineage_count} board-model executions over one shared "
                f"{self.rows_per_lineage}-record test sequence; event_count is not an "
                "independent predictive-sample count."
            ),
            "event_time_boundary": (
                "The source snapshot has no natural event timestamp; replay time is added only "
                "by the delivery layer."
            ),
        }


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _write_json_line(handle: TextIO, value: object) -> None:
    handle.write(_canonical_json(value))
    handle.write("\n")


def _format_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise EvidenceContractError("created_at must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _snapshot_id(
    *,
    sealed_report_sha256: str,
    events_sha256: str,
    lineages_sha256: str,
    event_count: int,
) -> str:
    payload = _canonical_json(
        {
            "schema_version": "telemetry-snapshot-v1",
            "sealed_report_sha256": sealed_report_sha256,
            "events_sha256": events_sha256,
            "lineages_sha256": lineages_sha256,
            "event_count": event_count,
        }
    )
    return sha256(payload.encode("ascii")).hexdigest()


def create_snapshot(
    catalog: EvidenceCatalog,
    output_directory: Path,
    *,
    created_at: datetime | None = None,
) -> SnapshotManifest:
    """Audit all sources and atomically publish a canonical JSONL snapshot."""
    output = ensure_output_outside_evidence_roots(output_directory, catalog.protected_roots)
    if output.exists():
        raise EvidenceContractError(f"Snapshot destination already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)

    bundle_manifest = catalog.evidence_root / "evidence_bundle_manifest.json"
    if catalog.evidence_bundle_id is not None or bundle_manifest.is_file():
        verify_evidence_bundle(catalog)
    audit = audit_catalog(catalog)
    timestamp = _format_utc(datetime.now(UTC) if created_at is None else created_at)
    lineages = tuple(sorted(audit.lineages, key=lambda lineage: lineage.lineage_id))

    with TemporaryDirectory(prefix=".telemetry-snapshot-", dir=output.parent) as temporary:
        staging = Path(temporary)
        lineages_path = staging / "lineages.jsonl"
        with lineages_path.open("w", encoding="utf-8", newline="\n") as handle:
            for lineage in lineages:
                _write_json_line(handle, lineage.to_dict())

        events_path = staging / "events.jsonl"
        event_count = 0
        with events_path.open("w", encoding="utf-8", newline="\n") as handle:
            for lineage in lineages:
                for event in iter_lineage_events(catalog.evidence_root, lineage):
                    _write_json_line(handle, event.to_dict())
                    event_count += 1

        if event_count != audit.total_expected_events:
            raise EvidenceContractError(
                f"Snapshot event count mismatch: expected {audit.total_expected_events}, "
                f"got {event_count}"
            )
        events_hash = sha256_file(events_path)
        lineages_hash = sha256_file(lineages_path)
        manifest = SnapshotManifest(
            schema_version="telemetry-snapshot-v1",
            snapshot_id=_snapshot_id(
                sealed_report_sha256=audit.sealed_report_sha256,
                events_sha256=events_hash,
                lineages_sha256=lineages_hash,
                event_count=event_count,
            ),
            created_at_utc=timestamp,
            sealed_report_sha256=audit.sealed_report_sha256,
            event_count=event_count,
            lineage_count=len(lineages),
            rows_per_lineage=lineages[0].expected_rows,
            evaluation_sequence_sha256=audit.evaluation_sequence_sha256,
            events_sha256=events_hash,
            lineages_sha256=lineages_hash,
            lineage_ids=tuple(lineage.lineage_id for lineage in lineages),
        )
        manifest_path = staging / "snapshot_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(staging, output)
        return manifest
