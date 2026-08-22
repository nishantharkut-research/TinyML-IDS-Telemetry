from __future__ import annotations

import os
from pathlib import Path

import pytest

from tinyml_ids_telemetry.evidence.catalog import load_catalog
from tinyml_ids_telemetry.evidence.snapshot import create_snapshot

EXPECTED_SNAPSHOT_ID = "d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff"
EXPECTED_EVENTS_SHA256 = "dd790ff824e2c53e9661da9e03a6f0d05eb78fc61b36e10c57ac4b6d83314f8f"
EXPECTED_LINEAGES_SHA256 = "92fe3ae37fadf616e7e9d7e3e043feec792a1d355cf044c87aac03245136daab"


@pytest.mark.full_evidence
def test_all_four_hardware_lineages_rebuild_the_frozen_snapshot(tmp_path: Path) -> None:
    root_value = os.environ.get("TINYML_IDS_EVIDENCE_ROOT")
    if not root_value:
        pytest.skip("TINYML_IDS_EVIDENCE_ROOT is not configured")

    catalog = load_catalog(
        Path("configs/evidence-catalog.toml"),
        evidence_root=Path(root_value),
    )
    manifest = create_snapshot(catalog, tmp_path / "snapshot")

    assert manifest.event_count == 225_204
    assert manifest.lineage_count == 4
    assert manifest.snapshot_id == EXPECTED_SNAPSHOT_ID
    assert manifest.events_sha256 == EXPECTED_EVENTS_SHA256
    assert manifest.lineages_sha256 == EXPECTED_LINEAGES_SHA256
