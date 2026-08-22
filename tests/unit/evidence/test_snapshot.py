import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.helpers.evidence_factory import create_evidence_tree, sha256_file
from tinyml_ids_telemetry.evidence.catalog import EvidenceContractError, load_catalog
from tinyml_ids_telemetry.evidence.snapshot import create_snapshot

FIXED_TIME = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)


def test_snapshot_atomically_writes_events_lineages_and_manifest(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=root)
    output = tmp_path / "artifacts/snapshot"

    manifest = create_snapshot(catalog, output, created_at=FIXED_TIME)

    assert manifest.event_count == 2
    assert manifest.lineage_count == 1
    assert manifest.created_at_utc == "2026-08-22T12:00:00Z"
    assert (output / "events.jsonl").is_file()
    assert (output / "lineages.jsonl").is_file()
    assert (output / "snapshot_manifest.json").is_file()
    assert sha256_file(output / "events.jsonl") == manifest.events_sha256
    assert sha256_file(output / "lineages.jsonl") == manifest.lineages_sha256

    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    assert [event["row_id"] for event in events] == [0, 1]
    assert all(event["schema_version"] == "telemetry-event-v1" for event in events)


def test_snapshot_identity_is_content_deterministic(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=root)

    first = create_snapshot(catalog, tmp_path / "first", created_at=FIXED_TIME)
    second = create_snapshot(catalog, tmp_path / "second", created_at=FIXED_TIME)

    assert first.snapshot_id == second.snapshot_id
    assert first.events_sha256 == second.events_sha256
    assert first.lineages_sha256 == second.lineages_sha256
    assert (tmp_path / "first/events.jsonl").read_bytes() == (
        tmp_path / "second/events.jsonl"
    ).read_bytes()


def test_snapshot_rejects_output_inside_evidence_root(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=root)

    with pytest.raises(EvidenceContractError, match="outside"):
        create_snapshot(catalog, root / "generated", created_at=FIXED_TIME)


def test_snapshot_rejects_existing_destination(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=root)
    output = tmp_path / "snapshot"
    output.mkdir()

    with pytest.raises(EvidenceContractError, match="already exists"):
        create_snapshot(catalog, output, created_at=FIXED_TIME)


def test_snapshot_failure_does_not_publish_partial_destination(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=root)
    output = tmp_path / "snapshot"
    mcu_path = root / "results/sessions/esp32c3_A/full_mcu.csv"
    mcu_path.write_text(mcu_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(EvidenceContractError, match="mcu_csv"):
        create_snapshot(catalog, output, created_at=FIXED_TIME)

    assert not output.exists()
