import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.helpers.evidence_factory import create_evidence_tree, sha256_file
from tinyml_ids_telemetry.evidence.bundle import create_evidence_bundle
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
    payload = json.loads((output / "snapshot_manifest.json").read_text(encoding="utf-8"))
    assert payload["execution_lineage_count"] == 1
    assert payload["rows_per_lineage"] == 2
    assert payload["evaluation_sequence_sha256"] == manifest.evaluation_sequence_sha256
    assert payload["source_interpretation"].startswith("1 board-model execution")

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


def test_snapshot_rejects_output_inside_separately_configured_source_root(
    tmp_path: Path,
) -> None:
    bundle_root, catalog_path = create_evidence_tree(tmp_path)
    source_root = tmp_path / "source-repository"
    source_root.mkdir()
    catalog = load_catalog(
        catalog_path,
        evidence_root=bundle_root,
        environment={"CUKD_XAI_ROOT": str(source_root)},
    )

    with pytest.raises(EvidenceContractError, match="outside"):
        create_snapshot(catalog, source_root / "generated", created_at=FIXED_TIME)


def test_snapshot_rejects_existing_destination(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=root)
    output = tmp_path / "snapshot"
    output.mkdir()

    with pytest.raises(EvidenceContractError, match="already exists"):
        create_snapshot(catalog, output, created_at=FIXED_TIME)


def test_snapshot_rejects_invalid_evidence_bundle_manifest(tmp_path: Path) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    source_catalog = load_catalog(catalog_path, evidence_root=source_root)
    bundle_root = tmp_path / "bundle"
    create_evidence_bundle(source_catalog, bundle_root)
    bundle_manifest = bundle_root / "evidence_bundle_manifest.json"
    payload = json.loads(bundle_manifest.read_text(encoding="utf-8"))
    payload["bundle_id"] = "0" * 64
    bundle_manifest.write_text(json.dumps(payload), encoding="utf-8")
    bundle_catalog = load_catalog(catalog_path, evidence_root=bundle_root)

    with pytest.raises(EvidenceContractError, match="bundle ID"):
        create_snapshot(bundle_catalog, tmp_path / "snapshot", created_at=FIXED_TIME)


def test_snapshot_rejects_missing_externally_anchored_bundle_manifest(
    tmp_path: Path,
) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    source_catalog = load_catalog(catalog_path, evidence_root=source_root)
    bundle_root = tmp_path / "bundle"
    manifest = create_evidence_bundle(source_catalog, bundle_root)
    (bundle_root / "evidence_bundle_manifest.json").unlink()
    bundle_catalog = replace(
        load_catalog(catalog_path, evidence_root=bundle_root),
        evidence_bundle_id=manifest.bundle_id,
    )

    with pytest.raises(EvidenceContractError, match="Missing evidence_bundle_manifest"):
        create_snapshot(bundle_catalog, tmp_path / "snapshot", created_at=FIXED_TIME)


def test_snapshot_failure_does_not_publish_partial_destination(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=root)
    output = tmp_path / "snapshot"
    mcu_path = root / "results/sessions/esp32c3_A/full_mcu.csv"
    mcu_path.write_text(mcu_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(EvidenceContractError, match="mcu_csv"):
        create_snapshot(catalog, output, created_at=FIXED_TIME)

    assert not output.exists()
