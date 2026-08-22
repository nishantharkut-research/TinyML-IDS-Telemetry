from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from tests.helpers.evidence_factory import create_evidence_tree, sha256_file
from tinyml_ids_telemetry.evidence.audit import audit_catalog
from tinyml_ids_telemetry.evidence.bundle import (
    create_evidence_bundle,
    verify_evidence_bundle,
)
from tinyml_ids_telemetry.evidence.catalog import EvidenceContractError, load_catalog


def test_bundle_copies_only_verified_evidence_and_reaudits(tmp_path: Path) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    source_catalog = load_catalog(catalog_path, evidence_root=source_root)
    source_hashes = {
        path.relative_to(source_root).as_posix(): sha256_file(path)
        for path in source_root.rglob("*")
        if path.is_file()
    }
    output = tmp_path / "portable-evidence"

    manifest = create_evidence_bundle(source_catalog, output)

    assert manifest.schema_version == "evidence-bundle-v1"
    assert manifest.artifact_count == 12
    assert len(manifest.artifacts) == 12
    assert (output / "evidence_bundle_manifest.json").is_file()
    for artifact in manifest.artifacts:
        copied = output / artifact.relative_path
        assert copied.is_file()
        assert sha256_file(copied) == artifact.sha256

    copied_catalog = load_catalog(catalog_path, evidence_root=output)
    assert verify_evidence_bundle(copied_catalog) == manifest
    copied_audit = audit_catalog(copied_catalog)
    assert copied_audit.total_expected_events == 2
    assert copied_audit.sealed_report_sha256 == manifest.sealed_report_sha256

    after_hashes = {
        path.relative_to(source_root).as_posix(): sha256_file(path)
        for path in source_root.rglob("*")
        if path.is_file()
    }
    assert after_hashes == source_hashes


def test_bundle_verification_rejects_tampered_copied_artifact(tmp_path: Path) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    source_catalog = load_catalog(catalog_path, evidence_root=source_root)
    output = tmp_path / "portable-evidence"
    create_evidence_bundle(source_catalog, output)
    copied_catalog = load_catalog(catalog_path, evidence_root=output)
    copied_mcu = output / "results/sessions/esp32c3_A/full_mcu.csv"
    copied_mcu.write_text("tampered\n", encoding="utf-8")

    with pytest.raises(EvidenceContractError, match="bundle artifact"):
        verify_evidence_bundle(copied_catalog)


def test_bundle_verification_rejects_artifact_symlink_outside_root(tmp_path: Path) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    source_catalog = load_catalog(catalog_path, evidence_root=source_root)
    output = tmp_path / "portable-evidence"
    manifest = create_evidence_bundle(source_catalog, output)
    artifact = manifest.artifacts[0]
    bundled_path = output / artifact.relative_path
    outside_path = tmp_path / "outside-artifact"
    outside_path.write_bytes(bundled_path.read_bytes())
    bundled_path.unlink()
    try:
        bundled_path.symlink_to(outside_path)
    except OSError as error:
        pytest.skip(f"File symlinks are unavailable: {error}")

    with pytest.raises(EvidenceContractError, match="leaves the evidence bundle root"):
        verify_evidence_bundle(load_catalog(catalog_path, evidence_root=output))


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("execution_lineage_count", 2),
        ("rows_per_lineage", 3),
        ("evaluation_sequence_sha256", "f" * 64),
    ],
)
def test_bundle_id_binds_derived_manifest_facts(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    source_catalog = load_catalog(catalog_path, evidence_root=source_root)
    output = tmp_path / "portable-evidence"
    create_evidence_bundle(source_catalog, output)
    manifest_path = output / "evidence_bundle_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload[field] = replacement
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(EvidenceContractError, match="bundle ID"):
        verify_evidence_bundle(load_catalog(catalog_path, evidence_root=output))


def test_bundle_manifest_is_portable_and_does_not_record_source_root(tmp_path: Path) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=source_root)
    output = tmp_path / "portable-evidence"

    create_evidence_bundle(catalog, output)

    text = (output / "evidence_bundle_manifest.json").read_text(encoding="utf-8")
    payload = json.loads(text)
    assert str(source_root) not in text
    assert payload["execution_lineage_count"] == 1
    assert payload["rows_per_lineage"] == 2
    assert payload["source_interpretation"].startswith("1 board-model execution")


def test_bundle_rejects_destination_inside_source_or_already_present(tmp_path: Path) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=source_root)

    with pytest.raises(EvidenceContractError, match="outside"):
        create_evidence_bundle(catalog, source_root / "portable")

    output = tmp_path / "existing"
    output.mkdir()
    with pytest.raises(EvidenceContractError, match="already exists"):
        create_evidence_bundle(catalog, output)


def test_bundle_failure_does_not_publish_partial_output(tmp_path: Path) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=source_root)
    output = tmp_path / "portable-evidence"
    (source_root / "results/sessions/esp32c3_A/full_mcu.csv").write_text(
        "tampered\n",
        encoding="utf-8",
    )

    with pytest.raises(EvidenceContractError, match="SHA-256"):
        create_evidence_bundle(catalog, output)

    assert not output.exists()


def test_bundle_creation_rejects_external_bundle_id_mismatch(tmp_path: Path) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    catalog = replace(
        load_catalog(catalog_path, evidence_root=source_root),
        evidence_bundle_id="0" * 64,
    )
    output = tmp_path / "portable-evidence"

    with pytest.raises(EvidenceContractError, match="external trust anchor"):
        create_evidence_bundle(catalog, output)

    assert not output.exists()
