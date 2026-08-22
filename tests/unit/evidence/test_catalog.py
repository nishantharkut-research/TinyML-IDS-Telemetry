from pathlib import Path

import pytest

from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceContractError,
    ensure_output_outside_evidence_root,
    ensure_output_outside_evidence_roots,
    load_catalog,
)

CATALOG_TEXT = """
version = 1
sealed_report = "results/final_report.json"
sealed_report_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[[models]]
name = "student_A"
preprocess_metadata = "models/student_A/preprocess_metadata.json"

[[lineages]]
label = "esp32c3_student_A"
board = "esp32c3"
student_model = "student_A"
completion_manifest = "results/sessions/esp32c3_A/completion.json"
compile_record = "results/compile/esp32c3_A.json"
mcu_csv = "results/sessions/esp32c3_A/full_mcu.csv"
"""


def write_catalog(path: Path, text: str = CATALOG_TEXT) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_catalog_resolves_root_from_environment(tmp_path: Path) -> None:
    catalog_path = write_catalog(tmp_path / "catalog.toml")
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    catalog = load_catalog(
        catalog_path,
        environment={"CUKD_XAI_ROOT": str(evidence_root)},
    )

    assert catalog.evidence_root == evidence_root.resolve()
    assert catalog.sealed_report == (evidence_root / "results/final_report.json").resolve()
    assert catalog.sealed_report_sha256 == "a" * 64
    assert catalog.models["student_A"].name == "student_A"
    assert catalog.lineages[0].label == "esp32c3_student_A"


def test_catalog_prefers_portable_bundle_environment_root(tmp_path: Path) -> None:
    catalog_path = write_catalog(tmp_path / "catalog.toml")
    bundle_root = tmp_path / "bundle"
    source_root = tmp_path / "source"
    bundle_root.mkdir()
    source_root.mkdir()

    catalog = load_catalog(
        catalog_path,
        environment={
            "TINYML_IDS_EVIDENCE_ROOT": str(bundle_root),
            "CUKD_XAI_ROOT": str(source_root),
        },
    )

    assert catalog.evidence_root == bundle_root.resolve()


def test_model_catalog_does_not_require_board_specific_export_report(tmp_path: Path) -> None:
    catalog_path = write_catalog(tmp_path / "catalog.toml", CATALOG_TEXT)
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    catalog = load_catalog(catalog_path, evidence_root=evidence_root)

    assert catalog.models["student_A"].preprocess_metadata.name == "preprocess_metadata.json"


def test_catalog_reads_optional_bundle_trust_anchor(tmp_path: Path) -> None:
    catalog_path = write_catalog(
        tmp_path / "catalog.toml",
        CATALOG_TEXT.replace(
            'sealed_report_sha256 = "' + "a" * 64 + '"',
            'sealed_report_sha256 = "' + "a" * 64 + '"\n'
            'evidence_bundle_id = "' + "b" * 64 + '"',
        ),
    )
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    catalog = load_catalog(catalog_path, evidence_root=evidence_root)

    assert catalog.evidence_bundle_id == "b" * 64


def test_catalog_rejects_missing_evidence_root(tmp_path: Path) -> None:
    catalog_path = write_catalog(tmp_path / "catalog.toml")

    with pytest.raises(EvidenceContractError, match="TINYML_IDS_EVIDENCE_ROOT"):
        load_catalog(catalog_path, environment={})


def test_catalog_rejects_unsupported_version(tmp_path: Path) -> None:
    catalog_path = write_catalog(
        tmp_path / "catalog.toml",
        CATALOG_TEXT.replace("version = 1", "version = 2"),
    )
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    with pytest.raises(EvidenceContractError, match="version"):
        load_catalog(catalog_path, evidence_root=evidence_root)


def test_catalog_wraps_malformed_toml_as_contract_error(tmp_path: Path) -> None:
    catalog_path = write_catalog(tmp_path / "catalog.toml", "version = [")
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    with pytest.raises(EvidenceContractError, match="TOML"):
        load_catalog(catalog_path, evidence_root=evidence_root)


def test_catalog_rejects_path_traversal(tmp_path: Path) -> None:
    catalog_path = write_catalog(
        tmp_path / "catalog.toml",
        CATALOG_TEXT.replace(
            'sealed_report = "results/final_report.json"',
            'sealed_report = "../outside.json"',
        ),
    )
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    with pytest.raises(EvidenceContractError, match="sealed_report"):
        load_catalog(catalog_path, evidence_root=evidence_root)


def test_output_guard_rejects_evidence_root_and_descendants(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    for output in (evidence_root, evidence_root / "generated" / "snapshot.jsonl"):
        with pytest.raises(EvidenceContractError, match="outside"):
            ensure_output_outside_evidence_root(output, evidence_root)


def test_output_guard_allows_sibling_directory(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()
    output = tmp_path / "telemetry" / "snapshot.jsonl"

    resolved = ensure_output_outside_evidence_root(output, evidence_root)

    assert resolved == output.resolve()


def test_output_guard_rejects_all_configured_evidence_roots(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    source_root = tmp_path / "source"
    bundle_root.mkdir()
    source_root.mkdir()

    with pytest.raises(EvidenceContractError, match="outside"):
        ensure_output_outside_evidence_roots(
            source_root / "generated",
            (bundle_root, source_root),
        )
