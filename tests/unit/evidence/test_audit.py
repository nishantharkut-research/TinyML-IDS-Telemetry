from pathlib import Path

import pytest

from tests.helpers.evidence_factory import create_evidence_tree, read_json, write_json
from tinyml_ids_telemetry.evidence.audit import audit_catalog
from tinyml_ids_telemetry.evidence.catalog import EvidenceContractError, load_catalog


def test_audit_builds_validated_lineage_from_complete_hash_chain(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)

    result = audit_catalog(load_catalog(catalog_path, evidence_root=root))

    assert result.sealed_report_sha256
    assert result.total_expected_events == 2
    assert len(result.lineages) == 1
    lineage = result.lineages[0]
    assert lineage.board == "esp32c3"
    assert lineage.student_model == "student_A"
    assert lineage.expected_rows == 2
    assert lineage.normal_class_id == 3
    assert lineage.logit_frac_bits == 9
    assert lineage.mcu_relative_path == "results/sessions/esp32c3_A/full_mcu.csv"


def test_audit_rejects_mcu_file_changed_after_manifest_sealing(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    mcu_path = root / "results/sessions/esp32c3_A/full_mcu.csv"
    mcu_path.write_text(mcu_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(EvidenceContractError, match="mcu_csv"):
        audit_catalog(load_catalog(catalog_path, evidence_root=root))


def test_audit_rejects_coherently_resealed_tree_without_trust_anchor_change(
    tmp_path: Path,
) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    report_path = root / "results/final_report.json"
    report = read_json(report_path)
    report["untrusted_reseal"] = True
    write_json(report_path, report)

    with pytest.raises(EvidenceContractError, match="sealed_report SHA-256"):
        audit_catalog(load_catalog(catalog_path, evidence_root=root))


def test_audit_reads_and_validates_every_telemetry_row(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    mcu_path = root / "results/sessions/esp32c3_A/full_mcu.csv"
    text = mcu_path.read_text(encoding="utf-8").replace("0,OK,3,", "0,FAILED,3,")
    mcu_path.write_text(text, encoding="utf-8")

    completion_path = root / "results/sessions/esp32c3_A/completion.json"
    completion = read_json(completion_path)
    completion["stage_evidence"]["full_56301"]["mcu_csv_sha256"] = _sha(mcu_path)
    completion["files"][0]["sha256"] = _sha(mcu_path)
    completion["files"][0]["size_bytes"] = mcu_path.stat().st_size
    write_json(completion_path, completion)

    report_path = root / "results/final_report.json"
    report = read_json(report_path)
    report["input_evidence"]["esp32c3_student_A"]["completion_manifest_sha256"] = _sha(
        completion_path
    )
    write_json(report_path, report)
    catalog_text = catalog_path.read_text(encoding="utf-8")
    catalog_path.write_text(
        catalog_text.replace(
            _catalog_anchor(catalog_text),
            _sha(report_path),
        ),
        encoding="utf-8",
    )

    with pytest.raises(EvidenceContractError, match="status"):
        audit_catalog(load_catalog(catalog_path, evidence_root=root))


def test_audit_rejects_compile_identity_mismatch(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    compile_path = root / "results/compile/esp32c3_A.json"
    compile_record = read_json(compile_path)
    compile_record["board"] = "wrong_board"
    write_json(compile_path, compile_record)
    report_path = root / "results/final_report.json"
    report = read_json(report_path)
    report["input_evidence"]["esp32c3_student_A"]["compile_sha256"] = _sha(compile_path)
    write_json(report_path, report)
    _update_catalog_anchor(catalog_path, report_path)

    with pytest.raises(EvidenceContractError, match="board"):
        audit_catalog(load_catalog(catalog_path, evidence_root=root))


def test_audit_rejects_tampered_portable_compile_artifact(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    compile_log = root / "results/compile/esp32c3_A_artifacts/compile_log.txt"
    compile_log.write_text("tampered\n", encoding="utf-8")

    with pytest.raises(EvidenceContractError, match="compile_log"):
        audit_catalog(load_catalog(catalog_path, evidence_root=root))


def test_audit_rejects_invalid_strict_manifest_identity(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    strict_manifest_path = (
        root / "results/compile/esp32c3_A_artifacts/strict_export_manifest.json"
    )
    strict_manifest = read_json(strict_manifest_path)
    strict_manifest["student"] = "student_B"
    write_json(strict_manifest_path, strict_manifest)
    _reseal_compile_record(root, catalog_path)

    with pytest.raises(EvidenceContractError, match=r"strict_export_manifest\.student"):
        audit_catalog(load_catalog(catalog_path, evidence_root=root))


def test_audit_rejects_missing_normal_class_mapping(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    metadata_path = root / "models/student_A/preprocess_metadata.json"
    metadata = read_json(metadata_path)
    metadata["class_names"][3] = "Benign"
    write_json(metadata_path, metadata)
    _reseal_model_artifacts(root, catalog_path)

    with pytest.raises(EvidenceContractError, match="Normal"):
        audit_catalog(load_catalog(catalog_path, evidence_root=root))


def test_audit_rejects_missing_final_logit_scale(tmp_path: Path) -> None:
    root, catalog_path = create_evidence_tree(tmp_path)
    strict_path = root / "results/compile/esp32c3_A_artifacts/strict_export_report.json"
    strict = read_json(strict_path)
    strict["fixed_point_calibration"]["layers"] = []
    write_json(strict_path, strict)
    _reseal_model_artifacts(root, catalog_path)

    with pytest.raises(EvidenceContractError, match="output_frac"):
        audit_catalog(load_catalog(catalog_path, evidence_root=root))


def _sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _catalog_anchor(text: str) -> str:
    prefix = 'sealed_report_sha256 = "'
    return text.split(prefix, maxsplit=1)[1].split('"', maxsplit=1)[0]


def _update_catalog_anchor(catalog_path: Path, report_path: Path) -> None:
    catalog_text = catalog_path.read_text(encoding="utf-8")
    catalog_path.write_text(
        catalog_text.replace(_catalog_anchor(catalog_text), _sha(report_path)),
        encoding="utf-8",
    )


def _reseal_model_artifacts(root: Path, catalog_path: Path) -> None:
    preprocess_path = root / "models/student_A/preprocess_metadata.json"
    strict_path = root / "results/compile/esp32c3_A_artifacts/strict_export_report.json"
    strict_manifest_path = root / "results/compile/esp32c3_A_artifacts/strict_export_manifest.json"
    strict_manifest = read_json(strict_manifest_path)
    hashes = {
        "preprocess_metadata.json": _sha(preprocess_path),
        "strict_export_report.json": _sha(strict_path),
    }
    for record in strict_manifest["files"]:
        if record["path"] in hashes:
            record["sha256"] = hashes[record["path"]]
    write_json(strict_manifest_path, strict_manifest)

    compile_path = root / "results/compile/esp32c3_A.json"
    compile_record = read_json(compile_path)
    compile_record["portable_artifacts"]["strict_export_report"]["sha256"] = _sha(strict_path)
    compile_record["portable_artifacts"]["strict_export_manifest"]["sha256"] = _sha(
        strict_manifest_path
    )
    write_json(compile_path, compile_record)

    report_path = root / "results/final_report.json"
    report = read_json(report_path)
    report["input_evidence"]["esp32c3_student_A"]["compile_sha256"] = _sha(compile_path)
    write_json(report_path, report)
    _update_catalog_anchor(catalog_path, report_path)


def _reseal_compile_record(root: Path, catalog_path: Path) -> None:
    strict_manifest_path = root / "results/compile/esp32c3_A_artifacts/strict_export_manifest.json"
    compile_path = root / "results/compile/esp32c3_A.json"
    compile_record = read_json(compile_path)
    compile_record["portable_artifacts"]["strict_export_manifest"]["sha256"] = _sha(
        strict_manifest_path
    )
    write_json(compile_path, compile_record)
    report_path = root / "results/final_report.json"
    report = read_json(report_path)
    report["input_evidence"]["esp32c3_student_A"]["compile_sha256"] = _sha(compile_path)
    write_json(report_path, report)
    _update_catalog_anchor(catalog_path, report_path)
