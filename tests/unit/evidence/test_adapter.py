from dataclasses import replace
from pathlib import Path

import pytest

from tests.helpers.evidence_factory import create_evidence_tree, sha256_file
from tinyml_ids_telemetry.evidence.adapter import iter_lineage_events
from tinyml_ids_telemetry.evidence.audit import audit_catalog
from tinyml_ids_telemetry.evidence.catalog import EvidenceContractError, load_catalog


def audited_fixture(tmp_path: Path):  # type: ignore[no-untyped-def]
    root, catalog_path = create_evidence_tree(tmp_path)
    catalog = load_catalog(catalog_path, evidence_root=root)
    lineage = audit_catalog(catalog).lineages[0]
    return root, lineage


def test_adapter_yields_complete_ordered_events(tmp_path: Path) -> None:
    root, lineage = audited_fixture(tmp_path)

    events = list(iter_lineage_events(root, lineage))

    assert [event.row_id for event in events] == [0, 1]
    assert [event.source_row_index for event in events] == [8, 10]
    assert all(event.lineage_id == lineage.lineage_id for event in events)
    assert events[0].fixed_logits == (-5297, -5038, -1654, 4441, 1112)
    assert events[1].attempts == 2
    assert events[1].response_timeout_count == 1


def test_adapter_produces_deterministic_source_identifiers(tmp_path: Path) -> None:
    root, lineage = audited_fixture(tmp_path)

    first = [event.source_event_id for event in iter_lineage_events(root, lineage)]
    second = [event.source_event_id for event in iter_lineage_events(root, lineage)]

    assert first == second
    assert len(set(first)) == lineage.expected_rows


def test_adapter_rejects_source_hash_changed_after_audit(tmp_path: Path) -> None:
    root, lineage = audited_fixture(tmp_path)
    mcu_path = root / lineage.mcu_relative_path
    mcu_path.write_text(mcu_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(EvidenceContractError, match="mcu_csv"):
        list(iter_lineage_events(root, lineage))


def test_adapter_rejects_row_gap(tmp_path: Path) -> None:
    root, lineage = audited_fixture(tmp_path)
    mcu_path = root / lineage.mcu_relative_path
    reference_path = root / lineage.reference_relative_path
    _replace_line_start(mcu_path, "1,OK", "2,OK")
    _replace_line_start(reference_path, "1,10", "2,10")
    lineage = replace(
        lineage,
        mcu_csv_sha256=sha256_file(mcu_path),
        reference_csv_sha256=sha256_file(reference_path),
    )

    with pytest.raises(EvidenceContractError, match="contiguous"):
        list(iter_lineage_events(root, lineage))


def test_adapter_rejects_prediction_mismatch(tmp_path: Path) -> None:
    root, lineage = audited_fixture(tmp_path)
    mcu_path = root / lineage.mcu_relative_path
    _replace_line_start(mcu_path, "0,OK,3,", "0,OK,2,")
    lineage = replace(lineage, mcu_csv_sha256=sha256_file(mcu_path))

    with pytest.raises(EvidenceContractError, match="prediction"):
        list(iter_lineage_events(root, lineage))


def test_adapter_rejects_logit_mismatch(tmp_path: Path) -> None:
    root, lineage = audited_fixture(tmp_path)
    mcu_path = root / lineage.mcu_relative_path
    _replace_line_start(mcu_path, "0,OK,3,-5297", "0,OK,3,-5296")
    lineage = replace(lineage, mcu_csv_sha256=sha256_file(mcu_path))

    with pytest.raises(EvidenceContractError, match="logit"):
        list(iter_lineage_events(root, lineage))


def test_adapter_rejects_mismatched_row_counts(tmp_path: Path) -> None:
    root, lineage = audited_fixture(tmp_path)
    reference_path = root / lineage.reference_relative_path
    lines = reference_path.read_text(encoding="utf-8").splitlines()
    reference_path.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
    lineage = replace(lineage, reference_csv_sha256=sha256_file(reference_path))

    with pytest.raises(EvidenceContractError, match="row count"):
        list(iter_lineage_events(root, lineage))


def _replace_line_start(path: Path, old: str, new: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    replaced = [new + line[len(old) :] if line.startswith(old) else line for line in lines]
    path.write_text("\n".join(replaced) + "\n", encoding="utf-8")
