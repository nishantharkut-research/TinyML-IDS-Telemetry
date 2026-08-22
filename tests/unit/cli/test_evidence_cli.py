from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.evidence_factory import create_evidence_tree
from tinyml_ids_telemetry.cli.main import main


def test_audit_command_emits_machine_readable_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)

    exit_code = main(
        [
            "evidence",
            "audit",
            "--catalog",
            str(catalog_path),
            "--evidence-root",
            str(source_root),
        ]
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "passed"
    assert output["total_expected_events"] == 2
    assert output["lineage_count"] == 1


def test_bundle_and_snapshot_commands_make_source_independent_copy(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_root, catalog_path = create_evidence_tree(tmp_path)
    bundle = tmp_path / "bundle"
    snapshot = tmp_path / "snapshot"

    bundle_exit = main(
        [
            "evidence",
            "bundle",
            "--catalog",
            str(catalog_path),
            "--evidence-root",
            str(source_root),
            "--output",
            str(bundle),
        ]
    )
    bundle_output = json.loads(capsys.readouterr().out)
    snapshot_exit = main(
        [
            "evidence",
            "snapshot",
            "--catalog",
            str(catalog_path),
            "--evidence-root",
            str(bundle),
            "--output",
            str(snapshot),
        ]
    )
    snapshot_output = json.loads(capsys.readouterr().out)

    assert bundle_exit == 0
    assert bundle_output["schema_version"] == "evidence-bundle-v1"
    assert snapshot_exit == 0
    assert snapshot_output["schema_version"] == "telemetry-snapshot-v1"
    assert snapshot_output["event_count"] == 2


def test_command_reports_contract_failure_without_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _, catalog_path = create_evidence_tree(tmp_path)

    exit_code = main(["evidence", "audit", "--catalog", str(catalog_path)])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert "CUKD_XAI_ROOT" in captured.err
    assert "Traceback" not in captured.err
