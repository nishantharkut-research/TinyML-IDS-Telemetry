"""Evidence audit, import, and snapshot commands."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from tinyml_ids_telemetry.evidence.audit import audit_catalog
from tinyml_ids_telemetry.evidence.bundle import create_evidence_bundle
from tinyml_ids_telemetry.evidence.catalog import load_catalog
from tinyml_ids_telemetry.evidence.snapshot import create_snapshot


def configure_evidence_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("evidence", help="Audit and import source evidence")
    commands = parser.add_subparsers(dest="evidence_command", required=True)
    for command in ("audit", "bundle", "snapshot"):
        command_parser = commands.add_parser(command)
        command_parser.add_argument(
            "--catalog",
            type=Path,
            default=Path("configs/evidence-catalog.toml"),
        )
        command_parser.add_argument(
            "--evidence-root",
            type=Path,
            help="Source repository or imported evidence bundle root",
        )
        if command in {"bundle", "snapshot"}:
            command_parser.add_argument("--output", type=Path, required=True)
    parser.set_defaults(command_handler=run_evidence_command)


def run_evidence_command(arguments: argparse.Namespace) -> int:
    catalog = load_catalog(arguments.catalog, evidence_root=arguments.evidence_root)
    if arguments.evidence_command == "audit":
        audit = audit_catalog(catalog)
        payload: dict[str, object] = {
            "status": "passed",
            "sealed_report_sha256": audit.sealed_report_sha256,
            "total_expected_events": audit.total_expected_events,
            "lineage_count": len(audit.lineages),
            "lineages": [lineage.to_dict() for lineage in audit.lineages],
        }
    elif arguments.evidence_command == "bundle":
        payload = create_evidence_bundle(catalog, arguments.output).to_dict()
    elif arguments.evidence_command == "snapshot":
        payload = create_snapshot(catalog, arguments.output).to_dict()
    else:
        raise AssertionError(f"Unsupported evidence command: {arguments.evidence_command}")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0
