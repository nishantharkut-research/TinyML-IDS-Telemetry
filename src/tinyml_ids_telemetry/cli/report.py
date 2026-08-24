"""Publish course reports from finalized Spark and Hadoop evidence."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from dashboard.data import load_finalized_run
from tinyml_ids_telemetry.quality.report import build_course_report, publish_course_report


class ReportCommandError(ValueError):
    """Course report input cannot be decoded as a JSON object."""


def configure_report_parser(subparsers: Any) -> None:
    """Register the finalized course-report command."""
    parser = subparsers.add_parser("report", help="Publish a finalized course report")
    parser.add_argument("--finalized-run", type=Path, required=True)
    parser.add_argument("--reconciliation", type=Path, required=True)
    parser.add_argument("--pipeline-execution", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.set_defaults(command_handler=run_report_command)


def _load_json_object(path: Path, label: str) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReportCommandError(f"{label} must contain valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise ReportCommandError(f"{label} must be a JSON object")
    return cast(Mapping[str, object], value)


def run_report_command(arguments: argparse.Namespace) -> int:
    """Build and atomically publish JSON and Markdown reports."""
    run = load_finalized_run(arguments.finalized_run)
    reconciliation = _load_json_object(arguments.reconciliation, "reconciliation")
    pipeline_execution = _load_json_object(
        arguments.pipeline_execution,
        "pipeline execution",
    )
    report = build_course_report(run, reconciliation, pipeline_execution)
    paths = publish_course_report(arguments.output, report)
    print(
        json.dumps(
            {
                "json_path": str(paths.json_path),
                "markdown_path": str(paths.markdown_path),
                "replay_run_id": run.replay_run_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0
