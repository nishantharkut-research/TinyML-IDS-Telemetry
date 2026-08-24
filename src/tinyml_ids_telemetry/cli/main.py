"""Top-level command-line dispatcher."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence

from dashboard.data import FinalizedRunError
from tinyml_ids_telemetry.cli.evidence import configure_evidence_parser
from tinyml_ids_telemetry.cli.replay import (
    ReplayCommandError,
    ReplayCommandErrorGroup,
    configure_replay_parser,
)
from tinyml_ids_telemetry.cli.report import ReportCommandError, configure_report_parser
from tinyml_ids_telemetry.evidence.catalog import EvidenceContractError
from tinyml_ids_telemetry.quality.report import CourseReportError


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tinyml-ids-telemetry")
    subparsers = parser.add_subparsers(dest="command", required=True)
    configure_evidence_parser(subparsers)
    configure_replay_parser(subparsers)
    configure_report_parser(subparsers)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one project command and return a process exit code."""
    arguments = _parser().parse_args(argv)
    handler: Callable[[argparse.Namespace], int] = arguments.command_handler
    try:
        return handler(arguments)
    except (
        CourseReportError,
        EvidenceContractError,
        FinalizedRunError,
        ReplayCommandError,
        ReplayCommandErrorGroup,
        ReportCommandError,
        OSError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
