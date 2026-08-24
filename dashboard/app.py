"""Read-only Streamlit presentation for finalized telemetry aggregates."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from dashboard.data import (
    CourseReportSummary,
    FinalizedRun,
    FinalizedRunError,
    load_course_report,
    load_finalized_run,
)


@dataclass(frozen=True)
class DashboardViews:
    """Compact, read-only tables derived from finalized aggregate rows."""

    overview: Mapping[str, object]
    quality: Mapping[str, object]
    operations: tuple[Mapping[str, object], ...]
    predicted_classes: tuple[Mapping[str, object], ...]
    retention: tuple[Mapping[str, object], ...]
    retention_reasons: tuple[Mapping[str, object], ...]
    reconciliation: Mapping[str, object]


def _integer(row: Mapping[str, object], field: str) -> int:
    value = row.get(field)
    if type(value) is not int or value < 0:
        raise FinalizedRunError(f"aggregate field {field} must be a nonnegative integer")
    return value


def _text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise FinalizedRunError(f"aggregate field {field} must be a nonempty string")
    return value


def _metric_values(run: FinalizedRun) -> dict[str, int]:
    values: dict[str, int] = {}
    for row in run.aggregates["predictive_row_metrics"]:
        metric = _text(row, "metric")
        if metric in values:
            raise FinalizedRunError(f"duplicate predictive metric: {metric}")
        values[metric] = _integer(row, "value")
    required = {"execution_lineage_count", "unique_predictive_row_count"}
    if not required.issubset(values):
        raise FinalizedRunError("finalized aggregates are missing predictive row metrics")
    return values


def _operation_rows(run: FinalizedRun) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for row in run.aggregates["operational_metrics"]:
        executions = _integer(row, "hardware_execution_count")
        if executions == 0:
            raise FinalizedRunError("operational metric cannot have zero hardware executions")
        rows.append(
            {
                "board": _text(row, "board"),
                "student_model": _text(row, "student_model"),
                "hardware_execution_count": executions,
                "successful_execution_count": _integer(row, "successful_execution_count"),
                "predicted_attack_execution_count": _integer(
                    row, "predicted_attack_execution_count"
                ),
                "retry_event_count": _integer(row, "retry_event_count"),
                "retry_count": _integer(row, "retry_count"),
                "response_timeout_count": _integer(row, "response_timeout_count_sum"),
                "mean_preprocess_us": _integer(row, "preprocess_us_sum") / executions,
                "mean_inference_us": _integer(row, "inference_us_sum") / executions,
                "mean_total_us": _integer(row, "total_us_sum") / executions,
                "mean_wifi_rtt_us": (
                    _integer(row, "host_observed_datagram_rtt_us_sum") / executions
                ),
                "mean_transaction_us": _integer(row, "transaction_elapsed_us_sum") / executions,
            }
        )
    rows.sort(key=lambda item: (str(item["board"]), str(item["student_model"])))
    if sum(cast(int, row["hardware_execution_count"]) for row in rows) != run.source_event_count:
        raise FinalizedRunError("operational execution counts do not match the finalized run")
    return tuple(rows)


def _predicted_class_rows(run: FinalizedRun) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for row in run.aggregates["execution_dimension_counts"]:
        if row.get("dimension_kind") != "predicted_class":
            continue
        class_id = _integer(row, "class_id")
        rows.append(
            {
                "class_id": class_id,
                "class_name": _text(row, "class_name"),
                "hardware_execution_count": _integer(row, "hardware_execution_count"),
            }
        )
    rows.sort(key=lambda item: cast(int, item["class_id"]))
    if sum(cast(int, row["hardware_execution_count"]) for row in rows) != run.source_event_count:
        raise FinalizedRunError("predicted class counts do not match the finalized run")
    return tuple(rows)


def _retention_rows(run: FinalizedRun) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for row in run.aggregates["retention_metrics"]:
        executions = _integer(row, "hardware_execution_count")
        retained = _integer(row, "retained_hardware_execution_count")
        actual_attacks = _integer(row, "actual_attack_execution_count")
        retained_attacks = _integer(row, "retained_actual_attack_execution_count")
        if executions == 0 or retained > executions or retained_attacks > actual_attacks:
            raise FinalizedRunError("retention aggregate counts are inconsistent")
        rows.append(
            {
                "policy_id": _text(row, "policy_id"),
                "hardware_execution_count": executions,
                "retained_hardware_execution_count": retained,
                "retention_rate": retained / executions,
                "actual_attack_execution_count": actual_attacks,
                "retained_actual_attack_execution_count": retained_attacks,
                "actual_attack_retention_rate": (
                    retained_attacks / actual_attacks if actual_attacks else None
                ),
            }
        )
    rows.sort(key=lambda item: str(item["policy_id"]))
    return tuple(rows)


def _retention_reason_rows(run: FinalizedRun) -> tuple[Mapping[str, object], ...]:
    rows = tuple(dict(row) for row in run.aggregates["retention_reason_counts"])
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                str(row.get("policy_id", "")),
                str(row.get("reason", "")),
                str(row.get("retained", "")),
            ),
        )
    )


def build_dashboard_views(
    run: FinalizedRun,
    report: CourseReportSummary | None = None,
) -> DashboardViews:
    """Derive dashboard tables without reading raw telemetry or mutable outputs."""
    metrics = _metric_values(run)
    operations = _operation_rows(run)
    predicted_classes = _predicted_class_rows(run)
    hardware = sum(cast(int, row["hardware_execution_count"]) for row in operations)
    successful = sum(cast(int, row["successful_execution_count"]) for row in operations)
    if successful > hardware:
        raise FinalizedRunError("successful execution count exceeds hardware execution count")
    reconciliation: Mapping[str, object] = {
        "status": "not_loaded",
        "matched": False,
    }
    if report is not None:
        if report.replay_run_id != run.replay_run_id:
            raise FinalizedRunError("course report belongs to another finalized run")
        reconciliation = {"status": "passed", "matched": report.reconciliation_matched}

    return DashboardViews(
        overview={
            "replay_run_id": run.replay_run_id,
            "hardware_execution_count": run.source_event_count,
            "replay_delivery_count": run.delivery_count,
            "duplicate_delivery_count": run.delivery_count - run.source_event_count,
            "execution_lineage_count": metrics["execution_lineage_count"],
            "unique_predictive_row_count": metrics["unique_predictive_row_count"],
        },
        quality={
            "successful_execution_count": successful,
            "failed_execution_count": hardware - successful,
            "retry_event_count": sum(cast(int, row["retry_event_count"]) for row in operations),
            "retry_count": sum(cast(int, row["retry_count"]) for row in operations),
            "response_timeout_count": sum(
                cast(int, row["response_timeout_count"]) for row in operations
            ),
        },
        operations=operations,
        predicted_classes=predicted_classes,
        retention=_retention_rows(run),
        retention_reasons=_retention_reason_rows(run),
        reconciliation=reconciliation,
    )


def main() -> None:
    """Run the Streamlit application."""
    import streamlit as st  # type: ignore[import-not-found]

    st.set_page_config(page_title="TinyML IDS telemetry", layout="wide")
    st.title("TinyML IDS telemetry")

    default_run = os.environ.get("TINYML_IDS_FINALIZED_RUN", "")
    default_report = os.environ.get("TINYML_IDS_COURSE_REPORT", "")
    run_path = st.sidebar.text_input("Finalized run directory", value=default_run)
    report_path = st.sidebar.text_input("Course report JSON", value=default_report)
    if not run_path.strip():
        st.info("Select a finalized replay run.")
        return

    try:
        run = load_finalized_run(Path(run_path))
        report = load_course_report(Path(report_path), run) if report_path.strip() else None
        views = build_dashboard_views(run, report)
    except (FinalizedRunError, OSError) as error:
        st.error(str(error))
        return

    overview = views.overview
    columns = st.columns(5)
    columns[0].metric("Hardware executions", f"{overview['hardware_execution_count']:,}")
    columns[1].metric("Replay deliveries", f"{overview['replay_delivery_count']:,}")
    columns[2].metric("Duplicate deliveries", f"{overview['duplicate_delivery_count']:,}")
    columns[3].metric("Execution lineages", f"{overview['execution_lineage_count']:,}")
    columns[4].metric("Predictive rows", f"{overview['unique_predictive_row_count']:,}")

    operations_tab, classes_tab, retention_tab, verification_tab = st.tabs(
        ["Operations", "Classes", "Retention", "Verification"]
    )
    with operations_tab:
        quality_columns = st.columns(4)
        quality_columns[0].metric(
            "Successful executions", f"{views.quality['successful_execution_count']:,}"
        )
        quality_columns[1].metric(
            "Failed executions", f"{views.quality['failed_execution_count']:,}"
        )
        quality_columns[2].metric("Retries", f"{views.quality['retry_count']:,}")
        quality_columns[3].metric(
            "Response timeouts", f"{views.quality['response_timeout_count']:,}"
        )
        st.dataframe(list(views.operations), hide_index=True, use_container_width=True)
        st.caption("Timing columns are arithmetic means over finalized hardware executions.")
    with classes_tab:
        st.dataframe(list(views.predicted_classes), hide_index=True, use_container_width=True)
    with retention_tab:
        st.dataframe(list(views.retention), hide_index=True, use_container_width=True)
        st.dataframe(list(views.retention_reasons), hide_index=True, use_container_width=True)
    with verification_tab:
        status = cast(str, views.reconciliation["status"])
        if status == "passed":
            st.success("Spark and Hadoop exact reconciliation passed.")
        else:
            st.warning("No matching reconciliation report is loaded.")
        st.caption(
            "Replay timestamps and throughput belong to the analytics experiment, not the device."
        )


if __name__ == "__main__":
    main()
