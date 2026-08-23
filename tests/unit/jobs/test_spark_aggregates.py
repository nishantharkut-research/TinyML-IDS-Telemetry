"""Exact Spark aggregate contracts and statistical units."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

pytest.importorskip(
    "pyspark",
    reason="Spark job tests require the optional .[spark] dependencies",
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from jobs.spark.build_aggregates import (
    build_operational_metrics,
    build_predictive_row_metrics,
    build_retention_metrics,
    build_retention_reason_counts,
)
from jobs.spark.policies import build_policy_decisions
from jobs.spark.stream_telemetry import bronze_from_kafka, split_silver_and_quarantine


def _gold(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
):
    attack = event_factory(
        source_event_id="1" * 64,
        predicted_class_id=0,
        true_class_id=0,
        fixed_logits=(5000, 1, 2, 3, 4),
        attempts=2,
        response_timeout_count=1,
        ignored_datagram_count=2,
        row_id=0,
        source_row_index=7,
    )
    normal = event_factory(
        source_event_id="2" * 64,
        predicted_class_id=3,
        true_class_id=0,
        fixed_logits=(0, 1, 2, 5000, 3),
        row_id=1,
        source_row_index=8,
    )
    bronze = bronze_from_kafka(
        kafka_frame_factory(
            spark,
            [envelope_factory(attack), envelope_factory(normal)],
        )
    )
    silver, quarantine = split_silver_and_quarantine(bronze)
    assert quarantine.count() == 0
    return silver


def test_operational_aggregates_use_exact_counts_and_integer_sums(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    gold = _gold(spark, event_factory, envelope_factory, kafka_frame_factory)

    row = build_operational_metrics(gold).collect()[0].asDict()

    assert row == {
        "replay_run_id": "run-1",
        "lineage_id": "a" * 64,
        "board": "esp32c3",
        "student_model": "student_A",
        "statistical_unit": "hardware_execution",
        "hardware_execution_count": 2,
        "successful_execution_count": 2,
        "predicted_attack_execution_count": 1,
        "retry_event_count": 1,
        "retry_count": 1,
        "response_timeout_count_sum": 1,
        "ignored_datagram_count_sum": 2,
        "preprocess_us_sum": 38,
        "inference_us_sum": 286,
        "total_us_sum": 324,
        "request_datagram_bytes_sent_sum": 476,
        "response_bytes_sum": 392,
        "host_observed_datagram_rtt_us_sum": 19160,
        "transaction_elapsed_us_sum": 19160,
    }
    assert not any("mean" in name or "percentile" in name for name in row)


def test_predictive_rows_are_not_conflated_with_hardware_executions(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    lineage_a = _gold(spark, event_factory, envelope_factory, kafka_frame_factory)
    lineage_b = lineage_a.withColumn("lineage_id", F.lit("b" * 64))
    duplicated_execution = lineage_a.unionByName(lineage_b)

    row = build_predictive_row_metrics(duplicated_execution).collect()[0]

    assert row.statistical_unit == "predictive_source_row"
    assert row.hardware_execution_count == 4
    assert row.unique_predictive_row_count == 2


def test_retention_aggregates_are_exact_and_offline_labels_do_not_drive_policy(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    gold = _gold(spark, event_factory, envelope_factory, kafka_frame_factory)
    decisions = build_policy_decisions(gold)

    metrics = {
        row.policy_id: row.asDict() for row in build_retention_metrics(gold, decisions).collect()
    }
    reasons = {
        (row.policy_id, row.reason): row.decision_count
        for row in build_retention_reason_counts(decisions).collect()
    }

    assert metrics["retain-all-v1"]["retained_hardware_execution_count"] == 2
    assert metrics["security-aware-static-v1"]["predicted_attack_execution_count"] == 1
    assert metrics["security-aware-static-v1"]["retained_predicted_attack_execution_count"] == 1
    assert metrics["security-aware-static-v1"]["actual_attack_execution_count"] == 2
    assert metrics["security-aware-static-v1"]["retained_actual_attack_execution_count"] == 1
    assert reasons[("security-aware-static-v1", "predicted_attack")] == 1
    assert reasons[("security-aware-static-v1", "normal_not_sampled")] == 1


def test_checked_in_sql_uses_exact_counts_and_sums_without_invented_percentiles(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    gold = _gold(spark, event_factory, envelope_factory, kafka_frame_factory)
    decisions = build_policy_decisions(gold)
    gold.createOrReplaceTempView("finalized_gold")
    decisions.createOrReplaceTempView("policy_decisions")
    root = Path(__file__).parents[3]
    operational_sql = (root / "queries/operational_metrics.sql").read_text(encoding="utf-8")
    retention_sql = (root / "queries/retention_metrics.sql").read_text(encoding="utf-8")

    operational = spark.sql(operational_sql).collect()[0]
    retention = {row.policy_id: row for row in spark.sql(retention_sql).collect()}

    assert operational.hardware_execution_count == 2
    assert operational.total_us_sum == 324
    assert retention["security-aware-static-v1"].retained_predicted_attack_execution_count == 1
    combined = f"{operational_sql}\n{retention_sql}".casefold()
    assert "percentile" not in combined
    assert "avg(" not in combined
