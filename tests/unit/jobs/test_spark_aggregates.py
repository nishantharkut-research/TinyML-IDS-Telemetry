"""Exact Spark aggregate contracts and statistical units."""

from __future__ import annotations

import json
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip(
    "pyspark",
    reason="Spark job tests require the optional .[spark] dependencies",
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from jobs.spark import build_aggregates as aggregate_module
from jobs.spark import finalize_run as finalization_module
from jobs.spark.build_aggregates import (
    build_operational_metrics,
    build_predictive_row_metrics,
    build_retention_metrics,
    build_retention_reason_counts,
)
from jobs.spark.common import EVENT_SCHEMA
from jobs.spark.policies import build_policy_decisions
from jobs.spark.stream_telemetry import bronze_from_kafka, split_silver_and_quarantine


def _gold(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
):
    attack = replace(
        event_factory(
            source_event_id="1" * 64,
            predicted_class_id=0,
            true_class_id=0,
            fixed_logits=(5000, 1, 2, 3, 4),
            attempts=2,
            response_timeout_count=1,
            ignored_datagram_count=2,
            row_id=0,
            source_row_index=7,
        ),
        ignored_wrong_endpoint_count=1,
        ignored_bad_length_count=2,
        ignored_non_ascii_count=3,
        ignored_malformed_count=4,
        ignored_bad_prefix_count=5,
        ignored_bad_identity_count=6,
        ignored_bad_attempt_count=7,
        ignored_bad_inner_hex_count=8,
        ignored_bad_crc_count=9,
        ignored_bad_inner_text_count=10,
        ignored_wrong_session_count=11,
        ignored_wrong_stage_count=12,
        ignored_wrong_transaction_count=13,
        ignored_wrong_attempt_count=14,
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
        "ignored_wrong_endpoint_count_sum": 1,
        "ignored_bad_length_count_sum": 2,
        "ignored_non_ascii_count_sum": 3,
        "ignored_malformed_count_sum": 4,
        "ignored_bad_prefix_count_sum": 5,
        "ignored_bad_identity_count_sum": 6,
        "ignored_bad_attempt_count_sum": 7,
        "ignored_bad_inner_hex_count_sum": 8,
        "ignored_bad_crc_count_sum": 9,
        "ignored_bad_inner_text_count_sum": 10,
        "ignored_wrong_session_count_sum": 11,
        "ignored_wrong_stage_count_sum": 12,
        "ignored_wrong_transaction_count_sum": 13,
        "ignored_wrong_attempt_count_sum": 14,
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

    rows = {
        row.metric: row.asDict()
        for row in build_predictive_row_metrics(duplicated_execution).collect()
    }

    assert rows == {
        "execution_lineage_count": {
            "replay_run_id": "run-1",
            "metric": "execution_lineage_count",
            "statistical_unit": "execution_lineage",
            "value": 2,
        },
        "hardware_execution_count": {
            "replay_run_id": "run-1",
            "metric": "hardware_execution_count",
            "statistical_unit": "hardware_execution",
            "value": 4,
        },
        "unique_predictive_row_count": {
            "replay_run_id": "run-1",
            "metric": "unique_predictive_row_count",
            "statistical_unit": "predictive_source_row",
            "value": 2,
        },
    }


def test_execution_dimension_counts_use_one_typed_dataset_with_exact_shapes(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    gold = _gold(spark, event_factory, envelope_factory, kafka_frame_factory)

    result = aggregate_module.build_execution_dimension_counts(gold)
    rows = sorted(
        (row.asDict() for row in result.collect()),
        key=lambda row: (str(row["dimension_kind"]), int(row["class_id"] or -1)),
    )

    assert result.schema.simpleString() == (
        "struct<replay_run_id:string,dimension_kind:string,board:string,student_model:string,"
        "class_id:int,class_name:string,statistical_unit:string,hardware_execution_count:bigint>"
    )
    assert rows == [
        {
            "replay_run_id": "run-1",
            "dimension_kind": "board",
            "board": "esp32c3",
            "student_model": None,
            "class_id": None,
            "class_name": None,
            "statistical_unit": "hardware_execution",
            "hardware_execution_count": 2,
        },
        {
            "replay_run_id": "run-1",
            "dimension_kind": "board_student_model",
            "board": "esp32c3",
            "student_model": "student_A",
            "class_id": None,
            "class_name": None,
            "statistical_unit": "hardware_execution",
            "hardware_execution_count": 2,
        },
        {
            "replay_run_id": "run-1",
            "dimension_kind": "predicted_class",
            "board": None,
            "student_model": None,
            "class_id": 0,
            "class_name": "Blackhole",
            "statistical_unit": "hardware_execution",
            "hardware_execution_count": 1,
        },
        {
            "replay_run_id": "run-1",
            "dimension_kind": "predicted_class",
            "board": None,
            "student_model": None,
            "class_id": 3,
            "class_name": "Normal",
            "statistical_unit": "hardware_execution",
            "hardware_execution_count": 1,
        },
        {
            "replay_run_id": "run-1",
            "dimension_kind": "student_model",
            "board": None,
            "student_model": "student_A",
            "class_id": None,
            "class_name": None,
            "statistical_unit": "hardware_execution",
            "hardware_execution_count": 2,
        },
        {
            "replay_run_id": "run-1",
            "dimension_kind": "true_class",
            "board": None,
            "student_model": None,
            "class_id": 0,
            "class_name": "Blackhole",
            "statistical_unit": "hardware_execution",
            "hardware_execution_count": 2,
        },
    ]


def test_finalized_outputs_include_execution_dimension_counts(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    gold = _gold(spark, event_factory, envelope_factory, kafka_frame_factory)

    outputs = finalization_module._build_finalized_outputs(gold)

    assert "execution_dimension_counts" in outputs
    assert outputs["execution_dimension_counts"].count() == 6


def test_finalized_aggregate_bundle_is_canonical_and_excludes_row_level_datasets(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    gold = _gold(spark, event_factory, envelope_factory, kafka_frame_factory)
    outputs = finalization_module._build_finalized_outputs(gold)

    payload = finalization_module._spark_aggregates_payload(outputs)
    parsed = json.loads(payload)

    assert payload.endswith(b"\n")
    assert payload == (json.dumps(parsed, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "ascii"
    )
    assert set(parsed) == {
        "execution_dimension_counts",
        "operational_metrics",
        "predictive_row_metrics",
        "retention_metrics",
        "retention_reason_counts",
    }


def test_canonical_event_identity_is_order_independent_and_payload_sensitive(
    spark: SparkSession,
    event_factory: Any,
    envelope_factory: Any,
    kafka_frame_factory: Any,
) -> None:
    gold = _gold(spark, event_factory, envelope_factory, kafka_frame_factory)
    reordered = gold.orderBy(F.col("source_event_id").desc())
    mutated = gold.withColumn("total_us", F.col("total_us") + F.lit(1))

    identity = finalization_module._canonical_event_identity_sha256(gold)

    assert identity == finalization_module._canonical_event_identity_sha256(reordered)
    assert identity != finalization_module._canonical_event_identity_sha256(mutated)


def test_canonical_event_identity_matches_original_event_serialization(
    spark: SparkSession,
    event_factory: Any,
) -> None:
    event = event_factory(
        source_event_id="3" * 64,
        lineage_id="4" * 64,
        row_id=9,
        source_row_index=17,
        fixed_logits=(1, 2, 3, 5001, 4),
    ).to_dict()
    canonical_event = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("ascii")
    payload_sha256 = sha256(canonical_event).hexdigest()
    expected = sha256(
        (
            f"{event['source_event_id']}|{event['lineage_id']}|{event['row_id']}|"
            f"{event['source_row_index']}|{payload_sha256}\n"
        ).encode("ascii")
    ).hexdigest()
    gold = spark.createDataFrame([event], schema=EVENT_SCHEMA)

    assert finalization_module._canonical_event_identity_sha256(gold) == expected


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
    assert operational.ignored_wrong_endpoint_count_sum == 1
    assert operational.ignored_wrong_attempt_count_sum == 14
    assert retention["security-aware-static-v1"].retained_predicted_attack_execution_count == 1
    combined = f"{operational_sql}\n{retention_sql}".casefold()
    assert "percentile" not in combined
    assert "avg(" not in combined
