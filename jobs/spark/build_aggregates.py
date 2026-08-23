"""Exact integer Spark aggregates for finalized telemetry and retention."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812


def _count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).cast("long")


def build_operational_metrics(gold: DataFrame) -> DataFrame:
    """Aggregate exact operational counts and sums per hardware lineage."""
    return gold.groupBy("replay_run_id", "lineage_id", "board", "student_model").agg(
        F.lit("hardware_execution").alias("statistical_unit"),
        F.count(F.lit(1)).cast("long").alias("hardware_execution_count"),
        _count_if(F.col("status") == "OK").alias("successful_execution_count"),
        _count_if(F.col("is_predicted_attack")).alias("predicted_attack_execution_count"),
        _count_if(F.col("attempts") > 1).alias("retry_event_count"),
        F.sum(F.greatest(F.col("attempts") - 1, F.lit(0))).cast("long").alias("retry_count"),
        F.sum("response_timeout_count").cast("long").alias("response_timeout_count_sum"),
        F.sum("ignored_datagram_count").cast("long").alias("ignored_datagram_count_sum"),
        F.sum("preprocess_us").cast("long").alias("preprocess_us_sum"),
        F.sum("inference_us").cast("long").alias("inference_us_sum"),
        F.sum("total_us").cast("long").alias("total_us_sum"),
        F.sum("request_datagram_bytes_sent").cast("long").alias("request_datagram_bytes_sent_sum"),
        F.sum("response_bytes").cast("long").alias("response_bytes_sum"),
        F.sum("host_observed_datagram_rtt_us")
        .cast("long")
        .alias("host_observed_datagram_rtt_us_sum"),
        F.sum("transaction_elapsed_us").cast("long").alias("transaction_elapsed_us_sum"),
    )


def build_predictive_row_metrics(gold: DataFrame) -> DataFrame:
    """Keep unique predictive source rows separate from hardware executions."""
    return gold.groupBy("replay_run_id").agg(
        F.lit("predictive_source_row").alias("statistical_unit"),
        F.count(F.lit(1)).cast("long").alias("hardware_execution_count"),
        F.countDistinct("lineage_id").cast("long").alias("execution_lineage_count"),
        F.countDistinct("source_row_index").cast("long").alias("unique_predictive_row_count"),
    )


def build_retention_metrics(gold: DataFrame, decisions: DataFrame) -> DataFrame:
    """Compute exact offline retention outcomes without feeding labels to policy."""
    joined = decisions.join(
        gold.select(
            "replay_run_id",
            "source_event_id",
            "is_predicted_attack",
            "true_class_id",
            "normal_class_id",
        ),
        ["replay_run_id", "source_event_id"],
        "inner",
    )
    actual_attack = F.col("true_class_id") != F.col("normal_class_id")
    return joined.groupBy("replay_run_id", "policy_id").agg(
        F.lit("hardware_execution").alias("statistical_unit"),
        F.count(F.lit(1)).cast("long").alias("hardware_execution_count"),
        _count_if(F.col("retained")).alias("retained_hardware_execution_count"),
        _count_if(F.col("is_predicted_attack")).alias("predicted_attack_execution_count"),
        _count_if(F.col("retained") & F.col("is_predicted_attack")).alias(
            "retained_predicted_attack_execution_count"
        ),
        _count_if(actual_attack).alias("actual_attack_execution_count"),
        _count_if(F.col("retained") & actual_attack).alias(
            "retained_actual_attack_execution_count"
        ),
        F.sum("inclusion_probability_numerator")
        .cast("long")
        .alias("inclusion_probability_numerator_sum"),
        F.sum("inclusion_probability_denominator")
        .cast("long")
        .alias("inclusion_probability_denominator_sum"),
    )


def build_retention_reason_counts(decisions: DataFrame) -> DataFrame:
    """Count exact decision rows by immutable policy and reason."""
    return decisions.groupBy("replay_run_id", "policy_id", "reason", "retained").agg(
        F.lit("hardware_execution").alias("statistical_unit"),
        F.count(F.lit(1)).cast("long").alias("decision_count"),
    )
