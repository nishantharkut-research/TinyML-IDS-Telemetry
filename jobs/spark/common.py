"""Shared Spark schemas and fail-closed job contracts."""

from __future__ import annotations

from typing import Final

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CONTROLLED_TOPIC_PARTITIONS: Final[int] = 1
SILVER_GUARANTEE: Final[str] = (
    "provisional watermark-bounded deduplication for a controlled one-partition replay"
)
QUARANTINE_REASON_PRECEDENCE: Final[tuple[str, ...]] = (
    "invalid_utf8",
    "malformed_json",
    "unsupported_schema_version",
    "missing_required_field",
    "unexpected_property",
    "type_mismatch",
    "invalid_kafka_contract",
    "invalid_delivery_identity",
    "invalid_event_value",
)


class SparkContractError(ValueError):
    """Raised before a Spark job starts with an invalid contract."""


EVENT_FIELDS: Final[tuple[StructField, ...]] = (
    StructField("schema_version", StringType(), True),
    StructField("source_event_id", StringType(), True),
    StructField("lineage_id", StringType(), True),
    StructField("source_session_id", StringType(), True),
    StructField("source_stage_id", StringType(), True),
    StructField("row_id", LongType(), True),
    StructField("source_row_index", LongType(), True),
    StructField("board", StringType(), True),
    StructField("student_model", StringType(), True),
    StructField("status", StringType(), True),
    StructField("true_class_id", IntegerType(), True),
    StructField("true_class_name", StringType(), True),
    StructField("predicted_class_id", IntegerType(), True),
    StructField("predicted_class_name", StringType(), True),
    StructField("fixed_predicted_class_id", IntegerType(), True),
    StructField("fp32_predicted_class_id", IntegerType(), True),
    StructField("normal_class_id", IntegerType(), True),
    StructField("is_predicted_attack", BooleanType(), True),
    StructField("fixed_logits", ArrayType(IntegerType(), False), True),
    StructField("logit_frac_bits", IntegerType(), True),
    StructField("normalized_logit_margin", DoubleType(), True),
    StructField("preprocess_us", LongType(), True),
    StructField("inference_us", LongType(), True),
    StructField("total_us", LongType(), True),
    StructField("attempts", IntegerType(), True),
    StructField("response_timeout_count", IntegerType(), True),
    StructField("ignored_datagram_count", IntegerType(), True),
    StructField("ignored_wrong_endpoint_count", IntegerType(), True),
    StructField("ignored_bad_length_count", IntegerType(), True),
    StructField("ignored_non_ascii_count", IntegerType(), True),
    StructField("ignored_malformed_count", IntegerType(), True),
    StructField("ignored_bad_prefix_count", IntegerType(), True),
    StructField("ignored_bad_identity_count", IntegerType(), True),
    StructField("ignored_bad_attempt_count", IntegerType(), True),
    StructField("ignored_bad_inner_hex_count", IntegerType(), True),
    StructField("ignored_bad_crc_count", IntegerType(), True),
    StructField("ignored_bad_inner_text_count", IntegerType(), True),
    StructField("ignored_wrong_session_count", IntegerType(), True),
    StructField("ignored_wrong_stage_count", IntegerType(), True),
    StructField("ignored_wrong_transaction_count", IntegerType(), True),
    StructField("ignored_wrong_attempt_count", IntegerType(), True),
    StructField("successful_request_bytes", LongType(), True),
    StructField("request_datagram_bytes_sent", LongType(), True),
    StructField("response_bytes", LongType(), True),
    StructField("host_observed_datagram_rtt_us", LongType(), True),
    StructField("transaction_elapsed_us", LongType(), True),
    StructField("mcu_fixed_prediction_match", BooleanType(), True),
    StructField("mcu_fixed_logits_match", BooleanType(), True),
)

EVENT_SCHEMA: Final[StructType] = StructType(list(EVENT_FIELDS))
ENVELOPE_SCHEMA: Final[StructType] = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("replay_run_id", StringType(), True),
        StructField("delivery_id", StringType(), True),
        StructField("source_offset", LongType(), True),
        StructField("emission_index", LongType(), True),
        StructField("duplicate_ordinal", IntegerType(), True),
        StructField("synthetic_event_time", StringType(), True),
        StructField("event", EVENT_SCHEMA, True),
    ]
)

ENVELOPE_REQUIRED_PATHS: Final[tuple[str, ...]] = (
    "$.schema_version",
    "$.replay_run_id",
    "$.delivery_id",
    "$.source_offset",
    "$.emission_index",
    "$.duplicate_ordinal",
    "$.synthetic_event_time",
    "$.event",
)
EVENT_REQUIRED_PATHS: Final[tuple[str, ...]] = tuple(
    f"$.event.{field.name}" for field in EVENT_FIELDS
)

IGNORED_COUNTER_COLUMNS: Final[tuple[str, ...]] = (
    "ignored_datagram_count",
    "ignored_wrong_endpoint_count",
    "ignored_bad_length_count",
    "ignored_non_ascii_count",
    "ignored_malformed_count",
    "ignored_bad_prefix_count",
    "ignored_bad_identity_count",
    "ignored_bad_attempt_count",
    "ignored_bad_inner_hex_count",
    "ignored_bad_crc_count",
    "ignored_bad_inner_text_count",
    "ignored_wrong_session_count",
    "ignored_wrong_stage_count",
    "ignored_wrong_transaction_count",
    "ignored_wrong_attempt_count",
)


def validate_watermark_delay(
    delay_ms: object,
    replay_interval_ms: object,
    out_of_order_window: object,
) -> str:
    """Return a delay strictly above skew derived from the replay schedule."""
    for name, value in (
        ("watermark_delay_ms", delay_ms),
        ("replay_interval_ms", replay_interval_ms),
        ("out_of_order_window", out_of_order_window),
    ):
        if type(value) is not int or value < 0:
            raise SparkContractError(f"{name} must be a non-negative integer")
    assert isinstance(delay_ms, int)
    assert isinstance(replay_interval_ms, int)
    assert isinstance(out_of_order_window, int)
    if delay_ms == 0 or replay_interval_ms == 0 or out_of_order_window == 0:
        raise SparkContractError("watermark, interval, and disorder window must be positive")
    maximum_skew_ms = (out_of_order_window - 1) * replay_interval_ms
    if delay_ms <= maximum_skew_ms:
        raise SparkContractError(
            "watermark_delay_ms must strictly exceed replay-derived timestamp skew"
        )
    return f"{delay_ms} milliseconds"
