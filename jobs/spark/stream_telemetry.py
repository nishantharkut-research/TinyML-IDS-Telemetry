"""Pure Spark transformations and streaming telemetry job startup."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812

from jobs.spark.common import (
    ENVELOPE_REQUIRED_PATHS,
    ENVELOPE_SCHEMA,
    EVENT_FIELDS,
    EVENT_REQUIRED_PATHS,
    validate_watermark_delay,
)

_ENVELOPE_PROPERTIES = [field.name for field in ENVELOPE_SCHEMA.fields]
_EVENT_PROPERTIES = [field.name for field in EVENT_FIELDS]
_REQUIRED_HEADERS = ["delivery_id", "envelope_schema", "replay_run_id", "source_event_id"]


def bronze_from_kafka(kafka: DataFrame) -> DataFrame:
    """Project every Kafka delivery without deduplication or payload mutation."""
    return kafka.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("key").alias("kafka_key"),
        F.col("headers").alias("kafka_headers"),
        F.col("value").alias("original_payload_binary"),
        F.col("value").cast("string").alias("original_payload_string"),
        F.current_timestamp().alias("ingestion_time"),
    )


def _any_null(columns: list[Column]) -> Column:
    return reduce(lambda left, right: left | right, columns)


def _any_invalid(columns: list[Column]) -> Column:
    return reduce(lambda left, right: left | right, columns)


def parse_bronze_envelopes(bronze: DataFrame) -> DataFrame:
    """Parse envelope JSON once while retaining every Bronze field."""
    parsed = bronze.withColumn(
        "_binary_payload_string", F.col("original_payload_binary").cast("string")
    )
    payload = F.col("_binary_payload_string")
    return parsed.withColumn("_malformed_json", F.try_parse_json(payload).isNull()).withColumn(
        "_envelope", F.from_json(payload, ENVELOPE_SCHEMA, {"mode": "PERMISSIVE"})
    )


def classify_bronze_envelopes(parsed_bronze: DataFrame) -> DataFrame:
    """Apply the frozen fail-closed reason precedence to parsed Bronze."""
    payload = F.col("_binary_payload_string")
    parsed = F.col("_envelope")
    missing = _any_null(
        [F.get_json_object(payload, path).isNull() for path in ENVELOPE_REQUIRED_PATHS]
        + [F.get_json_object(payload, path).isNull() for path in EVENT_REQUIRED_PATHS]
    )
    typed_null = _any_null(
        [parsed[field].isNull() for field in ENVELOPE_SCHEMA.fieldNames() if field != "event"]
        + [parsed.event[field.name].isNull() for field in EVENT_FIELDS]
        + [F.exists(parsed.event.fixed_logits, lambda value: value.isNull())]
    )
    unsupported = (
        F.get_json_object(payload, "$.schema_version") != F.lit("telemetry-replay-envelope-v1")
    ) | (F.get_json_object(payload, "$.event.schema_version") != F.lit("telemetry-event-v1"))
    envelope_keys = F.json_object_keys(payload)
    event_keys = F.json_object_keys(F.get_json_object(payload, "$.event"))
    unexpected = (
        F.size(F.array_except(envelope_keys, F.array(*[F.lit(v) for v in _ENVELOPE_PROPERTIES])))
        > 0
    ) | (F.size(F.array_except(event_keys, F.array(*[F.lit(v) for v in _EVENT_PROPERTIES]))) > 0)

    non_negative = ["source_offset", "emission_index", "duplicate_ordinal"] + [
        field.name
        for field in EVENT_FIELDS
        if field.name.endswith("_count")
        or field.name.endswith("_us")
        or field.name
        in {
            "row_id",
            "source_row_index",
            "logit_frac_bits",
            "successful_request_bytes",
            "request_datagram_bytes_sent",
            "response_bytes",
        }
    ]
    envelope_negative = _any_invalid([parsed[name] < F.lit(0) for name in non_negative[:3]])
    event_negative = _any_invalid([parsed.event[name] < F.lit(0) for name in non_negative[3:]])
    event = parsed.event
    sorted_logits = F.sort_array(event.fixed_logits, asc=False)
    first_argmax = F.array_position(event.fixed_logits, F.array_max(event.fixed_logits)) - F.lit(1)
    class_names = F.array(
        F.lit("Blackhole"),
        F.lit("Flooding"),
        F.lit("Grayhole"),
        F.lit("Normal"),
        F.lit("TDMA"),
    )
    expected_margin = (F.element_at(sorted_logits, 1) - F.element_at(sorted_logits, 2)).cast(
        "double"
    ) / F.pow(F.lit(2.0), event.logit_frac_bits)
    empty_required_string = _any_invalid(
        [
            F.length(F.trim(event[name])) == 0
            for name in (
                "source_session_id",
                "source_stage_id",
                "board",
                "student_model",
                "status",
            )
        ]
    )
    invalid_event_value = (
        (F.length(F.trim(parsed.replay_run_id)) == 0)
        | (~event.source_event_id.rlike("^[0-9a-f]{64}$"))
        | (~event.lineage_id.rlike("^[0-9a-f]{64}$"))
        | empty_required_string
        | envelope_negative
        | event_negative
        | (event.attempts < 1)
        | (F.size(event.fixed_logits) != 5)
        | (~event.true_class_id.between(0, 4))
        | (~event.predicted_class_id.between(0, 4))
        | (~event.fixed_predicted_class_id.between(0, 4))
        | (~event.fp32_predicted_class_id.between(0, 4))
        | (~event.normal_class_id.between(0, 4))
        | (event.total_us != event.preprocess_us + event.inference_us)
        | (event.fixed_predicted_class_id != event.predicted_class_id)
        | (event.predicted_class_id != first_argmax)
        | (event.is_predicted_attack != (event.predicted_class_id != event.normal_class_id))
        | (event.true_class_name != F.element_at(class_names, event.true_class_id + 1))
        | (event.predicted_class_name != F.element_at(class_names, event.predicted_class_id + 1))
        | (event.normal_class_id != 3)
        | (event.normalized_logit_margin != expected_margin)
        | (F.element_at(sorted_logits, 1).isNull())
        | (~event.mcu_fixed_prediction_match)
        | (~event.mcu_fixed_logits_match)
    )
    header_names = F.array_sort(F.transform(F.col("kafka_headers"), lambda item: item["key"]))

    def header_text(name: str) -> Column:
        matches = F.filter(F.col("kafka_headers"), lambda item: item["key"] == F.lit(name))
        return F.element_at(matches, 1)["value"].cast("string")

    header_values_utf8 = F.forall(
        F.col("kafka_headers"),
        lambda item: item["value"].isNotNull() & F.is_valid_utf8(item["value"]),
    )
    payload_mismatch = ~F.col("original_payload_string").eqNullSafe(F.col("_binary_payload_string"))
    invalid_kafka = (
        payload_mismatch
        | F.col("kafka_key").isNull()
        | (~F.is_valid_utf8(F.col("kafka_key")))
        | (F.col("kafka_key").cast("string") != event.lineage_id)
        | F.col("kafka_headers").isNull()
        | (F.size(F.col("kafka_headers")) != len(_REQUIRED_HEADERS))
        | (~header_values_utf8)
        | (header_names != F.array(*[F.lit(name) for name in _REQUIRED_HEADERS]))
        | (header_text("envelope_schema") != parsed.schema_version)
        | (header_text("replay_run_id") != parsed.replay_run_id)
        | (header_text("source_event_id") != event.source_event_id)
        | (header_text("delivery_id") != parsed.delivery_id)
    )

    def quoted_json_string(column: Column) -> Column:
        encoded_array = F.to_json(F.array(column))
        return F.substring(encoded_array, F.lit(2), F.length(encoded_array) - F.lit(2))

    delivery_identity = F.concat(
        F.lit("["),
        quoted_json_string(parsed.replay_run_id),
        F.lit(","),
        quoted_json_string(event.source_event_id),
        F.lit(","),
        parsed.source_offset.cast("string"),
        F.lit(","),
        parsed.duplicate_ordinal.cast("string"),
        F.lit("]"),
    )
    canonical_timestamp = (
        parsed.synthetic_event_time.rlike(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{6})?Z$"
        )
        & parsed.synthetic_event_time.cast("timestamp").isNotNull()
    )
    invalid_delivery = (
        (~parsed.delivery_id.rlike("^[0-9a-f]{64}$"))
        | (parsed.delivery_id != F.sha2(delivery_identity, 256))
        | (~canonical_timestamp)
    )
    reason = (
        F.when(~F.is_valid_utf8(F.col("original_payload_binary")), F.lit("invalid_utf8"))
        .when(F.col("_malformed_json"), F.lit("malformed_json"))
        .when(unsupported, F.lit("unsupported_schema_version"))
        .when(missing, F.lit("missing_required_field"))
        .when(unexpected, F.lit("unexpected_property"))
        .when(typed_null, F.lit("type_mismatch"))
        .when(invalid_kafka, F.lit("invalid_kafka_contract"))
        .when(invalid_delivery, F.lit("invalid_delivery_identity"))
        .when(invalid_event_value, F.lit("invalid_event_value"))
    )
    return parsed_bronze.withColumn("quarantine_reason", reason).drop("_malformed_json")


def split_classified_bronze(
    classified: DataFrame,
    bronze_columns: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Project valid typed rows and quarantined original Bronze rows."""
    valid = classified.filter(F.col("quarantine_reason").isNull())
    event_columns = [
        F.col(f"_envelope.event.{field.name}").alias(field.name) for field in EVENT_FIELDS
    ]
    silver = valid.select(
        *[F.col(name) for name in bronze_columns],
        F.col("_envelope.schema_version").alias("envelope_schema_version"),
        F.col("_envelope.replay_run_id").alias("replay_run_id"),
        F.col("_envelope.delivery_id").alias("delivery_id"),
        F.col("_envelope.source_offset").alias("source_offset"),
        F.col("_envelope.emission_index").alias("emission_index"),
        F.col("_envelope.duplicate_ordinal").alias("duplicate_ordinal"),
        F.col("_envelope.synthetic_event_time").cast("timestamp").alias("synthetic_event_time"),
        *event_columns,
    )
    event_payload = F.to_json(F.struct(*[F.col(field.name) for field in EVENT_FIELDS]))
    silver = silver.withColumn("event_payload_sha256", F.sha2(event_payload, 256))
    quarantine = classified.filter(F.col("quarantine_reason").isNotNull()).select(
        *[F.col(name) for name in bronze_columns],
        "quarantine_reason",
    )
    return silver, quarantine


def split_silver_and_quarantine(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse one Bronze frame into typed valid rows and deterministic quarantine."""
    classified = classify_bronze_envelopes(parse_bronze_envelopes(bronze))
    return split_classified_bronze(
        classified,
        bronze.columns,
    )


def bounded_deduplicate(
    silver: DataFrame,
    *,
    watermark_delay_ms: object,
    replay_interval_ms: object,
    out_of_order_window: object,
) -> DataFrame:
    """Deduplicate only within a declared synthetic-event-time watermark."""
    delay = validate_watermark_delay(
        watermark_delay_ms,
        replay_interval_ms,
        out_of_order_window,
    )
    return silver.withWatermark("synthetic_event_time", delay).dropDuplicatesWithinWatermark(
        ["replay_run_id", "source_event_id"]
    )
