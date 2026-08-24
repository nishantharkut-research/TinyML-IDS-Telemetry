"""Exact Bronze-to-canonical-Gold proof and atomic publication."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import cast

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F  # noqa: N812

from jobs.spark.build_aggregates import (
    build_execution_dimension_counts,
    build_operational_metrics,
    build_predictive_row_metrics,
    build_retention_metrics,
    build_retention_reason_counts,
)
from jobs.spark.common import EVENT_FIELDS, EVENT_SCHEMA
from jobs.spark.policies import build_policy_decisions, validate_policy_config
from jobs.spark.stream_telemetry import (
    classify_bronze_envelopes,
    parse_bronze_envelopes,
    split_classified_bronze,
)
from tinyml_ids_telemetry.contracts.identity import source_event_id
from tinyml_ids_telemetry.contracts.lineage import LineageRecord
from tinyml_ids_telemetry.contracts.schema import (
    SchemaContractError,
    ValidatedSnapshotManifest,
    validate_event_mapping,
    validate_lineage_mapping,
    validate_snapshot_manifest_mapping,
)
from tinyml_ids_telemetry.evidence.catalog import (
    EvidenceContractError,
    ensure_output_outside_evidence_roots,
)
from tinyml_ids_telemetry.evidence.hashes import sha256_file
from tinyml_ids_telemetry.replay.receipt import (
    ReplayReceiptError,
    expected_delivery_count,
    load_completion_receipt,
    validate_completion_receipt,
)

_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_EVENT_NAMES = [field.name for field in EVENT_FIELDS]
_CANONICAL_EVENT_KEYS = (
    "source_event_id",
    "lineage_id",
    "source_offset",
    "event_payload_sha256",
)
_SCHEDULE_KEYS = (
    "delivery_id",
    "source_event_id",
    "source_offset",
    "duplicate_ordinal",
    "emission_index",
    "synthetic_event_time",
)
_DEFAULT_POLICY_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "retention-policies.toml"
_OUTPUT_NAMES = (
    "canonical_gold",
    "policy_decisions",
    "retained_security_aware",
    "execution_dimension_counts",
    "operational_metrics",
    "predictive_row_metrics",
    "retention_metrics",
    "retention_reason_counts",
)
_AGGREGATE_OUTPUT_NAMES = (
    "execution_dimension_counts",
    "operational_metrics",
    "predictive_row_metrics",
    "retention_metrics",
    "retention_reason_counts",
)


class GoldFinalizationError(ValueError):
    """Raised when exact finalization cannot prove the completed run."""


@dataclass(frozen=True, slots=True)
class GoldProof:
    """Exact cardinalities proved before canonical Gold publication."""

    source_event_count: int
    delivery_count: int
    identical_bronze_repeats: int
    lineage_counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class FinalizationResult:
    """Location and publication outcome for one finalized replay run."""

    run_directory: Path
    manifest_path: Path
    published: bool


@dataclass(frozen=True, slots=True)
class _FinalizationContract:
    receipt: dict[str, object]
    manifest: ValidatedSnapshotManifest
    snapshot_directory: Path


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def _iter_jsonl(path: Path) -> Iterator[tuple[int, object]]:
    with path.open("rb") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                yield line_number, json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise GoldFinalizationError(
                    f"canonical snapshot {path.name} line {line_number} is not valid UTF-8 JSON"
                ) from error


def _validate_event_lineage_invariants(
    event: Mapping[str, object],
    lineage: LineageRecord,
    line_number: int,
) -> None:
    for field, expected in (
        ("source_session_id", lineage.source_session_id),
        ("source_stage_id", lineage.source_stage_id),
        ("board", lineage.board),
        ("student_model", lineage.student_model),
        ("normal_class_id", lineage.normal_class_id),
        ("logit_frac_bits", lineage.logit_frac_bits),
    ):
        if event[field] != expected:
            raise GoldFinalizationError(
                f"canonical snapshot events.jsonl line {line_number} {field} disagrees with lineage"
            )

    fixed_logits = cast(list[int], event["fixed_logits"])
    predicted_class_id = cast(int, event["predicted_class_id"])
    first_argmax = max(range(len(fixed_logits)), key=fixed_logits.__getitem__)
    if (
        predicted_class_id != first_argmax
        or event["fixed_predicted_class_id"] != predicted_class_id
    ):
        raise GoldFinalizationError(
            f"canonical snapshot events.jsonl line {line_number} has inconsistent prediction"
        )
    true_class_id = cast(int, event["true_class_id"])
    if (
        event["true_class_name"] != lineage.class_names[true_class_id]
        or event["predicted_class_name"] != lineage.class_names[predicted_class_id]
    ):
        raise GoldFinalizationError(
            f"canonical snapshot events.jsonl line {line_number} has inconsistent class names"
        )
    if event["is_predicted_attack"] != (predicted_class_id != lineage.normal_class_id):
        raise GoldFinalizationError(
            f"canonical snapshot events.jsonl line {line_number} has inconsistent attack flag"
        )
    top_two = sorted(fixed_logits, reverse=True)[:2]
    expected_margin = (top_two[0] - top_two[1]) / (1 << lineage.logit_frac_bits)
    if event["normalized_logit_margin"] != expected_margin:
        raise GoldFinalizationError(
            f"canonical snapshot events.jsonl line {line_number} has inconsistent logit margin"
        )
    if event["total_us"] != cast(int, event["preprocess_us"]) + cast(int, event["inference_us"]):
        raise GoldFinalizationError(
            f"canonical snapshot events.jsonl line {line_number} has inconsistent total_us"
        )


def _validate_canonical_snapshot_rows(
    snapshot: Path,
    manifest: ValidatedSnapshotManifest,
) -> None:
    lineages: list[LineageRecord] = []
    identifiers: set[str] = set()
    for line_number, value in _iter_jsonl(snapshot / "lineages.jsonl"):
        try:
            lineage = validate_lineage_mapping(value)
        except SchemaContractError as error:
            raise GoldFinalizationError(
                f"canonical snapshot lineages.jsonl line {line_number}: {error}"
            ) from error
        if lineage.lineage_id in identifiers:
            raise GoldFinalizationError("canonical snapshot contains duplicate lineage_id")
        identifiers.add(lineage.lineage_id)
        lineages.append(lineage)

    if tuple(lineage.lineage_id for lineage in lineages) != manifest.lineage_ids:
        raise GoldFinalizationError(
            "canonical snapshot lineages.jsonl IDs do not exactly match the manifest"
        )
    if len(lineages) != manifest.lineage_count:
        raise GoldFinalizationError("canonical snapshot lineage count is incomplete")
    if any(lineage.expected_rows != manifest.rows_per_lineage for lineage in lineages):
        raise GoldFinalizationError(
            "canonical snapshot lineage expected_rows disagrees with the manifest"
        )

    counts = {lineage.lineage_id: 0 for lineage in lineages}
    evaluation_hashes = {lineage.lineage_id: sha256() for lineage in lineages}
    lineage_index = 0
    expected_row_id = 0
    observed_count = 0
    for line_number, value in _iter_jsonl(snapshot / "events.jsonl"):
        observed_count += 1
        try:
            event = validate_event_mapping(value)
        except SchemaContractError as error:
            raise GoldFinalizationError(
                f"canonical snapshot events.jsonl line {line_number}: {error}"
            ) from error
        if lineage_index >= len(lineages):
            raise GoldFinalizationError(
                "canonical snapshot event row coverage exceeds the manifest"
            )
        lineage = lineages[lineage_index]
        if event["lineage_id"] != lineage.lineage_id or event["row_id"] != expected_row_id:
            raise GoldFinalizationError(
                "canonical snapshot event lineage order or row coverage is invalid"
            )
        expected_source_event_id = source_event_id(
            protocol_id=lineage.wireless_protocol_id,
            export_id=lineage.export_id,
            wireless_bundle_id=lineage.wireless_bundle_id,
            source_session_id=lineage.source_session_id,
            source_stage_id=lineage.source_stage_id,
            row_id=event["row_id"],
        )
        if event["source_event_id"] != expected_source_event_id:
            raise GoldFinalizationError(
                f"canonical snapshot events.jsonl line {line_number} source_event_id "
                "does not match its deterministic source identity"
            )
        _validate_event_lineage_invariants(event, lineage, line_number)
        counts[lineage.lineage_id] += 1
        evaluation_hashes[lineage.lineage_id].update(
            f"{event['source_row_index']}|{event['true_class_id']}\n".encode("ascii")
        )
        expected_row_id += 1
        if expected_row_id == lineage.expected_rows:
            lineage_index += 1
            expected_row_id = 0

    if observed_count != manifest.event_count:
        raise GoldFinalizationError("canonical snapshot event count is incomplete")
    if lineage_index != len(lineages) or expected_row_id != 0:
        raise GoldFinalizationError("canonical snapshot event row coverage is incomplete")
    for lineage in lineages:
        if counts[lineage.lineage_id] != lineage.expected_rows:
            raise GoldFinalizationError("canonical snapshot lineage event count is incomplete")
        if evaluation_hashes[lineage.lineage_id].hexdigest() != manifest.evaluation_sequence_sha256:
            raise GoldFinalizationError(
                "canonical snapshot evaluation sequence disagrees with the manifest"
            )


def _read_contract(
    snapshot_directory: Path,
    completion_receipt: Mapping[str, object],
) -> _FinalizationContract:
    snapshot = snapshot_directory.expanduser().resolve()
    try:
        receipt = validate_completion_receipt(completion_receipt)
        manifest_value = json.loads(
            (snapshot / "snapshot_manifest.json").read_text(encoding="utf-8")
        )
        manifest = validate_snapshot_manifest_mapping(manifest_value)
    except (OSError, json.JSONDecodeError, ReplayReceiptError, SchemaContractError) as error:
        raise GoldFinalizationError(f"invalid finalization contract: {error}") from error

    receipt_snapshot = cast(dict[str, object], receipt["snapshot"])
    expected_snapshot = {
        "snapshot_id": manifest.snapshot_id,
        "sealed_report_sha256": manifest.sealed_report_sha256,
        "events_sha256": manifest.events_sha256,
        "lineages_sha256": manifest.lineages_sha256,
        "evaluation_sequence_sha256": manifest.evaluation_sequence_sha256,
        "snapshot_event_count": manifest.event_count,
        "lineage_ids": list(manifest.lineage_ids),
    }
    if receipt_snapshot != expected_snapshot:
        raise GoldFinalizationError("completion receipt does not identify the canonical snapshot")
    if receipt["destination"] != "kafka":
        raise GoldFinalizationError("exact Gold finalization requires a completed Kafka replay")
    events_path = snapshot / "events.jsonl"
    lineages_path = snapshot / "lineages.jsonl"
    if not events_path.is_file() or sha256_file(events_path) != manifest.events_sha256:
        raise GoldFinalizationError("canonical snapshot events SHA-256 mismatch")
    if not lineages_path.is_file() or sha256_file(lineages_path) != manifest.lineages_sha256:
        raise GoldFinalizationError("canonical snapshot lineages SHA-256 mismatch")
    _validate_canonical_snapshot_rows(snapshot, manifest)
    return _FinalizationContract(receipt, manifest, snapshot)


def _event_hash() -> Column:
    return F.sha2(F.to_json(F.struct(*[F.col(name) for name in _EVENT_NAMES])), 256)


def _load_expected_events(
    spark: SparkSession,
    contract: _FinalizationContract,
) -> DataFrame:
    raw = spark.read.text(str(contract.snapshot_directory / "events.jsonl"))
    parsed = raw.select(F.from_json("value", EVENT_SCHEMA).alias("event"))
    if parsed.filter(F.col("event").isNull()).limit(1).count():
        raise GoldFinalizationError("canonical snapshot contains an untyped event")
    events = parsed.select("event.*")
    manifest = contract.manifest
    lineage_rows = [(lineage_id, index) for index, lineage_id in enumerate(manifest.lineage_ids)]
    lineage_index = spark.createDataFrame(lineage_rows, ["lineage_id", "lineage_index"])
    events = events.join(lineage_index, "lineage_id", "left")
    invalid_offsets = events.filter(
        F.col("lineage_index").isNull()
        | (~F.col("row_id").between(0, manifest.rows_per_lineage - 1))
    )
    if invalid_offsets.limit(1).count():
        raise GoldFinalizationError("canonical snapshot lineage or row identity is invalid")
    events = events.withColumn(
        "source_offset",
        F.col("lineage_index") * F.lit(manifest.rows_per_lineage) + F.col("row_id"),
    ).drop("lineage_index")
    if events.count() != manifest.event_count:
        raise GoldFinalizationError("canonical snapshot event count is incomplete")
    if events.select("source_event_id").distinct().count() != manifest.event_count:
        raise GoldFinalizationError("canonical snapshot source_event_id values are not unique")
    if events.select("source_offset").distinct().count() != manifest.event_count:
        raise GoldFinalizationError("canonical snapshot source offsets are not unique")
    return events.withColumn("event_payload_sha256", _event_hash()).localCheckpoint(eager=True)


def _fail_if_any(frame: DataFrame, message: str) -> None:
    if frame.limit(1).count():
        raise GoldFinalizationError(message)


def _assert_equal_relations(
    observed: DataFrame,
    expected: DataFrame,
    keys: Sequence[str],
    message: str,
) -> None:
    observed_keys = observed.select(*keys).distinct()
    expected_keys = expected.select(*keys).distinct()
    _fail_if_any(observed_keys.join(expected_keys, list(keys), "left_anti"), message)
    _fail_if_any(expected_keys.join(observed_keys, list(keys), "left_anti"), message)


def _quoted_json_string(column: Column) -> Column:
    encoded = F.to_json(F.array(column))
    return F.substring(encoded, F.lit(2), F.length(encoded) - F.lit(2))


def _expected_schedule(expected: DataFrame, contract: _FinalizationContract) -> DataFrame:
    receipt = contract.receipt
    options = cast(dict[str, object], receipt["replay_options"])
    run_id = cast(str, receipt["replay_run_id"])
    duplicate_every = cast(int | None, options["duplicate_every"])
    interval_ms = cast(int, options["interval_ms"])
    window = cast(int, options["out_of_order_window"])
    total = expected_delivery_count(contract.manifest.event_count, 0, duplicate_every)
    start = datetime.fromisoformat(cast(str, options["start_utc"]).removesuffix("Z") + "+00:00")
    epoch_delta = start.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    start_epoch_micros = (
        epoch_delta.days * 86_400 + epoch_delta.seconds
    ) * 1_000_000 + epoch_delta.microseconds

    if duplicate_every is None:
        ordinals = F.array(F.lit(0))
        prior_duplicates = F.lit(0)
    else:
        ordinals = F.when(
            F.pmod(F.col("source_offset") + 1, F.lit(duplicate_every)) == 0,
            F.array(F.lit(0), F.lit(1)),
        ).otherwise(F.array(F.lit(0)))
        prior_duplicates = F.floor(F.col("source_offset") / F.lit(duplicate_every))

    schedule = expected.withColumn("duplicate_ordinal", F.explode(ordinals))
    base_position = F.col("source_offset") + prior_duplicates + F.col("duplicate_ordinal")
    group_start = F.floor(base_position / F.lit(window)) * F.lit(window)
    group_length = F.least(F.lit(window), F.lit(total) - group_start)
    schedule = schedule.withColumn(
        "emission_index",
        group_start + group_length - F.lit(1) - (base_position - group_start),
    ).withColumn(
        "synthetic_event_time",
        F.timestamp_micros(
            F.lit(start_epoch_micros) + F.col("source_offset") * F.lit(interval_ms * 1_000)
        ),
    )
    identity = F.concat(
        F.lit("["),
        _quoted_json_string(F.lit(run_id)),
        F.lit(","),
        _quoted_json_string(F.col("source_event_id")),
        F.lit(","),
        F.col("source_offset").cast("string"),
        F.lit(","),
        F.col("duplicate_ordinal").cast("string"),
        F.lit("]"),
    )
    return schedule.withColumn("delivery_id", F.sha2(identity, 256))


def _target_run_candidates(bronze: DataFrame, run_id: str) -> DataFrame:
    header_identifies_target = F.coalesce(
        F.exists(
            F.col("kafka_headers"),
            lambda item: (
                (item["key"] == F.lit("replay_run_id"))
                & item["value"].isNotNull()
                & F.is_valid_utf8(item["value"])
                & (item["value"].cast("string") == F.lit(run_id))
            ),
        ),
        F.lit(False),
    )
    string_identifies_target = F.coalesce(
        F.get_json_object(F.col("original_payload_string"), "$.replay_run_id") == F.lit(run_id),
        F.lit(False),
    )
    binary_identifies_target = F.coalesce(
        F.is_valid_utf8(F.col("original_payload_binary"))
        & (
            F.get_json_object(
                F.col("original_payload_binary").cast("string"),
                "$.replay_run_id",
            )
            == F.lit(run_id)
        ),
        F.lit(False),
    )
    return bronze.filter(
        header_identifies_target | string_identifies_target | binary_identifies_target
    )


def _validated_observed_bronze(
    bronze: DataFrame,
    contract: _FinalizationContract,
) -> tuple[DataFrame, int]:
    receipt = contract.receipt
    run_id = cast(str, receipt["replay_run_id"])
    topic = cast(str, receipt["kafka_topic"])
    candidates = _target_run_candidates(bronze, run_id)
    valid_timestamp_type = F.col("kafka_timestamp_type").eqNullSafe(F.lit(0)) | F.col(
        "kafka_timestamp_type"
    ).eqNullSafe(F.lit(1))
    invalid_metadata = (
        (~F.col("kafka_topic").eqNullSafe(F.lit(topic)))
        | (~F.col("kafka_partition").eqNullSafe(F.lit(0)))
        | F.col("kafka_offset").isNull()
        | (F.col("kafka_offset") < F.lit(0))
        | F.col("kafka_timestamp").isNull()
        | (~valid_timestamp_type)
    )
    _fail_if_any(
        candidates.filter(invalid_metadata),
        "Target Bronze candidate has invalid Kafka metadata",
    )
    target_positions = candidates.select(
        "kafka_topic", "kafka_partition", "kafka_offset"
    ).distinct()
    occupied_positions = bronze.join(
        target_positions,
        ["kafka_topic", "kafka_partition", "kafka_offset"],
        "inner",
    )
    kafka_record = F.struct(
        "kafka_timestamp",
        "kafka_timestamp_type",
        "kafka_key",
        "kafka_headers",
        "original_payload_binary",
        "original_payload_string",
    )
    kafka_conflicts = occupied_positions.groupBy(
        "kafka_topic", "kafka_partition", "kafka_offset"
    ).agg(F.countDistinct(kafka_record).alias("versions"))
    _fail_if_any(
        kafka_conflicts.filter(F.col("versions") != 1),
        "Kafka position contains conflicting record metadata or payload",
    )
    parsed = parse_bronze_envelopes(candidates).localCheckpoint(eager=True)
    classified = classify_bronze_envelopes(parsed).localCheckpoint(eager=True)
    silver, quarantine = split_classified_bronze(
        classified,
        candidates.columns,
    )
    _fail_if_any(quarantine, "Bronze validation produced quarantine rows")
    _fail_if_any(
        silver.filter(F.col("replay_run_id") != run_id),
        "Bronze contains an unexpected replay_run_id",
    )
    original_count = silver.count()
    position_window = Window.partitionBy("kafka_topic", "kafka_partition", "kafka_offset").orderBy(
        F.col("ingestion_time").asc_nulls_last()
    )
    silver = (
        silver.withColumn("_position_rank", F.row_number().over(position_window))
        .filter(F.col("_position_rank") == 1)
        .drop("_position_rank")
    )
    delivery_conflicts = silver.groupBy("delivery_id").agg(
        F.countDistinct("event_payload_sha256").alias("event_versions"),
        F.countDistinct(F.sha2("original_payload_binary", 256)).alias("payload_versions"),
    )
    _fail_if_any(
        delivery_conflicts.filter(
            (F.col("event_versions") != 1) | (F.col("payload_versions") != 1)
        ),
        "delivery_id identifies conflicting payload or event content",
    )
    source_conflicts = silver.groupBy("replay_run_id", "source_event_id").agg(
        F.countDistinct("event_payload_sha256").alias("event_versions")
    )
    _fail_if_any(
        source_conflicts.filter(F.col("event_versions") != 1),
        "source identity identifies conflicting canonical event content",
    )
    unique_deliveries = silver.dropDuplicates(["delivery_id"]).localCheckpoint(eager=True)
    delivery_count = unique_deliveries.count()
    return unique_deliveries, original_count - delivery_count


def prepare_exact_gold(
    bronze: DataFrame,
    *,
    snapshot_directory: Path,
    completion_receipt: Mapping[str, object],
) -> tuple[DataFrame, GoldProof]:
    """Prove a completed Bronze replay and return canonical snapshot-derived Gold."""
    contract = _read_contract(snapshot_directory, completion_receipt)
    expected = _load_expected_events(bronze.sparkSession, contract)
    observed, repeat_count = _validated_observed_bronze(bronze, contract)
    schedule = _expected_schedule(expected, contract)
    observed_schedule = observed.select(*_SCHEDULE_KEYS).distinct()
    full_schedule = schedule.select(*_SCHEDULE_KEYS).distinct()
    _fail_if_any(
        observed_schedule.join(full_schedule, list(_SCHEDULE_KEYS), "left_anti"),
        "Bronze replay schedule mismatch",
    )
    resume_offset = cast(
        int,
        cast(dict[str, object], contract.receipt["replay_options"])["resume_offset"],
    )
    required_tail = full_schedule.filter(F.col("source_offset") >= F.lit(resume_offset))
    _fail_if_any(
        required_tail.join(observed_schedule, list(_SCHEDULE_KEYS), "left_anti"),
        "Bronze resume tail is incomplete",
    )
    _assert_equal_relations(
        observed,
        expected,
        _CANONICAL_EVENT_KEYS,
        "Bronze canonical snapshot mismatch",
    )

    run_id = cast(str, contract.receipt["replay_run_id"])
    gold = expected.select(
        F.lit(run_id).alias("replay_run_id"),
        *[F.col(name) for name in _EVENT_NAMES],
        "source_offset",
        "event_payload_sha256",
    )
    lineage_counts = tuple(
        (row.lineage_id, row.event_count)
        for row in gold.groupBy("lineage_id")
        .agg(F.count(F.lit(1)).alias("event_count"))
        .orderBy("lineage_id")
        .collect()
    )
    proof = GoldProof(
        source_event_count=contract.manifest.event_count,
        delivery_count=observed.count(),
        identical_bronze_repeats=repeat_count,
        lineage_counts=lineage_counts,
    )
    return gold, proof


def _logical_gold_sha256(gold: DataFrame) -> str:
    digest = sha256()
    rows = gold.select(
        "source_event_id", "lineage_id", "source_offset", "event_payload_sha256"
    ).orderBy("source_event_id")
    for row in rows.toLocalIterator():
        digest.update(
            f"{row.source_event_id}|{row.lineage_id}|{row.source_offset}|"
            f"{row.event_payload_sha256}\n".encode("ascii")
        )
    return digest.hexdigest()


def _canonical_event_identity_sha256(gold: DataFrame) -> str:
    digest = sha256()
    rows = gold.select(*_EVENT_NAMES).orderBy("source_event_id")
    for row in rows.toLocalIterator():
        event = {name: row[name] for name in _EVENT_NAMES}
        payload_sha256 = sha256(
            json.dumps(event, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest()
        digest.update(
            f"{row.source_event_id}|{row.lineage_id}|{row.row_id}|"
            f"{row.source_row_index}|{payload_sha256}\n".encode("ascii")
        )
    return digest.hexdigest()


def _spark_aggregates_payload(outputs: Mapping[str, DataFrame]) -> bytes:
    aggregates: dict[str, list[dict[str, object]]] = {}
    for name in _AGGREGATE_OUTPUT_NAMES:
        rows = [row.asDict(recursive=True) for row in outputs[name].collect()]
        aggregates[name] = sorted(
            rows,
            key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
        )
    return _canonical_bytes(aggregates)


def _manifest(
    contract: _FinalizationContract,
    proof: GoldProof,
    gold: DataFrame,
    output_counts: Mapping[str, int],
    policy_config_sha256: str,
    policy_ids: Sequence[str],
    *,
    completion_receipt_sha256: str,
    spark_aggregates_sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": "telemetry-finalized-run-v1",
        "replay_run_id": contract.receipt["replay_run_id"],
        "snapshot_id": contract.manifest.snapshot_id,
        "events_sha256": contract.manifest.events_sha256,
        "schedule_identity_sha256": contract.receipt["schedule_identity_sha256"],
        "completion_receipt_sha256": completion_receipt_sha256,
        "source_event_count": proof.source_event_count,
        "delivery_count": proof.delivery_count,
        "lineage_counts": [
            {"lineage_id": lineage_id, "hardware_execution_rows": count}
            for lineage_id, count in proof.lineage_counts
        ],
        "canonical_gold_identity_sha256": _logical_gold_sha256(gold),
        "canonical_event_identity_sha256": _canonical_event_identity_sha256(gold),
        "spark_aggregates_sha256": spark_aggregates_sha256,
        "policy_config_sha256": policy_config_sha256,
        "policy_ids": list(policy_ids),
        "outputs": {name: {"row_count": output_counts[name]} for name in sorted(output_counts)},
        "statistical_unit": (
            "rows are hardware executions; repeated source_row_index values across lineages "
            "are not independent predictive rows"
        ),
        "publication_contract": (
            "canonical snapshot rows published only after exact Bronze and completion proof"
        ),
    }


def _build_finalized_outputs(gold: DataFrame) -> dict[str, DataFrame]:
    decisions = build_policy_decisions(gold)
    retained_security_aware = gold.join(
        decisions.filter(
            (F.col("policy_id") == F.lit("security-aware-static-v1")) & F.col("retained")
        ).select("replay_run_id", "source_event_id"),
        ["replay_run_id", "source_event_id"],
        "inner",
    )
    return {
        "canonical_gold": gold,
        "policy_decisions": decisions,
        "retained_security_aware": retained_security_aware,
        "execution_dimension_counts": build_execution_dimension_counts(gold),
        "operational_metrics": build_operational_metrics(gold),
        "predictive_row_metrics": build_predictive_row_metrics(gold),
        "retention_metrics": build_retention_metrics(gold, decisions),
        "retention_reason_counts": build_retention_reason_counts(decisions),
    }


def _relations_match_exactly(observed: DataFrame, expected: DataFrame) -> bool:
    if observed.schema.jsonValue() != expected.schema.jsonValue():
        return False
    return (
        observed.exceptAll(expected).limit(1).count() == 0
        and expected.exceptAll(observed).limit(1).count() == 0
    )


def _persisted_relations_match_exactly(observed: DataFrame, expected: DataFrame) -> bool:
    if observed.schema.jsonValue() != expected.schema.toNullable().jsonValue():
        return False
    return (
        observed.exceptAll(expected).limit(1).count() == 0
        and expected.exceptAll(observed).limit(1).count() == 0
    )


def _existing_output_matches(
    spark: SparkSession,
    target: Path,
    manifest_payload: bytes,
    manifest: Mapping[str, object],
    expected_outputs: Mapping[str, DataFrame],
    spark_aggregates_payload: bytes,
) -> bool:
    manifest_path = target / "finalized_run_manifest.json"
    if not manifest_path.is_file() or manifest_path.read_bytes() != manifest_payload:
        return False
    aggregate_path = target / "spark_aggregates.json"
    if not aggregate_path.is_file() or aggregate_path.read_bytes() != spark_aggregates_payload:
        return False
    outputs = cast(dict[str, dict[str, int]], manifest["outputs"])
    try:
        for name in _OUTPUT_NAMES:
            path = target / name
            if not (path / "_SUCCESS").is_file():
                return False
            observed = spark.read.parquet(str(path))
            if observed.count() != outputs[name]["row_count"]:
                return False
            if not _persisted_relations_match_exactly(observed, expected_outputs[name]):
                return False
        existing_gold = spark.read.parquet(str(target / "canonical_gold"))
        return _logical_gold_sha256(existing_gold) == manifest["canonical_gold_identity_sha256"]
    except Exception:
        return False


def _protected_finalization_roots(
    snapshot_directory: Path,
    protected_roots: tuple[Path, ...],
) -> tuple[Path, ...]:
    candidates = [snapshot_directory.expanduser().resolve()]
    candidates.extend(root.expanduser().resolve() for root in protected_roots)
    for variable in ("CUKD_XAI_ROOT", "TINYML_IDS_EVIDENCE_ROOT"):
        value = os.environ.get(variable)
        if value is not None and value.strip():
            candidates.append(Path(value).expanduser().resolve())
    return tuple(dict.fromkeys(candidates))


def finalize_run(
    bronze: DataFrame,
    *,
    snapshot_directory: Path,
    completion_receipt_path: Path,
    output_parent: Path,
    protected_roots: tuple[Path, ...] = (),
    policy_config_path: Path = _DEFAULT_POLICY_CONFIG,
) -> FinalizationResult:
    """Atomically publish canonical Gold, or accept an exact existing manifest."""
    all_protected_roots = _protected_finalization_roots(snapshot_directory, protected_roots)
    if not isinstance(completion_receipt_path, Path):
        raise GoldFinalizationError("completion receipt path must be a pathlib.Path")
    try:
        resolved_receipt = ensure_output_outside_evidence_roots(
            completion_receipt_path,
            all_protected_roots,
        )
    except EvidenceContractError as error:
        raise GoldFinalizationError("completion receipt is inside an evidence root") from error
    try:
        completion_receipt, completion_receipt_sha256 = load_completion_receipt(resolved_receipt)
    except ReplayReceiptError as error:
        raise GoldFinalizationError(f"invalid completion receipt: {error}") from error
    try:
        parent = ensure_output_outside_evidence_roots(output_parent, all_protected_roots)
    except EvidenceContractError as error:
        raise GoldFinalizationError("Gold output is inside an evidence root") from error
    gold, proof = prepare_exact_gold(
        bronze,
        snapshot_directory=snapshot_directory,
        completion_receipt=completion_receipt,
    )
    contract = _read_contract(snapshot_directory, completion_receipt)
    try:
        policy_contract = validate_policy_config(policy_config_path)
    except ValueError as error:
        raise GoldFinalizationError(f"invalid policy configuration: {error}") from error
    outputs = _build_finalized_outputs(gold)
    output_counts = {name: frame.count() for name, frame in outputs.items()}
    spark_aggregates_payload = _spark_aggregates_payload(outputs)
    run_id = cast(str, contract.receipt["replay_run_id"])
    if _RUN_ID.fullmatch(run_id) is None:
        raise GoldFinalizationError("replay_run_id is unsafe for a publication directory")
    parent.mkdir(parents=True, exist_ok=True)
    target = parent / f"replay_run_id={run_id}"
    manifest = _manifest(
        contract,
        proof,
        gold,
        output_counts,
        policy_contract.sha256,
        policy_contract.policy_ids,
        completion_receipt_sha256=completion_receipt_sha256,
        spark_aggregates_sha256=sha256(spark_aggregates_payload).hexdigest(),
    )
    manifest_payload = _canonical_bytes(manifest)
    target_manifest = target / "finalized_run_manifest.json"
    claim = parent / f".{target.name}.publication-claim"
    staging = parent / f".{target.name}.staging-{uuid.uuid4().hex}"
    claim_owned = False
    try:
        try:
            claim.mkdir()
            claim_owned = True
        except FileExistsError:
            if target.exists() and _existing_output_matches(
                bronze.sparkSession,
                target,
                manifest_payload,
                manifest,
                outputs,
                spark_aggregates_payload,
            ):
                return FinalizationResult(target, target_manifest, False)
            raise GoldFinalizationError("Gold publication is in progress or conflicted") from None

        if target.exists():
            if _existing_output_matches(
                bronze.sparkSession,
                target,
                manifest_payload,
                manifest,
                outputs,
                spark_aggregates_payload,
            ):
                return FinalizationResult(target, target_manifest, False)
            raise GoldFinalizationError("Gold destination contains incompatible output")

        for name, frame in outputs.items():
            frame.write.mode("errorifexists").parquet(str(staging / name))
        (staging / "spark_aggregates.json").write_bytes(spark_aggregates_payload)
        (staging / "finalized_run_manifest.json").write_bytes(manifest_payload)
        if target.exists():
            if _existing_output_matches(
                bronze.sparkSession,
                target,
                manifest_payload,
                manifest,
                outputs,
                spark_aggregates_payload,
            ):
                return FinalizationResult(target, target_manifest, False)
            raise GoldFinalizationError("Gold atomic publication encountered a conflict")
        try:
            os.rename(staging, target)
        except OSError as error:
            if _existing_output_matches(
                bronze.sparkSession,
                target,
                manifest_payload,
                manifest,
                outputs,
                spark_aggregates_payload,
            ):
                return FinalizationResult(target, target_manifest, False)
            raise GoldFinalizationError("Gold atomic publication encountered a conflict") from error
        return FinalizationResult(target, target_manifest, True)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
        if claim_owned:
            claim.rmdir()
