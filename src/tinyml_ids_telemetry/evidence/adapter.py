"""Streaming MCU/reference join into canonical telemetry events."""

from __future__ import annotations

import csv
from collections.abc import Iterator
from itertools import zip_longest
from pathlib import Path, PurePosixPath

from tinyml_ids_telemetry.contracts.event import TelemetryEvent
from tinyml_ids_telemetry.contracts.identity import source_event_id
from tinyml_ids_telemetry.contracts.lineage import LineageRecord
from tinyml_ids_telemetry.evidence.catalog import EvidenceContractError
from tinyml_ids_telemetry.evidence.hashes import sha256_file

_MCU_HEADER = (
    "row_id",
    "status",
    "predicted_class",
    "logits",
    "preprocess_us",
    "inference_us",
    "total_us",
    "attempts",
    "response_timeout_count",
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
    "successful_request_bytes",
    "request_datagram_bytes_sent",
    "response_bytes",
    "host_observed_datagram_rtt_us",
    "transaction_elapsed_us",
)

_REFERENCE_HEADER = (
    "row_id",
    "source_row_index",
    "true_label",
    "fixed_pred",
    "fp32_pred",
    "fixed_logit_0",
    "fixed_logit_1",
    "fixed_logit_2",
    "fixed_logit_3",
    "fixed_logit_4",
)


def _resolve_source_path(root: Path, relative_path: str, context: str) -> Path:
    portable = PurePosixPath(relative_path)
    path = (root / Path(*portable.parts)).resolve()
    if not path.is_relative_to(root):
        raise EvidenceContractError(f"{context} leaves the evidence root")
    return path


def _verify_hash(path: Path, expected: str, context: str) -> None:
    if not path.is_file() or sha256_file(path) != expected:
        raise EvidenceContractError(f"{context} SHA-256 mismatch")


def _required_text(row: dict[str, str | None], key: str, context: str) -> str:
    value = row.get(key)
    if value is None or value == "":
        raise EvidenceContractError(f"{context}.{key} is missing")
    return value


def _required_int(row: dict[str, str | None], key: str, context: str) -> int:
    value = _required_text(row, key, context)
    try:
        return int(value)
    except ValueError as error:
        raise EvidenceContractError(f"{context}.{key} is not an integer") from error


def _validate_header(reader: csv.DictReader[str], expected: tuple[str, ...], context: str) -> None:
    if tuple(reader.fieldnames or ()) != expected:
        raise EvidenceContractError(f"{context} header does not match the source contract")


def iter_lineage_events(
    evidence_root: Path,
    lineage: LineageRecord,
) -> Iterator[TelemetryEvent]:
    """Yield one validated event per contiguous source row without bulk loading."""
    root = evidence_root.resolve()
    mcu_path = _resolve_source_path(root, lineage.mcu_relative_path, "mcu_csv")
    reference_path = _resolve_source_path(root, lineage.reference_relative_path, "reference_csv")
    _verify_hash(mcu_path, lineage.mcu_csv_sha256, "mcu_csv")
    _verify_hash(reference_path, lineage.reference_csv_sha256, "reference_csv")

    with (
        mcu_path.open("r", newline="", encoding="utf-8") as mcu_handle,
        reference_path.open("r", newline="", encoding="utf-8") as reference_handle,
    ):
        mcu_reader = csv.DictReader(mcu_handle)
        reference_reader = csv.DictReader(reference_handle)
        _validate_header(mcu_reader, _MCU_HEADER, "mcu_csv")
        _validate_header(reference_reader, _REFERENCE_HEADER, "reference_csv")

        count = 0
        for expected_row_id, pair in enumerate(
            zip_longest(mcu_reader, reference_reader, fillvalue=None)
        ):
            mcu_row, reference_row = pair
            if mcu_row is None or reference_row is None:
                raise EvidenceContractError("MCU and reference row counts do not match")
            context = f"row[{expected_row_id}]"
            mcu_row_id = _required_int(mcu_row, "row_id", context)
            reference_row_id = _required_int(reference_row, "row_id", context)
            if mcu_row_id != expected_row_id or reference_row_id != expected_row_id:
                raise EvidenceContractError("Source row IDs must be contiguous from zero")

            mcu_prediction = _required_int(mcu_row, "predicted_class", context)
            fixed_prediction = _required_int(reference_row, "fixed_pred", context)
            if mcu_prediction != fixed_prediction:
                raise EvidenceContractError(f"{context} MCU/fixed prediction mismatch")

            try:
                mcu_logits = tuple(
                    int(value) for value in _required_text(mcu_row, "logits", context).split()
                )
            except ValueError as error:
                raise EvidenceContractError(f"{context}.logits contains a non-integer") from error
            fixed_logits = tuple(
                _required_int(reference_row, f"fixed_logit_{index}", context)
                for index in range(len(lineage.class_names))
            )
            if mcu_logits != fixed_logits:
                raise EvidenceContractError(f"{context} MCU/fixed logit mismatch")

            event_identifier = source_event_id(
                protocol_id=lineage.wireless_protocol_id,
                export_id=lineage.export_id,
                wireless_bundle_id=lineage.wireless_bundle_id,
                source_session_id=lineage.source_session_id,
                source_stage_id=lineage.source_stage_id,
                row_id=mcu_row_id,
            )
            try:
                yield TelemetryEvent(
                    source_event_id=event_identifier,
                    lineage_id=lineage.lineage_id,
                    source_session_id=lineage.source_session_id,
                    source_stage_id=lineage.source_stage_id,
                    row_id=mcu_row_id,
                    source_row_index=_required_int(reference_row, "source_row_index", context),
                    board=lineage.board,
                    student_model=lineage.student_model,
                    status=_required_text(mcu_row, "status", context),
                    true_class_id=_required_int(reference_row, "true_label", context),
                    predicted_class_id=mcu_prediction,
                    fixed_predicted_class_id=fixed_prediction,
                    fp32_predicted_class_id=_required_int(reference_row, "fp32_pred", context),
                    class_names=lineage.class_names,
                    normal_class_id=lineage.normal_class_id,
                    fixed_logits=fixed_logits,
                    logit_frac_bits=lineage.logit_frac_bits,
                    preprocess_us=_required_int(mcu_row, "preprocess_us", context),
                    inference_us=_required_int(mcu_row, "inference_us", context),
                    total_us=_required_int(mcu_row, "total_us", context),
                    attempts=_required_int(mcu_row, "attempts", context),
                    response_timeout_count=_required_int(
                        mcu_row, "response_timeout_count", context
                    ),
                    ignored_datagram_count=_required_int(
                        mcu_row, "ignored_datagram_count", context
                    ),
                    ignored_wrong_endpoint_count=_required_int(
                        mcu_row, "ignored_wrong_endpoint_count", context
                    ),
                    ignored_bad_length_count=_required_int(
                        mcu_row, "ignored_bad_length_count", context
                    ),
                    ignored_non_ascii_count=_required_int(
                        mcu_row, "ignored_non_ascii_count", context
                    ),
                    ignored_malformed_count=_required_int(
                        mcu_row, "ignored_malformed_count", context
                    ),
                    ignored_bad_prefix_count=_required_int(
                        mcu_row, "ignored_bad_prefix_count", context
                    ),
                    ignored_bad_identity_count=_required_int(
                        mcu_row, "ignored_bad_identity_count", context
                    ),
                    ignored_bad_attempt_count=_required_int(
                        mcu_row, "ignored_bad_attempt_count", context
                    ),
                    ignored_bad_inner_hex_count=_required_int(
                        mcu_row, "ignored_bad_inner_hex_count", context
                    ),
                    ignored_bad_crc_count=_required_int(
                        mcu_row, "ignored_bad_crc_count", context
                    ),
                    ignored_bad_inner_text_count=_required_int(
                        mcu_row, "ignored_bad_inner_text_count", context
                    ),
                    ignored_wrong_session_count=_required_int(
                        mcu_row, "ignored_wrong_session_count", context
                    ),
                    ignored_wrong_stage_count=_required_int(
                        mcu_row, "ignored_wrong_stage_count", context
                    ),
                    ignored_wrong_transaction_count=_required_int(
                        mcu_row, "ignored_wrong_transaction_count", context
                    ),
                    ignored_wrong_attempt_count=_required_int(
                        mcu_row, "ignored_wrong_attempt_count", context
                    ),
                    successful_request_bytes=_required_int(
                        mcu_row, "successful_request_bytes", context
                    ),
                    request_datagram_bytes_sent=_required_int(
                        mcu_row, "request_datagram_bytes_sent", context
                    ),
                    response_bytes=_required_int(mcu_row, "response_bytes", context),
                    host_observed_datagram_rtt_us=_required_int(
                        mcu_row, "host_observed_datagram_rtt_us", context
                    ),
                    transaction_elapsed_us=_required_int(
                        mcu_row, "transaction_elapsed_us", context
                    ),
                )
            except ValueError as error:
                raise EvidenceContractError(
                    f"{context} violates the event contract: {error}"
                ) from error
            count += 1

        if count != lineage.expected_rows:
            raise EvidenceContractError(
                f"Source row count mismatch: expected {lineage.expected_rows}, got {count}"
            )
