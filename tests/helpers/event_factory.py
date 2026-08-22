from __future__ import annotations

from hashlib import sha256

from tinyml_ids_telemetry.contracts.event import TelemetryEvent


def make_event(
    *,
    row_id: int = 0,
    board: str = "fixture-board",
    student_model: str = "student_A",
    true_class_id: int = 3,
    predicted_class_id: int = 3,
    margin_units: int = 5_120,
    logit_frac_bits: int = 9,
    status: str = "OK",
    attempts: int = 1,
    response_timeout_count: int = 0,
    ignored_datagram_count: int = 0,
) -> TelemetryEvent:
    logits = [-512, -512, -512, -512, -512]
    runner_up = 0 if predicted_class_id != 0 else 3
    logits[runner_up] = 0
    logits[predicted_class_id] = margin_units
    lineage_id = sha256(f"fixture-lineage|{board}|{student_model}".encode("ascii")).hexdigest()
    event_id = sha256(f"fixture-event|{board}|{student_model}|{row_id}".encode("ascii")).hexdigest()
    return TelemetryEvent(
        source_event_id=event_id,
        lineage_id=lineage_id,
        source_session_id="fixture-session",
        source_stage_id="fixture-stage",
        row_id=row_id,
        source_row_index=row_id,
        board=board,
        student_model=student_model,
        status=status,
        true_class_id=true_class_id,
        predicted_class_id=predicted_class_id,
        fixed_predicted_class_id=predicted_class_id,
        fp32_predicted_class_id=predicted_class_id,
        class_names=("Blackhole", "Flooding", "Grayhole", "Normal", "TDMA"),
        normal_class_id=3,
        fixed_logits=tuple(logits),
        logit_frac_bits=logit_frac_bits,
        preprocess_us=10,
        inference_us=20,
        total_us=30,
        attempts=attempts,
        response_timeout_count=response_timeout_count,
        ignored_datagram_count=ignored_datagram_count,
        ignored_wrong_endpoint_count=0,
        ignored_bad_length_count=0,
        ignored_non_ascii_count=0,
        ignored_malformed_count=0,
        ignored_bad_prefix_count=0,
        ignored_bad_identity_count=0,
        ignored_bad_attempt_count=0,
        ignored_bad_inner_hex_count=0,
        ignored_bad_crc_count=0,
        ignored_bad_inner_text_count=0,
        ignored_wrong_session_count=0,
        ignored_wrong_stage_count=0,
        ignored_wrong_transaction_count=0,
        ignored_wrong_attempt_count=0,
        successful_request_bytes=200,
        request_datagram_bytes_sent=200,
        response_bytes=160,
        host_observed_datagram_rtt_us=1_000,
        transaction_elapsed_us=1_000,
    )
