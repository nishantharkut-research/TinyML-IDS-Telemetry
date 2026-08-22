from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

MCU_HEADER = (
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(65_536), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return path


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def create_evidence_tree(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "evidence"
    session_dir = root / "results/sessions/esp32c3_A"
    compile_dir = root / "results/compile"
    artifact_dir = compile_dir / "esp32c3_A_artifacts"
    model_dir = root / "models/student_A"
    for directory in (session_dir, artifact_dir, model_dir):
        directory.mkdir(parents=True, exist_ok=True)

    mcu_path = session_dir / "full_mcu.csv"
    with mcu_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MCU_HEADER)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "row_id": 0,
                    "status": "OK",
                    "predicted_class": 3,
                    "logits": "-5297 -5038 -1654 4441 1112",
                    "preprocess_us": 19,
                    "inference_us": 143,
                    "total_us": 162,
                    "attempts": 1,
                    "response_timeout_count": 0,
                    "ignored_datagram_count": 0,
                    "ignored_wrong_endpoint_count": 0,
                    "ignored_bad_length_count": 0,
                    "ignored_non_ascii_count": 0,
                    "ignored_malformed_count": 0,
                    "ignored_bad_prefix_count": 0,
                    "ignored_bad_identity_count": 0,
                    "ignored_bad_attempt_count": 0,
                    "ignored_bad_inner_hex_count": 0,
                    "ignored_bad_crc_count": 0,
                    "ignored_bad_inner_text_count": 0,
                    "ignored_wrong_session_count": 0,
                    "ignored_wrong_stage_count": 0,
                    "ignored_wrong_transaction_count": 0,
                    "ignored_wrong_attempt_count": 0,
                    "successful_request_bytes": 238,
                    "request_datagram_bytes_sent": 238,
                    "response_bytes": 196,
                    "host_observed_datagram_rtt_us": 9580,
                    "transaction_elapsed_us": 9580,
                },
                {
                    "row_id": 1,
                    "status": "OK",
                    "predicted_class": 3,
                    "logits": "-5190 -5464 248 1688 -1621",
                    "preprocess_us": 19,
                    "inference_us": 149,
                    "total_us": 168,
                    "attempts": 2,
                    "response_timeout_count": 1,
                    "ignored_datagram_count": 0,
                    "ignored_wrong_endpoint_count": 0,
                    "ignored_bad_length_count": 0,
                    "ignored_non_ascii_count": 0,
                    "ignored_malformed_count": 0,
                    "ignored_bad_prefix_count": 0,
                    "ignored_bad_identity_count": 0,
                    "ignored_bad_attempt_count": 0,
                    "ignored_bad_inner_hex_count": 0,
                    "ignored_bad_crc_count": 0,
                    "ignored_bad_inner_text_count": 0,
                    "ignored_wrong_session_count": 0,
                    "ignored_wrong_stage_count": 0,
                    "ignored_wrong_transaction_count": 0,
                    "ignored_wrong_attempt_count": 0,
                    "successful_request_bytes": 244,
                    "request_datagram_bytes_sent": 488,
                    "response_bytes": 194,
                    "host_observed_datagram_rtt_us": 9208,
                    "transaction_elapsed_us": 19208,
                },
            ]
        )

    reference_path = artifact_dir / "hil_reference_predictions.csv"
    reference_path.write_text(
        "row_id,source_row_index,true_label,fixed_pred,fp32_pred,"
        "fixed_logit_0,fixed_logit_1,fixed_logit_2,fixed_logit_3,fixed_logit_4\n"
        "0,8,3,3,3,-5297,-5038,-1654,4441,1112\n"
        "1,10,3,3,3,-5190,-5464,248,1688,-1621\n",
        encoding="utf-8",
    )

    firmware_path = artifact_dir / "firmware.bin"
    firmware_path.write_bytes(b"fixture-firmware")

    strict_report_path = write_json(
        artifact_dir / "strict_export_report.json",
        {
            "status": "passed",
            "export_id": "4" * 64,
            "provenance": {
                "protocol_id": "wsnds_feature_group_split_train_only_scaler_5seed_v1",
                "student": "student_A",
                "seed": 42,
                "dataset_sha256": "1" * 64,
                "split_hashes": {"test": "2" * 64},
                "scaler_sha256": "3" * 64,
                "model_file_sha256": "6" * 64,
                "model_artifact_sha256": "7" * 64,
            },
            "fixed_point_calibration": {
                "layers": [{"index": 0, "output_frac": 8}, {"index": 1, "output_frac": 9}]
            },
        },
    )

    preprocess_path = write_json(
        model_dir / "preprocess_metadata.json",
        {
            "dataset": "WSN-DS",
            "class_names": ["Blackhole", "Flooding", "Grayhole", "Normal", "TDMA"],
            "split_sizes": {"test": 2},
            "protocol_id": "wsnds_feature_group_split_train_only_scaler_5seed_v1",
            "dataset_sha256": "1" * 64,
            "split_hashes": {"test": "2" * 64},
            "scaler_sha256": "3" * 64,
            "scaler_fit_partition": "train only",
        },
    )

    strict_manifest_path = write_json(
        artifact_dir / "strict_export_manifest.json",
        {
            "status": "passed",
            "export_id": "4" * 64,
            "protocol_id": "wsnds_feature_group_split_train_only_scaler_5seed_v1",
            "student": "student_A",
            "files": [
                {
                    "path": "preprocess_metadata.json",
                    "sha256": sha256_file(preprocess_path),
                },
                {
                    "path": "strict_export_report.json",
                    "sha256": sha256_file(strict_report_path),
                },
                {
                    "path": "hil_reference_predictions.csv",
                    "sha256": sha256_file(reference_path),
                },
            ],
        },
    )

    compile_path = write_json(
        compile_dir / "esp32c3_A.json",
        {
            "status": "passed",
            "protocol_id": "cukd_fgds_wifi_udp_session_v2",
            "student": "student_A",
            "board": "esp32c3",
            "export_id": "4" * 64,
            "wireless_bundle_id": "5" * 64,
            "firmware_binary_sha256": sha256_file(firmware_path),
            "portable_artifacts": {
                "firmware_binary": {
                    "path": "esp32c3_A_artifacts/firmware.bin",
                    "sha256": sha256_file(firmware_path),
                },
                "strict_export_report": {
                    "path": "esp32c3_A_artifacts/strict_export_report.json",
                    "sha256": sha256_file(strict_report_path),
                },
                "strict_export_manifest": {
                    "path": "esp32c3_A_artifacts/strict_export_manifest.json",
                    "sha256": sha256_file(strict_manifest_path),
                },
                "hil_reference_predictions": {
                    "path": "esp32c3_A_artifacts/hil_reference_predictions.csv",
                    "sha256": sha256_file(reference_path),
                },
            },
        },
    )

    completion_path = write_json(
        session_dir / "completion.json",
        {
            "status": "complete",
            "protocol_id": "fgds_seed42_wifi_udp_three_stage_v2",
            "wireless_application_protocol_id": "cukd_fgds_wifi_udp_session_v2",
            "board": "esp32c3",
            "student": "student_A",
            "export_id": "4" * 64,
            "wireless_bundle_id": "5" * 64,
            "session_id": "800BB679CF655CC17125D62D04EC04F3",
            "stage_evidence": {
                "full_56301": {
                    "rows": 2,
                    "stage_id": "B4DBB03070AE393B",
                    "mcu_csv_sha256": sha256_file(mcu_path),
                }
            },
            "full_stage_inferences": 2,
            "files": [
                {
                    "path": "full_mcu.csv",
                    "sha256": sha256_file(mcu_path),
                    "size_bytes": mcu_path.stat().st_size,
                }
            ],
        },
    )

    sealed_path = write_json(
        root / "results/final_report.json",
        {
            "status": "passed",
            "protocol_id": "cukd_fgds_wifi_udp_session_v2",
            "full_test_rows_per_board_model_pair": 2,
            "full_test_board_model_predictions": 2,
            "input_evidence": {
                "esp32c3_student_A": {
                    "completion_manifest_sha256": sha256_file(completion_path),
                    "compile_sha256": sha256_file(compile_path),
                    "session_id": "800BB679CF655CC17125D62D04EC04F3",
                    "export_id": "4" * 64,
                    "wireless_bundle_id": "5" * 64,
                }
            },
        },
    )
    assert sealed_path.is_file()
    assert preprocess_path.is_file()

    catalog_path = tmp_path / "catalog.toml"
    catalog_path.write_text(
        """
version = 1
sealed_report = "results/final_report.json"

[[models]]
name = "student_A"
preprocess_metadata = "models/student_A/preprocess_metadata.json"

[[lineages]]
label = "esp32c3_student_A"
board = "esp32c3"
student_model = "student_A"
completion_manifest = "results/sessions/esp32c3_A/completion.json"
compile_record = "results/compile/esp32c3_A.json"
mcu_csv = "results/sessions/esp32c3_A/full_mcu.csv"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return root, catalog_path
