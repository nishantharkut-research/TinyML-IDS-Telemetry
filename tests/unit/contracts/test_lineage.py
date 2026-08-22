import pytest

from tinyml_ids_telemetry.contracts.identity import lineage_id
from tinyml_ids_telemetry.contracts.lineage import LineageRecord

LINEAGE_ID = lineage_id(
    protocol_id="cukd_fgds_wifi_udp_session_v2",
    export_id="4" * 64,
    wireless_bundle_id="5" * 64,
    source_session_id="800BB679CF655CC17125D62D04EC04F3",
    source_stage_id="B4DBB03070AE393B",
)


def make_lineage(**changes: object) -> LineageRecord:
    values: dict[str, object] = {
        "lineage_id": LINEAGE_ID,
        "board": "esp32c3",
        "student_model": "student_A",
        "seed": 42,
        "experiment_protocol_id": "fgds_seed42_wifi_udp_three_stage_v2",
        "wireless_protocol_id": "cukd_fgds_wifi_udp_session_v2",
        "dataset_protocol_id": "wsnds_feature_group_split_train_only_scaler_5seed_v1",
        "dataset_sha256": "1" * 64,
        "test_split_sha256": "2" * 64,
        "scaler_sha256": "3" * 64,
        "export_id": "4" * 64,
        "wireless_bundle_id": "5" * 64,
        "model_file_sha256": "6" * 64,
        "model_artifact_sha256": "7" * 64,
        "firmware_binary_sha256": "8" * 64,
        "source_session_id": "800BB679CF655CC17125D62D04EC04F3",
        "source_stage_id": "B4DBB03070AE393B",
        "expected_rows": 56_301,
        "class_names": ("Blackhole", "Flooding", "Grayhole", "Normal", "TDMA"),
        "normal_class_id": 3,
        "logit_frac_bits": 9,
        "mcu_csv_sha256": "9" * 64,
        "reference_csv_sha256": "a" * 64,
        "completion_manifest_sha256": "b" * 64,
        "compile_record_sha256": "c" * 64,
        "mcu_relative_path": "results/session/full_56301_mcu.csv",
        "reference_relative_path": "results/reference/hil_reference_predictions.csv",
    }
    values.update(changes)
    return LineageRecord(**values)  # type: ignore[arg-type]


def test_lineage_accepts_computed_identity() -> None:
    lineage = make_lineage()

    assert lineage.lineage_id == lineage.computed_lineage_id


def test_lineage_rejects_identity_that_disagrees_with_fields() -> None:
    with pytest.raises(ValueError, match="lineage_id"):
        make_lineage(lineage_id="b" * 64)


def test_lineage_rejects_non_sha_source_hash() -> None:
    with pytest.raises(ValueError, match="dataset_sha256"):
        make_lineage(dataset_sha256="bad")


def test_lineage_rejects_non_positive_row_count() -> None:
    with pytest.raises(ValueError, match="expected_rows"):
        make_lineage(expected_rows=0)


def test_lineage_rejects_normal_mapping_mismatch() -> None:
    with pytest.raises(ValueError, match="normal_class_id"):
        make_lineage(normal_class_id=2)


def test_lineage_serialization_uses_version_and_json_lists() -> None:
    lineage = make_lineage()

    serialized = lineage.to_dict()

    assert serialized["schema_version"] == "telemetry-lineage-v1"
    assert serialized["class_names"] == ["Blackhole", "Flooding", "Grayhole", "Normal", "TDMA"]
