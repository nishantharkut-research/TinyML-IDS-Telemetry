import json
import tomllib
from importlib import resources
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from tinyml_ids_telemetry.contracts.event import TelemetryEvent
from tinyml_ids_telemetry.contracts.schema import SchemaContractError, validate_event_mapping

PROJECT_ROOT = Path(__file__).parents[2]
ROOT_SCHEMA_PATH = PROJECT_ROOT / "schemas" / "telemetry-event-v1.schema.json"


def _event_schema() -> dict[str, object]:
    schema_resource = resources.files("tinyml_ids_telemetry").joinpath(
        "schemas", "telemetry-event-v1.schema.json"
    )
    return json.loads(schema_resource.read_text(encoding="utf-8"))


def make_event(**changes: object) -> TelemetryEvent:
    values: dict[str, object] = {
        "source_event_id": "a" * 64,
        "lineage_id": "b" * 64,
        "source_session_id": "800BB679CF655CC17125D62D04EC04F3",
        "source_stage_id": "B4DBB03070AE393B",
        "row_id": 0,
        "source_row_index": 8,
        "board": "esp32c3",
        "student_model": "student_A",
        "status": "OK",
        "true_class_id": 3,
        "predicted_class_id": 3,
        "fixed_predicted_class_id": 3,
        "fp32_predicted_class_id": 3,
        "class_names": ("Blackhole", "Flooding", "Grayhole", "Normal", "TDMA"),
        "normal_class_id": 3,
        "fixed_logits": (-5297, -5038, -1654, 4441, 1112),
        "logit_frac_bits": 9,
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
    }
    values.update(changes)
    return TelemetryEvent(**values)  # type: ignore[arg-type]


def test_serialized_event_validates_against_versioned_json_schema() -> None:
    validator = Draft202012Validator(_event_schema())

    errors = list(validator.iter_errors(make_event().to_dict()))

    assert errors == []


def test_event_schema_has_one_packaged_authoritative_copy() -> None:
    schema_resource = resources.files("tinyml_ids_telemetry").joinpath(
        "schemas", "telemetry-event-v1.schema.json"
    )

    assert schema_resource.is_file()
    assert not ROOT_SCHEMA_PATH.exists()


def test_jsonschema_is_a_runtime_dependency() -> None:
    configuration = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert "jsonschema>=4.23,<5" in configuration["project"]["dependencies"]
    assert "jsonschema>=4.23,<5" not in configuration["project"]["optional-dependencies"]["dev"]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda event: event.pop("total_us"), "total_us"),
        (lambda event: event.update({"replay_run_id": "not-source-data"}), "replay_run_id"),
    ],
)
def test_full_event_mapping_validation_fails_closed(
    mutation: object,
    message: str,
) -> None:
    event = make_event().to_dict()
    mutation(event)  # type: ignore[operator]

    with pytest.raises(SchemaContractError, match=message):
        validate_event_mapping(event)


def test_event_serialization_includes_derived_fields() -> None:
    event = make_event()

    serialized = event.to_dict()

    assert serialized["schema_version"] == "telemetry-event-v1"
    assert serialized["true_class_name"] == "Normal"
    assert serialized["predicted_class_name"] == "Normal"
    assert serialized["is_predicted_attack"] is False
    assert serialized["mcu_fixed_prediction_match"] is True
    assert serialized["mcu_fixed_logits_match"] is True
    assert serialized["normalized_logit_margin"] == pytest.approx((4441 - 1112) / 512)


def test_event_rejects_non_hex_source_identifier() -> None:
    with pytest.raises(ValueError, match="source_event_id"):
        make_event(source_event_id="not-a-digest")


def test_event_rejects_wrong_logit_count() -> None:
    with pytest.raises(ValueError, match="fixed_logits"):
        make_event(fixed_logits=(1, 2, 3, 4))


def test_event_rejects_prediction_that_is_not_first_argmax() -> None:
    with pytest.raises(ValueError, match="predicted_class_id"):
        make_event(predicted_class_id=1)


def test_event_uses_first_index_to_break_logit_ties() -> None:
    event = make_event(
        predicted_class_id=0,
        fixed_predicted_class_id=0,
        fp32_predicted_class_id=0,
        fixed_logits=(10, 10, 0, -1, -2),
    )

    assert event.predicted_class_id == 0


def test_event_rejects_inconsistent_compute_total() -> None:
    with pytest.raises(ValueError, match="total_us"):
        make_event(total_us=999)


def test_event_rejects_negative_counter() -> None:
    with pytest.raises(ValueError, match="response_timeout_count"):
        make_event(response_timeout_count=-1)


def test_event_rejects_normal_class_mapping_mismatch() -> None:
    with pytest.raises(ValueError, match="normal_class_id"):
        make_event(normal_class_id=2)
