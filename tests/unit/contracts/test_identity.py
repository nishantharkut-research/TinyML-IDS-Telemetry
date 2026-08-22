import pytest

from tinyml_ids_telemetry.contracts.identity import lineage_id, source_event_id

IDENTITY = {
    "protocol_id": "cukd_fgds_wifi_udp_session_v2",
    "export_id": "2b09ff",
    "wireless_bundle_id": "b8e465",
    "source_session_id": "800BB679",
    "source_stage_id": "B4DBB030",
}


def test_source_event_id_matches_frozen_canonical_digest() -> None:
    result = source_event_id(**IDENTITY, row_id=0)

    assert result == "b909ff7f8bd9da706448846f87ab21f29a22aa7855d9592dfa965571f3132652"


def test_lineage_id_excludes_row_identifier() -> None:
    result = lineage_id(**IDENTITY)

    assert result == "ef823feacd31a13d4420a49510a8bccb32d5dbe1166c8ca7141c6fe7225b21e5"


@pytest.mark.parametrize("field", tuple(IDENTITY))
def test_identity_rejects_empty_components(field: str) -> None:
    values = dict(IDENTITY)
    values[field] = ""

    with pytest.raises(ValueError, match=field):
        lineage_id(**values)


def test_identity_rejects_delimiter_inside_component() -> None:
    values = dict(IDENTITY)
    values["protocol_id"] = "invalid|protocol"

    with pytest.raises(ValueError, match="protocol_id"):
        lineage_id(**values)


def test_source_event_id_rejects_negative_row() -> None:
    with pytest.raises(ValueError, match="row_id"):
        source_event_id(**IDENTITY, row_id=-1)
