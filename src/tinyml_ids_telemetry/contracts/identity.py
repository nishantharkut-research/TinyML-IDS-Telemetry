"""Deterministic source and lineage identifiers."""

from __future__ import annotations

from hashlib import sha256

_DELIMITER = "|"


def _validated_component(name: str, value: str) -> str:
    if not value or _DELIMITER in value:
        raise ValueError(f"{name} must be non-empty and cannot contain '{_DELIMITER}'")
    return value


def _lineage_payload(
    *,
    protocol_id: str,
    export_id: str,
    wireless_bundle_id: str,
    source_session_id: str,
    source_stage_id: str,
) -> str:
    components = (
        _validated_component("protocol_id", protocol_id),
        _validated_component("export_id", export_id),
        _validated_component("wireless_bundle_id", wireless_bundle_id),
        _validated_component("source_session_id", source_session_id),
        _validated_component("source_stage_id", source_stage_id),
    )
    return _DELIMITER.join(components)


def lineage_id(
    *,
    protocol_id: str,
    export_id: str,
    wireless_bundle_id: str,
    source_session_id: str,
    source_stage_id: str,
) -> str:
    """Return the stable SHA-256 identifier for one physical source lineage."""
    payload = _lineage_payload(
        protocol_id=protocol_id,
        export_id=export_id,
        wireless_bundle_id=wireless_bundle_id,
        source_session_id=source_session_id,
        source_stage_id=source_stage_id,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def source_event_id(
    *,
    protocol_id: str,
    export_id: str,
    wireless_bundle_id: str,
    source_session_id: str,
    source_stage_id: str,
    row_id: int,
) -> str:
    """Return the stable SHA-256 identifier for one source execution row."""
    if row_id < 0:
        raise ValueError("row_id must be non-negative")
    lineage = _lineage_payload(
        protocol_id=protocol_id,
        export_id=export_id,
        wireless_bundle_id=wireless_bundle_id,
        source_session_id=source_session_id,
        source_stage_id=source_stage_id,
    )
    payload = f"{lineage}{_DELIMITER}{row_id}"
    return sha256(payload.encode("utf-8")).hexdigest()
