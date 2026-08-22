from __future__ import annotations

from pathlib import Path

from tinyml_ids_telemetry.evidence.hashes import sha256_handle, sha256_size_handle


def test_sha256_handle_hashes_current_file_and_rewinds(tmp_path: Path) -> None:
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"immutable-evidence")

    with path.open("rb") as handle:
        digest = sha256_handle(handle)

        assert digest == "6da17499ca0ebd2dd4cfb96bcfc0dcf69e30e38658d3f5926bac537308ece100"
        assert handle.tell() == 0
        assert handle.read() == b"immutable-evidence"


def test_sha256_size_handle_returns_digest_and_size_from_one_handle(tmp_path: Path) -> None:
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"immutable-evidence")

    with path.open("rb") as handle:
        digest, size = sha256_size_handle(handle)

        assert digest == "6da17499ca0ebd2dd4cfb96bcfc0dcf69e30e38658d3f5926bac537308ece100"
        assert size == 18
        assert handle.tell() == 0
