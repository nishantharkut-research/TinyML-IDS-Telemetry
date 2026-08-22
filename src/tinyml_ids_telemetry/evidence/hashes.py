"""Streaming file hashing for evidence verification."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Protocol


class SeekableBinaryReader(Protocol):
    def read(self, size: int = -1) -> bytes: ...

    def seek(self, offset: int, whence: int = 0) -> int: ...


def sha256_size_handle(
    handle: SeekableBinaryReader,
    *,
    block_size: int = 1024 * 1024,
) -> tuple[str, int]:
    """Hash and size one open binary file identity, then rewind it."""
    handle.seek(0)
    digest = sha256()
    size = 0
    for block in iter(lambda: handle.read(block_size), b""):
        digest.update(block)
        size += len(block)
    handle.seek(0)
    return digest.hexdigest(), size


def sha256_handle(handle: SeekableBinaryReader, *, block_size: int = 1024 * 1024) -> str:
    """Hash one open binary file identity and rewind it for subsequent parsing."""
    return sha256_size_handle(handle, block_size=block_size)[0]


def sha256_size_file(path: Path, *, block_size: int = 1024 * 1024) -> tuple[str, int]:
    """Return SHA-256 and byte size from one open file handle."""
    with path.open("rb") as handle:
        return sha256_size_handle(handle, block_size=block_size)


def sha256_file(path: Path, *, block_size: int = 1024 * 1024) -> str:
    """Return a lowercase SHA-256 digest without loading the file into memory."""
    return sha256_size_file(path, block_size=block_size)[0]
