"""Streaming file hashing for evidence verification."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path


def sha256_file(path: Path, *, block_size: int = 1024 * 1024) -> str:
    """Return a lowercase SHA-256 digest without loading the file into memory."""
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(block_size), b""):
            digest.update(block)
    return digest.hexdigest()
