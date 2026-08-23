"""Replay delivery sinks with explicit lifecycle handling."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal, Protocol, Self

from tinyml_ids_telemetry.replay.schedule import DeliveryEnvelope


class SinkClosedError(RuntimeError):
    """Raised when a delivery is written after sink closure."""


class ReplaySink(Protocol):
    """Minimal destination contract for replay deliveries."""

    def write(self, envelope: DeliveryEnvelope) -> None: ...

    def close(self) -> None: ...


class MemorySink:
    """In-memory sink for deterministic replay tests and callers."""

    def __init__(self) -> None:
        self._deliveries: list[DeliveryEnvelope] = []
        self._closed = False

    @property
    def deliveries(self) -> tuple[DeliveryEnvelope, ...]:
        return tuple(self._deliveries)

    def write(self, envelope: DeliveryEnvelope) -> None:
        if self._closed:
            raise SinkClosedError("memory sink is closed")
        self._deliveries.append(envelope)

    def close(self) -> None:
        self._closed = True


class JsonlSink:
    """Atomically publish canonical delivery-envelope JSON Lines."""

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path.resolve()
        if self.output_path.exists():
            raise FileExistsError(f"JSONL destination already exists: {self.output_path}")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = NamedTemporaryFile(  # noqa: SIM115 - sink owns this handle lifecycle.
            mode="w+b",
            prefix=f".{self.output_path.name}.",
            suffix=".tmp",
            dir=self.output_path.parent,
            delete=False,
        )
        self._handle = temporary
        self._temporary_path = Path(temporary.name)
        self._closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> Literal[False]:
        if exception_type is None:
            self.close()
        else:
            self._abort()
        return False

    def write(self, envelope: DeliveryEnvelope) -> None:
        if self._closed:
            raise SinkClosedError("JSONL sink is closed")
        try:
            self._handle.write(envelope.to_json_bytes())
            self._handle.write(b"\n")
        except BaseException:
            self._abort()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._handle.flush()
            os.fsync(self._handle.fileno())
            self._handle.close()
            try:
                os.link(self._temporary_path, self.output_path)
            except FileExistsError as error:
                raise FileExistsError(
                    f"JSONL destination already exists: {self.output_path}"
                ) from error
            self._temporary_path.unlink()
        except BaseException:
            if not self._handle.closed:
                self._handle.close()
            self._temporary_path.unlink(missing_ok=True)
            raise

    def _abort(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._handle.close()
        finally:
            self._temporary_path.unlink(missing_ok=True)
