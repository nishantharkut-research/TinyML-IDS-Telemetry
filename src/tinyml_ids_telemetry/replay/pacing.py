"""Monotonic wall-clock pacing for replay deliveries."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from time import monotonic as _monotonic
from time import sleep as _sleep


@dataclass(slots=True)
class DeliveryPacer:
    """Wait for delivery deadlines anchored to the first delivery."""

    rate_per_second: int
    monotonic: Callable[[], float] = _monotonic
    sleep: Callable[[float], None] = _sleep
    _start: float | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if type(self.rate_per_second) is not int or self.rate_per_second <= 0:
            raise ValueError("rate_per_second must be a positive integer")

    def wait_for_delivery(self, delivery_index: int) -> None:
        """Wait until ``start + delivery_index / rate`` using monotonic time."""
        if type(delivery_index) is not int or delivery_index < 0:
            raise ValueError("delivery_index must be a non-negative integer")
        if self._start is None:
            self._start = self.monotonic()
        deadline = self._start + delivery_index / self.rate_per_second
        remaining = deadline - self.monotonic()
        if remaining > 0:
            self.sleep(remaining)
