"""Token bucket rate limiter."""

from __future__ import annotations

import threading
import time


class RateLimiter:
    """Token bucket rate limiter.

    Args:
        max_per_second: Maximum number of tokens (calls) per second.
    """

    def __init__(self, max_per_second: int):
        self.max_per_second = max_per_second
        self._tokens = float(max_per_second)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._min_interval = 1.0 / max_per_second

    def acquire(self) -> None:
        """Block until a token is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens += elapsed * self.max_per_second
                if self._tokens > self.max_per_second:
                    self._tokens = float(self.max_per_second)
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Calculate wait time
                wait = (1.0 - self._tokens) / self.max_per_second

            time.sleep(wait)

    async def async_acquire(self) -> None:
        """Async version of acquire."""
        import asyncio

        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens += elapsed * self.max_per_second
                if self._tokens > self.max_per_second:
                    self._tokens = float(self.max_per_second)
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.max_per_second

            await asyncio.sleep(wait)
