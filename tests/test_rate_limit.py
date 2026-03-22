"""Tests for rate limiter."""

import time
import threading
import pytest
from batchly.rate_limit import RateLimiter


class TestRateLimiter:
    def test_basic_acquire(self):
        limiter = RateLimiter(max_per_second=100)
        start = time.monotonic()
        for _ in range(10):
            limiter.acquire()
        elapsed = time.monotonic() - start
        # 10 calls at 100/s should be fast
        assert elapsed < 0.5

    def test_rate_limiting(self):
        # 5 per second — after initial burst, should see throttling
        limiter = RateLimiter(max_per_second=5)
        # Drain initial tokens
        for _ in range(5):
            limiter.acquire()
        start = time.monotonic()
        for _ in range(5):
            limiter.acquire()
        elapsed = time.monotonic() - start
        # Should take at least ~0.8s for 5 more at 5/s
        assert elapsed >= 0.5

    def test_concurrent_acquires(self):
        limiter = RateLimiter(max_per_second=50)
        count = {"n": 0}
        lock = threading.Lock()

        def worker():
            for _ in range(10):
                limiter.acquire()
                with lock:
                    count["n"] += 1

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert count["n"] == 50

    def test_single_call_no_delay(self):
        limiter = RateLimiter(max_per_second=1000)
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    def test_refill_tokens(self):
        limiter = RateLimiter(max_per_second=1000)
        # Drain tokens
        for _ in range(5):
            limiter.acquire()

        time.sleep(0.01)  # let tokens refill
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1  # should have tokens after refill

    def test_async_acquire(self):
        import asyncio

        limiter = RateLimiter(max_per_second=100)

        async def run():
            for _ in range(5):
                await limiter.async_acquire()

        start = time.monotonic()
        asyncio.run(run())
        elapsed = time.monotonic() - start
        assert elapsed < 0.5

    def test_async_concurrent(self):
        import asyncio

        limiter = RateLimiter(max_per_second=50)

        async def worker(n):
            for _ in range(n):
                await limiter.async_acquire()

        async def main():
            await asyncio.gather(worker(5), worker(5), worker(5))

        start = time.monotonic()
        asyncio.run(main())
        elapsed = time.monotonic() - start
        # 15 calls at 50/s should be fast
        assert elapsed < 1.0

    def test_very_low_rate(self):
        limiter = RateLimiter(max_per_second=2)
        start = time.monotonic()
        for _ in range(4):
            limiter.acquire()
        elapsed = time.monotonic() - start
        # 4 calls at 2/s → ~1.5s min
        assert elapsed >= 0.8

    def test_max_tokens_cap(self):
        """Tokens should not exceed max_per_second."""
        limiter = RateLimiter(max_per_second=10)
        time.sleep(0.1)
        # After sleep, tokens should be at most max_per_second
        with limiter._lock:
            assert limiter._tokens <= limiter.max_per_second
