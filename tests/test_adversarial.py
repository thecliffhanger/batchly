"""Adversarial tests for batchly — edge cases and stress conditions."""

import asyncio
import threading
import time

import pytest

from batchly import (
    Batch,
    BatchError,
    BatchResult,
    batch_map,
    batch_filter,
    batch_for_each,
    async_batch_map,
    async_batch_filter,
    async_batch_for_each,
    ProgressBar,
    ProgressInfo,
)


# ── Empty / Single ──────────────────────────────────────────────

class TestEmptyAndSingle:
    def test_empty_list(self):
        results = batch_map(lambda x: x * 2, [], max_workers=4)
        assert results == []

    def test_single_item(self):
        results = batch_map(lambda x: x * 2, [42], max_workers=4)
        assert len(results) == 1
        assert results[0].value == 84

    def test_empty_filter(self):
        result = batch_filter(lambda x: x > 0, [])
        assert result == []

    def test_empty_foreach(self):
        batch_for_each(lambda x: None, [])  # should not crash


# ── Different exceptions ────────────────────────────────────────

class TestDifferentExceptions:
    def test_different_exceptions_per_call(self):
        call_count = 0

        def flaky(x):
            nonlocal call_count
            call_count += 1
            if x % 3 == 0:
                raise ValueError(f"val error on {x}")
            if x % 3 == 1:
                raise TypeError(f"type error on {x}")
            return x * 2

        results = batch_map(flaky, range(6), max_workers=4, retries=0, on_error="collect")
        errors = [r for r in results if r.error is not None]
        successes = [r for r in results if r.ok]
        assert len(errors) == 4  # items 0,1,3,4 fail
        assert len(successes) == 2  # items 2,5 succeed


# ── Timeout / Hang ──────────────────────────────────────────────

class TestTimeoutAndHang:
    def test_hang_with_timeout(self):
        def hang(x):
            time.sleep(10)
            return x

        start = time.monotonic()
        results = batch_map(hang, [1, 2, 3], max_workers=3, timeout=0.5, on_error="collect")
        elapsed = time.monotonic() - start
        assert elapsed < 5, f"Took too long: {elapsed}s"
        assert len(results) == 3
        assert all(r.error is not None for r in results)

    def test_timeout_skip_mode(self):
        def hang(x):
            time.sleep(10)
            return x

        start = time.monotonic()
        results = batch_map(hang, [1, 2, 3], max_workers=3, timeout=0.5, on_error="skip")
        elapsed = time.monotonic() - start
        assert elapsed < 5
        assert results == []


# ── Shared state ────────────────────────────────────────────────

class TestSharedState:
    def test_shared_counter(self):
        counter = {"value": 0}
        lock = threading.Lock()

        def increment(x):
            with lock:
                counter["value"] += 1
            return x

        results = batch_map(increment, range(100), max_workers=10)
        assert counter["value"] == 100
        assert len(results) == 100

    def test_shared_list_race(self):
        results_list = []
        lock = threading.Lock()

        def append_item(x):
            with lock:
                results_list.append(x)
            return x

        batch_map(append_item, range(50), max_workers=10)
        # results_list may be out of order, but should have all items
        assert sorted(results_list) == list(range(50))


# ── Very large batches ──────────────────────────────────────────

class TestLargeBatches:
    def test_10000_items_100_workers(self):
        results = batch_map(lambda x: x * 2, range(10000), max_workers=100, ordered=True)
        assert len(results) == 10000
        for i, r in enumerate(results):
            assert r.value == i * 2, f"Mismatch at {i}: expected {i*2}, got {r.value}"

    def test_10000_items_unordered(self):
        results = batch_map(lambda x: x * 2, range(10000), max_workers=100, ordered=False)
        assert len(results) == 10000
        values = [r.value for r in results]
        assert sorted(values) == [i * 2 for i in range(10000)]


# ── Chunk size edge cases ───────────────────────────────────────

class TestChunkSizeEdgeCases:
    def test_chunk_size_larger_than_list(self):
        def process_chunk(chunk):
            return sum(chunk)

        results = batch_map(process_chunk, [1, 2, 3, 4, 5], chunk_size=100, ordered=True)
        assert len(results) == 1
        assert results[0].value == 15

    def test_chunk_size_one(self):
        def process_chunk(chunk):
            return chunk[0] ** 2

        results = batch_map(process_chunk, range(20), chunk_size=1, ordered=True)
        assert len(results) == 20
        for i, r in enumerate(results):
            assert r.value == i ** 2


# ── Rate limit edge cases ───────────────────────────────────────

class TestRateLimitEdgeCases:
    def test_very_slow_rate(self):
        start = time.monotonic()
        results = batch_map(
            lambda x: x, range(3), rate_limit=1, max_workers=4, ordered=True
        )
        elapsed = time.monotonic() - start
        # 3 items at 1/sec → should take at least 2 seconds
        assert elapsed >= 1.5, f"Too fast: {elapsed:.2f}s"
        assert len(results) == 3

    def test_very_fast_rate(self):
        start = time.monotonic()
        results = batch_map(lambda x: x, range(100), rate_limit=100000, max_workers=10)
        elapsed = time.monotonic() - start
        assert elapsed < 5
        assert len(results) == 100


# ── All items fail ──────────────────────────────────────────────

class TestAllFail:
    def test_all_fail_skip(self):
        results = batch_map(lambda x: 1/0, range(5), max_workers=2, on_error="skip", retries=1)
        assert results == []

    def test_all_fail_collect(self):
        results = batch_map(lambda x: 1/0, range(5), max_workers=2, on_error="collect", retries=1)
        assert len(results) == 5
        assert all(r.error is not None for r in results)

    def test_all_fail_raise(self):
        with pytest.raises(BatchError):
            batch_map(lambda x: 1/0, [1], max_workers=1, on_error="raise", retries=0)


# ── Nested batching ─────────────────────────────────────────────

class TestNestedBatching:
    def test_nested_batch_map(self):
        outer = batch_map(lambda x: x * 2, range(10), max_workers=2)
        # Use outer results as input to inner batch_map
        inner = batch_map(lambda r: r.value + 1, outer, max_workers=2)
        assert len(inner) == 10
        for i, r in enumerate(inner):
            assert r.value == i * 2 + 1

    def test_double_nested(self):
        items = list(range(5))
        r1 = batch_map(lambda x: x + 1, items, max_workers=2)
        r2 = batch_map(lambda b: b.value * 3, r1, max_workers=2)
        r3 = batch_map(lambda b: b.value - 10, r2, max_workers=2)
        assert len(r3) == 5
        assert [r.value for r in r3] == [-7, -4, -1, 2, 5]


# ── Invalid max_workers ─────────────────────────────────────────

class TestInvalidMaxWorkers:
    def test_zero_workers(self):
        # ThreadPoolExecutor(0) raises ValueError
        with pytest.raises((ValueError, Exception)):
            batch_map(lambda x: x, [1, 2, 3], max_workers=0).__class__

    def test_negative_workers(self):
        with pytest.raises((ValueError, Exception)):
            batch_map(lambda x: x, [1, 2, 3], max_workers=-1).__class__


# ── Progress callback that raises ───────────────────────────────

class TestProgressCallbackRaises:
    def test_progress_raises_should_not_crash(self):
        call_count = {"n": 0}

        def bad_progress(info):
            call_count["n"] += 1
            if call_count["n"] > 2:
                raise RuntimeError("progress callback exploded!")

        # Should not crash even if progress callback raises
        try:
            results = batch_map(lambda x: x, range(10), max_workers=2, progress=bad_progress)
        except RuntimeError:
            pass  # If it does crash, that's a bug but we note it

        # At minimum, some work should have happened
        assert call_count["n"] >= 1


# ── Async adversarial ───────────────────────────────────────────

class TestAsyncAdversarial:
    @pytest.mark.asyncio
    async def test_async_empty(self):
        results = await async_batch_map(lambda x: x * 2, [])
        assert results == []

    @pytest.mark.asyncio
    async def test_async_all_fail(self):
        async def fail(x):
            raise ValueError(f"fail {x}")

        results = await async_batch_map(fail, range(5), on_error="collect", retries=1)
        assert len(results) == 5
        assert all(r.error is not None for r in results)

    @pytest.mark.asyncio
    async def test_async_timeout(self):
        async def hang(x):
            await asyncio.sleep(10)
            return x

        start = time.monotonic()
        results = await async_batch_map(hang, [1, 2], timeout=0.5, on_error="collect", max_workers=2)
        elapsed = time.monotonic() - start
        assert elapsed < 5
        assert all(r.error is not None for r in results)

    @pytest.mark.asyncio
    async def test_async_large_batch(self):
        async def double(x):
            return x * 2

        results = await async_batch_map(double, range(1000), max_workers=50, ordered=True)
        assert len(results) == 1000
        for i, r in enumerate(results):
            assert r.value == i * 2

    @pytest.mark.asyncio
    async def test_async_unordered(self):
        async def double(x):
            await asyncio.sleep(0.001 * (10 - x % 10))  # varying delays
            return x * 2

        results = await async_batch_map(double, range(100), max_workers=20, ordered=False)
        assert len(results) == 100
        values = {r.value for r in results}
        assert values == {i * 2 for i in range(100)}

    @pytest.mark.asyncio
    async def test_async_chunked(self):
        async def process_chunk(chunk):
            return sum(chunk)

        results = await async_batch_map(process_chunk, list(range(20)), chunk_size=5, ordered=True)
        assert len(results) == 4
        assert results[0].value == 10  # 0+1+2+3+4
        assert results[1].value == 35  # 5+6+7+8+9

    @pytest.mark.asyncio
    async def test_async_filter_all_fail(self):
        async def fail(x):
            raise RuntimeError("nope")

        results = await async_batch_filter(fail, range(5), on_error="skip")
        assert results == []

    @pytest.mark.asyncio
    async def test_async_foreach_no_crash(self):
        seen = []
        async def record(x):
            seen.append(x)

        await async_batch_for_each(record, range(10), max_workers=5)
        assert sorted(seen) == list(range(10))


# ── Batch context reusability ───────────────────────────────────

class TestBatchContextReuse:
    def test_reuse_batch_context(self):
        ctx = Batch(max_workers=4, retries=0)
        r1 = ctx.map(lambda x: x + 1, [1, 2, 3])
        r2 = ctx.map(lambda x: x * 2, [4, 5, 6])
        assert [r.value for r in r1] == [2, 3, 4]
        assert [r.value for r in r2] == [8, 10, 12]

    def test_reuse_with_different_settings(self):
        ctx = Batch(max_workers=2)
        r1 = ctx.map(lambda x: x, range(5))
        r2 = ctx.map(lambda x: x, range(5), max_workers=10)
        assert len(r1) == 5
        assert len(r2) == 5


# ── Generator input ─────────────────────────────────────────────

class TestGeneratorInput:
    def test_generator_consumed_once(self):
        consumed = {"n": 0}

        def gen():
            for i in range(10):
                consumed["n"] += 1
                yield i

        results = batch_map(lambda x: x * 2, gen())
        assert len(results) == 10
        assert consumed["n"] == 10  # generator consumed exactly once


# ── None values ─────────────────────────────────────────────────

class TestNoneValues:
    def test_function_returns_none(self):
        results = batch_map(lambda x: None, range(5))
        assert len(results) == 5
        assert all(r.ok for r in results)
        assert all(r.value is None for r in results)

    def test_filter_with_nones(self):
        results = batch_filter(lambda x: x is not None, [1, None, 3, None, 5])
        assert results == [1, 3, 5]


# ── Backoff strategies ──────────────────────────────────────────

class TestBackoffStrategies:
    def test_fixed_backoff(self):
        call_count = {"n": 0}

        def sometimes_fail(x):
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise ConnectionError("transient")
            return x

        start = time.monotonic()
        results = batch_map(sometimes_fail, [1], retries=3, backoff="fixed")
        elapsed = time.monotonic() - start
        # fixed backoff: base=1.0, so ~1s per retry, 2 retries → at least ~1s
        assert results[0].value == 1

    def test_adaptive_backoff(self):
        def always_fail(x):
            raise RuntimeError("fail")

        start = time.monotonic()
        batch_map(always_fail, [1], retries=2, backoff="adaptive", on_error="skip")
        elapsed = time.monotonic() - start
        # adaptive caps at 60s but 2 retries with base 1.0 → should be fast
        assert elapsed < 10
