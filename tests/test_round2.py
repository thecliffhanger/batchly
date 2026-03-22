"""Round 2 tests — hypothesis + adversarial/stress tests."""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
from hypothesis import given, settings, strategies as st

from batchly import (
    batch, Batch, batch_map, batch_filter, batch_for_each,
    async_batch_map, async_batch_filter, async_batch_for_each,
    BatchResult, BatchError, ProgressBar, RateLimiter,
)


# --- Hypothesis strategies ---

small_ints = st.integers(min_value=0, max_value=100)
small_pos_ints = st.integers(min_value=1, max_value=50)
lists_of_ints = st.lists(st.integers(min_value=-100, max_value=100), min_size=0, max_size=200)


# --- Hypothesis-driven tests ---


class TestHypothesisBatchMap:
    @given(items=lists_of_ints, workers=small_pos_ints)
    @settings(max_examples=20, deadline=None)
    def test_map_result_count(self, items, workers):
        """Result count always equals input count."""
        results = batch_map(lambda x: x, items, max_workers=workers, on_error="skip")
        assert len(results) == len(items)

    @given(items=lists_of_ints)
    @settings(max_examples=15, deadline=None)
    def test_map_identity(self, items):
        """Map with identity preserves all values."""
        results = batch_map(lambda x: x, items, max_workers=4, on_error="skip")
        assert [r.value for r in results] == items

    @given(items=lists_of_ints, workers=small_pos_ints)
    @settings(max_examples=15, deadline=None)
    def test_map_doubled(self, items, workers):
        """Map doubles all values correctly."""
        results = batch_map(lambda x: x * 2, items, max_workers=workers)
        assert [r.value for r in results] == [x * 2 for x in items]

    @given(items=lists_of_ints)
    @settings(max_examples=10, deadline=None)
    def test_map_ordered_preserved(self, items):
        """Order is preserved regardless of worker count."""
        def slow(x):
            time.sleep(0.001 * (abs(x) % 5))
            return x

        results = batch_map(slow, items, max_workers=10, ordered=True)
        assert [r.value for r in results] == items


class TestHypothesisBatchFilter:
    @given(items=lists_of_ints, threshold=small_ints)
    @settings(max_examples=15, deadline=None)
    def test_filter_correctness(self, items, threshold):
        results = batch_filter(lambda x: x > threshold, items, max_workers=4)
        expected = [x for x in items if x > threshold]
        assert sorted(results) == sorted(expected)


class TestHypothesisBatchForEach:
    @given(items=lists_of_ints)
    @settings(max_examples=10, deadline=None)
    def test_foreach_visits_all(self, items):
        collected = []
        batch_for_each(lambda x: collected.append(x), items, max_workers=4)
        assert sorted(collected) == sorted(items)


# --- Stress / adversarial tests ---


class TestStress:
    def test_concurrent_stress_1000_items(self):
        """1000 items with 50 workers."""
        items = list(range(1000))
        results = batch_map(lambda x: x ** 2, items, max_workers=50)
        assert len(results) == 1000
        assert [r.value for r in results] == [x ** 2 for x in items]

    def test_concurrent_stress_500_items_stream(self):
        """500 items streaming mode."""
        results = list(batch_map(lambda x: x, list(range(500)), max_workers=50, stream=True))
        assert len(results) == 500
        values = sorted([r.value for r in results])
        assert values == list(range(500))

    def test_all_errors_skip(self):
        """All items fail with on_error='skip'."""
        results = batch_map(lambda x: 1 / 0, list(range(100)), max_workers=10, on_error="skip")
        assert len(results) == 0

    def test_all_errors_collect(self):
        """All items fail with on_error='collect'."""
        results = batch_map(lambda x: 1 / 0, list(range(50)), max_workers=10, on_error="collect")
        assert len(results) == 50
        assert all(r.error is not None for r in results)

    def test_mixed_errors_collect(self):
        """Mix of success and failure with collect."""
        def maybe_fail(x):
            if x % 3 == 0:
                raise ValueError("bad")
            return x

        items = list(range(30))
        results = batch_map(maybe_fail, items, max_workers=10, on_error="collect")
        assert len(results) == 30
        errors = [r for r in results if r.error is not None]
        successes = [r for r in results if r.error is None]
        assert len(errors) == 10
        assert len(successes) == 20

    def test_interleaved_errors_raise(self):
        """First failure raises immediately."""
        items = list(range(20))

        def fail_fast(x):
            if x == 5:
                raise RuntimeError("stop")
            return x

        with pytest.raises(BatchError):
            batch_map(fail_fast, items, max_workers=1, on_error="raise")


class TestChunkedProcessing:
    def test_chunk_correctness(self):
        """Each chunk is processed correctly."""
        items = list(range(100))

        results = batch_map(
            lambda chunk: [x * 2 for x in chunk],
            items,
            max_workers=5,
            chunk_size=10,
        )
        # 10 chunks
        assert len(results) == 10
        flat = []
        for r in results:
            flat.extend(r.value)
        assert flat == [x * 2 for x in items]

    def test_chunk_size_1(self):
        items = list(range(20))
        results = batch_map(lambda chunk: chunk[0] + 100, items, max_workers=5, chunk_size=1)
        assert len(results) == 20
        assert [r.value for r in results] == [x + 100 for x in items]

    def test_chunk_size_equals_len(self):
        items = [1, 2, 3, 4, 5]
        results = batch_map(lambda chunk: sum(chunk), items, max_workers=2, chunk_size=5)
        assert len(results) == 1
        assert results[0].value == 15


class TestRateLimitingAccuracy:
    def test_rate_limit_respected(self):
        """With low rate limit, calls should take appropriate time."""
        # Use more items than initial burst to see throttling
        items = list(range(25))  # 25 items at 20/s

        start = time.monotonic()
        results = batch_map(lambda x: x, items, max_workers=10, rate_limit=20)
        elapsed = time.monotonic() - start
        # 25 items at 20/s: 20 instant, 5 throttled → at least ~0.25s
        assert elapsed >= 0.15
        assert len(results) == 25

    def test_high_rate_limit_is_fast(self):
        start = time.monotonic()
        results = batch_map(lambda x: x, list(range(50)), max_workers=10, rate_limit=1000)
        elapsed = time.monotonic() - start
        assert elapsed < 2.0
        assert len(results) == 50


class TestTimeoutBehavior:
    def test_all_timeout(self):
        results = batch_map(
            lambda x: time.sleep(10),
            list(range(5)),
            max_workers=5,
            timeout=0.05,
            on_error="skip",
        )
        assert len(results) == 0

    def test_mixed_timeout(self):
        def maybe_slow(x):
            if x % 2 == 0:
                time.sleep(10)
            return x

        results = batch_map(
            maybe_slow,
            list(range(10)),
            max_workers=5,
            timeout=0.1,
            on_error="skip",
        )
        values = [r.value for r in results]
        assert all(v % 2 != 0 for v in values)

    def test_timeout_collect(self):
        results = batch_map(
            lambda x: time.sleep(10),
            list(range(3)),
            max_workers=3,
            timeout=0.05,
            on_error="collect",
        )
        assert len(results) == 3
        assert all(r.error is not None for r in results)


class TestAsyncStress:
    @pytest.mark.asyncio
    async def test_async_500_items(self):
        async def square(x):
            return x ** 2

        items = list(range(500))
        results = await async_batch_map(square, items, max_workers=50)
        assert len(results) == 500
        values = sorted([r.value for r in results])
        assert values == [x ** 2 for x in items]

    @pytest.mark.asyncio
    async def test_async_with_errors(self):
        async def flaky(x):
            if x % 5 == 0:
                raise ValueError("bad")
            return x

        results = await async_batch_map(
            flaky, list(range(50)), max_workers=10, on_error="collect"
        )
        errors = [r for r in results if r.error]
        assert len(errors) == 10

    @pytest.mark.asyncio
    async def test_async_ordered(self):
        async def slow(x):
            await asyncio.sleep(0.001 * (10 - x))
            return x

        results = await async_batch_map(slow, list(range(10)), max_workers=10, ordered=True)
        assert [r.value for r in results] == list(range(10))

    @pytest.mark.asyncio
    async def test_async_stream(self):
        from batchly.map_ import _async_stream

        async def double(x):
            return x * 2

        gen = _async_stream(double, list(range(20)), max_workers=5)
        results = []
        async for r in gen:
            results.append(r)
        assert len(results) == 20
        values = sorted([r.value for r in results])
        assert values == [x * 2 for x in range(20)]

    @pytest.mark.asyncio
    async def test_async_chunked(self):
        async def process_chunk(chunk):
            return sum(chunk)

        results = await async_batch_map(
            process_chunk, list(range(20)), max_workers=4, chunk_size=5
        )
        assert len(results) == 4
        assert results[0].value == 10  # 0+1+2+3+4

    @pytest.mark.asyncio
    async def test_async_filter_stress(self):
        async def is_even(x):
            return x % 2 == 0

        results = await async_batch_filter(is_even, list(range(100)), max_workers=20)
        assert results == list(range(0, 100, 2))

    @pytest.mark.asyncio
    async def test_async_foreach_stress(self):
        collected = []
        await async_batch_for_each(
            lambda x: collected.append(x), list(range(100)), max_workers=20
        )
        assert sorted(collected) == list(range(100))


class TestEdgeCases:
    def test_single_worker(self):
        results = batch_map(lambda x: x + 1, list(range(10)), max_workers=1)
        assert [r.value for r in results] == list(range(1, 11))

    def test_many_workers_few_items(self):
        results = batch_map(lambda x: x, [1, 2], max_workers=100)
        assert len(results) == 2

    def test_progress_called_correct_times(self):
        calls = []
        batch_map(
            lambda x: x,
            list(range(20)),
            max_workers=4,
            progress=lambda p: calls.append(p),
        )
        assert len(calls) == 20
        assert calls[-1].completed == 20

    def test_batch_result_unwrap_success(self):
        r = BatchResult(value=42)
        assert r.unwrap() == 42

    def test_batch_result_unwrap_failure(self):
        r = BatchResult(error=ValueError("bad"))
        with pytest.raises(ValueError):
            r.unwrap()

    def test_batch_decorator_reusable(self):
        @batch(max_workers=2)
        def double(x):
            return x * 2

        r1 = double([1, 2, 3])
        r2 = double([4, 5])
        assert [x.value for x in r1] == [2, 4, 6]
        assert [x.value for x in r2] == [8, 10]

    def test_batch_context_reusable(self):
        b = Batch(max_workers=4)
        r1 = b.map(lambda x: x + 1, [1, 2])
        r2 = b.map(lambda x: x * 2, [3, 4])
        assert [r.value for r in r1] == [2, 3]
        assert [r.value for r in r2] == [6, 8]

    def test_empty_generator(self):
        results = batch_map(lambda x: x, (x for x in []), max_workers=2)
        assert results == []

    def test_none_values(self):
        results = batch_map(lambda x: None, [1, 2, 3], max_workers=2)
        assert len(results) == 3
        assert all(r.value is None for r in results)
        assert all(r.ok for r in results)


class TestBackoffStrategies:
    def test_fixed_backoff(self):
        count = {"n": 0}

        def flaky(x):
            count["n"] += 1
            if count["n"] < 3:
                raise ConnectionError("retry")
            return x

        start = time.monotonic()
        results = batch_map(flaky, [1], max_workers=1, retries=5, backoff="fixed")
        elapsed = time.monotonic() - start
        assert results[0].value == 1
        assert elapsed >= 1.5  # 2 retries * ~1s fixed

    def test_exponential_backoff(self):
        count = {"n": 0}

        def flaky(x):
            count["n"] += 1
            raise ConnectionError("always fail")

        start = time.monotonic()
        batch_map(flaky, [1], max_workers=1, retries=3, backoff="exponential", on_error="skip")
        elapsed = time.monotonic() - start
        # Should take noticeable time with exponential backoff
        assert elapsed >= 1.0
