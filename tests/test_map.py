"""Tests for batch_map."""

import time
import pytest
from batchly import batch_map, BatchResult, BatchError


class TestBatchMapBasic:
    def test_simple_map(self):
        results = batch_map(lambda x: x * 2, [1, 2, 3, 4], max_workers=2)
        assert [r.value for r in results] == [2, 4, 6, 8]

    def test_empty_items(self):
        results = batch_map(lambda x: x, [], max_workers=2)
        assert results == []

    def test_single_item(self):
        results = batch_map(lambda x: x + 10, [5], max_workers=2)
        assert results[0].value == 15

    def test_large_dataset(self):
        items = list(range(100))
        results = batch_map(lambda x: x ** 2, items, max_workers=8)
        assert [r.value for r in results] == [x ** 2 for x in items]

    def test_generator_input(self):
        results = batch_map(lambda x: x * 3, (x for x in range(5)), max_workers=2)
        assert [r.value for r in results] == [0, 3, 6, 9, 12]


class TestBatchMapOrdering:
    def test_ordered_results(self):
        def slow_identity(x):
            time.sleep(0.01 * (5 - x))  # higher items finish faster
            return x

        results = batch_map(slow_identity, [1, 2, 3, 4, 5], max_workers=5, ordered=True)
        assert [r.value for r in results] == [1, 2, 3, 4, 5]

    def test_ordered_is_default(self):
        results = batch_map(lambda x: x, [10, 20, 30], max_workers=2)
        assert [r.value for r in results] == [10, 20, 30]


class TestBatchMapErrorHandling:
    def test_on_error_skip(self):
        results = batch_map(
            lambda x: 1 // (1 - x) if x != 1 else 1 / 0,
            [3, 4, 5, 1, 2],
            max_workers=2,
            on_error="skip",
        )
        values = [r.value for r in results]
        # 1/0 should be skipped
        assert all(r.ok for r in results)

    def test_on_error_raise(self):
        with pytest.raises(BatchError):
            batch_map(lambda x: 1 / 0, [1, 2], max_workers=2, on_error="raise")

    def test_on_error_collect(self):
        results = batch_map(
            lambda x: 1 // x if x != 0 else 1 / 0,
            [2, 0, 4, 0, 6],
            max_workers=2,
            on_error="collect",
        )
        assert len(results) == 5
        errors = [r for r in results if r.error is not None]
        successes = [r for r in results if r.error is None]
        assert len(errors) == 2
        assert len(successes) == 3

    def test_collect_preserves_error_info(self):
        results = batch_map(
            lambda x: 1 // (x - 3),
            [1, 2, 3, 4, 5],
            max_workers=2,
            on_error="collect",
        )
        failed = [r for r in results if r.error]
        assert len(failed) >= 1
        for r in failed:
            assert r.item is not None


class TestBatchMapAsync:
    def test_async_map_raises(self):
        async def double(x):
            return x * 2

        with pytest.raises(TypeError, match="async function"):
            batch_map(double, [1, 2, 3], max_workers=2)

    @pytest.mark.asyncio
    async def test_async_map_via_import(self):
        from batchly import async_batch_map

        async def double(x):
            return x * 2

        results = await async_batch_map(double, [1, 2, 3, 4], max_workers=2)
        values = sorted([r.value for r in results])
        assert values == [2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_async_map_ordered(self):
        from batchly import async_batch_map
        import asyncio

        async def slow_val(x):
            await asyncio.sleep(0.01 * (5 - x))
            return x

        results = await async_batch_map(slow_val, [1, 2, 3, 4, 5], max_workers=5, ordered=True)
        assert [r.value for r in results] == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_async_map_on_error_collect(self):
        from batchly import async_batch_map

        async def flaky(x):
            if x == 3:
                raise ValueError("bad")
            return x * 10

        results = await async_batch_map(flaky, [1, 2, 3, 4], max_workers=2, on_error="collect")
        errors = [r for r in results if r.error]
        assert len(errors) == 1


class TestBatchMapChunked:
    def test_chunked_processing(self):
        def process_chunk(chunk):
            return sum(chunk)

        results = batch_map(
            lambda chunk: sum(chunk),
            list(range(10)),
            max_workers=2,
            chunk_size=3,
        )
        # 10 items, chunk_size=3 → chunks: [0,1,2], [3,4,5], [6,7,8], [9]
        assert len(results) == 4
        assert results[0].value == 3  # 0+1+2
        assert results[1].value == 12  # 3+4+5
        assert results[2].value == 21  # 6+7+8
        assert results[3].value == 9  # 9

    def test_chunk_size_larger_than_items(self):
        results = batch_map(
            lambda chunk: len(chunk),
            [1, 2, 3],
            max_workers=2,
            chunk_size=100,
        )
        assert len(results) == 1
        assert results[0].value == 3

    def test_chunk_size_one(self):
        results = batch_map(
            lambda chunk: chunk[0] * 2,
            [1, 2, 3],
            max_workers=2,
            chunk_size=1,
        )
        assert [r.value for r in results] == [2, 4, 6]


class TestBatchMapTimeout:
    def test_timeout_raises_on_slow(self):
        def slow(x):
            time.sleep(10)
            return x

        results = batch_map(slow, [1, 2], max_workers=2, timeout=0.1, on_error="skip")
        # Both should be skipped/timed out
        assert len(results) == 0

    def test_timeout_allows_fast(self):
        def fast(x):
            return x * 2

        results = batch_map(fast, [1, 2, 3], max_workers=2, timeout=5.0)
        assert [r.value for r in results] == [2, 4, 6]


class TestBatchMapStream:
    def test_stream_mode(self):
        results = list(batch_map(lambda x: x ** 2, [1, 2, 3, 4, 5], max_workers=2, stream=True))
        values = sorted([r.value for r in results])
        assert values == [1, 4, 9, 16, 25]

    def test_stream_with_errors_skip(self):
        results = list(
            batch_map(
                lambda x: x if x != 3 else 1 / 0,
                [1, 2, 3, 4, 5],
                max_workers=2,
                stream=True,
                on_error="skip",
            )
        )
        assert all(r.ok for r in results)


class TestBatchMapRetry:
    def test_retry_success_after_failures(self):
        call_count = {"n": 0}

        def flaky(x):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ConnectionError("transient")
            return x

        results = batch_map(flaky, [42], max_workers=1, retries=5)
        assert results[0].value == 42
        assert call_count["n"] >= 3

    def test_retry_exhausted(self):
        def always_fail(x):
            raise ValueError("permanent")

        results = batch_map(always_fail, [1], max_workers=1, retries=2, on_error="skip")
        assert len(results) == 0

    def test_retry_on_specific_exception(self):
        count = {"n": 0}

        def mixed_fail(x):
            count["n"] += 1
            if count["n"] <= 2:
                raise ConnectionError("retry me")
            raise TypeError("don't retry me")

        results = batch_map(
            mixed_fail, [1], max_workers=1, retries=5,
            retry_on=(ConnectionError,), on_error="skip",
        )
        assert len(results) == 0  # TypeError not retried


class TestBatchMapProgress:
    def test_progress_callback(self):
        collected = []

        def progress_handler(info):
            collected.append(info)

        batch_map(lambda x: x, list(range(5)), max_workers=2, progress=progress_handler)
        assert len(collected) == 5
        assert collected[-1].completed == 5
        assert collected[-1].total == 5

    def test_progress_eta(self):
        collected = []

        batch_map(lambda x: x, list(range(3)), max_workers=1, progress=collected.append)
        # Last progress should have eta close to 0
        assert collected[-1].eta >= 0
