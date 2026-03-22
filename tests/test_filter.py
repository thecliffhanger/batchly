"""Tests for batch_filter."""

import pytest
from batchly import batch_filter, async_batch_filter


class TestBatchFilter:
    def test_basic_filter(self):
        results = batch_filter(lambda x: x > 3, [1, 2, 3, 4, 5], max_workers=2)
        assert results == [4, 5]

    def test_filter_all_pass(self):
        results = batch_filter(lambda x: True, [1, 2, 3], max_workers=2)
        assert sorted(results) == [1, 2, 3]

    def test_filter_none_pass(self):
        results = batch_filter(lambda x: False, [1, 2, 3], max_workers=2)
        assert results == []

    def test_filter_empty(self):
        results = batch_filter(lambda x: True, [], max_workers=2)
        assert results == []

    def test_filter_with_errors_skip(self):
        def predicate(x):
            if x == 3:
                raise ValueError("bad")
            return x > 2

        results = batch_filter(predicate, [1, 2, 3, 4, 5], max_workers=2, on_error="skip")
        assert results == [4, 5]

    def test_filter_preserves_item_identity(self):
        items = ["a", "bb", "ccc", "dddd"]
        results = batch_filter(lambda x: len(x) > 2, items, max_workers=2)
        assert results == ["ccc", "dddd"]

    def test_filter_with_retries(self):
        call_count = {"n": 0}

        def flaky_pred(x):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise ConnectionError("transient")
            return x > 2

        results = batch_filter(flaky_pred, [1, 2, 3, 4], max_workers=1, retries=3)
        assert 3 in results
        assert 4 in results

    def test_async_filter_raises_for_sync_call(self):
        async def pred(x):
            return x > 2

        with pytest.raises(TypeError):
            batch_filter(pred, [1, 2, 3])


class TestAsyncBatchFilter:
    @pytest.mark.asyncio
    async def test_async_filter(self):
        async def pred(x):
            return x > 3

        results = await async_batch_filter(pred, [1, 2, 3, 4, 5], max_workers=2)
        assert results == [4, 5]

    @pytest.mark.asyncio
    async def test_async_filter_with_errors(self):
        async def pred(x):
            if x == 3:
                raise ValueError("bad")
            return x > 2

        results = await async_batch_filter(pred, [1, 2, 3, 4], max_workers=2, on_error="skip")
        assert results == [4]
