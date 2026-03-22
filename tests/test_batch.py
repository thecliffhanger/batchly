"""Tests for batch decorator and Batch context."""

import pytest
from batchly import batch, Batch, BatchResult, BatchError


class TestBatchDecorator:
    def test_single_item(self):
        @batch(max_workers=2)
        def double(x):
            return x * 2

        assert double(5) == 10

    def test_batch_of_items(self):
        @batch(max_workers=4)
        def double(x):
            return x * 2

        results = double([1, 2, 3, 4, 5])
        values = [r.value for r in results]
        assert sorted(values) == [2, 4, 6, 8, 10]

    def test_batch_preserves_order(self):
        @batch(max_workers=4, ordered=True)
        def identity(x):
            return x

        results = identity([10, 20, 30])
        assert [r.value for r in results] == [10, 20, 30]

    def test_batch_with_retries(self):
        call_count = 0

        @batch(max_workers=2, retries=3)
        def flaky(x):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ConnectionError("fail")
            return x

        results = flaky([42])
        assert results[0].value == 42

    def test_string_as_single_item(self):
        @batch(max_workers=2)
        def process(s):
            return s.upper()

        assert process("hello") == "HELLO"

    def test_batch_on_error_skip(self):
        @batch(max_workers=2, on_error="skip")
        def fail_on_one(x):
            if x == 2:
                raise ValueError("nope")
            return x

        results = fail_on_one([1, 2, 3])
        values = [r.value for r in results]
        assert values == [1, 3]


class TestBatchContext:
    def test_map(self):
        b = Batch(max_workers=4)
        results = b.map(lambda x: x + 1, [1, 2, 3])
        assert [r.value for r in results] == [2, 3, 4]

    def test_filter(self):
        b = Batch(max_workers=4)
        results = b.filter(lambda x: x > 2, [1, 2, 3, 4, 5])
        assert results == [3, 4, 5]

    def test_foreach(self):
        collected = []
        b = Batch(max_workers=4)
        b.foreach(lambda x: collected.append(x), [1, 2, 3])
        assert sorted(collected) == [1, 2, 3]

    def test_context_preserves_options(self):
        b = Batch(max_workers=1, retries=2, on_error="skip")
        results = b.map(lambda x: 1 // (1 - x), [3, 4, 5], on_error="collect")
        assert len(results) == 3

    def test_override_options(self):
        b = Batch(max_workers=1, on_error="skip")
        with pytest.raises(BatchError):
            b.map(lambda x: 1 / 0, [1], on_error="raise")


class TestBatchDecoratorAsync:
    @pytest.mark.asyncio
    async def test_async_single_item(self):
        @batch(max_workers=2)
        async def double(x):
            return x * 2

        result = await double(5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_async_batch(self):
        @batch(max_workers=4)
        async def double(x):
            return x * 2

        results = await double([1, 2, 3])
        values = [r.value for r in results]
        assert sorted(values) == [2, 4, 6]
