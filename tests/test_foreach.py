"""Tests for batch_for_each."""

import pytest
from batchly import batch_for_each, async_batch_for_each


class TestBatchForEach:
    def test_basic_foreach(self):
        collected = []
        batch_for_each(lambda x: collected.append(x), [1, 2, 3], max_workers=2)
        assert sorted(collected) == [1, 2, 3]

    def test_foreach_empty(self):
        collected = []
        batch_for_each(lambda x: collected.append(x), [], max_workers=2)
        assert collected == []

    def test_foreach_side_effects(self):
        counter = {"n": 0}

        def increment(x):
            counter["n"] += 1

        batch_for_each(increment, list(range(10)), max_workers=5)
        assert counter["n"] == 10

    def test_foreach_with_errors_skip(self):
        collected = []

        def may_fail(x):
            if x == 5:
                raise ValueError("nope")
            collected.append(x)

        batch_for_each(may_fail, list(range(10)), max_workers=2, on_error="skip")
        assert 5 not in collected
        assert len(collected) == 9

    def test_foreach_with_retries(self):
        counter = {"n": 0}
        call_count = {"c": 0}

        def flaky(x):
            call_count["c"] += 1
            if call_count["c"] <= 2:
                raise ConnectionError("retry")
            counter["n"] += 1

        batch_for_each(flaky, [1], max_workers=1, retries=5)
        assert counter["n"] == 1


class TestAsyncBatchForEach:
    @pytest.mark.asyncio
    async def test_async_foreach(self):
        collected = []
        await async_batch_for_each(
            lambda x: collected.append(x),
            [1, 2, 3],
            max_workers=2,
        )
        assert sorted(collected) == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_async_foreach_with_errors(self):
        collected = []
        await async_batch_for_each(
            lambda x: collected.append(x) if x != 2 else (_ for _ in ()).throw(ValueError("bad")),
            [1, 2, 3],
            max_workers=2,
            on_error="skip",
        )
        assert 2 not in collected
