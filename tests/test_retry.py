"""Tests for retry logic."""

import time
import pytest
from batchly.retry import retry_call, async_retry_call, _compute_backoff


class TestComputeBackoff:
    def test_fixed(self):
        b = _compute_backoff(1, "fixed", base=2.0)
        assert b == 2.0

    def test_exponential(self):
        b1 = _compute_backoff(1, "exponential", base=1.0)
        b2 = _compute_backoff(2, "exponential", base=1.0)
        assert b2 > b1

    def test_adaptive(self):
        b1 = _compute_backoff(1, "adaptive", base=1.0)
        b2 = _compute_backoff(10, "adaptive", base=1.0)
        # Adaptive caps at 60s
        assert b1 < 60
        assert b2 <= 60 + 0.2  # jitter

    def test_unknown_strategy(self):
        b = _compute_backoff(1, "unknown", base=1.0)
        assert b == 1.0


class TestRetryCall:
    def test_success_no_retry(self):
        result, error = retry_call(lambda: 42)
        assert result == 42
        assert error is None

    def test_success_with_args(self):
        result, error = retry_call(lambda x, y: x + y, args=(3, 4))
        assert result == 7

    def test_success_with_kwargs(self):
        result, error = retry_call(lambda x=0: x + 1, kwargs={"x": 10})
        assert result == 11

    def test_retry_then_success(self):
        count = {"n": 0}

        def flaky():
            count["n"] += 1
            if count["n"] < 3:
                raise ConnectionError("fail")
            return "ok"

        result, error = retry_call(flaky, retries=5)
        assert result == "ok"
        assert count["n"] == 3

    def test_retry_exhausted(self):
        def always_fail():
            raise ValueError("nope")

        result, error = retry_call(always_fail, retries=2)
        assert result is None
        assert isinstance(error, ValueError)

    def test_retry_on_specific_exception(self):
        count = {"n": 0}

        def mixed():
            count["n"] += 1
            if count["n"] <= 2:
                raise ConnectionError("retryable")
            raise TypeError("not retryable")

        result, error = retry_call(mixed, retries=5, retry_on=(ConnectionError,))
        assert isinstance(error, TypeError)

    def test_non_retryable_not_retried(self):
        count = {"n": 0}

        def fail_type_error():
            count["n"] += 1
            raise TypeError("instant fail")

        result, error = retry_call(fail_type_error, retries=5, retry_on=(ConnectionError,))
        assert count["n"] == 1
        assert isinstance(error, TypeError)

    def test_zero_retries(self):
        count = {"n": 0}

        def flaky():
            count["n"] += 1
            raise ValueError("fail")

        result, error = retry_call(flaky, retries=0)
        assert count["n"] == 1


class TestAsyncRetryCall:
    @pytest.mark.asyncio
    async def test_success(self):
        async def ok():
            return 42

        result, error = await async_retry_call(ok)
        assert result == 42
        assert error is None

    @pytest.mark.asyncio
    async def test_retry_then_success(self):
        count = {"n": 0}

        async def flaky():
            count["n"] += 1
            if count["n"] < 3:
                raise ConnectionError("fail")
            return "ok"

        result, error = await async_retry_call(flaky, retries=5)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_retry_exhausted(self):
        async def fail():
            raise ValueError("nope")

        result, error = await async_retry_call(fail, retries=2)
        assert result is None
        assert isinstance(error, ValueError)

    @pytest.mark.asyncio
    async def test_backoff_delays(self):
        count = {"n": 0}

        async def flaky():
            count["n"] += 1
            raise ConnectionError("fail")

        start = time.monotonic()
        result, error = await async_retry_call(flaky, retries=3, backoff="exponential")
        elapsed = time.monotonic() - start
        # Should have waited between retries
        assert count["n"] == 4
        assert elapsed > 0.5  # at least some delay
