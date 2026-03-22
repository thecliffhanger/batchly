"""Retry logic with backoff strategies."""

from __future__ import annotations

import random
import time
from typing import Any, Callable


def _compute_backoff(attempt: int, strategy: str, base: float = 1.0) -> float:
    """Compute backoff delay in seconds."""
    if strategy == "fixed":
        return base
    elif strategy == "exponential":
        return base * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
    elif strategy == "adaptive":
        return min(base * (2 ** (attempt - 1)), 60.0) + random.uniform(0, 0.1)
    else:
        return base


def retry_call(
    fn: Callable[..., Any],
    args: tuple = (),
    kwargs: dict | None = None,
    retries: int = 0,
    backoff: str = "exponential",
    retry_on: tuple[type[Exception], ...] = (Exception,),
    rate_limiter=None,
) -> Any:
    """Call fn with retry logic.

    Returns (result, error) tuple. error is None on success.
    """
    last_error = None
    for attempt in range(retries + 1):
        if rate_limiter is not None:
            rate_limiter.acquire()
        try:
            return fn(*args, **(kwargs or {})), None
        except retry_on as e:
            last_error = e
            if attempt < retries:
                delay = _compute_backoff(attempt + 1, backoff)
                time.sleep(delay)
            else:
                return None, e
        except Exception as e:
            return None, e
    return None, last_error


async def async_retry_call(
    fn: Callable[..., Any],
    args: tuple = (),
    kwargs: dict | None = None,
    retries: int = 0,
    backoff: str = "exponential",
    retry_on: tuple[type[Exception], ...] = (Exception,),
    rate_limiter=None,
) -> tuple[Any, Exception | None]:
    """Async version of retry_call."""
    import asyncio

    last_error = None
    for attempt in range(retries + 1):
        if rate_limiter is not None:
            await rate_limiter.async_acquire()
        try:
            return await fn(*args, **(kwargs or {})), None
        except retry_on as e:
            last_error = e
            if attempt < retries:
                delay = _compute_backoff(attempt + 1, backoff)
                await asyncio.sleep(delay)
            else:
                return None, e
        except Exception as e:
            return None, e
    return None, last_error
