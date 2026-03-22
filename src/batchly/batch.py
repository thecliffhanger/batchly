"""@batch decorator and Batch context class."""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable

from .filter_ import async_batch_filter, batch_filter
from .foreach import async_batch_for_each, batch_for_each
from .map_ import async_batch_map, batch_map


class Batch:
    """Reusable batch processing context.

    Usage:
        b = Batch(max_workers=10, retries=3)
        results = b.map(fn, items)
        filtered = b.filter(pred, items)
        b.foreach(fn, items)
    """

    def __init__(
        self,
        *,
        max_workers: int = 4,
        retries: int = 0,
        backoff: str = "exponential",
        retry_on: tuple[type[Exception], ...] = (Exception,),
        on_error: str = "skip",
        chunk_size: int | None = None,
        rate_limit: int | None = None,
        ordered: bool = True,
        timeout: float | None = None,
        progress: Callable | None = None,
    ):
        self.max_workers = max_workers
        self.retries = retries
        self.backoff = backoff
        self.retry_on = retry_on
        self.on_error = on_error
        self.chunk_size = chunk_size
        self.rate_limit = rate_limit
        self.ordered = ordered
        self.timeout = timeout
        self.progress = progress

    def _common_kwargs(self) -> dict:
        return dict(
            max_workers=self.max_workers,
            retries=self.retries,
            backoff=self.backoff,
            retry_on=self.retry_on,
            on_error=self.on_error,
            chunk_size=self.chunk_size,
            rate_limit=self.rate_limit,
            ordered=self.ordered,
            timeout=self.timeout,
            progress=self.progress,
        )

    def map(self, fn: Callable, items, **overrides):
        kw = {**self._common_kwargs(), **overrides}
        return batch_map(fn, items, **kw)

    async def amap(self, fn: Callable, items, **overrides):
        kw = {**self._common_kwargs(), **overrides}
        return await async_batch_map(fn, items, **kw)

    def filter(self, fn: Callable, items, **overrides):
        kw = {**self._common_kwargs(), **overrides}
        return batch_filter(fn, items, **kw)

    async def afilter(self, fn: Callable, items, **overrides):
        kw = {**self._common_kwargs(), **overrides}
        return await async_batch_filter(fn, items, **kw)

    def foreach(self, fn: Callable, items, **overrides):
        kw = {**self._common_kwargs(), **overrides}
        return batch_for_each(fn, items, **kw)

    async def aforeach(self, fn: Callable, items, **overrides):
        kw = {**self._common_kwargs(), **overrides}
        return await async_batch_for_each(fn, items, **kw)


def batch(*, max_workers: int = 4, retries: int = 0, **kwargs) -> Callable:
    """Decorator to turn a single-item function into a batch processor.

    Usage:
        @batch(max_workers=10, retries=3)
        def process(item):
            ...

        results = process([1, 2, 3])  # processes all in parallel
        single = process(42)          # calls directly for single item
    """

    def decorator(fn: Callable) -> Callable:
        _ctx = Batch(max_workers=max_workers, retries=retries, **kwargs)

        @functools.wraps(fn)
        def wrapper(items_or_single):
            # If it's a single item (not iterable of items), call directly
            if isinstance(items_or_single, (str, bytes, bytearray)):
                # Strings are iterable but usually single items
                if len(items_or_single) <= 1:
                    return fn(items_or_single)
                # Multi-char string: treat as single item
                return fn(items_or_single)

            try:
                iter(items_or_single)
            except TypeError:
                # Not iterable — single item
                return fn(items_or_single)

            # It's iterable — batch process
            return _ctx.map(fn, list(items_or_single))

        @functools.wraps(fn)
        async def async_wrapper(items_or_single):
            if isinstance(items_or_single, (str, bytes, bytearray)):
                return await fn(items_or_single)
            try:
                iter(items_or_single)
            except TypeError:
                return await fn(items_or_single)
            return await _ctx.amap(fn, list(items_or_single))

        if inspect.iscoroutinefunction(fn):
            return async_wrapper
        return wrapper

    return decorator
