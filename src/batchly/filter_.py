"""batch_filter implementation."""

from __future__ import annotations

import inspect
from typing import Any, Callable, Iterable

from .map_ import batch_map, async_batch_map


def batch_filter(
    fn: Callable[..., bool],
    items: Iterable[Any],
    **kwargs,
) -> list:
    """Filter items where fn(item) is truthy, processing in parallel.

    Returns list of items (not BatchResult) that pass the filter.
    """
    if inspect.iscoroutinefunction(fn):
        raise TypeError(
            "batch_filter called with async function. Use async_batch_filter."
        )

    results = batch_map(fn, items, **kwargs)
    return [r.item for r in results if r.ok and r.value]


async def async_batch_filter(
    fn: Callable[..., bool],
    items: Iterable[Any],
    **kwargs,
) -> list:
    """Async version of batch_filter."""
    results = await async_batch_map(fn, items, **kwargs)
    return [r.item for r in results if r.ok and r.value]
