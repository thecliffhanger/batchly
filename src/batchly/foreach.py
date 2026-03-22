"""batch_for_each implementation."""

from __future__ import annotations

import inspect
from typing import Any, Callable, Iterable

from .map_ import batch_map, async_batch_map


def batch_for_each(
    fn: Callable[..., Any],
    items: Iterable[Any],
    **kwargs,
) -> None:
    """Apply fn to each item (side effects only), processing in parallel."""
    if inspect.iscoroutinefunction(fn):
        raise TypeError(
            "batch_for_each called with async function. Use async_batch_for_each."
        )

    batch_map(fn, items, **kwargs)


async def async_batch_for_each(
    fn: Callable[..., Any],
    items: Iterable[Any],
    **kwargs,
) -> None:
    """Async version of batch_for_each."""
    await async_batch_map(fn, items, **kwargs)
