"""batch_map implementation — parallel map with all options."""

from __future__ import annotations

import asyncio
import inspect
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Generator, Iterable, AsyncGenerator

from .errors import BatchError, TimeoutError
from .progress import ProgressInfo
from .rate_limit import RateLimiter
from .result import BatchResult
from .retry import async_retry_call, retry_call


def batch_map(
    fn: Callable[..., Any],
    items: Iterable[Any],
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
    stream: bool = False,
    progress: Callable[[ProgressInfo], None] | None = None,
) -> list[BatchResult] | Generator[BatchResult, None, None]:
    """Map function over items in parallel."""
    items = list(items)

    if inspect.iscoroutinefunction(fn):
        raise TypeError(
            "batch_map called with async function but not awaited. "
            "Use: results = await async_batch_map(fn, items, ...)"
        )

    if stream:
        return _stream_sync(
            fn, items,
            max_workers=max_workers, retries=retries, backoff=backoff,
            retry_on=retry_on, on_error=on_error, chunk_size=chunk_size,
            rate_limit=rate_limit, timeout=timeout, progress=progress,
        )

    return _map_sync(
        fn, items,
        max_workers=max_workers, retries=retries, backoff=backoff,
        retry_on=retry_on, on_error=on_error, chunk_size=chunk_size,
        rate_limit=rate_limit, ordered=ordered, timeout=timeout, progress=progress,
    )


async def async_batch_map(
    fn: Callable[..., Any],
    items: Iterable[Any],
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
    stream: bool = False,
    progress: Callable[[ProgressInfo], None] | None = None,
) -> list[BatchResult] | AsyncGenerator[BatchResult, None]:
    """Async version of batch_map."""
    if stream:
        return _async_stream(
            fn, list(items),
            max_workers=max_workers, retries=retries, backoff=backoff,
            retry_on=retry_on, on_error=on_error, chunk_size=chunk_size,
            rate_limit=rate_limit, timeout=timeout, progress=progress,
        )

    return await _async_map(
        fn, list(items),
        max_workers=max_workers, retries=retries, backoff=backoff,
        retry_on=retry_on, on_error=on_error, chunk_size=chunk_size,
        rate_limit=rate_limit, ordered=ordered, timeout=timeout, progress=progress,
    )


async def _async_map(
    fn, items, *,
    max_workers=4, retries=0, backoff="exponential",
    retry_on=(Exception,), on_error="skip",
    chunk_size=None, rate_limit=None,
    ordered=True, timeout=None, progress=None,
) -> list[BatchResult]:
    """Async batch map that returns a list."""
    limiter = RateLimiter(rate_limit) if rate_limit else None

    if chunk_size is not None:
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        total_tasks = len(chunks)
        item_mapping = list(enumerate(chunks))
    else:
        total_tasks = len(items)
        item_mapping = list(enumerate(items))

    start = time.monotonic()
    completed = 0
    results = [None] * total_tasks
    semaphore = asyncio.Semaphore(max_workers)

    async def _process_task(task_idx: int, item: Any):
        nonlocal completed
        async with semaphore:
            if limiter:
                await limiter.async_acquire()
            t0 = time.monotonic()

            args = (item,) if chunk_size is None else ()
            kw = {"chunk": item} if chunk_size is not None else {}

            try:
                if timeout is not None:
                    val, err = await asyncio.wait_for(
                        async_retry_call(fn, args=args, kwargs=kw,
                                          retries=retries, backoff=backoff,
                                          retry_on=retry_on, rate_limiter=None),
                        timeout=timeout,
                    )
                else:
                    val, err = await async_retry_call(
                        fn, args=args, kwargs=kw,
                        retries=retries, backoff=backoff,
                        retry_on=retry_on, rate_limiter=None,
                    )
            except asyncio.TimeoutError as e:
                if on_error == "raise":
                    raise TimeoutError(f"Timed out: {item}", item=item, original_error=e)
                elif on_error == "collect":
                    err = e
                    val = None
                else:
                    completed += 1
                    if progress:
                        elapsed = time.monotonic() - start
                        eta = (elapsed / completed * (total_tasks - completed)) if completed > 0 else 0
                        progress(ProgressInfo(completed=completed, total=total_tasks, elapsed=elapsed, eta=eta))
                    return task_idx, None

            duration = time.monotonic() - t0

            if err is not None:
                if on_error == "raise":
                    raise BatchError(f"Failed: {item}", item=item, original_error=err)
                elif on_error == "collect":
                    br = BatchResult(value=None, error=err, item=item, duration=duration)
                else:
                    br = None
            else:
                br = BatchResult(value=val, error=None, item=item, duration=duration)

            completed += 1
            if progress:
                elapsed = time.monotonic() - start
                eta = (elapsed / completed * (total_tasks - completed)) if completed > 0 else 0
                progress(ProgressInfo(completed=completed, total=total_tasks, elapsed=elapsed, eta=eta))

            return task_idx, br

    coros = [_process_task(idx, item) for idx, item in item_mapping]
    gathered = await asyncio.gather(*coros, return_exceptions=True)

    for i, result in enumerate(gathered):
        if isinstance(result, Exception):
            if on_error == "raise":
                raise result
            continue
        task_idx, br = result
        if br is not None and ordered:
            results[task_idx] = br
        elif br is not None:
            results.append(br)

    if on_error in ("skip", "collect"):
        results = [r for r in results if r is not None]

    return results


async def _async_stream(
    fn, items, *,
    max_workers=4, retries=0, backoff="exponential",
    retry_on=(Exception,), on_error="skip",
    chunk_size=None, rate_limit=None,
    timeout=None, progress=None,
) -> AsyncGenerator[BatchResult, None]:
    """Async streaming batch map — yields results as they complete."""
    limiter = RateLimiter(rate_limit) if rate_limit else None

    if chunk_size is not None:
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        total_tasks = len(chunks)
        item_mapping = list(enumerate(chunks))
    else:
        total_tasks = len(items)
        item_mapping = list(enumerate(items))

    start = time.monotonic()
    completed = 0
    semaphore = asyncio.Semaphore(max_workers)

    async def _process_task(task_idx: int, item: Any):
        nonlocal completed
        async with semaphore:
            if limiter:
                await limiter.async_acquire()
            t0 = time.monotonic()

            args = (item,) if chunk_size is None else ()
            kw = {"chunk": item} if chunk_size is not None else {}

            try:
                if timeout is not None:
                    val, err = await asyncio.wait_for(
                        async_retry_call(fn, args=args, kwargs=kw,
                                          retries=retries, backoff=backoff,
                                          retry_on=retry_on, rate_limiter=None),
                        timeout=timeout,
                    )
                else:
                    val, err = await async_retry_call(
                        fn, args=args, kwargs=kw,
                        retries=retries, backoff=backoff,
                        retry_on=retry_on, rate_limiter=None,
                    )
            except asyncio.TimeoutError as e:
                if on_error == "raise":
                    raise TimeoutError(f"Timed out: {item}", item=item, original_error=e)
                elif on_error == "collect":
                    err = e
                    val = None
                else:
                    completed += 1
                    if progress:
                        elapsed = time.monotonic() - start
                        eta = (elapsed / completed * (total_tasks - completed)) if completed > 0 else 0
                        progress(ProgressInfo(completed=completed, total=total_tasks, elapsed=elapsed, eta=eta))
                    return task_idx, None

            duration = time.monotonic() - t0

            if err is not None:
                if on_error == "raise":
                    raise BatchError(f"Failed: {item}", item=item, original_error=err)
                elif on_error == "collect":
                    br = BatchResult(value=None, error=err, item=item, duration=duration)
                else:
                    br = None
            else:
                br = BatchResult(value=val, error=None, item=item, duration=duration)

            completed += 1
            if progress:
                elapsed = time.monotonic() - start
                eta = (elapsed / completed * (total_tasks - completed)) if completed > 0 else 0
                progress(ProgressInfo(completed=completed, total=total_tasks, elapsed=elapsed, eta=eta))

            return task_idx, br

    coros = [_process_task(idx, item) for idx, item in item_mapping]
    for coro in asyncio.as_completed(coros):
        task_idx, br = await coro
        if br is not None:
            yield br


def _takes_chunk(fn) -> bool:
    import inspect
    sig = inspect.signature(fn)
    return 'chunk' in sig.parameters


def _takes_items(fn) -> bool:
    import inspect
    sig = inspect.signature(fn)
    return 'items' in sig.parameters


def _map_sync(
    fn, items, *,
    max_workers=4, retries=0, backoff="exponential",
    retry_on=(Exception,), on_error="skip",
    chunk_size=None, rate_limit=None,
    ordered=True, timeout=None, progress=None,
) -> list[BatchResult]:
    """Synchronous batch map."""
    limiter = RateLimiter(rate_limit) if rate_limit else None

    if chunk_size is not None:
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        work_items = list(enumerate(chunks))
    else:
        work_items = list(enumerate(items))

    total = len(work_items)
    results = [None] * total
    completed = 0
    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, item in work_items:
            future = executor.submit(
                _process_single,
                fn, item, idx,
                chunk_size=chunk_size, retries=retries,
                backoff=backoff, retry_on=retry_on,
                rate_limiter=limiter, timeout=timeout,
            )
            futures[future] = idx

        for future in as_completed(futures):
            idx = futures[future]
            try:
                br = future.result()
            except BatchError as e:
                if on_error == "raise":
                    raise
                if on_error == "collect":
                    br = BatchResult(value=None, error=e.original_error or e, item=e.item, duration=0)
                else:
                    br = None
            except Exception as e:
                if on_error == "raise":
                    raise
                if on_error == "collect":
                    br = BatchResult(value=None, error=e, item=work_items[idx][1], duration=0)
                else:
                    br = None

            if br is not None:
                results[idx] = br

            completed += 1
            if progress:
                elapsed = time.monotonic() - start
                eta = (elapsed / completed * (total - completed)) if completed > 0 else 0
                progress(ProgressInfo(completed=completed, total=total, elapsed=elapsed, eta=eta))

    if on_error in ("skip", "collect"):
        results = [r for r in results if r is not None]

    return results


def _stream_sync(
    fn, items, *,
    max_workers=4, retries=0, backoff="exponential",
    retry_on=(Exception,), on_error="skip",
    chunk_size=None, rate_limit=None,
    timeout=None, progress=None,
) -> Generator[BatchResult, None, None]:
    """Streaming sync batch map — yields results as they complete."""
    limiter = RateLimiter(rate_limit) if rate_limit else None

    if chunk_size is not None:
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        work_items = list(enumerate(chunks))
    else:
        work_items = list(enumerate(items))

    total = len(work_items)
    completed = 0
    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, item in work_items:
            future = executor.submit(
                _process_single,
                fn, item, idx,
                chunk_size=chunk_size, retries=retries,
                backoff=backoff, retry_on=retry_on,
                rate_limiter=limiter, timeout=timeout,
            )
            futures[future] = idx

        for future in as_completed(futures):
            idx = futures[future]
            try:
                br = future.result()
            except BatchError as e:
                if on_error == "raise":
                    raise
                if on_error == "collect":
                    br = BatchResult(value=None, error=e.original_error or e, item=e.item, duration=0)
                else:
                    br = None
            except Exception as e:
                if on_error == "raise":
                    raise
                if on_error == "collect":
                    br = BatchResult(value=None, error=e, item=work_items[idx][1], duration=0)
                else:
                    br = None

            completed += 1
            if progress:
                elapsed = time.monotonic() - start
                eta = (elapsed / completed * (total - completed)) if completed > 0 else 0
                progress(ProgressInfo(completed=completed, total=total, elapsed=elapsed, eta=eta))

            if br is not None:
                yield br


def _process_single(fn, item, idx, *, chunk_size=None, retries=0,
                    backoff="exponential", retry_on=(Exception,),
                    rate_limiter=None, timeout=None):
    """Process a single item with retries, rate limiting, and timeout."""
    if chunk_size is not None:
        args = ()
        kwargs = {"chunk": item}
    else:
        args = (item,)
        kwargs = {}

    if timeout is not None:
        from concurrent.futures import ThreadPoolExecutor as TPE, TimeoutError as FuturesTimeout
        tpe = TPE(max_workers=1)
        future = tpe.submit(
            retry_call, fn, args=args, kwargs=kwargs,
            retries=retries, backoff=backoff, retry_on=retry_on,
            rate_limiter=rate_limiter,
        )
        try:
            val, err = future.result(timeout=timeout)
        except FuturesTimeout:
            future.cancel()
            tpe.shutdown(wait=False, cancel_futures=True)
            raise TimeoutError(
                f"Item {item} timed out after {timeout}s",
                item=item,
                original_error=FuturesTimeout(f"Timed out after {timeout}s"),
            )
        finally:
            tpe.shutdown(wait=False)
    else:
        val, err = retry_call(
            fn, args=args, kwargs=kwargs,
            retries=retries, backoff=backoff, retry_on=retry_on,
            rate_limiter=rate_limiter,
        )

    if err is not None:
        raise BatchError(f"Failed processing item: {item}", item=item, original_error=err)

    return BatchResult(value=val, error=None, item=item, duration=0)
