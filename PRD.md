# PRD — batchly v1.0

## What It Is
Batch processing made simple. Process thousands of items with concurrency control, retries, progress tracking, and error handling — all in a decorator or a one-liner.

## Why It Matters
- Every data pipeline, API migration, file processing task needs batching
- No lightweight stdlib solution — everyone writes their own
- `multiprocessing`/`concurrent.futures` are low-level
- `celery` is overkill for simple batch jobs
- Nobody owns "simple batch processing" on PyPI

## Core Features

### 1. Decorator-based batching
```python
from batchly import batch, batch_map

@batch(max_workers=10, retries=3)
def process_item(item):
    # Called for each item in the batch
    ...

results = process_item(items)  # processes all items in parallel
```

### 2. Functional API
```python
from batchly import batch_map, batch_filter, batch_for_each

# Map: transform items
results = batch_map(transform, items, max_workers=20, retries=3)

# Filter: keep items matching predicate
keep = batch_filter(should_keep, items, max_workers=10)

# For-each: side effects
batch_for_each(send_notification, users, max_workers=5, retries=2)
```

### 3. Progress tracking
```python
from batchly import batch_map, ProgressBar

results = batch_map(
    process,
    large_dataset,
    max_workers=10,
    progress=ProgressBar()  # or progress=print  or progress=my_handler
)
# [████████████████████░░░░░░░░] 67% (6700/10000) 2.3s elapsed, 1.1s remaining
```

### 4. Error handling
```python
results = batch_map(
    risky_operation,
    items,
    on_error="skip",        # skip failed items, continue
    # on_error="raise",     # raise on first failure
    # on_error="collect",   # collect errors, return with results
)

if on_error="collect":
    for item, result in results:
        if result.error:
            print(f"Failed: {item} — {result.error}")
        else:
            print(f"Success: {result.value}")
```

### 5. Retry with backoff
```python
results = batch_map(
    call_api,
    endpoints,
    max_workers=5,
    retries=5,
    backoff="exponential",
    retry_on=(ConnectionError, TimeoutError),
)
```

### 6. Chunked processing
```python
# Process in chunks of 100
results = batch_map(
    process,
    items,
    chunk_size=100,     # group 100 items per worker call
    max_workers=5,
)

def process(chunk):
    """Receives a list of 100 items"""
    return bulk_insert(chunk)
```

### 7. Rate limiting
```python
results = batch_map(
    call_api,
    items,
    max_workers=10,
    rate_limit=100,      # max 100 calls per second
)
```

### 8. Ordered results
```python
results = batch_map(transform, items, max_workers=10, ordered=True)
# results[i] corresponds to items[i], regardless of completion order
```

### 9. Timeout
```python
results = batch_map(
    slow_operation,
    items,
    max_workers=5,
    timeout=30,          # per-item timeout in seconds
)
```

### 10. Generator / streaming
```python
for result in batch_map(process, items, max_workers=10, stream=True):
    if result.ok:
        yield result.value
    # results available as they complete
```

### 11. Async support
```python
import asyncio
from batchly import batch_map

results = await batch_map(async_fetch, urls, max_workers=20)
```

### 12. Batch context
```python
from batchly import Batch

batch = Batch(max_workers=10, retries=3, progress=print)

results = batch.map(step1, items)
filtered = batch.filter(is_valid, results)
batch.foreach(save, filtered)
```

## API Surface
- `@batch(max_workers, retries, backoff, ...)` — decorator
- `batch_map(fn, items, ...)` — parallel map
- `batch_filter(fn, items, ...)` — parallel filter
- `batch_for_each(fn, items, ...)` — parallel for-each
- `Batch(max_workers, ...)` — reusable batch context
- `ProgressBar()` — progress display
- `BatchResult(value, error, item, duration)` — result wrapper

## Options
- `max_workers: int` — concurrency level
- `retries: int` — retry count per item
- `backoff: str` — "fixed", "exponential", "adaptive"
- `retry_on: tuple[type]` — exception types to retry
- `on_error: str` — "skip", "raise", "collect"
- `chunk_size: int | None` — group items
- `rate_limit: int | None` — max calls/second
- `ordered: bool` — preserve input order
- `timeout: float | None` — per-item timeout
- `stream: bool` — yield results as they complete
- `progress: callable | ProgressBar` — progress handler

## Dependencies
- Zero required (stdlib only)
- Optional: `rich` for pretty progress bars

## Testing
- 120+ tests
- Concurrency edge cases
- Error handling (skip, raise, collect)
- Retry behavior
- Rate limiting accuracy
- Chunked processing
- Ordered results
- Timeout behavior
- Async variants
- Progress tracking

## Target
- Python 3.10+
- MIT license
