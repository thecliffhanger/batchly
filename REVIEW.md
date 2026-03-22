# REVIEW.md — batchly Code Review

## Summary

Thorough review of the `batchly` library (v0.1.0) — a Python batch processing library with parallel execution, retries, rate limiting, chunked processing, and async support.

**Test suite: 189 tests all passing** (131 original + 38 adversarial + 20 integration + 10 existing round2).

---

## Bug Found & Fixed

### 🔴 Critical: Timeout blocking in `_process_single` (map_.py)

**Problem:** When `timeout` was set, `_process_single` created an inner `ThreadPoolExecutor` using a `with` statement. Even after `future.result(timeout=...)` raised `FuturesTimeout`, the `with` block's `__exit__` called `shutdown(wait=True)`, blocking until the timed-out thread finished its work. A 0.5s timeout on a 10s sleep would still take 10s.

**Fix:** Replaced `with TPE(...) as tpe:` with manual `tpe = TPE(...)`, `future.cancel()`, and `tpe.shutdown(wait=False, cancel_futures=True)` in the except path, plus `tpe.shutdown(wait=False)` in finally.

---

## Code Review Notes

### ✅ Correct
- **Thread pool management:** `ThreadPoolExecutor` used as context manager in `_map_sync` and `_stream_sync` — proper cleanup.
- **Retry logic:** Correct loop structure, backoff computation, exception type filtering.
- **Rate limiter:** Token bucket implementation is correct with proper locking.
- **Ordered results:** Pre-allocated `results` array with index-based insertion works correctly for `ordered=True`.
- **Chunked processing:** Correct chunking with `[items[i:i+chunk_size] for i in range(0, len(items), chunk_size)]`.
- **Streaming generators:** Sync and async streaming yield results as they complete.
- **Error handling:** All three modes (skip/collect/raise) work correctly in both sync and async paths.
- **Progress tracking:** `ProgressInfo` is accurate — completed count, elapsed time, ETA calculation.
- **BatchResult:** Clean dataclass with `ok` property and `unwrap()` method.
- **Batch context:** Reusable `Batch` class with per-call overrides works well.
- **`@batch` decorator:** Smart single-item vs batch detection with proper string handling.

### ⚠️ Minor Issues (not fixed — acceptable for v0.1.0)

1. **`_takes_chunk` and `_takes_items` are defined but never used.** Dead code in map_.py.

2. **Async functions silently fail in sync `batch_map`.** The code detects this with `inspect.iscoroutinefunction` and raises `TypeError` — good. But `async_batch_map` with sync functions silently returns empty results (sync fn treated as error, skipped). This is technically correct (async path should use async fns) but could be more explicit with a warning or error.

3. **Nested ThreadPoolExecutor for timeout.** `_process_single` creates a new TPE per item when timeout is set. This doubles thread usage. Could use `signal.alarm` (Unix-only) or restructure to avoid nesting.

4. **Rate limiter + high max_workers can starve.** If `max_workers=50` but `rate_limit=1`, 49 threads block waiting for rate limiter tokens while only 1 does useful work. Functional but wasteful. A better design would cap concurrency to `min(max_workers, rate_limit)` when rate limiting.

5. **Progress callback exceptions are not caught.** If a user's progress callback raises, it propagates up and kills the batch. The adversarial test confirmed this. Wrapping in try/except would be more robust.

6. **`_async_map` with `ordered=False` uses `results.append(br)`** but `results` was pre-allocated as `[None] * total_tasks`. The None values get filtered out at the end, so it works, but it's slightly wasteful.

7. **No `__all__` validation** — the exported names match the module's `__all__` which is good.

### 💡 Suggestions for Future Versions

- Add a `max_concurrency` that's separate from `max_workers` to work better with rate limiting
- Catch progress callback exceptions silently
- Remove dead code (`_takes_chunk`, `_takes_items`)
- Add type hints for the streaming generator return paths
- Consider `asyncio.to_thread()` wrapper so sync functions can work in async path (with explicit opt-in)
- Add batch progress events (per-item start/complete) for richer monitoring

---

## Test Coverage

| Category | Tests | Status |
|----------|-------|--------|
| Original suite | 131 | ✅ All pass |
| Adversarial | 38 | ✅ All pass |
| Integration | 20 | ✅ All pass |
| **Total** | **189** | **✅** |

### Adversarial Tests Cover
- Empty/single item edge cases
- Mixed exception types
- Timeout with hanging functions
- Shared state thread safety
- 10,000 items × 100 workers stress
- Chunk size extremes (1, larger than list)
- Rate limit extremes (1/sec, 100K/sec)
- All-fail with retries exhausted
- Nested batch_map calls
- Invalid worker counts (0, -1)
- Progress callback that raises
- Async equivalents of all above

### Integration Tests Cover
- Real HTTP calls via mock server (success, errors, retries, streaming)
- File I/O batch processing
- SQLite batch operations (insert, query with thread-safe locking)
- Chained operations: map → filter → foreach
- Batch context reuse
- Thread leak detection
- Graceful partial processing on errors
