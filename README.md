# batchly

Batch processing made simple — concurrency, retries, progress, and error handling.

## Install

```bash
pip install -e .
```

## Quick Start

```python
from batchly import batch, batch_map, batch_filter, batch_for_each

# Decorator
@batch(max_workers=10, retries=3)
def process(item):
    return item * 2

results = process([1, 2, 3, 4, 5])

# Functional
results = batch_map(transform, items, max_workers=20, retries=3)
keep = batch_filter(predicate, items, max_workers=10)
batch_for_each(side_effect, items, max_workers=5)

# Reusable context
b = Batch(max_workers=10, retries=3, progress=ProgressBar())
results = b.map(fn, items)
filtered = b.filter(pred, results)
b.foreach(save, filtered)
```

## Features

- **Concurrency** — ThreadPoolExecutor for sync, asyncio for async
- **Retries** — Exponential/fixed/adaptive backoff
- **Error handling** — skip, raise, or collect errors
- **Rate limiting** — Token bucket algorithm
- **Progress** — Built-in ProgressBar or custom callbacks
- **Timeouts** — Per-item timeout support
- **Chunked** — Group items for bulk processing
- **Ordered** — Results match input order
- **Streaming** — Generator mode for results as they complete
- **Zero dependencies** — stdlib only

MIT License.
