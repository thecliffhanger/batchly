# batchly

[![PyPI version](https://badge.fury.io/py/batchly.svg)](https://pypi.org/project/batchly)
[![Python versions](https://img.shields.io/pypi/pyversions/batchly.svg)](https://pypi.org/project/batchly)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Batch processing made simple — concurrency, retries, progress, and error handling.

## Install

```bash
pip install batchly
```

## Quick Start

```python
from batchly import batch, Batch, batch_map, batch_filter, batch_for_each, ProgressBar

# Decorator
@batch(max_workers=10, retries=3)
def process(item):
    return item * 2

results = process([1, 2, 3, 4, 5])  # list[BatchResult]

# Functional
results = batch_map(transform, items, max_workers=20, retries=3)  # list[BatchResult]
keep = batch_filter(predicate, items, max_workers=10)  # list of matching items
batch_for_each(side_effect, items, max_workers=5)  # side effects only

# Reusable context
b = Batch(max_workers=10, retries=3, progress=ProgressBar())
results = b.map(fn, items)  # list[BatchResult]
values = [r.value for r in results if r.ok]
filtered = b.filter(pred, values)  # list of matching items
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

## License

MIT

---

Part of the [thecliffhanger](https://github.com/thecliffhanger) open source suite.
