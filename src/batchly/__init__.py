"""batchly — Batch processing made simple."""

from .batch import Batch, batch
from .errors import BatchError, TimeoutError
from .filter_ import async_batch_filter, batch_filter
from .foreach import async_batch_for_each, batch_for_each
from .map_ import async_batch_map, batch_map
from .progress import ProgressBar, ProgressInfo
from .rate_limit import RateLimiter
from .result import BatchResult

__version__ = "0.1.0"
__all__ = [
    "batch",
    "Batch",
    "batch_map",
    "async_batch_map",
    "batch_filter",
    "async_batch_filter",
    "batch_for_each",
    "async_batch_for_each",
    "BatchResult",
    "BatchError",
    "TimeoutError",
    "ProgressBar",
    "ProgressInfo",
    "RateLimiter",
]
