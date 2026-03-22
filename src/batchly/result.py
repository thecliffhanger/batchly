"""Result wrapper for batch operations."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class BatchResult(Generic[T]):
    """Wraps the result of processing a single item."""

    value: T | None = None
    error: Exception | None = None
    item: Any = None
    duration: float = 0.0

    @property
    def ok(self) -> bool:
        return self.error is None

    def unwrap(self) -> T:
        if self.error is not None:
            raise self.error
        return self.value
