"""Progress tracking for batch operations."""

from __future__ import annotations

import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ProgressInfo:
    """Progress information passed to callbacks."""

    completed: int = 0
    total: int = 0
    elapsed: float = 0.0
    eta: float = 0.0

    @property
    def pct(self) -> float:
        if self.total == 0:
            return 0.0
        return self.completed / self.total * 100.0

    @property
    def rate(self) -> float:
        if self.elapsed == 0:
            return 0.0
        return self.completed / self.elapsed


class ProgressBar:
    """Terminal progress bar.

    Usage:
        results = batch_map(fn, items, progress=ProgressBar())
    """

    def __init__(self, width: int = 40, file=None):
        self.width = width
        self._file = file or sys.stderr
        self._lock = threading.Lock()
        self._start = time.monotonic()

    def __call__(self, info: ProgressInfo) -> None:
        pct = info.pct
        filled = int(self.width * pct / 100)
        bar = "█" * filled + "░" * (self.width - filled)
        elapsed = info.elapsed
        eta = info.eta
        if eta >= 60:
            eta_str = f"{eta / 60:.1f}m"
        elif eta > 0:
            eta_str = f"{eta:.1f}s"
        else:
            eta_str = "—"
        if elapsed >= 60:
            elapsed_str = f"{elapsed / 60:.1f}m"
        else:
            elapsed_str = f"{elapsed:.1f}s"

        line = f"\r[{bar}] {pct:5.1f}% ({info.completed}/{info.total}) {elapsed_str} elapsed, {eta_str} remaining"
        with self._lock:
            self._file.write(line)
            self._file.flush()
            if info.completed >= info.total and info.total > 0:
                self._file.write("\n")
                self._file.flush()

    def reset(self) -> None:
        self._start = time.monotonic()
