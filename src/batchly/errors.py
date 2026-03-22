"""Custom errors for batchly."""


class BatchError(Exception):
    """Raised when a batch operation fails (on_error='raise')."""

    def __init__(self, message: str, item=None, original_error: Exception | None = None):
        super().__init__(message)
        self.item = item
        self.original_error = original_error


class TimeoutError(BatchError):
    """Raised when an item exceeds its timeout."""
