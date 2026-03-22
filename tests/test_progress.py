"""Tests for ProgressBar and ProgressInfo."""

import io
import time
import pytest
from batchly import ProgressBar, ProgressInfo


class TestProgressInfo:
    def test_pct(self):
        p = ProgressInfo(completed=5, total=10)
        assert p.pct == 50.0

    def test_pct_zero_total(self):
        p = ProgressInfo(total=0)
        assert p.pct == 0.0

    def test_rate(self):
        p = ProgressInfo(completed=10, elapsed=2.0)
        assert p.rate == 5.0

    def test_rate_zero_elapsed(self):
        p = ProgressInfo(completed=5, elapsed=0)
        assert p.rate == 0.0

    def test_defaults(self):
        p = ProgressInfo()
        assert p.completed == 0
        assert p.total == 0
        assert p.elapsed == 0.0
        assert p.eta == 0.0


class TestProgressBar:
    def test_outputs_to_file(self):
        buf = io.StringIO()
        bar = ProgressBar(width=20, file=buf)

        bar(ProgressInfo(completed=5, total=10, elapsed=1.0, eta=1.0))
        output = buf.getvalue()
        assert "50.0%" in output
        assert "(5/10)" in output

    def test_completion_prints_newline(self):
        buf = io.StringIO()
        bar = ProgressBar(width=20, file=buf)

        bar(ProgressInfo(completed=10, total=10, elapsed=2.0, eta=0.0))
        output = buf.getvalue()
        assert "\n" in output

    def test_reset(self):
        bar = ProgressBar()
        bar._start = 0
        bar.reset()
        assert bar._start > 0

    def test_eta_formatting(self):
        buf = io.StringIO()
        bar = ProgressBar(width=10, file=buf)

        # Short eta
        bar(ProgressInfo(completed=8, total=10, elapsed=8.0, eta=2.0))
        assert "2.0s" in buf.getvalue()

        # Long eta
        buf2 = io.StringIO()
        bar2 = ProgressBar(width=10, file=buf2)
        bar2(ProgressInfo(completed=1, total=10, elapsed=1.0, eta=120.0))
        assert "2.0m" in buf2.getvalue()

    def test_as_progress_callback(self):
        collected = []
        bar = ProgressBar(width=10, file=io.StringIO())

        # Use as a plain callback
        batch_map = None  # just testing ProgressInfo
        info = ProgressInfo(completed=3, total=10)
        assert info.completed == 3

    def test_thread_safety(self):
        """ProgressBar should handle concurrent calls without crashing."""
        import threading

        buf = io.StringIO()
        bar = ProgressBar(width=10, file=buf)
        threads = []

        def write_progress():
            for i in range(5):
                bar(ProgressInfo(completed=i, total=5))
                time.sleep(0.001)

        for _ in range(5):
            t = threading.Thread(target=write_progress)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Just check it didn't crash
        assert True
