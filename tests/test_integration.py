"""Integration tests for batchly — real-world-ish scenarios."""

import asyncio
import os
import sqlite3
import tempfile
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen

import pytest

from batchly import (
    Batch,
    BatchError,
    BatchResult,
    batch_map,
    batch_filter,
    batch_for_each,
    async_batch_map,
    async_batch_filter,
    async_batch_for_each,
)


# ── Mock HTTP server ────────────────────────────────────────────

class _EchoHandler(BaseHTTPRequestHandler):
    """Returns the request path as the body."""

    def do_GET(self):
        if "/error" in self.path:
            self.send_error(500, "internal error")
        elif "/slow" in self.path:
            time.sleep(0.1)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(self.path.encode())
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(self.path.encode())

    def log_message(self, *args):
        pass  # silence logs


@pytest.fixture(scope="module")
def http_server():
    server = HTTPServer(("127.0.0.1", 0), _EchoHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.1)
    yield port
    server.shutdown()


class TestHTTPBatch:
    def test_fetch_urls(self, http_server):
        port = http_server
        urls = [f"http://127.0.0.1:{port}/item/{i}" for i in range(10)]

        def fetch(url):
            with urlopen(url) as resp:
                return resp.read().decode()

        results = batch_map(fetch, urls, max_workers=5, ordered=True)
        assert len(results) == 10
        for i, r in enumerate(results):
            assert r.ok
            assert f"/item/{i}" in r.value

    def test_fetch_with_errors(self, http_server):
        port = http_server
        urls = [
            f"http://127.0.0.1:{port}/item/{i}" if i % 2 == 0
            else f"http://127.0.0.1:{port}/error/{i}"
            for i in range(10)
        ]

        def fetch(url):
            with urlopen(url) as resp:
                return resp.read().decode()

        results = batch_map(fetch, urls, max_workers=4, on_error="collect")
        successes = [r for r in results if r.ok]
        errors = [r for r in results if r.error is not None]
        assert len(successes) == 5
        assert len(errors) == 5

    def test_fetch_with_retry(self, http_server):
        port = http_server
        # All error URLs — retries won't help but should not crash
        urls = [f"http://127.0.0.1:{port}/error/{i}" for i in range(3)]

        def fetch(url):
            with urlopen(url) as resp:
                return resp.read().decode()

        results = batch_map(fetch, urls, max_workers=2, retries=1, on_error="collect")
        assert len(results) == 3
        assert all(r.error is not None for r in results)

    def test_fetch_stream(self, http_server):
        port = http_server
        urls = [f"http://127.0.0.1:{port}/slow/{i}" for i in range(5)]

        def fetch(url):
            with urlopen(url) as resp:
                return resp.read().decode()

        gen = batch_map(fetch, urls, max_workers=5, stream=True)
        results = list(gen)
        assert len(results) == 5


# ── File processing ─────────────────────────────────────────────

class TestFileProcessing:
    def test_batch_file_read(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            for i in range(5):
                path = os.path.join(tmpdir, f"file_{i}.txt")
                with open(path, "w") as f:
                    f.write(f"content {i}")

            files = [os.path.join(tmpdir, f"file_{i}.txt") for i in range(5)]

            def read_file(path):
                with open(path) as f:
                    return f.read()

            results = batch_map(read_file, files, max_workers=3, ordered=True)
            assert len(results) == 5
            for i, r in enumerate(results):
                assert r.value == f"content {i}"

    def test_batch_file_write(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = [os.path.join(tmpdir, f"out_{i}.txt") for i in range(10)]

            def write_file(path):
                idx = paths.index(path)
                with open(path, "w") as f:
                    f.write(f"item {idx}")
                return idx

            results = batch_map(write_file, paths, max_workers=4, ordered=True)
            assert len(results) == 10

            # Verify all files written
            for i in range(10):
                with open(os.path.join(tmpdir, f"out_{i}.txt")) as f:
                    assert f.read() == f"item {i}"


# ── Database operations ─────────────────────────────────────────

class TestDatabaseOperations:
    def test_batch_db_insert(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()

            lock = threading.Lock()

            def insert_item(item):
                with lock:
                    conn.execute("INSERT INTO items (id, value) VALUES (?, ?)", (item, f"val_{item}"))
                    conn.commit()
                return item

            results = batch_map(insert_item, range(20), max_workers=5, ordered=True)
            assert len(results) == 20

            cursor = conn.execute("SELECT COUNT(*) FROM items")
            assert cursor.fetchone()[0] == 20
            conn.close()

    def test_batch_db_query(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
            for i in range(100):
                conn.execute("INSERT INTO items (id, value) VALUES (?, ?)", (i, f"item_{i}"))
            conn.commit()

            lock = threading.Lock()

            def query_item(item_id):
                with lock:
                    cursor = conn.execute("SELECT value FROM items WHERE id = ?", (item_id,))
                    row = cursor.fetchone()
                return row[0] if row else None

            results = batch_map(query_item, range(100), max_workers=10, ordered=True)
            assert len(results) == 100
            for i, r in enumerate(results):
                assert r.value == f"item_{i}"

            conn.close()


# ── Chained operations ──────────────────────────────────────────

class TestChainedOperations:
    def test_map_filter_foreach(self):
        seen = []
        lock = threading.Lock()

        results = batch_map(lambda x: x * 2, range(20), max_workers=4, ordered=True)
        filtered = batch_filter(lambda r: r.value % 3 == 0, results, max_workers=2)

        def collect(r):
            with lock:
                seen.append(r.value)

        batch_for_each(collect, filtered, max_workers=2)

        expected = [x * 2 for x in range(20) if (x * 2) % 3 == 0]
        assert sorted(seen) == sorted(expected)

    def test_map_then_map(self):
        r1 = batch_map(lambda x: x + 10, range(5), max_workers=2)
        r2 = batch_map(lambda b: b.value ** 2, r1, max_workers=2)
        assert [r.value for r in r2] == [100, 121, 144, 169, 196]

    def test_filter_chain(self):
        items = list(range(50))
        evens = batch_filter(lambda x: x % 2 == 0, items, max_workers=4)
        tens = batch_filter(lambda x: x % 5 == 0, evens, max_workers=4)
        assert tens == [0, 10, 20, 30, 40]


# ── Memory usage with large batches ─────────────────────────────

class TestMemoryUsage:
    def test_large_batch_does_not_leak_threads(self):
        import threading

        before = threading.active_count()

        results = batch_map(lambda x: [0] * 100, range(1000), max_workers=50)
        assert len(results) == 1000

        # Give threads time to clean up
        time.sleep(1)
        after = threading.active_count()
        # Allow some margin for background threads
        assert after <= before + 5, f"Thread leak detected: {before} → {after}"


# ── Graceful shutdown / cancellation ────────────────────────────

class TestGracefulShutdown:
    def test_partial_processing_on_error(self):
        """When some items fail, others should still be processed."""
        fail_indices = {2, 5, 8}

        def sometimes_fail(x):
            if x in fail_indices:
                raise ValueError(f"fail on {x}")
            return x * 2

        results = batch_map(sometimes_fail, range(10), max_workers=4, on_error="collect")
        successes = [r for r in results if r.ok]
        errors = [r for r in results if r.error is not None]

        assert len(successes) == 7
        assert len(errors) == 3

    def test_error_early_stop_on_raise(self):
        """on_error='raise' should stop processing on first error."""
        # With ordered=True and on_error='raise', it raises immediately
        # but other futures may already be running
        with pytest.raises(BatchError):
            batch_map(lambda x: 1/0 if x == 5 else x, range(10), on_error="raise", max_workers=1)


# ── Batch context reuse across operations ───────────────────────

class TestBatchContextReuseIntegration:
    def test_context_map_filter_foreach(self):
        ctx = Batch(max_workers=4, on_error="skip")

        mapped = ctx.map(lambda x: x ** 2, range(20))
        filtered = ctx.filter(lambda b: b.value > 50, mapped)
        collected = []
        lock = threading.Lock()

        def collect(b):
            with lock:
                collected.append(b.value)

        ctx.foreach(collect, filtered)

        expected = [x ** 2 for x in range(20) if x ** 2 > 50]
        assert sorted(collected) == sorted(expected)


# ── Async integration ───────────────────────────────────────────

class TestAsyncIntegration:
    @pytest.mark.asyncio
    async def test_async_chain(self):
        async def add_one(x):
            return x + 1
        r1 = await async_batch_map(add_one, range(10), max_workers=4)

        async def is_even(r):
            return r.value % 2 == 0
        r2 = await async_batch_filter(is_even, r1)
        assert len(r2) == 5  # 2,4,6,8,10

    @pytest.mark.asyncio
    async def test_async_context_reuse(self):
        async def triple(x):
            return x * 3
        async def inc(x):
            return x + 1
        ctx = Batch(max_workers=4)
        r1 = await ctx.amap(triple, range(5))
        r2 = await ctx.amap(inc, range(5))
        assert [r.value for r in r1] == [0, 3, 6, 9, 12]
        assert [r.value for r in r2] == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_async_stream(self):
        async def slow(x):
            await asyncio.sleep(0.01)
            return x * 2

        gen = await async_batch_map(slow, range(20), stream=True, max_workers=5)
        results = []
        async for r in gen:
            results.append(r)
        assert len(results) == 20
        assert {r.value for r in results} == {i * 2 for i in range(20)}

    @pytest.mark.asyncio
    async def test_async_foreach_collected(self):
        items = []
        async def append(x):
            items.append(x)

        await async_batch_for_each(append, range(50), max_workers=10)
        assert sorted(items) == list(range(50))

    @pytest.mark.asyncio
    async def test_async_rate_limit(self):
        async def identity(x):
            return x

        start = time.monotonic()
        results = await async_batch_map(identity, range(3), rate_limit=5, max_workers=10)
        elapsed = time.monotonic() - start
        assert len(results) == 3
        # At 5/sec, 3 items should be near-instant
        assert elapsed < 2
