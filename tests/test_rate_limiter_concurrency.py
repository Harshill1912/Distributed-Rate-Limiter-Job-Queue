"""
Concurrency test: proves the token-bucket limiter is atomic.

Fires many requests for the SAME user simultaneously (released together via a
barrier for maximum contention). A correct atomic limiter lets through at most
MAX_TOKENS; a naive read-modify-write limiter races and lets through more.
"""
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.redis_client import r
from app.rate_limiter import limiter


@pytest.fixture(autouse=True)
def require_redis():
    try:
        r.ping()
    except Exception:
        pytest.skip("Redis not reachable — start it with `docker compose up -d redis`")


def test_atomic_limiter_never_exceeds_burst_under_concurrency():
    user = "concurrency_user"
    r.delete(f"rate_limiter:{user}")

    n = 200
    start = threading.Barrier(n)   # release all threads at once
    results = []
    lock = threading.Lock()

    def hit():
        start.wait()
        ok = limiter.is_allowed(user)
        with lock:
            results.append(ok)

    with ThreadPoolExecutor(max_workers=n) as pool:
        for _ in range(n):
            pool.submit(hit)

    allowed = sum(results)
    # The whole point: even with 200 simultaneous requests, never more than the
    # burst size is allowed. A racy limiter would allow > MAX_TOKENS here.
    assert allowed <= limiter.MAX_TOKENS, (
        f"limiter allowed {allowed} requests, exceeding burst {limiter.MAX_TOKENS} "
        f"— the check-and-decrement is NOT atomic (race condition)"
    )
    assert allowed == limiter.MAX_TOKENS, (
        f"expected exactly {limiter.MAX_TOKENS} allowed, got {allowed}"
    )
