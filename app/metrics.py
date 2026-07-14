"""
Lightweight latency metrics backed by Redis.

Each sample is pushed onto a capped Redis list; percentiles are computed on read.
This keeps the app dependency-free (no Prometheus client needed) while still
exposing p50 / p95 / p99 — the numbers that actually matter for a rate limiter and
a job queue.
"""
import math
import time

from app.redis_client import r

MAX_SAMPLES = 2000   # rolling window per metric


def record_latency(name: str, ms: float) -> None:
    key = f"metrics:latency:{name}"
    pipe = r.pipeline()
    pipe.lpush(key, ms)
    pipe.ltrim(key, 0, MAX_SAMPLES - 1)   # keep only the most recent samples
    pipe.execute()


def _percentile(sorted_samples: list[float], p: float) -> float:
    if not sorted_samples:
        return 0.0
    k = max(0, math.ceil(p / 100 * len(sorted_samples)) - 1)
    return sorted_samples[min(k, len(sorted_samples) - 1)]


def latency_stats(name: str) -> dict:
    raw = r.lrange(f"metrics:latency:{name}", 0, -1)
    samples = sorted(float(x) for x in raw)
    if not samples:
        return {"count": 0, "avg_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0}
    return {
        "count": len(samples),
        "avg_ms": round(sum(samples) / len(samples), 3),
        "p50_ms": round(_percentile(samples, 50), 3),
        "p95_ms": round(_percentile(samples, 95), 3),
        "p99_ms": round(_percentile(samples, 99), 3),
    }


class timed:
    """Context manager that records the wrapped block's latency (ms).

        with timed("rate_limit"):
            is_allowed(user)
    """

    def __init__(self, name: str):
        self.name = name

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        record_latency(self.name, (time.perf_counter() - self._t0) * 1000)
