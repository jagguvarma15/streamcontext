"""Prometheus metrics for the catalog refresher.

Kept separate from the gateway metrics so each process's /metrics stays scoped
to what it actually does. See `streamcontext.observability` for the HTTP server.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import Counter, Gauge, Histogram

REFRESH_TOTAL = Counter(
    "sc_catalog_refresh_total",
    "Catalog aspect refreshes, by aspect and result.",
    ["aspect", "result"],
)
REFRESH_SECONDS = Histogram(
    "sc_catalog_refresh_seconds",
    "Wall time to refresh one catalog aspect.",
    ["aspect"],
)
LLM_SPEND_TODAY = Gauge(
    "sc_catalog_llm_spend_usd_today",
    "LLM inference spend so far in the current UTC day, per provider.",
    ["provider"],
)
LAST_CYCLE_TIMESTAMP = Gauge(
    "sc_catalog_last_cycle_timestamp_seconds",
    "Unix time of the end of the most recent refresh cycle.",
)
UP = Gauge(
    "sc_catalog_up",
    "1 once the refresher has completed at least one cycle, 0 otherwise.",
)


@contextmanager
def track(aspect: str) -> Iterator[None]:
    """Time a catalog aspect refresh and count its success or failure.

    Re-raises on error so existing control flow (a failed aspect aborting the
    topic refresh) is unchanged; it only records the outcome first.
    """
    started = time.perf_counter()
    ok = True
    try:
        yield
    except Exception:
        ok = False
        raise
    finally:
        REFRESH_SECONDS.labels(aspect=aspect).observe(time.perf_counter() - started)
        REFRESH_TOTAL.labels(aspect=aspect, result="ok" if ok else "error").inc()
