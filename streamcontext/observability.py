"""HTTP metrics + health endpoint shared by the long-running processes.

The ingestion gateway and the catalog refresher each start one of these on a
background thread. It serves Prometheus metrics at `/metrics` and a
liveness/readiness probe at `/health` (also `/healthz`). The MCP server runs
over stdio and does not use this.

The server is best-effort: a bind failure (for example, a port already taken by
a co-located streamcontext process) is logged and swallowed so it can never
take down ingestion or the refresher. Metric definitions live in the process
that owns them (`streamcontext.metrics`, `streamcontext.catalog.metrics`); this
module only serves whatever is in the default Prometheus registry.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from streamcontext.config import Settings
from streamcontext.logging import get_logger

log = get_logger("streamcontext.observability")

# A health check returns (healthy, details). `details` must be JSON-serialisable.
HealthCheck = Callable[[], "tuple[bool, dict]"]

_HEALTH_PATHS = frozenset({"/health", "/healthz", "/livez", "/readyz"})


def _default_health() -> tuple[bool, dict]:
    return True, {"status": "ok"}


def _make_handler(health_check: HealthCheck) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == "/metrics":
                self._write(200, generate_latest(), CONTENT_TYPE_LATEST)
            elif path in _HEALTH_PATHS:
                healthy, details = health_check()
                body = json.dumps(details).encode("utf-8")
                self._write(200 if healthy else 503, body, "application/json")
            else:
                self.send_response(404)
                self.end_headers()

        def _write(self, code: int, body: bytes, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            # Silence the default per-request stderr logging; we have structlog.
            return

    return _Handler


class MetricsServer:
    """A threaded HTTP server exposing /metrics and /health."""

    def __init__(self, host: str, port: int, health_check: HealthCheck) -> None:
        self._server = ThreadingHTTPServer((host, port), _make_handler(health_check))
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="sc-metrics", daemon=True
        )

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def start_metrics_server(
    settings: Settings,
    *,
    health_check: HealthCheck | None = None,
) -> MetricsServer | None:
    """Start the metrics/health server, or return None if disabled or unbindable."""
    if not settings.metrics_enabled:
        return None
    try:
        server = MetricsServer(
            settings.metrics_host,
            settings.metrics_port,
            health_check or _default_health,
        )
        server.start()
    except OSError as exc:
        log.warning(
            "metrics.server.bind_failed",
            host=settings.metrics_host,
            port=settings.metrics_port,
            error=str(exc),
        )
        return None
    log.info("metrics.server.started", host=settings.metrics_host, port=server.port)
    return server


__all__ = ["HealthCheck", "MetricsServer", "start_metrics_server"]
