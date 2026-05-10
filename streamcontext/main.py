"""Entrypoint for the streamcontext gateway.

Loads config, configures logging, and runs the pipeline until SIGTERM or a
fatal error. Fatal errors exit non-zero so process supervisors (compose,
systemd, k8s) can react.
"""

from __future__ import annotations

import asyncio
import sys

from streamcontext.config import load_settings
from streamcontext.errors import ConfigurationError, PipelineFatalError
from streamcontext.logging import configure_logging, get_logger


async def _amain() -> None:
    settings = load_settings()
    configure_logging(level=settings.log_level, json=settings.log_json)
    log = get_logger("streamcontext")
    log.info(
        "gateway.start",
        topics=settings.topics_list,
        embedder=settings.embedder_provider,
        sink=settings.sink_provider,
        redact_fields=sorted(settings.redact_fields_set),
        include_headers=settings.payload_include_headers,
    )

    from streamcontext.pipeline import build_and_run

    await build_and_run(settings)


def run() -> None:
    try:
        import uvloop  # type: ignore

        uvloop.install()
    except ImportError:
        pass
    try:
        asyncio.run(_amain())
    except ConfigurationError as exc:
        # Configuration errors are operator-fixable. Print clearly and exit 78
        # (sysexits.h EX_CONFIG) so supervisors can distinguish from crashes.
        print(f"streamcontext: configuration error: {exc}", file=sys.stderr)
        sys.exit(78)
    except PipelineFatalError as exc:
        print(f"streamcontext: pipeline halted: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run()
