"""Entrypoint for the streamcontext gateway.

Wires config → consumer → embedder → sink → pipeline, then runs until SIGTERM.
The actual pipeline implementation lands in Day 3.
"""

from __future__ import annotations

import asyncio

from streamcontext.config import load_settings
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
    )

    # Pipeline assembly arrives in Day 3. For now this is a stub that proves
    # config loading and logging work end to end.
    from streamcontext.pipeline import build_and_run

    await build_and_run(settings)


def run() -> None:
    try:
        import uvloop  # type: ignore

        uvloop.install()
    except ImportError:
        pass
    asyncio.run(_amain())


if __name__ == "__main__":
    run()
