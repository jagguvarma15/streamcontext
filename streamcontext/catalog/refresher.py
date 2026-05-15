"""Catalog refresher entrypoint.

Run as `python -m streamcontext.catalog.refresher` to refresh all configured
topics once, or with `--loop` to run on the configured cadence forever.

The refresher process is intentionally separate from both the ingestion
gateway and the MCP server. They share state via the SQLite catalog file
and the Qdrant collection only.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from qdrant_client import AsyncQdrantClient

from streamcontext.catalog.activity import ActivityProfiler
from streamcontext.catalog.builder import CatalogBuilder
from streamcontext.catalog.inference import (
    InferenceEngine,
    LLMUnavailableError,
    build_llm_provider,
)
from streamcontext.catalog.introspect import MessageSampler, SchemaIntrospector
from streamcontext.catalog.models import CatalogConfig
from streamcontext.catalog.store import CatalogStore
from streamcontext.config import Settings, load_settings
from streamcontext.logging import configure_logging, get_logger

log = get_logger("streamcontext.catalog.refresher")


def _try_schema_registry(url: str):
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
    except ImportError:
        return None
    try:
        client = SchemaRegistryClient({"url": url})
        client.get_subjects()
        return client
    except Exception as exc:
        log.warning("catalog.sr.unreachable", url=url, error=str(exc))
        return None


def build_catalog_config(settings: Settings) -> CatalogConfig:
    return CatalogConfig(
        schema_refresh_sec=settings.catalog_schema_refresh_sec,
        sample_refresh_sec=settings.catalog_sample_refresh_sec,
        stats_refresh_sec=settings.catalog_stats_refresh_sec,
        inference_refresh_sec=settings.catalog_inference_refresh_sec,
        sample_count=settings.catalog_sample_count,
        retain_samples=settings.catalog_retain_samples,
        daily_llm_spend_ceiling_usd=settings.catalog_llm_daily_ceiling_usd,
        pii_redact_patterns=settings.catalog_pii_patterns_list,
        pii_redact_fields=settings.catalog_pii_fields_list,
    )


def build_builder(settings: Settings) -> tuple[CatalogBuilder, AsyncQdrantClient]:
    store = CatalogStore(settings.catalog_db_path)
    sr_client = _try_schema_registry(settings.schema_registry_url)
    introspector = SchemaIntrospector(sr_client)
    qdrant = AsyncQdrantClient(url=settings.qdrant_url)
    profiler = ActivityProfiler(qdrant, settings.qdrant_collection)
    sampler: MessageSampler | None = None
    if settings.catalog_enable_sampling:
        sampler = MessageSampler(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            schema_registry_url=settings.schema_registry_url,
            timeout_sec=settings.catalog_sample_timeout_sec,
        )
    catalog_config = build_catalog_config(settings)
    inference: InferenceEngine | None = None
    if settings.catalog_llm_provider != "disabled":
        try:
            provider = build_llm_provider(
                provider=settings.catalog_llm_provider,
                model=settings.catalog_llm_model,
            )
            inference = InferenceEngine(
                provider=provider,
                store=store,
                config=catalog_config,
            )
            log.info(
                "catalog.inference.enabled",
                provider=settings.catalog_llm_provider,
                model=settings.catalog_llm_model,
                ceiling_usd=catalog_config.daily_llm_spend_ceiling_usd,
            )
        except LLMUnavailableError as exc:
            log.warning(
                "catalog.inference.unavailable",
                provider=settings.catalog_llm_provider,
                error=str(exc),
            )
    builder = CatalogBuilder(
        store=store,
        introspector=introspector,
        sampler=sampler,
        profiler=profiler,
        config=catalog_config,
        inference=inference,
    )
    return builder, qdrant


async def refresh_once(builder: CatalogBuilder, topics: list[str]) -> None:
    for topic in topics:
        try:
            await builder.refresh_topic(topic)
        except Exception:
            log.exception("catalog.refresh.failed", topic=topic)


async def refresh_loop(
    builder: CatalogBuilder, topics: list[str], interval_sec: int
) -> None:
    while True:
        await refresh_once(builder, topics)
        await asyncio.sleep(interval_sec)


async def _async_main(loop: bool) -> int:
    settings = load_settings()
    configure_logging(level=settings.log_level, json=settings.log_json)
    builder, qdrant = build_builder(settings)
    topics = settings.catalog_topics_list or settings.topics_list
    if not topics:
        log.error("catalog.no_topics_configured")
        return 78
    log.info(
        "catalog.refresher.start",
        topics=topics,
        db=str(builder.store.path),
        loop=loop,
    )
    try:
        if loop:
            await refresh_loop(builder, topics, settings.catalog_stats_refresh_sec)
        else:
            await refresh_once(builder, topics)
    finally:
        await qdrant.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="streamcontext catalog refresher")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously, refreshing on the configured cadence.",
    )
    args = parser.parse_args()
    code = asyncio.run(_async_main(loop=args.loop))
    if code != 0:
        sys.exit(code)


if __name__ == "__main__":
    main()
