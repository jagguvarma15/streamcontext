"""Builds and refreshes catalog entries for individual topics.

The deterministic aspects (schema, samples, stats) and the optional LLM
inference pass are coordinated here so callers refresh in one place.
"""

from __future__ import annotations

import time
from typing import Any

from streamcontext.catalog.activity import ActivityProfiler
from streamcontext.catalog.inference import InferenceEngine
from streamcontext.catalog.introspect import MessageSampler, SchemaIntrospector
from streamcontext.catalog.models import (
    CatalogConfig,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.store import CatalogStore
from streamcontext.logging import get_logger

log = get_logger("streamcontext.catalog.builder")


class CatalogBuilder:
    """Coordinates schema, sample, and activity refresh for a topic."""

    def __init__(
        self,
        *,
        store: CatalogStore,
        introspector: SchemaIntrospector,
        sampler: MessageSampler | None,
        profiler: ActivityProfiler,
        config: CatalogConfig | None = None,
        inference: InferenceEngine | None = None,
    ) -> None:
        self._store = store
        self._introspector = introspector
        self._sampler = sampler
        self._profiler = profiler
        self._config = config or CatalogConfig()
        self._inference = inference

    @property
    def config(self) -> CatalogConfig:
        return self._config

    @property
    def store(self) -> CatalogStore:
        return self._store

    async def refresh_topic(
        self,
        topic: str,
        *,
        schema: bool = True,
        samples: bool = True,
        stats: bool = True,
        inference: bool | None = None,
    ) -> TopicEntry:
        """Refresh the requested aspects of one topic and return the entry."""
        now_ms = int(time.time() * 1000)
        existing = self._store.get_topic(topic)
        entry = existing or TopicEntry(name=topic)

        if schema:
            subject, schema_id, version, fingerprint, raw_json, fields = (
                self._introspector.introspect(topic)
            )
            entry.schema_subject = subject
            entry.schema_id = schema_id
            entry.schema_version = version
            if fingerprint is not None:
                entry.schema_fingerprint = fingerprint
            entry.last_schema_refresh_ms = now_ms
            # Persist field list before topic so the FK is satisfied either way
            self._store.upsert_topic(entry, raw_schema_json=raw_json)
            self._store.replace_fields(topic, fields)
            entry.fields = fields
            log.info(
                "catalog.refresh.schema",
                topic=topic,
                subject=subject,
                version=version,
                fields=len(fields),
                fingerprint=fingerprint[:12] if fingerprint else None,
            )
        else:
            self._store.upsert_topic(entry)

        if samples and self._sampler is not None:
            sampled = await self._sampler.sample(topic, count=self._config.sample_count)
            entry.samples = sampled
            entry.last_sample_refresh_ms = now_ms
            self._store.replace_samples(
                topic, sampled, retain=self._config.retain_samples
            )
            self._store.upsert_topic(entry)
            log.info("catalog.refresh.samples", topic=topic, n=len(sampled))

        if stats:
            activity = await self._profiler.profile(topic)
            entry.activity = activity
            entry.last_stats_refresh_ms = now_ms
            self._store.upsert_activity(topic, activity)
            self._store.upsert_topic(entry)
            log.info(
                "catalog.refresh.stats",
                topic=topic,
                last_hour=activity.messages_last_hour,
                last_day=activity.messages_last_day,
            )

        run_inference = inference if inference is not None else (self._inference is not None)
        if run_inference and self._inference is not None:
            await self._run_inference(entry)

        return entry

    async def _run_inference(self, entry: "TopicEntry") -> None:
        assert self._inference is not None
        if not entry.fields:
            log.debug("catalog.inference.skip_no_fields", topic=entry.name)
            return
        status, description, conf, annotations = await self._inference.infer(entry)
        entry.inference_status = status
        if description is not None:
            entry.description = description
        if conf is not None:
            entry.description_confidence = conf
        entry.last_inference_refresh_ms = int(time.time() * 1000)
        self._store.upsert_topic(entry)
        if annotations:
            self._store.update_field_inference(entry.name, annotations)
            # Reload fields so callers see merged annotations on the returned entry.
            refreshed = self._store.get_topic(entry.name)
            if refreshed is not None:
                entry.fields = refreshed.fields
        log.info(
            "catalog.inference.applied",
            topic=entry.name,
            status=status,
            description=bool(description),
            n_annotations=len(annotations),
        )

    def stale_aspects(self, topic: str, now_ms: int | None = None) -> dict[str, bool]:
        """Return which refresh aspects are due for `topic`, given config TTLs."""
        now_ms = now_ms or int(time.time() * 1000)
        entry = self._store.get_topic(topic)
        if entry is None:
            return {"schema": True, "samples": True, "stats": True, "inference": True}
        return {
            "schema": _is_stale(entry.last_schema_refresh_ms, self._config.schema_refresh_sec, now_ms),
            "samples": _is_stale(
                entry.last_sample_refresh_ms, self._config.sample_refresh_sec, now_ms
            ),
            "stats": _is_stale(entry.last_stats_refresh_ms, self._config.stats_refresh_sec, now_ms),
            "inference": _is_stale(
                entry.last_inference_refresh_ms, self._config.inference_refresh_sec, now_ms
            ),
        }

    def get_topic_entry(self, topic: str) -> TopicEntry | None:
        return self._store.get_topic(topic)

    def list_topic_names(self) -> list[str]:
        return self._store.list_topic_names()

    async def ensure_fresh(self, topic: str) -> TopicEntry:
        """Refresh only the aspects that are past their TTL."""
        aspects = self.stale_aspects(topic)
        if not any(aspects.values()):
            entry = self._store.get_topic(topic)
            if entry is not None:
                return entry
        return await self.refresh_topic(
            topic,
            schema=aspects.get("schema", False),
            samples=aspects.get("samples", False),
            stats=aspects.get("stats", False),
            inference=aspects.get("inference", False) and self._inference is not None,
        )


def _is_stale(last_ms: int | None, ttl_sec: int, now_ms: int) -> bool:
    if last_ms is None:
        return True
    if ttl_sec <= 0:
        return True
    return (now_ms - last_ms) >= ttl_sec * 1000


__all__ = ["CatalogBuilder"]
