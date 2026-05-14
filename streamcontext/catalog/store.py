"""SQLite-backed persistence for the catalog.

One file lives in a Docker volume; SQLite is plenty for catalog-scale data.
Schemas, samples, stats, and relationships each get their own table. Raw
schema JSON is stored verbatim so an upgrade can re-derive structured fields
without re-fetching from Schema Registry.

Connections are short-lived and serialized by SQLite's own lock, so the store
is safe to call from both the refresher process and the MCP server process
sharing the same volume.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from streamcontext.catalog.models import (
    ActivityStats,
    FieldEntry,
    InferenceStatus,
    RelationshipEntry,
    SampleMessage,
    TopicEntry,
)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    name TEXT PRIMARY KEY,
    schema_subject TEXT,
    schema_id INTEGER,
    schema_version INTEGER,
    schema_fingerprint TEXT,
    raw_schema_json TEXT,
    description TEXT,
    description_confidence REAL,
    inference_status TEXT NOT NULL DEFAULT 'pending',
    last_schema_refresh_ms INTEGER,
    last_sample_refresh_ms INTEGER,
    last_stats_refresh_ms INTEGER,
    last_inference_refresh_ms INTEGER
);

CREATE TABLE IF NOT EXISTS fields (
    topic TEXT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    nullable INTEGER NOT NULL DEFAULT 0,
    default_json TEXT,
    doc TEXT,
    inferred_meaning TEXT,
    inferred_confidence REAL,
    PRIMARY KEY (topic, name),
    FOREIGN KEY (topic) REFERENCES topics(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS samples (
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    key TEXT,
    value_json TEXT NOT NULL,
    PRIMARY KEY (topic, partition, offset),
    FOREIGN KEY (topic) REFERENCES topics(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS activity (
    topic TEXT PRIMARY KEY,
    messages_last_hour INTEGER NOT NULL DEFAULT 0,
    messages_last_day INTEGER NOT NULL DEFAULT 0,
    rate_per_minute_last_hour REAL NOT NULL DEFAULT 0,
    observed_schema_versions_json TEXT NOT NULL DEFAULT '[]',
    last_observed_ts_ms INTEGER,
    FOREIGN KEY (topic) REFERENCES topics(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS relationships (
    source_topic TEXT NOT NULL,
    target_topic TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    source_field TEXT,
    target_field TEXT,
    confidence REAL NOT NULL,
    rationale TEXT,
    last_refresh_ms INTEGER,
    PRIMARY KEY (source_topic, target_topic, relationship_type, source_field, target_field)
);

CREATE TABLE IF NOT EXISTS inference_cache (
    cache_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    created_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS llm_spend_ledger (
    day TEXT NOT NULL,
    provider TEXT NOT NULL,
    spend_usd REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (day, provider)
);
"""


class CatalogStore:
    """SQLite persistence for the catalog. Thread-safe via short-lived connections."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    @property
    def path(self) -> Path:
        return self._path

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        # SQLite handles cross-process locking; the in-process lock just keeps
        # concurrent threads in the MCP server from interleaving statements on
        # one shared connection.
        with self._lock:
            conn = sqlite3.connect(str(self._path), isolation_level=None, timeout=10.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            try:
                yield conn
            finally:
                conn.close()

    def _init_schema(self) -> None:
        with self._conn() as c:
            c.executescript(_SCHEMA)

    # ------------------------------------------------------------------ topics

    def upsert_topic(self, entry: TopicEntry, raw_schema_json: str | None = None) -> None:
        with self._conn() as c:
            c.execute("BEGIN")
            c.execute(
                """
                INSERT INTO topics (
                    name, schema_subject, schema_id, schema_version, schema_fingerprint,
                    raw_schema_json, description, description_confidence, inference_status,
                    last_schema_refresh_ms, last_sample_refresh_ms, last_stats_refresh_ms,
                    last_inference_refresh_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    schema_subject=excluded.schema_subject,
                    schema_id=excluded.schema_id,
                    schema_version=excluded.schema_version,
                    schema_fingerprint=excluded.schema_fingerprint,
                    raw_schema_json=COALESCE(excluded.raw_schema_json, topics.raw_schema_json),
                    description=COALESCE(excluded.description, topics.description),
                    description_confidence=COALESCE(
                        excluded.description_confidence, topics.description_confidence
                    ),
                    inference_status=excluded.inference_status,
                    last_schema_refresh_ms=COALESCE(
                        excluded.last_schema_refresh_ms, topics.last_schema_refresh_ms
                    ),
                    last_sample_refresh_ms=COALESCE(
                        excluded.last_sample_refresh_ms, topics.last_sample_refresh_ms
                    ),
                    last_stats_refresh_ms=COALESCE(
                        excluded.last_stats_refresh_ms, topics.last_stats_refresh_ms
                    ),
                    last_inference_refresh_ms=COALESCE(
                        excluded.last_inference_refresh_ms, topics.last_inference_refresh_ms
                    )
                """,
                (
                    entry.name,
                    entry.schema_subject,
                    entry.schema_id,
                    entry.schema_version,
                    entry.schema_fingerprint,
                    raw_schema_json,
                    entry.description,
                    entry.description_confidence,
                    entry.inference_status,
                    entry.last_schema_refresh_ms,
                    entry.last_sample_refresh_ms,
                    entry.last_stats_refresh_ms,
                    entry.last_inference_refresh_ms,
                ),
            )
            c.execute("COMMIT")

    def list_topic_names(self) -> list[str]:
        with self._conn() as c:
            rows = c.execute("SELECT name FROM topics ORDER BY name").fetchall()
        return [r[0] for r in rows]

    def get_topic(self, name: str) -> TopicEntry | None:
        with self._conn() as c:
            row = c.execute(
                """
                SELECT name, schema_subject, schema_id, schema_version, schema_fingerprint,
                       description, description_confidence, inference_status,
                       last_schema_refresh_ms, last_sample_refresh_ms, last_stats_refresh_ms,
                       last_inference_refresh_ms
                FROM topics WHERE name = ?
                """,
                (name,),
            ).fetchone()
            if row is None:
                return None
            (
                t_name,
                schema_subject,
                schema_id,
                schema_version,
                schema_fingerprint,
                description,
                description_confidence,
                inference_status,
                last_schema_ms,
                last_sample_ms,
                last_stats_ms,
                last_inf_ms,
            ) = row
            fields = self._load_fields(c, t_name)
            samples = self._load_samples(c, t_name)
            activity = self._load_activity(c, t_name)
        return TopicEntry(
            name=t_name,
            schema_subject=schema_subject,
            schema_id=schema_id,
            schema_version=schema_version,
            schema_fingerprint=schema_fingerprint,
            fields=fields,
            samples=samples,
            activity=activity,
            description=description,
            description_confidence=description_confidence,
            inference_status=_coerce_inference_status(inference_status),
            last_schema_refresh_ms=last_schema_ms,
            last_sample_refresh_ms=last_sample_ms,
            last_stats_refresh_ms=last_stats_ms,
            last_inference_refresh_ms=last_inf_ms,
        )

    def get_raw_schema_json(self, topic: str) -> str | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT raw_schema_json FROM topics WHERE name = ?", (topic,)
            ).fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------ fields

    def replace_fields(self, topic: str, fields: list[FieldEntry]) -> None:
        with self._conn() as c:
            c.execute("BEGIN")
            c.execute("DELETE FROM fields WHERE topic = ?", (topic,))
            c.executemany(
                """
                INSERT INTO fields (
                    topic, name, type, nullable, default_json, doc,
                    inferred_meaning, inferred_confidence
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        topic,
                        f.name,
                        f.type,
                        1 if f.nullable else 0,
                        json.dumps(f.default) if f.default is not None else None,
                        f.doc,
                        f.inferred_meaning,
                        f.inferred_confidence,
                    )
                    for f in fields
                ],
            )
            c.execute("COMMIT")

    def update_field_inference(
        self, topic: str, annotations: dict[str, tuple[str, float]]
    ) -> None:
        """Apply LLM-inferred (meaning, confidence) per field name."""
        with self._conn() as c:
            c.execute("BEGIN")
            for name, (meaning, confidence) in annotations.items():
                c.execute(
                    """
                    UPDATE fields SET inferred_meaning = ?, inferred_confidence = ?
                    WHERE topic = ? AND name = ?
                    """,
                    (meaning, confidence, topic, name),
                )
            c.execute("COMMIT")

    @staticmethod
    def _load_fields(c: sqlite3.Connection, topic: str) -> list[FieldEntry]:
        rows = c.execute(
            """
            SELECT name, type, nullable, default_json, doc,
                   inferred_meaning, inferred_confidence
            FROM fields WHERE topic = ? ORDER BY name
            """,
            (topic,),
        ).fetchall()
        out: list[FieldEntry] = []
        for name, type_, nullable, default_json, doc, inferred, conf in rows:
            default = json.loads(default_json) if default_json else None
            out.append(
                FieldEntry(
                    name=name,
                    type=type_,
                    nullable=bool(nullable),
                    default=default,
                    doc=doc,
                    inferred_meaning=inferred,
                    inferred_confidence=conf,
                )
            )
        return out

    # ----------------------------------------------------------------- samples

    def replace_samples(
        self, topic: str, samples: list[SampleMessage], *, retain: bool = True
    ) -> None:
        with self._conn() as c:
            c.execute("BEGIN")
            c.execute("DELETE FROM samples WHERE topic = ?", (topic,))
            if retain and samples:
                c.executemany(
                    """
                    INSERT OR REPLACE INTO samples
                        (topic, partition, offset, timestamp_ms, key, value_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            topic,
                            s.partition,
                            s.offset,
                            s.timestamp_ms,
                            s.key,
                            json.dumps(s.value, default=str, ensure_ascii=False),
                        )
                        for s in samples
                    ],
                )
            c.execute("COMMIT")

    @staticmethod
    def _load_samples(c: sqlite3.Connection, topic: str) -> list[SampleMessage]:
        rows = c.execute(
            """
            SELECT partition, offset, timestamp_ms, key, value_json
            FROM samples WHERE topic = ? ORDER BY timestamp_ms DESC
            """,
            (topic,),
        ).fetchall()
        out: list[SampleMessage] = []
        for partition, offset, ts, key, value_json in rows:
            try:
                value = json.loads(value_json)
            except json.JSONDecodeError:
                value = {}
            out.append(
                SampleMessage(
                    partition=partition,
                    offset=offset,
                    timestamp_ms=ts,
                    key=key,
                    value=value if isinstance(value, dict) else {},
                )
            )
        return out

    # ---------------------------------------------------------------- activity

    def upsert_activity(self, topic: str, stats: ActivityStats) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO activity (
                    topic, messages_last_hour, messages_last_day,
                    rate_per_minute_last_hour, observed_schema_versions_json,
                    last_observed_ts_ms
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(topic) DO UPDATE SET
                    messages_last_hour=excluded.messages_last_hour,
                    messages_last_day=excluded.messages_last_day,
                    rate_per_minute_last_hour=excluded.rate_per_minute_last_hour,
                    observed_schema_versions_json=excluded.observed_schema_versions_json,
                    last_observed_ts_ms=excluded.last_observed_ts_ms
                """,
                (
                    topic,
                    stats.messages_last_hour,
                    stats.messages_last_day,
                    stats.rate_per_minute_last_hour,
                    json.dumps(stats.observed_schema_versions),
                    stats.last_observed_ts_ms,
                ),
            )

    @staticmethod
    def _load_activity(c: sqlite3.Connection, topic: str) -> ActivityStats:
        row = c.execute(
            """
            SELECT messages_last_hour, messages_last_day, rate_per_minute_last_hour,
                   observed_schema_versions_json, last_observed_ts_ms
            FROM activity WHERE topic = ?
            """,
            (topic,),
        ).fetchone()
        if row is None:
            return ActivityStats()
        last_hour, last_day, rate, versions_json, last_ts = row
        try:
            versions = json.loads(versions_json) if versions_json else []
            if not isinstance(versions, list):
                versions = []
        except json.JSONDecodeError:
            versions = []
        return ActivityStats(
            messages_last_hour=last_hour,
            messages_last_day=last_day,
            rate_per_minute_last_hour=rate,
            observed_schema_versions=[int(v) for v in versions if isinstance(v, int)],
            last_observed_ts_ms=last_ts,
        )

    # ------------------------------------------------------------ relationships

    def replace_relationships(
        self, source_topic: str, relationships: list[RelationshipEntry]
    ) -> None:
        """Replace all relationships *originating* from `source_topic`.

        Symmetric relationships should be stored once per direction by the
        caller (the builder does this for shared keys).
        """
        with self._conn() as c:
            c.execute("BEGIN")
            c.execute("DELETE FROM relationships WHERE source_topic = ?", (source_topic,))
            if relationships:
                c.executemany(
                    """
                    INSERT OR REPLACE INTO relationships (
                        source_topic, target_topic, relationship_type,
                        source_field, target_field, confidence, rationale, last_refresh_ms
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r.source_topic,
                            r.target_topic,
                            r.relationship_type,
                            r.source_field,
                            r.target_field,
                            r.confidence,
                            r.rationale,
                            r.last_refresh_ms,
                        )
                        for r in relationships
                    ],
                )
            c.execute("COMMIT")

    def get_relationships(self, topic: str) -> list[RelationshipEntry]:
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT source_topic, target_topic, relationship_type, source_field,
                       target_field, confidence, rationale, last_refresh_ms
                FROM relationships
                WHERE source_topic = ? OR target_topic = ?
                ORDER BY confidence DESC
                """,
                (topic, topic),
            ).fetchall()
        return [
            RelationshipEntry(
                source_topic=src,
                target_topic=tgt,
                relationship_type=rtype,  # type: ignore[arg-type]
                source_field=sf,
                target_field=tf,
                confidence=conf,
                rationale=rationale,
                last_refresh_ms=ts,
            )
            for src, tgt, rtype, sf, tf, conf, rationale, ts in rows
        ]

    # --------------------------------------------------------- inference cache

    def get_inference_cache(self, cache_key: str) -> dict | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT payload_json FROM inference_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        if row is None:
            return None
        try:
            return json.loads(row[0])
        except json.JSONDecodeError:
            return None

    def put_inference_cache(self, cache_key: str, payload: dict) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT OR REPLACE INTO inference_cache (cache_key, payload_json, created_ms)
                VALUES (?, ?, ?)
                """,
                (cache_key, json.dumps(payload), int(time.time() * 1000)),
            )

    # ---------------------------------------------------------- spend ledger

    def record_spend(self, provider: str, usd: float, day: str | None = None) -> float:
        """Add `usd` to today's ledger entry for `provider`, return new total."""
        day = day or time.strftime("%Y-%m-%d", time.gmtime())
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO llm_spend_ledger (day, provider, spend_usd)
                VALUES (?, ?, ?)
                ON CONFLICT(day, provider) DO UPDATE SET
                    spend_usd = llm_spend_ledger.spend_usd + excluded.spend_usd
                """,
                (day, provider, usd),
            )
            row = c.execute(
                "SELECT spend_usd FROM llm_spend_ledger WHERE day = ? AND provider = ?",
                (day, provider),
            ).fetchone()
        return float(row[0]) if row else 0.0

    def get_spend_today(self, provider: str, day: str | None = None) -> float:
        day = day or time.strftime("%Y-%m-%d", time.gmtime())
        with self._conn() as c:
            row = c.execute(
                "SELECT spend_usd FROM llm_spend_ledger WHERE day = ? AND provider = ?",
                (day, provider),
            ).fetchone()
        return float(row[0]) if row else 0.0


def _coerce_inference_status(value: str | None) -> InferenceStatus:
    if value in ("pending", "inferred", "disabled", "failed"):
        return value  # type: ignore[return-value]
    return "pending"
