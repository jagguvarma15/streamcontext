"""Semantic catalog for the streamcontext gateway.

The catalog is a persistent, queryable representation of what flows through
Kafka — schema, sample messages, activity stats, inferred descriptions, and
inferred relationships. It is built lazily per topic and refreshed on a
schedule. The MCP server reads from it to answer agent questions about what
topics exist, what fields mean, and how topics relate.
"""

from streamcontext.catalog.inference import (
    InferenceEngine,
    LLMProvider,
    LLMUnavailableError,
    build_llm_provider,
)
from streamcontext.catalog.relationships import (
    RelationshipDetector,
    detect_pair_heuristic,
)
from streamcontext.catalog.models import (
    ActivityStats,
    CatalogConfig,
    FieldEntry,
    InferenceStatus,
    RelationshipEntry,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.store import CatalogStore

__all__ = [
    "ActivityStats",
    "CatalogConfig",
    "CatalogStore",
    "FieldEntry",
    "InferenceEngine",
    "InferenceStatus",
    "LLMProvider",
    "LLMUnavailableError",
    "RelationshipDetector",
    "RelationshipEntry",
    "SampleMessage",
    "TopicEntry",
    "build_llm_provider",
    "detect_pair_heuristic",
]
