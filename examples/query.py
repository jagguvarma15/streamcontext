"""Query the streamcontext vector store with natural-language search.

Embeds the query with the same model the gateway uses, runs a similarity search
against Qdrant, and prints the top-K matching Kafka records with their full
metadata. This is the demo-able moment of truth: prove the stream is queryable.

Usage:
    python examples/query.py "high-value orders from California"
    python examples/query.py "refunded apparel" --topk 3 --topic orders
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as rest

from streamcontext.config import load_settings
from streamcontext.embedder import build_embedder


def _format_hit(idx: int, hit) -> str:
    payload = hit.payload or {}
    coord = f"{payload.get('topic')}:{payload.get('partition')}:{payload.get('offset')}"
    score = f"{hit.score:.4f}"
    value = payload.get("value", {})
    pretty = json.dumps(value, indent=2, sort_keys=True, default=str)
    return f"\n#{idx + 1}  score={score}  {coord}\n{pretty}"


async def amain() -> None:
    parser = argparse.ArgumentParser(description="Semantic search over the streamcontext store.")
    parser.add_argument("query", help="Natural language query.")
    parser.add_argument("--topk", type=int, default=5)
    parser.add_argument("--topic", default=None, help="Optional: restrict to a single Kafka topic.")
    parser.add_argument(
        "--collection",
        default=None,
        help="Override collection name (default: SC_QDRANT_COLLECTION).",
    )
    args = parser.parse_args()

    settings = load_settings()
    collection = args.collection or settings.qdrant_collection

    embedder = build_embedder(settings)
    [vector] = await embedder.embed([args.query])

    client = AsyncQdrantClient(url=settings.qdrant_url)
    try:
        flt = None
        if args.topic:
            flt = rest.Filter(
                must=[rest.FieldCondition(key="topic", match=rest.MatchValue(value=args.topic))]
            )

        results = await client.search(
            collection_name=collection,
            query_vector=vector,
            limit=args.topk,
            query_filter=flt,
            with_payload=True,
        )
    finally:
        await client.close()

    if not results:
        print(f"no results for: {args.query!r}", file=sys.stderr)
        return

    print(f"query: {args.query!r}  collection={collection}  topk={args.topk}")
    for i, hit in enumerate(results):
        print(_format_hit(i, hit))


def main() -> None:
    # Quiet HF logging unless the user asks for it
    os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
    asyncio.run(amain())


if __name__ == "__main__":
    main()
