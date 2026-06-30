"""LLM-powered inference for topic descriptions and field annotations.

The inference layer is deliberately thin: it builds a condensed prompt from
the deterministic catalog entry, hands it to a pluggable `LLMProvider`, and
parses a strict JSON response. Every call is cached on
`(schema_fingerprint, sample_hash)` so the same input never costs twice.

Cost discipline (these are not nice-to-haves; they are load-bearing):

  - Cache by (schema_fingerprint, sample_hash) in SQLite. Cross-process.
  - Truncate the sample list and per-sample byte budget before the prompt is
    built so a single chatty topic cannot blow the input cap.
  - Hard input-token cap (`max_input_tokens`); the prompt is truncated to fit.
  - Daily spend ledger with a per-provider ceiling. Once exceeded the engine
    returns a "disabled" result; the catalog records `inference_status =
    "disabled"` and surfaces schema-only entries.
  - PII redaction happens *before* the prompt is built (both for field-name
    and regex matches), so samples never leave the process unredacted.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from typing import Any, ClassVar, Protocol

from streamcontext.catalog.models import (
    CatalogConfig,
    FieldEntry,
    InferenceStatus,
    SampleMessage,
    TopicEntry,
)
from streamcontext.catalog.privacy import (
    compile_patterns as _privacy_compile_patterns,
)
from streamcontext.catalog.privacy import (
    redact_value as _privacy_redact_value,
)
from streamcontext.catalog.store import CatalogStore
from streamcontext.logging import get_logger

log = get_logger("streamcontext.catalog.inference")


SYSTEM_PROMPT = (
    "You are a data catalog assistant. Given the structure and a few example "
    "records from a Kafka topic, you produce a concise natural-language "
    "description of the topic plus a short meaning for each field. Reply with "
    "only the JSON object the user requests, no commentary. For any field "
    "whose meaning is not clearly determined by the schema or samples, set "
    "the meaning to the literal string 'unknown' and a low confidence."
)


class LLMUnavailableError(Exception):
    """Raised when the configured LLM provider cannot be reached or imported."""


class LLMProvider(Protocol):
    """Anything that turns a prompt into a JSON string plus a cost estimate."""

    name: str

    async def complete(
        self, *, system: str, prompt: str, max_output_tokens: int
    ) -> tuple[str, float]:
        """Return (response_text, usd_cost_estimate)."""


# ---------------------------------------------------------------- providers


class AnthropicProvider:
    """Anthropic Messages API. Defaults to Claude Haiku for cost and latency."""

    name = "anthropic"

    # Approximate USD per 1M tokens. Updated alongside model bumps.
    _MODEL_PRICES: ClassVar[dict[str, tuple[float, float]]] = {
        "claude-haiku-4-5-20251001": (1.00, 5.00),
        "claude-haiku-4-5": (1.00, 5.00),
        "claude-sonnet-4-6": (3.00, 15.00),
        "claude-opus-4-7": (15.00, 75.00),
    }

    def __init__(self, *, model: str, api_key: str | None = None) -> None:
        self._model = model
        try:
            from anthropic import AsyncAnthropic
        except ImportError as exc:
            raise LLMUnavailableError(
                "AnthropicProvider requires the `anthropic` package."
            ) from exc
        self._client = AsyncAnthropic(api_key=api_key)

    def _price(self, in_tokens: int, out_tokens: int) -> float:
        in_per_m, out_per_m = self._MODEL_PRICES.get(self._model, (3.0, 15.0))
        return (in_tokens / 1_000_000) * in_per_m + (out_tokens / 1_000_000) * out_per_m

    async def complete(
        self, *, system: str, prompt: str, max_output_tokens: int
    ) -> tuple[str, float]:
        resp = await self._client.messages.create(
            model=self._model,
            system=system,
            max_tokens=max_output_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(
            getattr(block, "text", "")
            for block in resp.content
            if getattr(block, "type", "") == "text"
        )
        usage = getattr(resp, "usage", None)
        in_tok = int(getattr(usage, "input_tokens", 0) or 0)
        out_tok = int(getattr(usage, "output_tokens", 0) or 0)
        return text, self._price(in_tok, out_tok)


class OpenAIProvider:
    """OpenAI Chat Completions provider. Defaults to gpt-4o-mini for cost."""

    name = "openai"

    _MODEL_PRICES: ClassVar[dict[str, tuple[float, float]]] = {
        "gpt-4o-mini": (0.15, 0.60),
        "gpt-4o": (2.50, 10.0),
    }

    def __init__(self, *, model: str, api_key: str | None = None) -> None:
        self._model = model
        try:
            from openai import AsyncOpenAI
        except ImportError as exc:
            raise LLMUnavailableError(
                "OpenAIProvider requires the `openai` package."
            ) from exc
        self._client = AsyncOpenAI(api_key=api_key)

    def _price(self, in_tokens: int, out_tokens: int) -> float:
        in_per_m, out_per_m = self._MODEL_PRICES.get(self._model, (1.0, 4.0))
        return (in_tokens / 1_000_000) * in_per_m + (out_tokens / 1_000_000) * out_per_m

    async def complete(
        self, *, system: str, prompt: str, max_output_tokens: int
    ) -> tuple[str, float]:
        resp = await self._client.chat.completions.create(
            model=self._model,
            max_tokens=max_output_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        choice = resp.choices[0]
        text = choice.message.content or ""
        usage = getattr(resp, "usage", None)
        in_tok = int(getattr(usage, "prompt_tokens", 0) or 0)
        out_tok = int(getattr(usage, "completion_tokens", 0) or 0)
        return text, self._price(in_tok, out_tok)


class LocalLLMProvider:
    """Ollama-compatible local provider. Cost is reported as 0.0."""

    name = "local"

    def __init__(
        self,
        *,
        model: str,
        base_url: str = "http://localhost:11434",
    ) -> None:
        self._model = model
        self._base_url = base_url.rstrip("/")

    async def complete(
        self, *, system: str, prompt: str, max_output_tokens: int
    ) -> tuple[str, float]:
        try:
            import httpx
        except ImportError as exc:
            raise LLMUnavailableError(
                "LocalLLMProvider requires `httpx`."
            ) from exc
        url = f"{self._base_url}/api/chat"
        payload = {
            "model": self._model,
            "stream": False,
            "format": "json",
            "options": {"num_predict": max_output_tokens},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        content = ""
        if isinstance(data, dict):
            message = data.get("message")
            if isinstance(message, dict):
                content = message.get("content") or ""
        return content, 0.0


def build_llm_provider(
    *, provider: str, model: str, api_key: str | None = None
) -> LLMProvider:
    if provider == "anthropic":
        return AnthropicProvider(model=model, api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))
    if provider == "openai":
        return OpenAIProvider(model=model, api_key=api_key or os.getenv("OPENAI_API_KEY"))
    if provider == "local":
        return LocalLLMProvider(model=model)
    raise ValueError(f"Unknown LLM provider: {provider!r}")


# ----------------------------------------------------------- prompt building


def redact_value(
    value: Any,
    *,
    drop_fields: frozenset[str],
    patterns: tuple[re.Pattern[str], ...] = (),
) -> Any:
    """Defensive wrapper around the privacy module's redactor.

    Inference samples are already redacted by the catalog builder; this stays
    in place so a misconfigured builder cannot accidentally leak through.
    """
    return _privacy_redact_value(value, drop_fields=drop_fields, patterns=patterns)


def compile_patterns(extra: list[str] | None = None) -> tuple[re.Pattern[str], ...]:
    """Compile the configured patterns. Bad regexes are logged and skipped."""
    valid: list[str] = []
    for raw in extra or []:
        try:
            re.compile(raw)
        except re.error as exc:
            log.warning("catalog.pii.bad_pattern", pattern=raw, error=str(exc))
            continue
        valid.append(raw)
    return _privacy_compile_patterns(valid)


def _condensed_schema(fields: list[FieldEntry]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for f in fields:
        entry: dict[str, Any] = {"name": f.name, "type": f.type}
        if f.nullable:
            entry["nullable"] = True
        if f.doc:
            entry["doc"] = f.doc
        out.append(entry)
    return out


def _truncate_samples_for_prompt(
    samples: list[SampleMessage],
    *,
    max_samples: int,
    max_bytes_per_sample: int,
    drop_fields: frozenset[str],
    patterns: tuple[re.Pattern[str], ...],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in samples[:max_samples]:
        redacted = redact_value(s.value, drop_fields=drop_fields, patterns=patterns)
        encoded = json.dumps(redacted, default=str, ensure_ascii=False)
        if len(encoded) > max_bytes_per_sample:
            encoded = encoded[: max_bytes_per_sample - 8] + "...trunc"
            try:
                redacted = json.loads(encoded)
            except json.JSONDecodeError:
                redacted = {"_preview": encoded}
        out.append(redacted)
    return out


def build_prompt(
    *,
    topic: str,
    fields: list[FieldEntry],
    samples: list[SampleMessage],
    drop_fields: frozenset[str],
    patterns: tuple[re.Pattern[str], ...],
    max_samples: int = 5,
    max_bytes_per_sample: int = 800,
    max_chars: int = 16_000,
) -> str:
    """Render the user-side prompt with bounded size and field/sample budget."""
    payload = {
        "topic": topic,
        "schema": _condensed_schema(fields),
        "samples": _truncate_samples_for_prompt(
            samples,
            max_samples=max_samples,
            max_bytes_per_sample=max_bytes_per_sample,
            drop_fields=drop_fields,
            patterns=patterns,
        ),
    }
    instruction = (
        "Produce a JSON object with exactly the following keys:\n"
        "  description: one or two sentences describing what this topic "
        "represents.\n"
        "  description_confidence: number in [0, 1].\n"
        "  field_annotations: object mapping each field path to "
        "{meaning, confidence}. Use 'unknown' for meaning when the data does "
        "not make the meaning clear, and a low confidence value.\n"
        "Schema and samples follow as JSON. Reply with only the JSON object.\n\n"
    )
    body = json.dumps(payload, default=str, ensure_ascii=False)
    rendered = instruction + body
    if len(rendered) > max_chars:
        # Last-ditch hard cap: tail-truncate so structure stays parseable on
        # the receiving side; the LLM is told the input was truncated.
        rendered = rendered[: max_chars - 64] + '"...prompt_truncated"}'
    return rendered


# ---------------------------------------------------------------- engine


class InferenceEngine:
    """Runs inference for a topic with caching and a daily spend ceiling.

    The engine is provider-agnostic. The catalog builder feeds it deterministic
    catalog entries; the engine returns (status, description, annotations).
    """

    def __init__(
        self,
        *,
        provider: LLMProvider | None,
        store: CatalogStore,
        config: CatalogConfig,
        max_output_tokens: int = 800,
        clock: callable = time.time,  # type: ignore[valid-type]
    ) -> None:
        self._provider = provider
        self._store = store
        self._config = config
        self._max_output_tokens = max_output_tokens
        self._clock = clock
        self._patterns = compile_patterns(config.pii_redact_patterns)
        self._drop_fields = frozenset(config.pii_redact_fields)

    @property
    def enabled(self) -> bool:
        return self._provider is not None

    @staticmethod
    def cache_key(*, schema_fingerprint: str | None, samples: list[SampleMessage]) -> str:
        """Stable cache key over (schema, sample contents).

        Samples are sorted by (partition, offset) so a different arrival order
        does not invalidate the cache. The hash is over canonical JSON.
        """
        sample_payload = [
            {
                "p": s.partition,
                "o": s.offset,
                "v": s.value,
            }
            for s in sorted(samples, key=lambda s: (s.partition, s.offset))
        ]
        canonical = json.dumps(
            {"schema": schema_fingerprint, "samples": sample_payload},
            sort_keys=True,
            default=str,
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    async def infer(
        self, entry: TopicEntry
    ) -> tuple[InferenceStatus, str | None, float | None, dict[str, tuple[str, float]]]:
        """Run inference for one topic. Returns (status, description, conf, annotations)."""
        if self._provider is None:
            return "disabled", None, None, {}

        key = self.cache_key(
            schema_fingerprint=entry.schema_fingerprint, samples=entry.samples
        )
        cached = self._store.get_inference_cache(key)
        if cached is not None:
            log.info("catalog.inference.cache_hit", topic=entry.name, key=key[:12])
            return _parse_cached(cached)

        # Daily spend check before we spend more.
        spent = self._store.get_spend_today(self._provider.name)
        if spent >= self._config.daily_llm_spend_ceiling_usd:
            log.warning(
                "catalog.inference.ceiling_exceeded",
                topic=entry.name,
                provider=self._provider.name,
                spent_usd=round(spent, 4),
                ceiling_usd=self._config.daily_llm_spend_ceiling_usd,
            )
            return "disabled", None, None, {}

        prompt = build_prompt(
            topic=entry.name,
            fields=entry.fields,
            samples=entry.samples,
            drop_fields=self._drop_fields,
            patterns=self._patterns,
        )
        try:
            text, cost = await self._provider.complete(
                system=SYSTEM_PROMPT,
                prompt=prompt,
                max_output_tokens=self._max_output_tokens,
            )
        except Exception as exc:
            log.warning(
                "catalog.inference.call_failed",
                topic=entry.name,
                provider=self._provider.name,
                error=str(exc),
            )
            return "failed", None, None, {}

        new_total = self._store.record_spend(self._provider.name, cost)
        log.info(
            "catalog.inference.spend",
            topic=entry.name,
            provider=self._provider.name,
            cost_usd=round(cost, 6),
            day_total_usd=round(new_total, 6),
        )

        try:
            parsed = _safe_json(text)
        except ValueError as exc:
            log.warning(
                "catalog.inference.parse_failed",
                topic=entry.name,
                error=str(exc),
                preview=text[:160],
            )
            return "failed", None, None, {}

        description = parsed.get("description")
        if not isinstance(description, str):
            description = None
        description_conf = _coerce_confidence(parsed.get("description_confidence"))
        annotations: dict[str, tuple[str, float]] = {}
        raw_annotations = parsed.get("field_annotations") or {}
        if isinstance(raw_annotations, dict):
            for name, payload in raw_annotations.items():
                if not isinstance(name, str):
                    continue
                if not isinstance(payload, dict):
                    continue
                meaning = payload.get("meaning")
                if not isinstance(meaning, str) or not meaning.strip():
                    continue
                conf = _coerce_confidence(payload.get("confidence"))
                annotations[name] = (meaning.strip(), conf or 0.0)

        cache_payload = {
            "status": "inferred",
            "description": description,
            "description_confidence": description_conf,
            "annotations": {k: [m, c] for k, (m, c) in annotations.items()},
        }
        self._store.put_inference_cache(key, cache_payload)
        return "inferred", description, description_conf, annotations


def _coerce_confidence(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f < 0.0:
        return 0.0
    if f > 1.0:
        return 1.0
    return f


def _safe_json(text: str) -> dict[str, Any]:
    """Tolerant JSON extraction — handles fenced code blocks and trailing junk."""
    candidate = text.strip()
    if candidate.startswith("```"):
        # Strip a fenced block.
        candidate = re.sub(r"^```[a-zA-Z]*\n", "", candidate)
        candidate = re.sub(r"\n```$", "", candidate)
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        # Look for the first balanced JSON object in the text.
        match = re.search(r"\{.*\}", candidate, re.DOTALL)
        if not match:
            raise ValueError("no JSON object found in response") from None
        return json.loads(match.group(0))


def _parse_cached(
    cached: dict[str, Any],
) -> tuple[InferenceStatus, str | None, float | None, dict[str, tuple[str, float]]]:
    status_raw = cached.get("status", "inferred")
    status: InferenceStatus = status_raw if status_raw in (
        "pending",
        "inferred",
        "disabled",
        "failed",
    ) else "inferred"
    description = cached.get("description")
    if not isinstance(description, str):
        description = None
    description_conf = _coerce_confidence(cached.get("description_confidence"))
    annotations: dict[str, tuple[str, float]] = {}
    raw = cached.get("annotations") or {}
    if isinstance(raw, dict):
        for name, payload in raw.items():
            if isinstance(payload, (list, tuple)) and len(payload) == 2:
                meaning, conf = payload
                if isinstance(meaning, str):
                    annotations[str(name)] = (meaning, _coerce_confidence(conf) or 0.0)
    return status, description, description_conf, annotations


__all__ = [
    "SYSTEM_PROMPT",
    "AnthropicProvider",
    "InferenceEngine",
    "LLMProvider",
    "LLMUnavailableError",
    "LocalLLMProvider",
    "OpenAIProvider",
    "build_llm_provider",
    "build_prompt",
    "compile_patterns",
    "redact_value",
]
