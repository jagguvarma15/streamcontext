"""Tests for Kafka/Schema-Registry security config, the MCP concurrency limiter,
and SSE bearer-token wiring."""

from __future__ import annotations

import asyncio

import pytest

from streamcontext.config import Settings
from streamcontext.connections import kafka_client_kwargs, schema_registry_config
from streamcontext.mcp_server import build_server
from streamcontext.rate_limit import ToolConcurrencyLimiter


def _settings(**kw) -> Settings:
    return Settings(_env_file=None, **kw)


# ---------- Kafka client kwargs (S4) ----------


def test_kafka_kwargs_plaintext_is_empty() -> None:
    assert kafka_client_kwargs(_settings()) == {}


def test_kafka_kwargs_sasl_plaintext_has_no_ssl() -> None:
    kw = kafka_client_kwargs(
        _settings(
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="SCRAM-SHA-256",
            kafka_sasl_username="u",
            kafka_sasl_password="p",
        )
    )
    assert kw["security_protocol"] == "SASL_PLAINTEXT"
    assert kw["sasl_mechanism"] == "SCRAM-SHA-256"
    assert kw["sasl_plain_username"] == "u"
    assert kw["sasl_plain_password"] == "p"
    assert "ssl_context" not in kw


def test_kafka_kwargs_sasl_ssl_builds_ssl_context() -> None:
    import ssl

    kw = kafka_client_kwargs(
        _settings(
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_username="u",
            kafka_sasl_password="p",
        )
    )
    assert kw["security_protocol"] == "SASL_SSL"
    assert isinstance(kw["ssl_context"], ssl.SSLContext)


# ---------- Schema Registry config (S5) ----------


def test_sr_config_url_only_by_default() -> None:
    assert schema_registry_config(_settings()) == {"url": "http://localhost:8081"}


def test_sr_config_adds_basic_auth_and_tls() -> None:
    conf = schema_registry_config(
        _settings(
            schema_registry_user="u",
            schema_registry_password="p",
            schema_registry_ssl_cafile="/etc/ssl/ca.pem",
        )
    )
    assert conf["basic.auth.user.info"] == "u:p"
    assert conf["ssl.ca.location"] == "/etc/ssl/ca.pem"


# ---------- per-tool concurrency limiter (M10) ----------


async def _peak_concurrency(limiter: ToolConcurrencyLimiter, workers: int) -> int:
    inside = 0
    peak = 0

    async def worker() -> None:
        nonlocal inside, peak
        async with limiter.slot("t"):
            inside += 1
            peak = max(peak, inside)
            await asyncio.sleep(0.02)
            inside -= 1

    await asyncio.gather(*(worker() for _ in range(workers)))
    return peak


@pytest.mark.asyncio
async def test_concurrency_limiter_serializes_to_max() -> None:
    limiter = ToolConcurrencyLimiter(1)
    assert limiter.enabled
    assert await _peak_concurrency(limiter, workers=3) == 1


@pytest.mark.asyncio
async def test_concurrency_limiter_disabled_allows_overlap() -> None:
    limiter = ToolConcurrencyLimiter(0)
    assert not limiter.enabled
    assert await _peak_concurrency(limiter, workers=2) == 2


# ---------- SSE bearer-token wiring (M11) ----------


def test_build_server_wires_sse_auth_when_token_set() -> None:
    mcp = build_server(object(), _settings(mcp_sse_auth_token="secret"), enable_sse_auth=True)
    assert mcp.auth is not None


def test_build_server_no_auth_without_token() -> None:
    mcp = build_server(object(), _settings(), enable_sse_auth=True)
    assert mcp.auth is None


def test_build_server_no_auth_for_stdio_even_with_token() -> None:
    mcp = build_server(object(), _settings(mcp_sse_auth_token="secret"), enable_sse_auth=False)
    assert mcp.auth is None


# ---------- config ----------


def test_security_config_defaults() -> None:
    s = _settings()
    assert s.kafka_security_protocol == "PLAINTEXT"
    assert s.kafka_sasl_mechanism == "PLAIN"
    assert s.schema_registry_user == ""
    assert s.mcp_max_concurrent_calls == 0
    assert s.mcp_sse_auth_token == ""
