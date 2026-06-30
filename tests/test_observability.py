"""Tests for the metrics/health server, the DLQ handler, and catalog metrics."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

import pytest
from prometheus_client import REGISTRY

from streamcontext.catalog import metrics as cat
from streamcontext.config import Settings
from streamcontext.consumer import AvroKafkaConsumer
from streamcontext.observability import MetricsServer, start_metrics_server


def _sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _get(port: int, path: str):
    return urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=3)


# ---------- metrics/health server ----------


def test_metrics_server_serves_metrics_and_health() -> None:
    server = MetricsServer("127.0.0.1", 0, lambda: (True, {"status": "ok"}))
    server.start()
    try:
        with _get(server.port, "/metrics") as r:
            assert r.status == 200
            assert b"# HELP" in r.read()
        with _get(server.port, "/health") as r:
            assert r.status == 200
            assert json.loads(r.read())["status"] == "ok"
    finally:
        server.stop()


def test_metrics_server_health_returns_503_when_unhealthy() -> None:
    server = MetricsServer("127.0.0.1", 0, lambda: (False, {"running": False}))
    server.start()
    try:
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(server.port, "/healthz")
        assert exc.value.code == 503
        assert json.loads(exc.value.read())["running"] is False
    finally:
        server.stop()


def test_metrics_server_unknown_path_404() -> None:
    server = MetricsServer("127.0.0.1", 0, lambda: (True, {}))
    server.start()
    try:
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(server.port, "/nope")
        assert exc.value.code == 404
    finally:
        server.stop()


def test_start_metrics_server_disabled_returns_none() -> None:
    assert start_metrics_server(Settings(metrics_enabled=False, _env_file=None)) is None


# ---------- DLQ handler ----------


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_and_wait(self, topic, value, key, headers):
        self.sent.append({"topic": topic, "value": value, "key": key, "headers": headers})


class _FakeRecord:
    topic = "orders"
    partition = 0
    offset = 42
    value = b"\x00\x01bad-avro"
    key = b"k1"


@pytest.mark.asyncio
async def test_dlq_handler_republishes_with_origin_headers() -> None:
    consumer = AvroKafkaConsumer(Settings(kafka_dlq_topic="orders.dlq", _env_file=None))
    fake = _FakeProducer()
    consumer._producer = fake  # type: ignore[assignment]

    before = _sample("sc_gateway_dlq_produced_total", topic="orders")
    await consumer._handle_decode_failure(_FakeRecord(), ValueError("boom"))

    assert len(fake.sent) == 1
    sent = fake.sent[0]
    assert sent["topic"] == "orders.dlq"
    assert sent["value"] == b"\x00\x01bad-avro"
    assert sent["key"] == b"k1"
    headers = dict(sent["headers"])
    assert headers["sc_origin_topic"] == b"orders"
    assert headers["sc_origin_offset"] == b"42"
    assert b"boom" in headers["sc_error"]
    assert _sample("sc_gateway_dlq_produced_total", topic="orders") == before + 1


@pytest.mark.asyncio
async def test_dlq_handler_without_producer_only_counts() -> None:
    consumer = AvroKafkaConsumer(Settings(_env_file=None))
    assert consumer._producer is None
    before = _sample("sc_gateway_deserialize_failures_total", topic="orders")
    await consumer._handle_decode_failure(_FakeRecord(), ValueError("x"))
    assert _sample("sc_gateway_deserialize_failures_total", topic="orders") == before + 1


# ---------- catalog aspect timing ----------


def test_track_counts_success() -> None:
    before = _sample("sc_catalog_refresh_total", aspect="unit_ok", result="ok")
    with cat.track("unit_ok"):
        pass
    assert _sample("sc_catalog_refresh_total", aspect="unit_ok", result="ok") == before + 1


def test_track_counts_error_and_reraises() -> None:
    before = _sample("sc_catalog_refresh_total", aspect="unit_err", result="error")
    with pytest.raises(ValueError), cat.track("unit_err"):
        raise ValueError("boom")
    assert _sample("sc_catalog_refresh_total", aspect="unit_err", result="error") == before + 1


# ---------- config ----------


def test_observability_and_dlq_config_defaults() -> None:
    s = Settings(_env_file=None)
    assert s.metrics_enabled is True
    assert s.metrics_host == "127.0.0.1"
    assert s.metrics_port == 9108
    assert s.kafka_dlq_topic == ""
