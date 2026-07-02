"""Security configuration for the Kafka and Schema Registry clients.

Turns the `SC_KAFKA_*` and `SC_SCHEMA_REGISTRY_*` settings into the keyword
arguments the aiokafka and confluent clients expect. Shared by the gateway
consumer, the dead-letter producer, and the catalog sampler so auth is
configured in exactly one place. PLAINTEXT / no-auth returns empty dicts, so
the local-dev defaults are unchanged.
"""

from __future__ import annotations

from typing import Any

from streamcontext.config import Settings


def kafka_client_kwargs(settings: Settings) -> dict[str, Any]:
    """aiokafka security kwargs for a consumer or producer. Empty for PLAINTEXT."""
    protocol = settings.kafka_security_protocol
    if protocol == "PLAINTEXT":
        return {}
    kwargs: dict[str, Any] = {"security_protocol": protocol}
    if protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        kwargs["sasl_mechanism"] = settings.kafka_sasl_mechanism or "PLAIN"
        if settings.kafka_sasl_username:
            kwargs["sasl_plain_username"] = settings.kafka_sasl_username
            kwargs["sasl_plain_password"] = settings.kafka_sasl_password
    if protocol in ("SSL", "SASL_SSL"):
        from aiokafka.helpers import create_ssl_context

        kwargs["ssl_context"] = create_ssl_context(
            cafile=settings.kafka_ssl_cafile or None,
            certfile=settings.kafka_ssl_certfile or None,
            keyfile=settings.kafka_ssl_keyfile or None,
        )
    return kwargs


def schema_registry_config(settings: Settings) -> dict[str, Any]:
    """confluent SchemaRegistryClient config from settings (url + optional auth/TLS)."""
    conf: dict[str, Any] = {"url": settings.schema_registry_url}
    if settings.schema_registry_user:
        conf["basic.auth.user.info"] = (
            f"{settings.schema_registry_user}:{settings.schema_registry_password}"
        )
    if settings.schema_registry_ssl_cafile:
        conf["ssl.ca.location"] = settings.schema_registry_ssl_cafile
    return conf


__all__ = ["kafka_client_kwargs", "schema_registry_config"]
