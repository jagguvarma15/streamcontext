"""Integration tests — fleshed out in Day 4."""

from streamcontext.config import Settings


def test_settings_defaults_are_sane():
    s = Settings()
    assert s.batch_size > 0
    assert s.batch_flush_interval_sec > 0
    assert s.qdrant_vector_dim == 384  # matches all-MiniLM-L6-v2
    assert "orders" in s.topics_list
