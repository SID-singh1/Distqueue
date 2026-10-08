"""
test_config.py — Unit tests for env-var configuration and key helpers.

Config values are read at import time, so these tests reload the module
under a patched environment and restore it afterwards.
"""

from __future__ import annotations

import importlib

import pytest

from distqueue import config


@pytest.fixture()
def reload_config(monkeypatch):
    """Yield a function that reloads config with given env vars set."""

    def _reload(**env: str):
        for name, value in env.items():
            monkeypatch.setenv(f"DISTQUEUE_{name}", value)
        return importlib.reload(config)

    yield _reload
    monkeypatch.undo()
    importlib.reload(config)


def test_env_override_is_applied_and_cast(reload_config) -> None:
    cfg = reload_config(HEARTBEAT_TTL_S="3", BASE_BACKOFF_S="0.5")
    assert cfg.HEARTBEAT_TTL_S == 3
    assert isinstance(cfg.HEARTBEAT_TTL_S, int)
    assert cfg.BASE_BACKOFF_S == 0.5


def test_empty_value_falls_back_to_default(reload_config) -> None:
    cfg = reload_config(HEARTBEAT_TTL_S="")
    assert cfg.HEARTBEAT_TTL_S == 15


def test_malformed_value_fails_loudly(reload_config) -> None:
    """A typo must not silently fall back to the default."""
    with pytest.raises(ValueError):
        reload_config(HEARTBEAT_TTL_S="fifteen")


def test_key_helpers() -> None:
    assert config.stream_key("emails") == "jobs:stream:emails"
    assert config.job_key("abc") == "job:abc"
    assert config.heartbeat_key("w-1") == "worker:w-1:heartbeat"
    assert config.idempotency_key("order-7") == "idem:order-7"


def test_legacy_queue_name_is_default_stream_key() -> None:
    assert config.QUEUE_NAME == config.stream_key(config.DEFAULT_QUEUE)


def test_heartbeat_ttl_tolerates_missed_beats() -> None:
    """The monitor's false-positive protection depends on TTL >= 2x interval."""
    assert config.HEARTBEAT_TTL_S >= 2 * config.HEARTBEAT_INTERVAL_S
