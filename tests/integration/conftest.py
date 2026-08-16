"""
conftest.py — Shared fixtures for integration tests.

Integration tests hit a real Redis instance, so they require:
    docker compose -f docker/docker-compose.yml up -d
running before execution.

The redis_client fixture connects via distqueue.client (same path the
production code uses, so we're testing the real wiring, not a mock) and
flushes the database before and after each test to guarantee a clean slate.
We flush on *both* setup and teardown because:
  - Setup flush: protects against leftover state from a test that crashed
    mid-run (teardown never ran).
  - Teardown flush: the polite default — don't leave garbage for the next
    test or the developer's manual Redis poking.
"""

from __future__ import annotations

import pytest
import redis

from distqueue.client import get_redis_client


@pytest.fixture()
def redis_client() -> redis.Redis:
    """Yield a connected Redis client with a clean DB for each test.

    The fixture uses the same get_redis_client() factory that production
    code uses, so any env-var overrides (REDIS_HOST, REDIS_PORT, REDIS_DB)
    apply here too — useful in CI where Redis might not be on localhost.
    """
    client = get_redis_client()

    # --- Setup: wipe everything so this test starts from zero. ---
    client.flushdb()

    yield client

    # --- Teardown: clean up after ourselves. ---
    client.flushdb()
