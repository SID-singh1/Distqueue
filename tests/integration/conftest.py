"""
conftest.py — Shared fixtures for integration tests.

Integration tests hit a real Redis.  Start one with ONLY the Redis service:

    docker compose -f docker/docker-compose.yml up -d redis

Isolation from a running demo stack
-----------------------------------
Tests use a dedicated database (index 15 by default; override with
DISTQUEUE_TEST_REDIS_DB) and FLUSHDB it before and after each test.
Previously they used DB 0 — the same database the Docker stack's workers
consume — so running the suite with the stack up meant (a) the stack's
workers raced the tests for test jobs, and (b) FLUSHDB deleted the live
consumer group out from under every running worker.

Missing Redis
-------------
Locally, if Redis isn't reachable the integration tests are *skipped* with
a clear message instead of erroring one by one.  In CI, set
DISTQUEUE_REQUIRE_REDIS=1 so a missing Redis fails the build instead of
silently skipping half the suite.
"""

from __future__ import annotations

import os

import pytest
import redis

from distqueue.client import get_redis_client

TEST_DB = int(os.environ.get("DISTQUEUE_TEST_REDIS_DB", "15"))

# None = not probed yet; "" = reachable; otherwise the reason it isn't.
# Probed once per session: a refused connection on Windows takes ~0.5 s,
# and paying that for each of ~90 tests made a Redis-less run take a minute.
_unavailable_reason: str | None = None


def _probe() -> str:
    global _unavailable_reason
    if _unavailable_reason is None:
        try:
            get_redis_client(db=TEST_DB).ping()
            _unavailable_reason = ""
        except redis.RedisError as exc:
            _unavailable_reason = (
                f"Redis not reachable ({exc}); start it with: "
                "docker compose -f docker/docker-compose.yml up -d redis"
            )
    return _unavailable_reason


@pytest.fixture(scope="session")
def _redis_available() -> None:
    if TEST_DB == 0 and os.environ.get("DISTQUEUE_TEST_ALLOW_DB0") != "1":
        pytest.exit(
            "Refusing to FLUSHDB database 0 (it may hold real data). "
            "Use another DISTQUEUE_TEST_REDIS_DB or set DISTQUEUE_TEST_ALLOW_DB0=1.",
            returncode=2,
        )
    reason = _probe()
    if reason:
        if os.environ.get("DISTQUEUE_REQUIRE_REDIS") == "1":
            pytest.fail(reason)
        pytest.skip(reason)


@pytest.fixture()
def redis_client(_redis_available) -> redis.Redis:
    """A client on the test database, flushed before and after each test.

    Flushed on setup (protects against leftovers from a crashed test) and
    on teardown (don't leave garbage for the next test or for a human
    poking at Redis).
    """
    client = get_redis_client(db=TEST_DB)
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture()
def make_client(_redis_available):
    """Factory for extra clients on the test DB (one per simulated process)."""
    clients: list[redis.Redis] = []

    def _make() -> redis.Redis:
        c = get_redis_client(db=TEST_DB)
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.close()
