"""
test_monitor.py — Integration tests for distqueue.monitor.Monitor.

These tests require a running Redis instance.  Start one with:

    docker compose -f docker/docker-compose.yml up -d

Tests simulate dead workers directly by manually placing entries into the
PEL (via XADD → XREADGROUP under a fake consumer name, without creating a
heartbeat key).  This avoids spinning up a real Worker thread and killing
it — that's what the chaos test (a later milestone) will do.

All tests are marked with @pytest.mark.integration.
"""

from __future__ import annotations

import time

import pytest

from distqueue import config
from distqueue.job import Job
from distqueue.monitor import Monitor


# Apply the integration marker to every test in this module.
pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_stream_and_group(redis_client) -> None:
    """Create the main stream and consumer group if they don't exist.

    Uses MKSTREAM so the stream is auto-created.  Catches BUSYGROUP
    if the group already exists (idempotent across tests).
    """
    try:
        redis_client.xgroup_create(
            config.QUEUE_NAME,
            config.CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


def _simulate_dead_worker(
    redis_client,
    job_id: str,
    consumer_name: str,
    max_attempts: int = 3,
    attempts: int = 0,
) -> str:
    """Place a job into the PEL under a fake consumer with no heartbeat.

    This simulates the state left behind when a worker crashes:
      - The job hash exists in Redis (with the given attempts/max_attempts)
      - A stream entry exists pointing to the job
      - The entry is in the PEL, owned by consumer_name (via XREADGROUP)
      - No heartbeat key exists for consumer_name

    Returns the stream entry ID.
    """
    # Create the job hash.
    job = Job(
        id=job_id,
        payload={"task": "orphaned"},
        status="RUNNING",
        attempts=attempts,
        max_attempts=max_attempts,
        last_worker=consumer_name,
    )
    hash_key = f"{config.JOB_HASH_KEY_PREFIX}{job_id}"
    redis_client.hset(hash_key, mapping=job.to_redis_hash())

    # Add to stream.
    entry_id = redis_client.xadd(config.QUEUE_NAME, {"job_id": job_id})

    # XREADGROUP under the fake consumer to put the entry in the PEL.
    # This is how entries get into the PEL in production — XREADGROUP
    # delivers the message and adds it to the consumer's pending list.
    redis_client.xreadgroup(
        config.CONSUMER_GROUP,
        consumer_name,
        {config.QUEUE_NAME: ">"},
        count=1,
    )

    return entry_id


# ---------------------------------------------------------------------------
# Dead worker → retry path
# ---------------------------------------------------------------------------


class TestMonitorReclaimRetry:
    """Verify that the monitor reclaims a job from a dead worker and
    schedules it for retry when attempts remain."""

    def test_dead_worker_job_retried(self, redis_client) -> None:
        """A job in the PEL whose worker has no heartbeat should be
        reclaimed: attempts incremented, status PENDING, placed in the
        delayed ZSet, and last_error mentions the dead worker."""
        _setup_stream_and_group(redis_client)

        job_id = "reclaim-retry-001"
        consumer = "dead-worker-retry-1"

        _simulate_dead_worker(
            redis_client,
            job_id=job_id,
            consumer_name=consumer,
            max_attempts=3,
            attempts=0,
        )

        # min_idle_ms=0 so we don't need to wait for the entry to age.
        monitor = Monitor(redis_client, min_idle_ms=0)
        reclaimed = monitor.tick()

        assert reclaimed == 1

        # Verify job state.
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)

        assert job.attempts == 1
        assert job.status == "PENDING"
        assert consumer in job.last_error
        assert "heartbeat expired" in job.last_error

        # Job should be in the delayed ZSet (scheduled for retry).
        score = redis_client.zscore(config.DELAYED_ZSET, job_id)
        assert score is not None

        # Original PEL entry should be acked (pending count = 0).
        pending_info = redis_client.xpending(
            config.QUEUE_NAME, config.CONSUMER_GROUP
        )
        assert pending_info["pending"] == 0


# ---------------------------------------------------------------------------
# Dead worker → DLQ path
# ---------------------------------------------------------------------------


class TestMonitorReclaimDLQ:
    """Verify that the monitor sends a reclaimed job to the DLQ when
    no retry attempts remain."""

    def test_dead_worker_job_dlq(self, redis_client) -> None:
        """A job at max_attempts=1 with attempts=0, reclaimed from a
        dead worker, should end up DEAD in the DLQ (not retried)."""
        _setup_stream_and_group(redis_client)

        job_id = "reclaim-dlq-001"
        consumer = "dead-worker-dlq-1"

        _simulate_dead_worker(
            redis_client,
            job_id=job_id,
            consumer_name=consumer,
            max_attempts=1,
            attempts=0,
        )

        monitor = Monitor(redis_client, min_idle_ms=0)
        reclaimed = monitor.tick()

        assert reclaimed == 1

        # Verify job state.
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)

        assert job.attempts == 1
        assert job.status == "DEAD"
        assert consumer in job.last_error

        # Job should be in the DLQ.
        dlq_entries = redis_client.xrange(config.DLQ_STREAM)
        assert len(dlq_entries) == 1
        _msg_id, fields = dlq_entries[0]
        assert fields["job_id"] == job_id
        assert "heartbeat expired" in fields["reason"]

        # PEL should be clear.
        pending_info = redis_client.xpending(
            config.QUEUE_NAME, config.CONSUMER_GROUP
        )
        assert pending_info["pending"] == 0


# ---------------------------------------------------------------------------
# Alive worker — heartbeat prevents reclamation
# ---------------------------------------------------------------------------


class TestMonitorSkipsAliveWorker:
    """Verify that the monitor does NOT reclaim from a worker whose
    heartbeat is still active — the worker is just slow, not dead."""

    def test_alive_worker_not_reclaimed(self, redis_client) -> None:
        """A PEL entry owned by a worker with a valid heartbeat should
        NOT be reclaimed.  tick() should return 0, attempts unchanged."""
        _setup_stream_and_group(redis_client)

        job_id = "alive-worker-001"
        consumer = "slow-but-alive-1"

        _simulate_dead_worker(
            redis_client,
            job_id=job_id,
            consumer_name=consumer,
            max_attempts=3,
            attempts=0,
        )

        # Now CREATE a heartbeat key for this consumer — simulating a
        # worker that's alive and just running a long handler.
        heartbeat_key = (
            f"{config.WORKER_HEARTBEAT_KEY_PREFIX}"
            f"{consumer}"
            f"{config.WORKER_HEARTBEAT_KEY_SUFFIX}"
        )
        redis_client.set(
            heartbeat_key, str(time.time()), ex=config.HEARTBEAT_TTL_S
        )

        monitor = Monitor(redis_client, min_idle_ms=0)
        reclaimed = monitor.tick()

        assert reclaimed == 0

        # Job should be untouched — still attempts=0, status=RUNNING.
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)

        assert job.attempts == 0
        assert job.status == "RUNNING"

        # PEL should still have the entry (not acked).
        pending_info = redis_client.xpending(
            config.QUEUE_NAME, config.CONSUMER_GROUP
        )
        assert pending_info["pending"] == 1


# ---------------------------------------------------------------------------
# Empty PEL
# ---------------------------------------------------------------------------


class TestMonitorEmptyPEL:
    """Verify tick() returns 0 when there are no pending entries."""

    def test_empty_pel_returns_zero(self, redis_client) -> None:
        """With nothing in the PEL, tick() should return 0 immediately."""
        _setup_stream_and_group(redis_client)

        monitor = Monitor(redis_client, min_idle_ms=0)
        reclaimed = monitor.tick()

        assert reclaimed == 0


# ---------------------------------------------------------------------------
# min_idle_ms threshold respected
# ---------------------------------------------------------------------------


class TestMonitorIdleThreshold:
    """Verify that entries idle for less than min_idle_ms are not
    reclaimed, even if the worker's heartbeat is absent."""

    def test_recently_idle_entry_not_reclaimed(self, redis_client) -> None:
        """An entry that was just delivered (idle time near 0) should
        NOT be reclaimed even if the worker has no heartbeat, when
        min_idle_ms is set high.

        This tests the XPENDING IDLE pre-filter: entries below the
        idle threshold never even reach the heartbeat check.
        """
        _setup_stream_and_group(redis_client)

        job_id = "fresh-entry-001"
        consumer = "just-crashed-1"

        _simulate_dead_worker(
            redis_client,
            job_id=job_id,
            consumer_name=consumer,
            max_attempts=3,
        )

        # Set min_idle_ms very high — the entry was just created, so its
        # idle time is near 0ms, well below 60 000ms.
        monitor = Monitor(redis_client, min_idle_ms=60_000)
        reclaimed = monitor.tick()

        assert reclaimed == 0

        # Job should be untouched.
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)
        assert job.attempts == 0

        # PEL should still hold the entry.
        pending_info = redis_client.xpending(
            config.QUEUE_NAME, config.CONSUMER_GROUP
        )
        assert pending_info["pending"] == 1
