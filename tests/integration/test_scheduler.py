"""
test_scheduler.py — Integration tests for distqueue.scheduler.Scheduler.

These tests require a running Redis instance.  Start one with:

    docker compose -f docker/docker-compose.yml up -d

Tests seed the delayed ZSet and job hashes directly (simulating what
worker.py's retry path produces) rather than going through the full
enqueue → fail → retry flow.  This keeps each test focused on the
scheduler's behaviour without coupling it to the worker implementation.

All tests are marked with @pytest.mark.integration.
"""

from __future__ import annotations

import threading
import time

import pytest

from distqueue import config
from distqueue.job import Job
from distqueue.scheduler import Scheduler


# Apply the integration marker to every test in this module.
pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_delayed_job(
    redis_client,
    job_id: str,
    next_retry_at: float,
    payload: dict | None = None,
) -> None:
    """Create a job hash and add it to the delayed ZSet, simulating what
    worker.py does when a handler fails and retries are still available.

    This lets scheduler tests set up their preconditions without going
    through the full enqueue → worker → fail → retry flow.
    """
    job = Job(
        id=job_id,
        payload=payload or {"test": True},
        status="PENDING",
        attempts=1,
        next_retry_at=next_retry_at,
    )
    hash_key = f"{config.JOB_HASH_KEY_PREFIX}{job_id}"
    redis_client.hset(hash_key, mapping=job.to_redis_hash())
    redis_client.zadd(config.DELAYED_ZSET, {job_id: next_retry_at})


# ---------------------------------------------------------------------------
# Basic re-injection
# ---------------------------------------------------------------------------


class TestSchedulerMovesDueJobs:
    """Verify that tick() finds due jobs and re-injects them."""

    def test_past_due_job_is_moved_to_stream(self, redis_client) -> None:
        """A job with next_retry_at in the past should be removed from
        the delayed ZSet, re-added to the main stream, and have its
        next_retry_at field cleared in the hash."""
        job_id = "due-job-001"
        past_time = time.time() - 10  # 10 seconds in the past

        _seed_delayed_job(redis_client, job_id, next_retry_at=past_time)

        # Verify preconditions: job is in the ZSet, stream is empty.
        assert redis_client.zscore(config.DELAYED_ZSET, job_id) is not None
        initial_stream_len = redis_client.xlen(config.QUEUE_NAME)

        scheduler = Scheduler(redis_client)
        moved = scheduler.tick()

        assert moved == 1

        # Job should be gone from the delayed ZSet.
        assert redis_client.zscore(config.DELAYED_ZSET, job_id) is None

        # Stream should have one new entry.
        assert redis_client.xlen(config.QUEUE_NAME) == initial_stream_len + 1

        # The stream entry should contain the job_id pointer.
        entries = redis_client.xrange(config.QUEUE_NAME)
        last_entry_fields = entries[-1][1]
        assert last_entry_fields == {"job_id": job_id}

        # The job hash's next_retry_at should be cleared (empty string =
        # None in our convention), since the job is no longer "waiting."
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        assert raw["next_retry_at"] == ""


class TestSchedulerIgnoresFutureJobs:
    """Verify that jobs not yet due are left untouched."""

    def test_future_job_stays_in_zset(self, redis_client) -> None:
        """A job with next_retry_at in the future should NOT be moved."""
        job_id = "future-job-001"
        future_time = time.time() + 3600  # 1 hour from now

        _seed_delayed_job(redis_client, job_id, next_retry_at=future_time)

        scheduler = Scheduler(redis_client)
        moved = scheduler.tick()

        assert moved == 0

        # Job should still be in the ZSet with its original score.
        score = redis_client.zscore(config.DELAYED_ZSET, job_id)
        assert score is not None
        assert score == pytest.approx(future_time, abs=0.01)

        # Stream should be empty (no injection happened).
        assert redis_client.xlen(config.QUEUE_NAME) == 0


# ---------------------------------------------------------------------------
# Batch size cap
# ---------------------------------------------------------------------------


class TestSchedulerBatchSize:
    """Verify that tick() respects the batch_size limit."""

    def test_only_batch_size_jobs_moved_per_tick(self, redis_client) -> None:
        """If more due jobs exist than batch_size, tick() should move
        exactly batch_size and leave the rest for the next call."""
        batch_size = 3
        total_due = 7
        past_time = time.time() - 10

        for i in range(total_due):
            _seed_delayed_job(
                redis_client,
                job_id=f"batch-job-{i:03d}",
                next_retry_at=past_time - i,  # all in the past
            )

        scheduler = Scheduler(redis_client, batch_size=batch_size)

        # First tick: should move exactly batch_size.
        moved_1 = scheduler.tick()
        assert moved_1 == batch_size

        # Remaining jobs should still be in the ZSet.
        remaining = redis_client.zcard(config.DELAYED_ZSET)
        assert remaining == total_due - batch_size

        # Second tick: should move another batch_size.
        moved_2 = scheduler.tick()
        assert moved_2 == batch_size

        # Third tick: only 1 remaining (7 - 3 - 3 = 1).
        moved_3 = scheduler.tick()
        assert moved_3 == total_due - (2 * batch_size)  # 1

        # ZSet should now be empty.
        assert redis_client.zcard(config.DELAYED_ZSET) == 0

        # Stream should have exactly total_due entries.
        assert redis_client.xlen(config.QUEUE_NAME) == total_due


# ---------------------------------------------------------------------------
# Double-move prevention (the race the Lua script exists to prevent)
# ---------------------------------------------------------------------------


class TestSchedulerNoDuplicateInjection:
    """Prove that the Lua script prevents double-injection of the same job."""

    def test_second_move_attempt_returns_zero(self, redis_client) -> None:
        """After a job has been moved once, invoking the Lua script again
        for the same job_id should return 0 and NOT add a second entry
        to the stream.

        This is the exact race condition the Lua script exists to prevent:
        two scheduler instances both see the same job_id via ZRANGEBYSCORE,
        but only the first one's ZREM succeeds.  The second sees ZREM
        return 0 and skips the XADD.
        """
        job_id = "race-job-001"
        past_time = time.time() - 10

        _seed_delayed_job(redis_client, job_id, next_retry_at=past_time)

        scheduler = Scheduler(redis_client)

        # First move: should succeed.
        result_1 = scheduler._move_due_job(
            keys=[
                config.DELAYED_ZSET,
                config.QUEUE_NAME,
                f"{config.JOB_HASH_KEY_PREFIX}{job_id}",
            ],
            args=[job_id],
        )
        assert int(result_1) == 1

        # Second move (same job_id): should return 0 because ZREM finds
        # nothing to remove — the job was already moved.
        result_2 = scheduler._move_due_job(
            keys=[
                config.DELAYED_ZSET,
                config.QUEUE_NAME,
                f"{config.JOB_HASH_KEY_PREFIX}{job_id}",
            ],
            args=[job_id],
        )
        assert int(result_2) == 0

        # Stream should have exactly ONE entry, not two.
        assert redis_client.xlen(config.QUEUE_NAME) == 1


# ---------------------------------------------------------------------------
# run() / stop_event integration
# ---------------------------------------------------------------------------


class TestSchedulerRunLoop:
    """Verify that run() polls and can be cleanly stopped."""

    def test_run_picks_up_due_job_and_stops_cleanly(
        self, redis_client
    ) -> None:
        """Start run() on a background thread with a short poll interval,
        seed a due job, and verify it appears on the stream within a
        reasonable window.  Then stop cleanly via stop_event."""
        stop = threading.Event()
        scheduler = Scheduler(
            redis_client,
            poll_interval_s=0.1,  # Fast polling for quick test.
            stop_event=stop,
        )

        # Start the scheduler loop in the background.
        run_thread = threading.Thread(target=scheduler.run, daemon=True)
        run_thread.start()

        # Seed a due job AFTER the scheduler has started.
        job_id = "run-loop-job-001"
        _seed_delayed_job(
            redis_client, job_id, next_retry_at=time.time() - 1
        )

        # Wait for the job to appear on the stream.  With poll_interval=0.1s,
        # it should show up within ~0.2s.  We allow up to 2s as a generous
        # timeout to avoid flaky tests under CI load.
        deadline = time.time() + 2.0
        found = False
        while time.time() < deadline:
            if redis_client.xlen(config.QUEUE_NAME) > 0:
                found = True
                break
            time.sleep(0.05)

        assert found, "Scheduler did not move the due job within 2 seconds"

        # Verify the job was moved correctly.
        assert redis_client.zscore(config.DELAYED_ZSET, job_id) is None
        entries = redis_client.xrange(config.QUEUE_NAME)
        assert any(
            fields.get("job_id") == job_id for _, fields in entries
        )

        # Clean shutdown.
        stop.set()
        run_thread.join(timeout=5)
        assert not run_thread.is_alive(), "Scheduler thread did not stop"
