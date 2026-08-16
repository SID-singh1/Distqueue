"""
test_worker.py — Integration tests for distqueue.worker.Worker.

These tests require a running Redis instance.  Start one with:

    docker compose -f docker/docker-compose.yml up -d

All tests in this file are marked with @pytest.mark.integration so they
can be excluded from fast unit-test runs via:

    pytest -m "not integration"
"""

from __future__ import annotations

import threading
import time

import pytest

from distqueue import config
from distqueue.job import Job
from distqueue.producer import enqueue
from distqueue.worker import Worker


# Apply the integration marker to every test in this module.
pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _noop_handler(payload: dict) -> None:
    """Handler that always succeeds — does nothing."""


def _always_fail_handler(payload: dict) -> None:
    """Handler that always raises, simulating a failing downstream call."""
    raise RuntimeError("simulated failure")


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------


class TestWorkerSuccess:
    """Verify the happy path: job enqueued → processed → COMPLETED."""

    def test_successful_job(self, redis_client) -> None:
        """A job whose handler returns normally should end up with
        status=COMPLETED and zero pending entries (fully acked)."""
        # Create worker FIRST so the consumer group exists before the
        # message is added.  The group is created with id="$" so it only
        # sees messages added after this point.
        worker = Worker(
            redis_client,
            handler=_noop_handler,
            block_ms=500,
        )

        job_id = enqueue(redis_client, {"task": "test_success"})

        result = worker.process_one()
        assert result is True

        # Verify job hash shows COMPLETED.
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)
        assert job.status == "COMPLETED"

        # Verify XPENDING shows zero pending entries — the stream entry
        # was fully acknowledged, nothing left in the PEL.
        pending_info = redis_client.xpending(
            config.QUEUE_NAME, config.CONSUMER_GROUP
        )
        assert pending_info["pending"] == 0


# ---------------------------------------------------------------------------
# Failure → DLQ path
# ---------------------------------------------------------------------------


class TestWorkerDLQ:
    """Verify that a job whose handler always raises, with no retries
    remaining, lands in the dead-letter queue."""

    def test_handler_raises_max_attempts_1_goes_to_dlq(
        self, redis_client
    ) -> None:
        """With max_attempts=1, the very first failure should DLQ the job.

        After process_one():
          - job.status == "DEAD"
          - job.attempts == 1  (0 → 1 on the failure)
          - DLQ stream contains the job
          - original stream entry is acked (pending == 0)
        """
        worker = Worker(
            redis_client,
            handler=_always_fail_handler,
            block_ms=500,
        )

        job_id = enqueue(
            redis_client, {"task": "test_dlq"}, max_attempts=1
        )

        result = worker.process_one()
        assert result is True

        # Job hash should be DEAD with attempts=1.
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)
        assert job.status == "DEAD"
        assert job.attempts == 1
        assert "simulated failure" in job.last_error

        # DLQ stream should contain exactly one entry with this job's ID.
        dlq_entries = redis_client.xrange(config.DLQ_STREAM)
        assert len(dlq_entries) == 1
        _msg_id, fields = dlq_entries[0]
        assert fields["job_id"] == job_id
        assert fields["reason"] == "simulated failure"

        # Original stream entry should be fully acked.
        pending_info = redis_client.xpending(
            config.QUEUE_NAME, config.CONSUMER_GROUP
        )
        assert pending_info["pending"] == 0


# ---------------------------------------------------------------------------
# Failure → retry path
# ---------------------------------------------------------------------------


class TestWorkerRetry:
    """Verify that a failed job with retries remaining gets scheduled
    in the delayed ZSet with the correct backoff delay."""

    def test_handler_raises_with_retries_remaining(
        self, redis_client
    ) -> None:
        """With max_attempts=3 and a handler that raises:
          - attempts should increment from 0 to 1
          - status should stay PENDING (not DEAD)
          - job should appear in the delayed ZSet
          - the ZSet score (next_retry_at) should be roughly
            BASE_BACKOFF_S * 2^1 seconds in the future (with jitter)
          - original stream entry should be acked
        """
        worker = Worker(
            redis_client,
            handler=_always_fail_handler,
            block_ms=500,
        )

        job_id = enqueue(
            redis_client, {"task": "test_retry"}, max_attempts=3
        )

        before = time.time()
        result = worker.process_one()
        after = time.time()
        assert result is True

        # Job hash should show attempts=1, status=PENDING.
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)
        assert job.status == "PENDING"
        assert job.attempts == 1
        assert "simulated failure" in job.last_error

        # Job should be in the delayed ZSet.
        score = redis_client.zscore(config.DELAYED_ZSET, job_id)
        assert score is not None

        # Verify the score is in the expected range.
        # delay = min(BASE_BACKOFF_S * 2^1 + jitter, MAX_BACKOFF_S)
        #       = min(2 * 2 + [0, 1], 300)
        #       = [4.0, 5.0]
        # So next_retry_at should be between before+4 and after+5 (with
        # some tolerance for test execution overhead).
        expected_min_delay = config.BASE_BACKOFF_S * (2 ** 1)  # 4.0
        expected_max_delay = expected_min_delay + config.JITTER_MAX_S  # 5.0
        assert score >= before + expected_min_delay - 0.5
        assert score <= after + expected_max_delay + 0.5

        # Original stream entry should be acked.
        pending_info = redis_client.xpending(
            config.QUEUE_NAME, config.CONSUMER_GROUP
        )
        assert pending_info["pending"] == 0


# ---------------------------------------------------------------------------
# Consumer group idempotency
# ---------------------------------------------------------------------------


class TestConsumerGroupIdempotent:
    """Verify that creating multiple Worker instances against the same
    stream doesn't raise — XGROUP CREATE handles BUSYGROUP gracefully."""

    def test_two_workers_same_group_no_error(self, redis_client) -> None:
        """Constructing two Workers against the same stream/group should
        succeed without errors.  The second Worker's XGROUP CREATE sees
        BUSYGROUP and silently ignores it."""
        worker_1 = Worker(
            redis_client,
            handler=_noop_handler,
            consumer_name="worker-1",
        )
        worker_2 = Worker(
            redis_client,
            handler=_noop_handler,
            consumer_name="worker-2",
        )
        # If we got here without an exception, the test passes.
        # Verify both workers have distinct consumer names.
        assert worker_1._consumer_name == "worker-1"
        assert worker_2._consumer_name == "worker-2"


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


class TestWorkerHeartbeat:
    """Verify the heartbeat daemon thread sets a key with a TTL in Redis."""

    def test_heartbeat_key_exists_after_run_starts(
        self, redis_client
    ) -> None:
        """After run() starts, the heartbeat key should appear in Redis
        with a positive TTL.  Uses short intervals so the test finishes
        quickly instead of waiting for the default 5s heartbeat interval."""
        stop = threading.Event()
        worker = Worker(
            redis_client,
            handler=_noop_handler,
            stop_event=stop,
            block_ms=100,
            # Short intervals for fast testing — don't wait 5s.
            heartbeat_interval_s=0.1,
            heartbeat_ttl_s=5,
        )

        # Run the worker in a background thread.
        run_thread = threading.Thread(target=worker.run, daemon=True)
        run_thread.start()

        # Give the heartbeat thread time to fire at least once.
        time.sleep(0.5)

        heartbeat_key = (
            f"{config.WORKER_HEARTBEAT_KEY_PREFIX}"
            f"{worker._consumer_name}"
            f"{config.WORKER_HEARTBEAT_KEY_SUFFIX}"
        )

        # The key should exist and have a positive TTL.
        assert redis_client.exists(heartbeat_key), (
            f"Heartbeat key {heartbeat_key} not found in Redis"
        )
        ttl = redis_client.ttl(heartbeat_key)
        assert ttl > 0, f"Expected positive TTL, got {ttl}"

        # Clean shutdown.
        stop.set()
        run_thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Empty queue
# ---------------------------------------------------------------------------


class TestWorkerEmptyQueue:
    """Verify process_one() returns False when no jobs are available."""

    def test_returns_false_on_empty_queue(self, redis_client) -> None:
        """With nothing enqueued, process_one() should block briefly
        (block_ms) and then return False."""
        worker = Worker(
            redis_client,
            handler=_noop_handler,
            # Short block so the test doesn't wait 2 full seconds.
            block_ms=100,
        )

        result = worker.process_one()
        assert result is False
