"""
test_worker.py — Integration tests for distqueue.worker.Worker.

Requires Redis (see conftest.py).  Tests drive the worker one step at a
time with process_one() wherever possible; run() is used only for the
behaviours that live in the loop (heartbeat, self-fencing, recovery).
"""

from __future__ import annotations

import threading

import pytest
import redis

from distqueue import config
from distqueue.errors import PermanentError
from distqueue.job import Job, JobStatus
from distqueue.producer import enqueue
from distqueue.worker import Worker
from tests.integration.helpers import (
    deliver,
    redis_now,
    sample,
    seed_job,
    wait_until,
)

pytestmark = pytest.mark.integration


def _noop(payload: dict) -> None:
    """Handler that always succeeds."""


def _always_fail(payload: dict) -> None:
    raise RuntimeError("simulated failure")


def _job(client, job_id: str) -> Job:
    return Job.from_redis_hash(client.hgetall(config.job_key(job_id)))


def _pending(client, queue: str = config.DEFAULT_QUEUE) -> int:
    return client.xpending(config.stream_key(queue), config.CONSUMER_GROUP)["pending"]


class _FlakyClient:
    """Proxy that makes SET (the heartbeat write) fail the first N times."""

    def __init__(self, real: redis.Redis, failures: int) -> None:
        self._real = real
        self.failures = failures

    def __getattr__(self, name: str):
        return getattr(self._real, name)

    def set(self, *args, **kwargs):
        if self.failures != 0:
            self.failures -= 1
            raise redis.ConnectionError("simulated network blip")
        return self._real.set(*args, **kwargs)


# ---------------------------------------------------------------------------
# Outcomes: success, retry, DLQ, permanent
# ---------------------------------------------------------------------------


class TestOutcomes:
    def test_success_completes_acks_and_sets_retention_ttl(self, redis_client) -> None:
        worker = Worker(redis_client, _noop, block_ms=200)
        job_id = enqueue(redis_client, {"task": "ok"})

        assert worker.process_one() is True

        job = _job(redis_client, job_id)
        assert job.status == JobStatus.COMPLETED
        assert job.last_worker == worker.consumer_name
        assert _pending(redis_client) == 0
        ttl = redis_client.ttl(config.job_key(job_id))
        assert 0 < ttl <= config.COMPLETED_JOB_TTL_S

    def test_failure_with_retries_left_schedules_backoff(self, redis_client) -> None:
        worker = Worker(redis_client, _always_fail, block_ms=200)
        job_id = enqueue(redis_client, {}, max_attempts=3)

        before = redis_now(redis_client)
        worker.process_one()
        after = redis_now(redis_client)

        job = _job(redis_client, job_id)
        assert job.status == JobStatus.PENDING
        assert job.attempts == 1
        assert job.last_error == "RuntimeError: simulated failure"
        # First retry: equal jitter over d = 2 * 2**1 = 4 s  ->  [2, 4] s.
        score = redis_client.zscore(config.DELAYED_ZSET, job_id)
        assert before + 2.0 - 0.05 <= score <= after + 4.0 + 0.05
        assert _pending(redis_client) == 0

    def test_failure_at_max_attempts_dead_letters(self, redis_client) -> None:
        worker = Worker(redis_client, _always_fail, block_ms=200)
        job_id = enqueue(redis_client, {}, max_attempts=1)

        worker.process_one()

        job = _job(redis_client, job_id)
        assert job.status == JobStatus.DEAD
        assert job.attempts == 1
        [(_, fields)] = redis_client.xrange(config.DLQ_STREAM)
        assert fields["job_id"] == job_id
        assert fields["queue"] == config.DEFAULT_QUEUE
        assert fields["reason"] == "RuntimeError: simulated failure"
        assert fields["attempts"] == "1"
        assert fields["trigger"] == "exception"
        assert _pending(redis_client) == 0
        assert 0 < redis_client.ttl(config.job_key(job_id)) <= config.DEAD_JOB_TTL_S

    def test_permanent_error_skips_retries(self, redis_client) -> None:
        def reject(payload: dict) -> None:
            raise PermanentError("invalid payload")

        worker = Worker(redis_client, reject, block_ms=200)
        job_id = enqueue(redis_client, {}, max_attempts=5)

        worker.process_one()

        job = _job(redis_client, job_id)
        assert job.status == JobStatus.DEAD
        assert job.attempts == 1
        assert redis_client.zcard(config.DELAYED_ZSET) == 0
        [(_, fields)] = redis_client.xrange(config.DLQ_STREAM)
        assert fields["trigger"] == "permanent"
        assert fields["reason"] == "PermanentError: invalid payload"

    def test_empty_queue_returns_false(self, redis_client) -> None:
        assert Worker(redis_client, _noop, block_ms=100).process_one() is False


# ---------------------------------------------------------------------------
# Delivery edge cases
# ---------------------------------------------------------------------------


class TestDeliveryEdgeCases:
    def test_jobs_enqueued_before_first_worker_are_processed(
        self, redis_client
    ) -> None:
        """Regression test for the cold-start bug.

        The consumer group used to be created at "$" (only *new* entries),
        so jobs enqueued before the first worker ever started were skipped
        forever.  It is now created at "0".
        """
        ids = [enqueue(redis_client, {"n": i}) for i in range(3)]
        worker = Worker(redis_client, _noop, block_ms=200)

        for _ in ids:
            assert worker.process_one() is True

        assert all(_job(redis_client, i).status == JobStatus.COMPLETED for i in ids)

    def test_duplicate_delivery_of_completed_job_is_skipped(self, redis_client) -> None:
        calls: list[dict] = []
        worker = Worker(redis_client, calls.append, block_ms=200)
        seed_job(redis_client, Job(id="done-1", status=JobStatus.COMPLETED))
        redis_client.xadd(config.QUEUE_NAME, {"job_id": "done-1"})
        skipped = sample(
            "distqueue_jobs_skipped_total", queue="default", reason="terminal"
        )

        assert worker.process_one() is True

        assert calls == [], "a COMPLETED job must never run again"
        assert _job(redis_client, "done-1").status == JobStatus.COMPLETED
        assert _pending(redis_client) == 0
        delta = sample(
            "distqueue_jobs_skipped_total", queue="default", reason="terminal"
        )
        assert delta - skipped == 1

    def test_missing_hash_is_acked(self, redis_client) -> None:
        worker = Worker(redis_client, _noop, block_ms=200)
        redis_client.xadd(config.QUEUE_NAME, {"job_id": "ghost"})
        assert worker.process_one() is True
        assert _pending(redis_client) == 0
        # The guard must not create a stub hash as a side effect.
        assert not redis_client.exists(config.job_key("ghost"))

    def test_corrupt_hash_is_dead_lettered_not_crashed_on(self, redis_client) -> None:
        """A hash missing required fields used to raise KeyError out of
        process_one, crashing the worker and leaving a poison entry."""
        worker = Worker(redis_client, _noop, block_ms=200)
        redis_client.hset(config.job_key("bad"), mapping={"next_retry_at": ""})
        redis_client.xadd(config.QUEUE_NAME, {"job_id": "bad"})

        assert worker.process_one() is True

        [(_, fields)] = redis_client.xrange(config.DLQ_STREAM)
        assert fields["job_id"] == "bad"
        assert fields["trigger"] == "corrupt"
        assert "KeyError" in fields["reason"]
        assert _pending(redis_client) == 0

    def test_entry_without_job_id_is_acked(self, redis_client) -> None:
        worker = Worker(redis_client, _noop, block_ms=200)
        redis_client.xadd(config.QUEUE_NAME, {"something": "else"})
        assert worker.process_one() is True
        assert _pending(redis_client) == 0

    def test_worker_only_consumes_its_own_queue(self, redis_client) -> None:
        email_job = enqueue(redis_client, {}, queue="emails")
        default_job = enqueue(redis_client, {})
        worker = Worker(redis_client, _noop, queue="emails", block_ms=200)

        assert worker.process_one() is True
        assert worker.process_one() is False

        assert _job(redis_client, email_job).status == JobStatus.COMPLETED
        assert _job(redis_client, default_job).status == JobStatus.PENDING

    def test_two_workers_same_group_no_error(self, redis_client) -> None:
        """XGROUP CREATE's BUSYGROUP error is swallowed (idempotent)."""
        w1 = Worker(redis_client, _noop, consumer_name="worker-1")
        w2 = Worker(redis_client, _noop, consumer_name="worker-2")
        assert (w1.consumer_name, w2.consumer_name) == ("worker-1", "worker-2")


# ---------------------------------------------------------------------------
# Heartbeat, self-fencing, and run-loop resilience
# ---------------------------------------------------------------------------


def _start(worker: Worker) -> threading.Thread:
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    return t


class TestRunLoop:
    def test_heartbeat_key_exists_with_ttl(self, redis_client) -> None:
        stop = threading.Event()
        worker = Worker(
            redis_client,
            _noop,
            stop_event=stop,
            block_ms=100,
            heartbeat_interval_s=0.05,
            heartbeat_ttl_s=5,
        )
        thread = _start(worker)
        key = config.heartbeat_key(worker.consumer_name)
        try:
            assert wait_until(lambda: redis_client.exists(key))
            assert redis_client.ttl(key) > 0
        finally:
            stop.set()
            thread.join(timeout=5)

    def test_heartbeat_survives_redis_errors(self, redis_client) -> None:
        """Regression test for the zombie-worker bug.

        The heartbeat loop had no exception handling: one failed SET killed
        the heartbeat thread while the worker kept consuming jobs, invisible
        to the monitor.  Now failures are counted and the next beat retries.
        """
        flaky = _FlakyClient(redis_client, failures=3)
        stop = threading.Event()
        worker = Worker(
            flaky,
            _noop,
            stop_event=stop,
            block_ms=100,
            heartbeat_interval_s=0.05,
            heartbeat_ttl_s=5,
        )
        failures_before = sample("distqueue_heartbeat_failures_total")
        thread = _start(worker)
        try:
            key = config.heartbeat_key(worker.consumer_name)
            assert wait_until(lambda: redis_client.exists(key), timeout=5)
            assert sample("distqueue_heartbeat_failures_total") - failures_before >= 3
            job_id = enqueue(redis_client, {})
            assert wait_until(
                lambda: _job(redis_client, job_id).status == JobStatus.COMPLETED
            )
        finally:
            stop.set()
            thread.join(timeout=5)

    def test_worker_without_heartbeat_takes_no_work(self, redis_client) -> None:
        """Self-fencing: if the worker can't prove it's alive, any job it
        took could be reclaimed and run twice — so it takes none."""
        always_down = _FlakyClient(redis_client, failures=-1)  # never succeeds
        stop = threading.Event()
        worker = Worker(
            always_down, _noop, stop_event=stop, block_ms=100, heartbeat_interval_s=0.05
        )
        job_id = enqueue(redis_client, {})
        thread = _start(worker)
        try:
            assert not wait_until(
                lambda: _job(redis_client, job_id).status != JobStatus.PENDING,
                timeout=0.75,
            )
            assert _pending(redis_client) == 0
        finally:
            stop.set()
            thread.join(timeout=5)

    def test_run_loop_survives_lost_consumer_group(self, redis_client) -> None:
        """Regression test: NOGROUP used to crash run() (and the container).

        FLUSHDB stands in for "Redis restarted without persistence"; the
        worker must recreate its group and keep processing.
        """
        stop = threading.Event()
        worker = Worker(
            redis_client,
            _noop,
            stop_event=stop,
            block_ms=100,
            heartbeat_interval_s=0.05,
        )
        thread = _start(worker)
        try:
            first = enqueue(redis_client, {})
            assert wait_until(lambda: _job(redis_client, first).status == "COMPLETED")
            redis_client.flushdb()
            second = enqueue(redis_client, {})
            assert wait_until(
                lambda: (
                    redis_client.hget(config.job_key(second), "status") == "COMPLETED"
                ),
                timeout=10,
            )
            assert thread.is_alive()
        finally:
            stop.set()
            thread.join(timeout=5)

    def test_graceful_shutdown_deregisters(self, redis_client) -> None:
        stop = threading.Event()
        worker = Worker(
            redis_client,
            _noop,
            stop_event=stop,
            block_ms=100,
            heartbeat_interval_s=0.05,
        )
        thread = _start(worker)
        key = config.heartbeat_key(worker.consumer_name)
        assert wait_until(lambda: redis_client.exists(key))
        assert wait_until(
            lambda: len(redis_client.xinfo_consumers(config.QUEUE_NAME, "workers")) == 1
        )

        stop.set()
        thread.join(timeout=5)

        assert not thread.is_alive()
        assert not redis_client.exists(key)
        assert redis_client.xinfo_consumers(config.QUEUE_NAME, "workers") == []

    def test_deliver_helper_places_entry_in_pel(self, redis_client) -> None:
        """Sanity check for the helper other suites rely on."""
        seed_job(redis_client, Job(id="h-1"))
        deliver(redis_client, "h-1", "someone")
        assert _pending(redis_client) == 1
