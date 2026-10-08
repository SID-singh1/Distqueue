"""
test_fencing.py — A worker that lost its job must not be able to write it.

The scenario (see transitions.py for the full argument):

  1. Worker W starts job J and stalls past its heartbeat TTL.
  2. The monitor reclaims J: attempts 0 -> 1, status PENDING, retry queued.
  3. W wakes up and reports its result.

Before fencing, step 3 overwrote the hash from W's stale in-memory copy:
status=COMPLETED with attempts=0 for a job that was about to run again,
and the reset counter let a poison job dodge the DLQ.  Every worker-side
transition now checks PEL ownership atomically and rejects the write.
"""

from __future__ import annotations

import threading

import pytest

from distqueue import config
from distqueue.job import Job, JobStatus
from distqueue.monitor import Monitor
from distqueue.producer import enqueue
from distqueue.transitions import (
    FailOutcome,
    StartStatus,
    complete_job,
    fail_job,
    start_job,
)
from distqueue.worker import Worker
from tests.integration.helpers import deliver, sample, seed_job, wait_until

pytestmark = pytest.mark.integration


def _job(client, job_id: str) -> Job:
    return Job.from_redis_hash(client.hgetall(config.job_key(job_id)))


class TestTransitionsRejectNonOwners:
    """Unit-level checks of each fenced transition."""

    def _delivered(self, client, owner: str = "owner") -> tuple[str, str]:
        seed_job(client, Job(id="j1"))
        return "j1", deliver(client, "j1", owner)

    def test_start_rejects_non_owner(self, redis_client) -> None:
        job_id, entry_id = self._delivered(redis_client)
        result = start_job(
            redis_client,
            job_id=job_id,
            queue="default",
            entry_id=entry_id,
            group="workers",
            consumer="intruder",
        )
        assert result.status is StartStatus.LOST
        assert _job(redis_client, job_id).status == JobStatus.PENDING

    def test_complete_rejects_non_owner(self, redis_client) -> None:
        job_id, entry_id = self._delivered(redis_client)
        result = complete_job(
            redis_client,
            job_id=job_id,
            queue="default",
            entry_id=entry_id,
            group="workers",
            consumer="intruder",
        )
        assert result.applied is False
        assert _job(redis_client, job_id).status == JobStatus.PENDING
        # The entry is still pending: the intruder's XACK never happened.
        assert redis_client.xpending(config.QUEUE_NAME, "workers")["pending"] == 1

    def test_fail_rejects_non_owner(self, redis_client) -> None:
        job_id, entry_id = self._delivered(redis_client)
        outcome = fail_job(
            redis_client,
            _job(redis_client, job_id),
            entry_id=entry_id,
            group="workers",
            consumer="intruder",
            error="boom",
            trigger="exception",
        )
        assert outcome is FailOutcome.LEASE_LOST
        assert _job(redis_client, job_id).attempts == 0
        assert redis_client.zcard(config.DELAYED_ZSET) == 0

    def test_fail_rejects_stale_attempts_version(self, redis_client) -> None:
        """Even the owner can't apply a decision based on a stale counter."""
        job_id, entry_id = self._delivered(redis_client)
        stale = _job(redis_client, job_id)
        redis_client.hset(config.job_key(job_id), "attempts", "3")
        outcome = fail_job(
            redis_client,
            stale,
            entry_id=entry_id,
            group="workers",
            consumer="owner",
            error="boom",
            trigger="exception",
        )
        assert outcome is FailOutcome.LEASE_LOST
        assert _job(redis_client, job_id).attempts == 3

    def test_owner_succeeds(self, redis_client) -> None:
        job_id, entry_id = self._delivered(redis_client)
        result = complete_job(
            redis_client,
            job_id=job_id,
            queue="default",
            entry_id=entry_id,
            group="workers",
            consumer="owner",
        )
        assert result.applied is True
        assert _job(redis_client, job_id).status == JobStatus.COMPLETED


class TestZombieWorker:
    """The full scenario with a real Worker and Monitor."""

    def _stalled_worker(self, client, outcome: str):
        """Start a worker on a job whose handler blocks until released.

        process_one() is used (not run()), so the worker never heartbeats —
        to the monitor it looks exactly like a worker that stalled.
        """
        release = threading.Event()
        entered = threading.Event()

        def handler(payload: dict) -> None:
            entered.set()
            release.wait(10)
            if outcome == "fail":
                raise RuntimeError("late failure")

        worker = Worker(client, handler, block_ms=200)
        job_id = enqueue(client, {}, max_attempts=5)
        thread = threading.Thread(target=worker.process_one, daemon=True)
        thread.start()
        assert entered.wait(5)
        return job_id, release, thread

    def test_late_completion_is_rejected(self, redis_client, make_client) -> None:
        job_id, release, thread = self._stalled_worker(make_client(), "complete")
        lost_before = sample(
            "distqueue_lease_lost_total", queue="default", transition="complete"
        )

        # Monitor reclaims the stalled job (min_idle 0: no need to wait).
        assert Monitor(redis_client, min_idle_ms=0).tick() == 1
        reclaimed = _job(redis_client, job_id)
        assert (reclaimed.status, reclaimed.attempts) == (JobStatus.PENDING, 1)

        # The zombie wakes up and "finishes".
        release.set()
        thread.join(timeout=5)

        after = _job(redis_client, job_id)
        assert after.status == JobStatus.PENDING, "zombie must not mark it COMPLETED"
        assert after.attempts == 1, "zombie must not reset the attempt counter"
        assert redis_client.zscore(config.DELAYED_ZSET, job_id) is not None
        lost_after = sample(
            "distqueue_lease_lost_total", queue="default", transition="complete"
        )
        assert lost_after - lost_before == 1

    def test_late_failure_does_not_double_count(
        self, redis_client, make_client
    ) -> None:
        job_id, release, thread = self._stalled_worker(make_client(), "fail")

        assert Monitor(redis_client, min_idle_ms=0).tick() == 1
        release.set()
        thread.join(timeout=5)

        job = _job(redis_client, job_id)
        assert job.attempts == 1, "one lost attempt must be counted once, not twice"
        assert "presumed dead" in job.last_error

    def test_retry_after_reclaim_completes_normally(
        self, redis_client, make_client
    ) -> None:
        """After the zombie is fenced off, the retried job still finishes."""
        job_id, release, thread = self._stalled_worker(make_client(), "complete")
        Monitor(redis_client, min_idle_ms=0).tick()
        release.set()
        thread.join(timeout=5)

        # Make the retry due now and run it on a healthy worker.
        redis_client.zadd(config.DELAYED_ZSET, {job_id: 0})
        from distqueue.scheduler import Scheduler

        assert Scheduler(redis_client).tick() == 1
        assert Worker(redis_client, lambda p: None, block_ms=200).process_one()
        assert wait_until(lambda: _job(redis_client, job_id).status == "COMPLETED")
        assert _job(redis_client, job_id).attempts == 1
