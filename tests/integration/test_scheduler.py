"""
test_scheduler.py — Integration tests for distqueue.scheduler.Scheduler.

Tests seed the delayed ZSet and job hashes directly (what the FAIL
transition produces) rather than going through enqueue → fail → retry,
so each test isolates the scheduler's behaviour.
"""

from __future__ import annotations

import threading
import time

import pytest

from distqueue import config
from distqueue.job import Job, JobStatus
from distqueue.producer import enqueue
from distqueue.scheduler import Scheduler
from distqueue.transitions import move_due_job
from tests.integration.helpers import sample, seed_job, wait_until

pytestmark = pytest.mark.integration


def _seed_delayed(client, job_id: str, due: float, queue: str = "default") -> None:
    seed_job(
        client,
        Job(
            id=job_id,
            queue=queue,
            status=JobStatus.PENDING,
            attempts=1,
            next_retry_at=due,
        ),
    )
    client.zadd(config.DELAYED_ZSET, {job_id: due})


class TestTick:
    def test_past_due_job_is_moved_and_retry_time_cleared(self, redis_client) -> None:
        _seed_delayed(redis_client, "due-1", time.time() - 10)

        assert Scheduler(redis_client).tick() == 1

        assert redis_client.zscore(config.DELAYED_ZSET, "due-1") is None
        assert redis_client.xrange(config.QUEUE_NAME)[-1][1] == {"job_id": "due-1"}
        assert redis_client.hget(config.job_key("due-1"), "next_retry_at") == ""

    def test_future_job_stays(self, redis_client) -> None:
        future = time.time() + 3600
        _seed_delayed(redis_client, "future-1", future)

        assert Scheduler(redis_client).tick() == 0

        assert redis_client.zscore(config.DELAYED_ZSET, "future-1") == pytest.approx(
            future
        )
        assert redis_client.xlen(config.QUEUE_NAME) == 0

    def test_batch_size_caps_each_tick(self, redis_client) -> None:
        for i in range(7):
            _seed_delayed(redis_client, f"batch-{i}", time.time() - 10 - i)
        scheduler = Scheduler(redis_client, batch_size=3)

        assert [scheduler.tick() for _ in range(4)] == [3, 3, 1, 0]
        assert redis_client.xlen(config.QUEUE_NAME) == 7

    def test_job_returns_to_its_own_queue(self, redis_client) -> None:
        """Regression test: retries used to be re-injected into the default
        queue no matter which queue they came from."""
        _seed_delayed(redis_client, "email-1", time.time() - 10, queue="emails")

        assert Scheduler(redis_client).tick() == 1

        assert redis_client.xlen(config.stream_key("emails")) == 1
        assert redis_client.xlen(config.QUEUE_NAME) == 0

    def test_orphan_is_dropped_without_creating_stub_hash(self, redis_client) -> None:
        """Regression test: the old script's HSET created a stub hash
        {next_retry_at: ''} for a job whose hash was gone, which then
        crashed deserialization in the worker and the monitor."""
        redis_client.zadd(config.DELAYED_ZSET, {"orphan": time.time() - 10})
        orphans = sample("distqueue_scheduler_orphans_dropped_total")

        assert Scheduler(redis_client).tick() == 0

        assert redis_client.zcard(config.DELAYED_ZSET) == 0
        assert redis_client.xlen(config.QUEUE_NAME) == 0
        assert not redis_client.exists(config.job_key("orphan"))
        assert sample("distqueue_scheduler_orphans_dropped_total") - orphans == 1

    def test_scheduled_enqueue_runs_when_due(self, redis_client) -> None:
        job_id = enqueue(redis_client, {}, delay_s=0.3)
        scheduler = Scheduler(redis_client)

        assert scheduler.tick() == 0
        time.sleep(0.4)
        assert scheduler.tick() == 1
        assert redis_client.xrange(config.QUEUE_NAME)[-1][1] == {"job_id": job_id}


class TestNoDuplicateInjection:
    def test_second_move_of_same_job_is_a_noop(self, redis_client) -> None:
        """The ZREM gate: only the caller whose ZREM succeeded may XADD."""
        _seed_delayed(redis_client, "race-1", time.time() - 10)

        assert int(move_due_job(redis_client, job_id="race-1", queue="default")) == 1
        assert int(move_due_job(redis_client, job_id="race-1", queue="default")) == 0
        assert redis_client.xlen(config.QUEUE_NAME) == 1

    def test_concurrent_schedulers_move_each_job_exactly_once(
        self, redis_client, make_client
    ) -> None:
        """Why scheduler replicas need no leader election: run three at
        once over 300 due jobs and every job lands on the stream once."""
        n = 300
        for i in range(n):
            _seed_delayed(redis_client, f"c-{i}", time.time() - 5)

        moved: list[int] = []
        lock = threading.Lock()

        def drain() -> None:
            scheduler = Scheduler(make_client(), batch_size=25)
            total = 0
            while True:
                count = scheduler.tick()
                total += count
                if count == 0 and redis_client.zcard(config.DELAYED_ZSET) == 0:
                    break
            with lock:
                moved.append(total)

        threads = [threading.Thread(target=drain) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert sum(moved) == n
        injected = [f["job_id"] for _, f in redis_client.xrange(config.QUEUE_NAME)]
        assert len(injected) == n
        assert len(set(injected)) == n


class TestRunLoop:
    def test_run_picks_up_due_job_and_stops_cleanly(self, redis_client) -> None:
        stop = threading.Event()
        scheduler = Scheduler(redis_client, poll_interval_s=0.05, stop_event=stop)
        thread = threading.Thread(target=scheduler.run, daemon=True)
        thread.start()
        try:
            _seed_delayed(redis_client, "loop-1", time.time() - 10)
            assert wait_until(
                lambda: redis_client.xlen(config.QUEUE_NAME) == 1, timeout=2
            )
        finally:
            stop.set()
            thread.join(timeout=5)
        assert not thread.is_alive()
