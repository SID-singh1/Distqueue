"""
test_monitor.py — Integration tests for distqueue.monitor.Monitor.

Dead workers are simulated by delivering an entry to a fake consumer name
that has no heartbeat key (see helpers.simulate_dead_worker); the real
kill-a-container version lives in chaos/.
"""

from __future__ import annotations

import threading
import time

import pytest

from distqueue import config
from distqueue.job import Job, JobStatus
from distqueue.monitor import Monitor
from distqueue.producer import enqueue
from distqueue.stats import queue_stats
from distqueue.worker import Worker
from tests.integration.helpers import (
    deliver,
    ensure_group,
    sample,
    seed_job,
    set_heartbeat,
    simulate_dead_worker,
)

pytestmark = pytest.mark.integration


def _job(client, job_id: str) -> Job:
    return Job.from_redis_hash(client.hgetall(config.job_key(job_id)))


def _pending(client, queue: str = "default") -> int:
    return client.xpending(config.stream_key(queue), config.CONSUMER_GROUP)["pending"]


def _owner(client, entry_id: str, queue: str = "default") -> str | None:
    rows = client.xpending_range(
        config.stream_key(queue),
        config.CONSUMER_GROUP,
        min=entry_id,
        max=entry_id,
        count=1,
    )
    return rows[0]["consumer"] if rows else None


# ---------------------------------------------------------------------------
# Reclaiming from dead workers
# ---------------------------------------------------------------------------


class TestDeadWorker:
    def test_dead_worker_job_is_retried(self, redis_client) -> None:
        simulate_dead_worker(redis_client, "r-1", "dead-1", max_attempts=3)

        assert Monitor(redis_client, min_idle_ms=0).tick() == 1

        job = _job(redis_client, "r-1")
        assert (job.status, job.attempts) == (JobStatus.PENDING, 1)
        assert "dead-1" in job.last_error and "heartbeat expired" in job.last_error
        assert redis_client.zscore(config.DELAYED_ZSET, "r-1") is not None
        assert _pending(redis_client) == 0

    def test_dead_worker_job_at_max_attempts_is_dead_lettered(
        self, redis_client
    ) -> None:
        simulate_dead_worker(redis_client, "d-1", "dead-2", max_attempts=1)

        assert Monitor(redis_client, min_idle_ms=0).tick() == 1

        assert _job(redis_client, "d-1").status == JobStatus.DEAD
        [(_, fields)] = redis_client.xrange(config.DLQ_STREAM)
        assert fields["job_id"] == "d-1"
        assert fields["trigger"] == "worker_death"
        assert _pending(redis_client) == 0

    def test_alive_worker_is_left_alone(self, redis_client) -> None:
        simulate_dead_worker(redis_client, "a-1", "slow-but-alive")
        set_heartbeat(redis_client, "slow-but-alive")

        assert Monitor(redis_client, min_idle_ms=0).tick() == 0

        job = _job(redis_client, "a-1")
        assert (job.status, job.attempts) == (JobStatus.RUNNING, 0)
        assert _pending(redis_client) == 1

    def test_recently_delivered_entry_is_not_considered(self, redis_client) -> None:
        simulate_dead_worker(redis_client, "fresh-1", "just-crashed")
        assert Monitor(redis_client, min_idle_ms=60_000).tick() == 0
        assert _pending(redis_client) == 1

    def test_empty_pel(self, redis_client) -> None:
        ensure_group(redis_client)
        assert Monitor(redis_client, min_idle_ms=0).tick() == 0

    def test_queue_without_group_is_skipped(self, redis_client) -> None:
        """A queue nobody has consumed yet has no group; NOGROUP must not
        crash the tick."""
        enqueue(redis_client, {}, queue="nobody-listens")
        assert Monitor(redis_client, min_idle_ms=0).tick() == 0

    def test_terminal_job_entry_is_acked_not_failed(self, redis_client) -> None:
        """A stale entry for a COMPLETED job must not drag it back to PENDING."""
        seed_job(redis_client, Job(id="t-1", status=JobStatus.COMPLETED))
        deliver(redis_client, "t-1", "dead-3")

        assert Monitor(redis_client, min_idle_ms=0).tick() == 0

        assert _job(redis_client, "t-1").status == JobStatus.COMPLETED
        assert _pending(redis_client) == 0

    def test_corrupt_hash_is_dead_lettered_and_monitor_survives(
        self, redis_client
    ) -> None:
        """Previously Job.from_redis_hash raised out of tick(), crashing the
        monitor — and on restart it hit the same entry again, forever."""
        redis_client.hset(config.job_key("bad"), mapping={"next_retry_at": ""})
        deliver(redis_client, "bad", "dead-4")
        monitor = Monitor(redis_client, min_idle_ms=0)

        monitor.tick()
        monitor.tick()

        [(_, fields)] = redis_client.xrange(config.DLQ_STREAM)
        assert fields["trigger"] == "corrupt"
        assert _pending(redis_client) == 0

    def test_reclaim_metrics(self, redis_client) -> None:
        simulate_dead_worker(redis_client, "m-1", "dead-5")
        reclaimed = sample(
            "distqueue_jobs_reclaimed_total", queue="default", reason="worker_death"
        )
        failed = sample(
            "distqueue_jobs_failed_total",
            queue="default",
            outcome="retried",
            trigger="worker_death",
        )

        Monitor(redis_client, min_idle_ms=0).tick()

        assert (
            sample(
                "distqueue_jobs_reclaimed_total", queue="default", reason="worker_death"
            )
            - reclaimed
            == 1
        )
        assert (
            sample(
                "distqueue_jobs_failed_total",
                queue="default",
                outcome="retried",
                trigger="worker_death",
            )
            - failed
            == 1
        )


# ---------------------------------------------------------------------------
# Claim precision and concurrency
# ---------------------------------------------------------------------------


class TestClaimPrecision:
    def test_already_claimed_entry_does_not_spill_onto_neighbour(
        self, redis_client
    ) -> None:
        """Regression test for the XAUTOCLAIM bug.

        Setup: E1 belongs to a dead worker; E2, right after it, to a live
        worker on a long job.  Monitor B reclaims E1 first (its idle time
        resets).  Monitor A then tries to reclaim E1 from its now-stale
        XPENDING snapshot.

        Old code — XAUTOCLAIM(start=E1, count=1) — skipped the no-longer-idle
        E1 and claimed E2, stealing a live worker's job.  XCLAIM on the exact
        id returns nothing instead.
        """
        simulate_dead_worker(redis_client, "e1", "dead-worker")
        e2 = simulate_dead_worker(redis_client, "e2", "alive-worker")
        set_heartbeat(redis_client, "alive-worker")
        time.sleep(0.3)  # both entries now idle ~300 ms

        e1 = redis_client.xrange(config.QUEUE_NAME)[0][0]
        monitor_b = Monitor(redis_client, min_idle_ms=200)
        assert monitor_b.tick() == 1  # B reclaims E1

        monitor_a = Monitor(redis_client, min_idle_ms=200)
        assert (
            monitor_a._reclaim_entry("default", e1, 200, "stale view", "worker_death")
            is False
        )

        assert _owner(redis_client, e2) == "alive-worker", (
            "live worker's job was stolen"
        )
        assert _job(redis_client, "e2").attempts == 0

    def test_concurrent_monitors_reclaim_each_job_once(
        self, redis_client, make_client
    ) -> None:
        n = 40
        for i in range(n):
            simulate_dead_worker(
                redis_client, f"cm-{i}", f"dead-{i % 4}", max_attempts=5
            )

        monitors = [Monitor(make_client(), min_idle_ms=0) for _ in range(3)]
        totals: list[int] = []
        lock = threading.Lock()

        def run(m: Monitor) -> None:
            count = sum(m.tick() for _ in range(3))
            with lock:
                totals.append(count)

        threads = [threading.Thread(target=run, args=(m,)) for m in monitors]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert sum(totals) == n
        assert all(_job(redis_client, f"cm-{i}").attempts == 1 for i in range(n))
        assert redis_client.zcard(config.DELAYED_ZSET) == n

    def test_cursor_reaches_entries_past_the_first_page(self, redis_client) -> None:
        """Regression test: the PEL scan always restarted at "-", so if the
        first page was all live workers' long jobs, dead workers' entries
        behind them were never examined."""
        for i in range(4):
            simulate_dead_worker(redis_client, f"busy-{i}", f"alive-{i}")
            set_heartbeat(redis_client, f"alive-{i}")
        simulate_dead_worker(redis_client, "behind", "dead-behind")

        monitor = Monitor(redis_client, min_idle_ms=0, scan_count=2)
        reclaimed = sum(monitor.tick() for _ in range(3))

        assert reclaimed == 1
        assert _job(redis_client, "behind").attempts == 1


# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------


class TestTimeouts:
    def test_job_past_timeout_is_reclaimed_from_live_worker(self, redis_client) -> None:
        simulate_dead_worker(redis_client, "slow-1", "alive-slow", timeout_s=0.2)
        set_heartbeat(redis_client, "alive-slow")
        time.sleep(0.3)

        assert Monitor(redis_client, min_idle_ms=0).tick() == 1

        job = _job(redis_client, "slow-1")
        assert job.attempts == 1
        assert "exceeded timeout" in job.last_error

    def test_job_within_timeout_is_left_alone(self, redis_client) -> None:
        simulate_dead_worker(redis_client, "ok-1", "alive-ok", timeout_s=60)
        set_heartbeat(redis_client, "alive-ok")
        assert Monitor(redis_client, min_idle_ms=0).tick() == 0

    def test_zero_timeout_disables_the_limit(self, redis_client) -> None:
        simulate_dead_worker(redis_client, "inf-1", "alive-inf", timeout_s=0)
        set_heartbeat(redis_client, "alive-inf")
        time.sleep(0.1)
        assert Monitor(redis_client, min_idle_ms=0).tick() == 0


# ---------------------------------------------------------------------------
# Housekeeping: trimming, consumer GC, gauges
# ---------------------------------------------------------------------------


class TestTrimming:
    def test_trim_drops_only_finished_entries(self, redis_client) -> None:
        """Acked entries go; pending and undelivered entries stay."""
        worker = Worker(redis_client, lambda p: None, block_ms=100)
        done = [enqueue(redis_client, {}) for _ in range(5)]
        for _ in done:
            worker.process_one()
        simulate_dead_worker(redis_client, "inflight", "holder")
        set_heartbeat(redis_client, "holder")
        enqueue(redis_client, {})  # undelivered
        assert redis_client.xlen(config.QUEUE_NAME) == 7

        removed = Monitor(redis_client, min_idle_ms=0, approximate_trim=False).trim(
            "default"
        )

        assert removed == 5
        assert redis_client.xlen(config.QUEUE_NAME) == 2
        assert _pending(redis_client) == 1

    def test_backlog_stays_correct_after_trimming(self, redis_client) -> None:
        """Backlog comes from the group's lag, which must survive XTRIM."""
        worker = Worker(redis_client, lambda p: None, block_ms=100)
        for _ in range(4):
            enqueue(redis_client, {})
        worker.process_one()
        worker.process_one()

        Monitor(redis_client, min_idle_ms=0, approximate_trim=False).trim("default")
        stats = queue_stats(redis_client, "default")

        assert stats.backlog == 2
        assert stats.pending == 0
        assert stats.stream_length == 2

    def test_nothing_trimmed_before_any_delivery(self, redis_client) -> None:
        ensure_group(redis_client)
        enqueue(redis_client, {})
        monitor = Monitor(redis_client, min_idle_ms=0, approximate_trim=False)
        assert monitor.trim("default") == 0
        assert redis_client.xlen(config.QUEUE_NAME) == 1


class TestConsumerGC:
    def test_idle_dead_consumer_removed_live_and_busy_kept(self, redis_client) -> None:
        ensure_group(redis_client)
        stream = config.QUEUE_NAME
        for name in ("ghost", "alive"):
            redis_client.xgroup_createconsumer(stream, "workers", name)
        set_heartbeat(redis_client, "alive")
        simulate_dead_worker(redis_client, "held", "busy-ghost")

        live = Monitor(redis_client, consumer_gc_idle_ms=0).collect_consumers("default")

        names = {c["name"] for c in redis_client.xinfo_consumers(stream, "workers")}
        assert "ghost" not in names
        assert {"alive", "busy-ghost"} <= names, "never delete a consumer with pending"
        assert live == {"alive"}


class TestGauges:
    def test_tick_publishes_queue_state(self, redis_client) -> None:
        # Deliver first: deliver() hands out the *next undelivered* entry.
        simulate_dead_worker(redis_client, "g-1", "alive-g")
        set_heartbeat(redis_client, "alive-g")
        for _ in range(3):
            enqueue(redis_client, {})
        enqueue(redis_client, {}, delay_s=3600)
        redis_client.xadd(config.DLQ_STREAM, {"job_id": "x"})

        Monitor(redis_client, min_idle_ms=60_000).tick()

        # Backlog = the 3 undelivered entries; g-1 is pending, not backlog.
        assert sample("distqueue_queue_backlog", queue="default") == 3
        assert sample("distqueue_pending_entries", queue="default") == 1
        assert sample("distqueue_delayed_jobs") == 1
        assert sample("distqueue_dlq_depth") == 1
        assert sample("distqueue_live_workers") == 1
