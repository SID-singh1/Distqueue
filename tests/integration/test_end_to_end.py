"""
test_end_to_end.py — Producer, workers, scheduler and monitor together.

The component tests seed Redis directly to isolate one module.  These run
the real loops on threads, end to end, to prove the pieces compose:
failures flow through backoff and the scheduler back to a worker, and many
concurrent workers share a queue without running anything twice.
"""

from __future__ import annotations

import threading
from collections import Counter

import pytest

from distqueue import config, transitions
from distqueue.monitor import Monitor
from distqueue.producer import enqueue
from distqueue.scheduler import Scheduler
from distqueue.stats import queue_stats
from distqueue.worker import Worker
from tests.integration.helpers import wait_until

pytestmark = pytest.mark.integration


@pytest.fixture()
def fast_backoff(monkeypatch):
    """Shrink retry delays to ~50 ms so lifecycle tests run in seconds."""
    monkeypatch.setattr(transitions, "compute_backoff", lambda attempts: 0.05)


class _Cluster:
    """Workers + scheduler + monitor running on background threads."""

    def __init__(self, make_client, handler, workers: int = 1) -> None:
        self.stop = threading.Event()
        roles: list = [
            Worker(
                make_client(),
                handler,
                stop_event=self.stop,
                block_ms=100,
                heartbeat_interval_s=0.1,
                heartbeat_ttl_s=3,
            )
            for _ in range(workers)
        ]
        roles.append(
            Scheduler(make_client(), poll_interval_s=0.05, stop_event=self.stop)
        )
        roles.append(Monitor(make_client(), poll_interval_s=0.2, stop_event=self.stop))
        self.threads = [
            threading.Thread(target=r.run, daemon=True, name=f"role-{i}")
            for i, r in enumerate(roles)
        ]

    def __enter__(self) -> _Cluster:
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop.set()
        for t in self.threads:
            t.join(timeout=10)


def _status(client, job_id: str) -> str | None:
    return client.hget(config.job_key(job_id), "status")


def test_flaky_job_retries_through_scheduler_then_succeeds(
    redis_client, make_client, fast_backoff
) -> None:
    """enqueue -> fail -> delayed set -> scheduler -> fail -> ... -> success."""
    calls: Counter[str] = Counter()
    lock = threading.Lock()

    def flaky(payload: dict) -> None:
        with lock:
            calls[payload["key"]] += 1
            n = calls[payload["key"]]
        if n <= 2:
            raise RuntimeError(f"transient failure #{n}")

    with _Cluster(make_client, flaky):
        job_id = enqueue(redis_client, {"key": "flaky"}, max_attempts=5)
        assert wait_until(lambda: _status(redis_client, job_id) == "COMPLETED", 10)

    job = redis_client.hgetall(config.job_key(job_id))
    assert job["attempts"] == "2"
    assert calls["flaky"] == 3
    assert job["last_error"] == "RuntimeError: transient failure #2"


def test_poison_job_ends_in_dlq_after_max_attempts(
    redis_client, make_client, fast_backoff
) -> None:
    def always_fail(payload: dict) -> None:
        raise RuntimeError("poison")

    with _Cluster(make_client, always_fail):
        job_id = enqueue(redis_client, {}, max_attempts=3)
        assert wait_until(lambda: _status(redis_client, job_id) == "DEAD", 10)

    assert redis_client.hget(config.job_key(job_id), "attempts") == "3"
    assert redis_client.xlen(config.DLQ_STREAM) == 1


def test_scheduled_job_runs_after_its_delay(redis_client, make_client) -> None:
    ran = threading.Event()
    with _Cluster(make_client, lambda p: ran.set()):
        job_id = enqueue(redis_client, {}, delay_s=0.5)
        assert not ran.wait(0.3), "ran before its scheduled time"
        assert wait_until(lambda: _status(redis_client, job_id) == "COMPLETED", 5)


def test_many_workers_complete_every_job_exactly_once(
    redis_client, make_client
) -> None:
    """Horizontal scaling: 4 workers drain 400 jobs; each runs once.

    "Exactly once" here is the no-failure case.  The system guarantees
    at-least-once; this asserts the consumer group never hands the same
    entry to two healthy workers, and that the work is actually shared.
    """
    seen: Counter[int] = Counter()
    per_worker: Counter[str] = Counter()
    lock = threading.Lock()

    def record(payload: dict) -> None:
        with lock:
            seen[payload["n"]] += 1
            per_worker[threading.current_thread().name] += 1

    n = 400
    ids = [enqueue(redis_client, {"n": i}) for i in range(n)]

    with _Cluster(make_client, record, workers=4):
        assert wait_until(lambda: sum(seen.values()) >= n, 30)
        assert wait_until(lambda: queue_stats(redis_client, "default").pending == 0, 5)

    assert len(seen) == n
    assert max(seen.values()) == 1, "a job ran twice"
    assert len(per_worker) == 4, "every worker should have taken a share"
    stats = queue_stats(redis_client, "default")
    assert (stats.backlog, stats.pending) == (0, 0)
    pipe = redis_client.pipeline()
    for job_id in ids:
        pipe.hget(config.job_key(job_id), "status")
    assert set(pipe.execute()) == {"COMPLETED"}
