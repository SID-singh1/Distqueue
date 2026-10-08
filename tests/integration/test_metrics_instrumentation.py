"""
test_metrics_instrumentation.py — The production code paths emit the
metrics the dashboard and alerts are built on.

Counter strategy: metrics are process-global and persist across the whole
test session, so every assertion is on the DELTA between a sample taken
before the action and one taken after — never on an absolute value.
Gauges represent current state and are set by the monitor, so those are
asserted absolutely in test_monitor.py.
"""

from __future__ import annotations

import pytest

from distqueue.producer import enqueue
from distqueue.scheduler import Scheduler
from distqueue.worker import Worker
from tests.integration.helpers import redis_now, sample

pytestmark = pytest.mark.integration

Q = "default"


def _fail(payload: dict) -> None:
    raise RuntimeError("boom")


def test_enqueue_increments_counter(redis_client) -> None:
    before = sample("distqueue_jobs_enqueued_total", queue=Q)
    enqueue(redis_client, {})
    assert sample("distqueue_jobs_enqueued_total", queue=Q) - before == 1


def test_success_records_completion_and_all_three_latencies(redis_client) -> None:
    worker = Worker(redis_client, lambda p: None, block_ms=200)
    enqueue(redis_client, {})
    names = (
        "distqueue_job_duration_seconds_count",
        "distqueue_job_queue_wait_seconds_count",
        "distqueue_job_end_to_end_seconds_count",
    )
    completed = sample("distqueue_jobs_completed_total", queue=Q)
    counts = {n: sample(n, queue=Q) for n in names}

    worker.process_one()

    assert sample("distqueue_jobs_completed_total", queue=Q) - completed == 1
    for name in names:
        assert sample(name, queue=Q) - counts[name] == 1, name


def test_retried_failure(redis_client) -> None:
    worker = Worker(redis_client, _fail, block_ms=200)
    enqueue(redis_client, {}, max_attempts=3)
    labels = {"queue": Q, "outcome": "retried", "trigger": "exception"}
    before = sample("distqueue_jobs_failed_total", **labels)
    worker.process_one()
    assert sample("distqueue_jobs_failed_total", **labels) - before == 1


def test_dead_lettered_failure(redis_client) -> None:
    worker = Worker(redis_client, _fail, block_ms=200)
    enqueue(redis_client, {}, max_attempts=1)
    labels = {"queue": Q, "outcome": "dlq", "trigger": "exception"}
    before = sample("distqueue_jobs_failed_total", **labels)
    worker.process_one()
    assert sample("distqueue_jobs_failed_total", **labels) - before == 1


def test_scheduler_counts_moves_per_queue(redis_client) -> None:
    enqueue(redis_client, {}, queue="reports", run_at=redis_now(redis_client) - 10)
    before = sample("distqueue_scheduler_jobs_moved_total", queue="reports")
    assert Scheduler(redis_client).tick() == 1
    assert sample("distqueue_scheduler_jobs_moved_total", queue="reports") - before == 1
