"""
test_metrics_instrumentation.py — Integration tests verifying that the
production modules (producer, worker, scheduler, monitor) correctly
increment/set the Prometheus metrics defined in distqueue.metrics.

IMPORTANT — counter assertion strategy:
    prometheus_client counters are module-level singletons on a global
    registry that persist across the entire test session.  Tests in this
    file MUST capture each counter's value BEFORE the action under test,
    then assert on the DELTA afterward — never assert an absolute value.
    This ensures tests are order-independent and won't break when new
    tests are added above them.

    Gauges are an exception: they represent current state (not cumulative),
    so they CAN be asserted as absolute values after seeding known state.

These tests require a running Redis instance:
    docker compose -f docker/docker-compose.yml up -d

All tests are marked with @pytest.mark.integration.
"""

from __future__ import annotations

import pytest

from distqueue import config, metrics
from distqueue.job import Job
from distqueue.monitor import Monitor
from distqueue.producer import enqueue
from distqueue.scheduler import Scheduler
from distqueue.worker import Worker


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_stream_and_group(redis_client) -> None:
    """Create the main stream and consumer group (idempotent)."""
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

    Reuses the same pattern from test_monitor.py — see that file for the
    full rationale on why XREADGROUP under a fake consumer is the right
    way to seed PEL entries.
    """
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
    entry_id = redis_client.xadd(config.QUEUE_NAME, {"job_id": job_id})
    redis_client.xreadgroup(
        config.CONSUMER_GROUP,
        consumer_name,
        {config.QUEUE_NAME: ">"},
        count=1,
    )
    return entry_id


def _get_counter_value(counter, **labels) -> float:
    """Read the current value of a labelled counter.

    prometheus_client counters expose their value via ._value on the
    child metric returned by .labels().  This returns 0.0 if the label
    combination hasn't been incremented yet.
    """
    return counter.labels(**labels)._value.get()


def _get_gauge_value(gauge, **labels) -> float:
    """Read the current value of a labelled gauge."""
    return gauge.labels(**labels)._value.get()


# ---------------------------------------------------------------------------
# Producer: enqueue increments JOBS_ENQUEUED
# ---------------------------------------------------------------------------


class TestProducerMetrics:
    """Verify enqueue() increments the enqueued counter."""

    def test_enqueue_increments_counter(self, redis_client) -> None:
        _setup_stream_and_group(redis_client)

        before = _get_counter_value(
            metrics.JOBS_ENQUEUED, queue=config.QUEUE_NAME
        )
        enqueue(redis_client, {"task": "metrics-test"})
        after = _get_counter_value(
            metrics.JOBS_ENQUEUED, queue=config.QUEUE_NAME
        )

        assert after - before == 1


# ---------------------------------------------------------------------------
# Worker: success increments JOBS_COMPLETED + records duration
# ---------------------------------------------------------------------------


class TestWorkerSuccessMetrics:
    """Verify process_one() on a successful job increments completed
    counter and records duration."""

    def test_successful_job_metrics(self, redis_client) -> None:
        _setup_stream_and_group(redis_client)

        enqueue(redis_client, {"task": "success-metrics"})

        worker = Worker(
            redis_client,
            handler=lambda payload: None,  # instant success
            block_ms=100,
        )

        completed_before = _get_counter_value(
            metrics.JOBS_COMPLETED, queue=config.QUEUE_NAME
        )
        # Duration histogram: use REGISTRY.get_sample_value() to read
        # the _count sample — this is the official prometheus_client API
        # for reading metric values in tests, rather than poking at
        # internal attributes like _buckets or _count which vary across
        # library versions.
        from prometheus_client import REGISTRY

        duration_count_before = REGISTRY.get_sample_value(
            "distqueue_job_duration_seconds_count",
            {"queue": config.QUEUE_NAME},
        ) or 0.0

        worker.process_one()

        completed_after = _get_counter_value(
            metrics.JOBS_COMPLETED, queue=config.QUEUE_NAME
        )
        duration_count_after = REGISTRY.get_sample_value(
            "distqueue_job_duration_seconds_count",
            {"queue": config.QUEUE_NAME},
        ) or 0.0

        assert completed_after - completed_before == 1
        assert duration_count_after - duration_count_before == 1



# ---------------------------------------------------------------------------
# Worker: failure with retries remaining → JOBS_FAILED retried/exception
# ---------------------------------------------------------------------------


class TestWorkerRetryMetrics:
    """Verify a handler exception with retries remaining increments the
    failed counter with outcome=retried, trigger=exception."""

    def test_retried_failure_metrics(self, redis_client) -> None:
        _setup_stream_and_group(redis_client)

        enqueue(
            redis_client,
            {"task": "retry-metrics"},
            max_attempts=3,
        )

        worker = Worker(
            redis_client,
            handler=lambda payload: (_ for _ in ()).throw(
                RuntimeError("boom")
            ),
            block_ms=100,
        )

        before = _get_counter_value(
            metrics.JOBS_FAILED,
            queue=config.QUEUE_NAME,
            outcome="retried",
            trigger="exception",
        )

        worker.process_one()

        after = _get_counter_value(
            metrics.JOBS_FAILED,
            queue=config.QUEUE_NAME,
            outcome="retried",
            trigger="exception",
        )

        assert after - before == 1


# ---------------------------------------------------------------------------
# Worker: failure at max attempts → JOBS_FAILED dlq/exception
# ---------------------------------------------------------------------------


class TestWorkerDLQMetrics:
    """Verify a handler exception at max_attempts increments the failed
    counter with outcome=dlq, trigger=exception."""

    def test_dlq_failure_metrics(self, redis_client) -> None:
        _setup_stream_and_group(redis_client)

        enqueue(
            redis_client,
            {"task": "dlq-metrics"},
            max_attempts=1,
        )

        def failing_handler(payload):
            raise RuntimeError("permanent failure")

        worker = Worker(
            redis_client,
            handler=failing_handler,
            block_ms=100,
        )

        before = _get_counter_value(
            metrics.JOBS_FAILED,
            queue=config.QUEUE_NAME,
            outcome="dlq",
            trigger="exception",
        )

        worker.process_one()

        after = _get_counter_value(
            metrics.JOBS_FAILED,
            queue=config.QUEUE_NAME,
            outcome="dlq",
            trigger="exception",
        )

        assert after - before == 1


# ---------------------------------------------------------------------------
# Monitor: dead-worker reclaim → JOBS_RECLAIMED + JOBS_FAILED/worker_death
# ---------------------------------------------------------------------------


class TestMonitorReclaimMetrics:
    """Verify Monitor.tick() increments both the reclaimed counter and
    the failed counter with trigger=worker_death.

    Both should fire: JOBS_RECLAIMED is the monitor-specific signal
    (how often the monitor intervenes), JOBS_FAILED with trigger=worker_death
    is the shared outcome signal (what happened to the job).
    """

    def test_reclaim_increments_both_counters(self, redis_client) -> None:
        _setup_stream_and_group(redis_client)

        _simulate_dead_worker(
            redis_client,
            job_id="reclaim-metrics-001",
            consumer_name="dead-worker-metrics-1",
            max_attempts=3,
            attempts=0,
        )

        reclaimed_before = _get_counter_value(
            metrics.JOBS_RECLAIMED, queue=config.QUEUE_NAME
        )
        failed_before = _get_counter_value(
            metrics.JOBS_FAILED,
            queue=config.QUEUE_NAME,
            outcome="retried",
            trigger="worker_death",
        )

        monitor = Monitor(redis_client, min_idle_ms=0)
        monitor.tick()

        reclaimed_after = _get_counter_value(
            metrics.JOBS_RECLAIMED, queue=config.QUEUE_NAME
        )
        failed_after = _get_counter_value(
            metrics.JOBS_FAILED,
            queue=config.QUEUE_NAME,
            outcome="retried",
            trigger="worker_death",
        )

        assert reclaimed_after - reclaimed_before == 1
        assert failed_after - failed_before == 1


# ---------------------------------------------------------------------------
# Scheduler: tick updates QUEUE_DEPTH and DELAYED_JOBS gauges
# ---------------------------------------------------------------------------


class TestSchedulerGaugeMetrics:
    """Verify Scheduler.tick() sets the queue_depth and delayed_jobs
    gauges to values reflecting current Redis state.

    Gauges represent current state (not cumulative), so these CAN be
    asserted as absolute values — unlike counters above where we must
    assert deltas.  We seed known state and verify the gauges match.
    """

    def test_gauge_values_after_seeding(self, redis_client) -> None:
        _setup_stream_and_group(redis_client)

        # Seed one due job in the delayed ZSet (score in the past).
        import time

        job_id = "sched-gauge-001"
        job = Job(id=job_id, payload={"task": "gauge-test"}, status="PENDING")
        hash_key = f"{config.JOB_HASH_KEY_PREFIX}{job_id}"
        redis_client.hset(hash_key, mapping=job.to_redis_hash())
        redis_client.zadd(config.DELAYED_ZSET, {job_id: time.time() - 10})

        scheduler = Scheduler(redis_client)
        scheduler.tick()

        # After tick(), the due job was moved to the stream, so:
        # - DELAYED_ZSET should be empty (0)
        # - QUEUE_NAME should have at least 1 entry
        delayed = _get_gauge_value(
            metrics.DELAYED_JOBS, queue=config.QUEUE_NAME
        )
        depth = _get_gauge_value(
            metrics.QUEUE_DEPTH, queue=config.QUEUE_NAME
        )

        assert delayed == 0
        # The stream might have entries from other tests that ran in
        # the same session, but it should have at least the one we just
        # moved.
        assert depth >= 1
