"""
helpers.py — Shared setup helpers for integration tests.

These used to be copy-pasted into several test modules; one definition
means a change to how the PEL is seeded happens in one place.
"""

from __future__ import annotations

import time

import redis
from prometheus_client import REGISTRY

from distqueue import config
from distqueue.job import Job, JobStatus


def ensure_group(client: redis.Redis, queue: str = config.DEFAULT_QUEUE) -> None:
    """Create the queue's stream and consumer group (idempotent)."""
    try:
        client.xgroup_create(
            config.stream_key(queue), config.CONSUMER_GROUP, id="0", mkstream=True
        )
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def seed_job(client: redis.Redis, job: Job) -> None:
    """Write a job hash directly, bypassing enqueue."""
    client.hset(config.job_key(job.id), mapping=job.to_redis_hash())


def deliver(
    client: redis.Redis,
    job_id: str,
    consumer: str,
    queue: str = config.DEFAULT_QUEUE,
) -> str:
    """XADD a pointer to job_id and deliver it to ``consumer`` (into the PEL).

    This is exactly how entries reach the PEL in production: XREADGROUP
    delivers the entry and records ``consumer`` as its owner.

    XREADGROUP ">" hands out the *oldest undelivered* entry, so this only
    delivers the new entry when nothing else in the stream is undelivered.
    """
    ensure_group(client, queue)
    stream = config.stream_key(queue)
    entry_id = client.xadd(stream, {"job_id": job_id})
    client.xreadgroup(config.CONSUMER_GROUP, consumer, {stream: ">"}, count=1)
    return entry_id


def simulate_dead_worker(
    client: redis.Redis,
    job_id: str,
    consumer_name: str,
    max_attempts: int = 3,
    attempts: int = 0,
    queue: str = config.DEFAULT_QUEUE,
    timeout_s: float = config.DEFAULT_JOB_TIMEOUT_S,
) -> str:
    """Leave behind exactly what a crashed worker leaves behind.

      - the job hash, status RUNNING, last_worker = consumer_name
      - a stream entry pointing at it, in the PEL, owned by consumer_name
      - NO heartbeat key for consumer_name

    Returns the stream entry id.
    """
    seed_job(
        client,
        Job(
            id=job_id,
            queue=queue,
            payload={"task": "orphaned"},
            status=JobStatus.RUNNING,
            attempts=attempts,
            max_attempts=max_attempts,
            timeout_s=timeout_s,
            last_worker=consumer_name,
        ),
    )
    return deliver(client, job_id, consumer_name, queue)


def set_heartbeat(client: redis.Redis, consumer: str, ttl: int = 30) -> None:
    client.set(config.heartbeat_key(consumer), str(time.time()), ex=ttl)


def redis_now(client: redis.Redis) -> float:
    seconds, micros = client.time()
    return seconds + micros / 1_000_000


def sample(name: str, **labels: str) -> float:
    """Current value of a metric sample (0.0 if never observed).

    Uses REGISTRY.get_sample_value — the public prometheus_client API for
    reading values in tests — rather than private attributes like ._value.
    Metrics are process-global and persist across tests, so tests must
    assert on the DELTA between two samples, never an absolute value.
    """
    return REGISTRY.get_sample_value(name, labels) or 0.0


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> bool:
    """Poll ``predicate`` until it's truthy or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())
