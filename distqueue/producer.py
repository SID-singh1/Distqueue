"""
producer.py — Enqueue logic for distqueue.

This module owns the "write side" of the queue: constructing a Job and
handing it to the ENQUEUE transition (see transitions.py), which persists
the job hash and makes the job runnable in one atomic step.

The producer does NOT create the consumer group (XGROUP CREATE) — that's
the worker's responsibility, because group membership is a consumer-side
concern.  Jobs enqueued before any worker exists are safe: the worker
creates its group at id "0" (the start of the stream), so it receives
every entry already waiting there.
"""

from __future__ import annotations

from typing import Any

import redis

from distqueue import config
from distqueue.job import Job
from distqueue.metrics import JOBS_DEDUPLICATED, JOBS_ENQUEUED
from distqueue.transitions import enqueue_job


def enqueue(
    client: redis.Redis,
    payload: dict[str, Any],
    max_attempts: int | None = None,
    *,
    queue: str = config.DEFAULT_QUEUE,
    timeout_s: float | None = None,
    delay_s: float | None = None,
    run_at: float | None = None,
    idempotency_key: str | None = None,
) -> str:
    """Create a new job and atomically insert it into the queue.

    Parameters
    ----------
    client : redis.Redis
        A connected Redis client (from distqueue.client.get_redis_client).
    payload : dict
        Arbitrary JSON-serialisable data the handler will receive.
    max_attempts : int | None
        How many times the job may be attempted before moving to the DLQ.
        Defaults to config.DEFAULT_MAX_ATTEMPTS.
    queue : str
        Logical queue name.  Workers consume one queue each.
    timeout_s : float | None
        Max run time before the monitor reclaims the job even though its
        worker is alive.  Defaults to config.DEFAULT_JOB_TIMEOUT_S; 0
        disables the limit.
    delay_s, run_at : float | None
        Schedule the job instead of running it immediately: ``delay_s``
        seconds from now (measured on Redis's clock), or at the absolute
        epoch ``run_at``.  At most one may be given.
    idempotency_key : str | None
        If given, a second enqueue with the same key (within
        config.IDEMPOTENCY_TTL_S) creates nothing and returns the original
        job's id.

    Returns
    -------
    str
        The job's ID — a new UUID4 hex, or the existing job's id when the
        idempotency key was already used.

    Design decisions
    ----------------
    **Why the stream entry carries only ``job_id``, not the payload:**
    the authoritative job lives in the ``job:{id}`` hash.  If the stream
    entry duplicated mutable fields, the copies would diverge the moment a
    retry incremented ``attempts``.  The entry is a pointer; the hash is the
    single source of truth.

    **Why one atomic script instead of HSET then XADD:** without atomicity,
    a fast worker could read the stream entry before the HSET lands and
    find no hash; or the XADD could fail after the HSET, orphaning a hash
    nobody will ever process.  A Lua script runs with no other command
    interleaved, so a consumer can never observe a pointer without its job.

    **Why idempotency keys:** a client whose enqueue call timed out cannot
    know whether the job was created; retrying the call is the only safe
    move, and without a key that retry creates a duplicate job.  The key
    check and the job creation happen in the same script, so two
    concurrent enqueues with the same key cannot both create a job.  This
    gives exactly-once *enqueue*; it does not make *execution* exactly-once
    (see transitions.py on at-least-once delivery).

    **Why scheduled jobs reuse the delayed set:** a job scheduled for later
    and a job waiting out its retry backoff are the same thing — "becomes
    runnable at time T" — so the scheduler that re-injects retries runs
    scheduled jobs too, with no new moving parts.
    """
    if delay_s is not None and run_at is not None:
        raise ValueError("pass at most one of delay_s and run_at")
    if delay_s is not None and delay_s < 0:
        raise ValueError("delay_s must be >= 0")

    job = Job(
        queue=queue,
        payload=payload,
        max_attempts=(
            max_attempts if max_attempts is not None else config.DEFAULT_MAX_ATTEMPTS
        ),
        timeout_s=timeout_s if timeout_s is not None else config.DEFAULT_JOB_TIMEOUT_S,
    )

    result = enqueue_job(
        client,
        job,
        delay_s=delay_s,
        run_at=run_at,
        idempotency_key=idempotency_key,
    )

    if result.created:
        JOBS_ENQUEUED.labels(queue=queue).inc()
    else:
        JOBS_DEDUPLICATED.labels(queue=queue).inc()

    return result.job_id
