"""
producer.py — Enqueue logic for distqueue.

This module owns the "write side" of the queue: constructing a Job,
persisting it to Redis, and inserting a pointer into the stream so that
consumer-group workers can pick it up.

The producer does NOT create the consumer group (XGROUP CREATE) — that's
the worker's responsibility, because group membership is a consumer-side
concern.  If the producer created the group, starting the producer before
any worker would silently succeed but mask the fact that nobody's
listening yet.
"""

from __future__ import annotations

from typing import Any

import redis

from distqueue import config
from distqueue.job import Job
from distqueue.metrics import JOBS_ENQUEUED


def enqueue(
    client: redis.Redis,
    payload: dict[str, Any],
    max_attempts: int | None = None,
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
        Defaults to config.DEFAULT_MAX_ATTEMPTS if not provided.

    Returns
    -------
    str
        The newly created job's ID (a UUID4 hex string).

    Design decisions
    ----------------
    **Why the stream entry carries only ``job_id``, not the full payload:**

    The authoritative representation of a job lives in the ``job:{id}`` hash.
    If we duplicated the payload (or any other mutable field) into the stream
    entry, we'd have two copies of the same data that could diverge.  The
    moment a retry increments ``attempts`` or a worker writes ``last_error``
    into the hash, the stream entry's snapshot would be stale.  Keeping the
    stream entry as a lightweight pointer — just ``{"job_id": id}`` — means
    there is exactly one source of truth: the hash.  Workers read the stream
    to discover *which* job to work on, then HGETALL the hash to get the
    current state.

    **Why the hash write and stream XADD are wrapped in a MULTI/EXEC
    transaction:**

    Without a transaction, there's a window between the HSET and the XADD
    where another client's command could execute.  Two bad things can happen:

    1. XADD runs first, a fast worker reads the stream entry via XREADGROUP,
       tries HGETALL ``job:{id}`` — and gets an empty dict because the HSET
       hasn't happened yet.  The worker either crashes or silently drops the
       job.
    2. HSET runs first but XADD fails (e.g. OOM).  Now there's an orphaned
       hash that nobody will ever process because there's no stream entry
       pointing to it.

    Wrapping both in MULTI/EXEC guarantees isolation: no other client can
    interleave commands between the two, so a consumer will never see the
    stream entry without the hash already being present.

    **An important caveat about Redis transactions vs. SQL transactions:**

    MULTI/EXEC guarantees *isolation* (commands are batched and executed as
    a single uninterrupted block) but it does NOT provide *atomicity* in the
    SQL sense.  If one command inside the EXEC block errors — say, the XADD
    fails because the key exists as the wrong type — Redis will still execute
    the remaining commands and return per-command results.  There is no
    automatic rollback.  This means error handling after ``.execute()`` needs
    to check individual responses rather than assuming "it all worked or none
    of it did."  For our use case this is acceptable because both commands
    target keys we fully control (no type conflicts), but it's a real
    distinction worth understanding rather than glossing over.
    """
    job = Job(
        payload=payload,
        max_attempts=max_attempts if max_attempts is not None else config.DEFAULT_MAX_ATTEMPTS,
    )

    # Build the Redis hash key for this job's metadata.
    hash_key = f"{config.JOB_HASH_KEY_PREFIX}{job.id}"

    # Use a pipeline in transaction mode (MULTI/EXEC) so the hash write
    # and the stream entry appear atomically to every other client.
    pipe = client.pipeline(transaction=True)

    # 1. Persist the full job state as a hash.  This is the single source
    #    of truth — every field lives here, and workers/scheduler/monitor
    #    all read/update this hash.
    pipe.hset(hash_key, mapping=job.to_redis_hash())

    # 2. Append a pointer to the main stream.  The stream entry is
    #    intentionally minimal: just the job_id.  Workers use this to
    #    look up the full job hash.  The "*" tells Redis to auto-generate
    #    a monotonic stream message ID (timestamp-sequence), which gives
    #    us FIFO ordering without managing our own sequence numbers.
    pipe.xadd(config.QUEUE_NAME, {"job_id": job.id})

    # Execute both commands as one atomic block.
    # Returns a list of per-command results: [hset_result, xadd_result].
    pipe.execute()

    JOBS_ENQUEUED.labels(queue=config.QUEUE_NAME).inc()

    return job.id
