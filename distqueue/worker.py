"""
worker.py — Consumer-side logic for distqueue.

A Worker reads jobs from a Redis Stream via a consumer group, executes a
user-supplied handler function, and manages the full success/failure lifecycle:
acknowledging completed jobs, scheduling retries with exponential backoff, and
moving permanently failed jobs to the dead-letter queue.

This module also exports handle_job_failure() — the shared retry/DLQ logic
that both Worker (on handler exception) and Monitor (on dead-worker detection)
call.  It lives here rather than in a separate module because this is where
job-failure semantics originate; the monitor is reusing worker-domain logic,
not the other way around.

This module writes INTO the delayed ZSet (jobs:delayed) on failure, but does
NOT poll or re-inject from it — that's the scheduler's job (scheduler.py,
a separate milestone).  The separation keeps each module focused: the worker
owns "what happens when a job succeeds or fails," and the scheduler owns
"when should a delayed job become eligible again."
"""

from __future__ import annotations

import logging
import os
import random
import socket
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

import redis

from distqueue import config
from distqueue.job import Job
from distqueue.metrics import JOBS_COMPLETED, JOBS_DURATION, JOBS_FAILED

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared failure-handling logic (used by both Worker and Monitor)
# ---------------------------------------------------------------------------
#
# Why extract this instead of duplicating it in monitor.py?
#
# The state transition on failure — increment attempts, compute backoff,
# schedule retry or DLQ — is genuinely the same whether the failure was
# triggered by a handler exception (worker) or a dead-worker heartbeat
# expiry (monitor).  The only difference is the error_message string.
# Duplicating this logic would mean two places to update if the backoff
# formula, DLQ format, or pipeline structure ever changes — a classic
# source of subtle divergence bugs.
#
# We keep it in worker.py (rather than a new _transitions.py module)
# because this IS worker-domain logic: the monitor is reusing the
# worker's failure semantics, not defining its own.


def handle_job_failure(
    client: redis.Redis,
    job: Job,
    hash_key: str,
    entry_id: str,
    queue_name: str,
    group_name: str,
    error_message: str,
    trigger: str,
) -> None:
    """Apply the retry-or-DLQ state transition to a failed job.

    This is the single implementation of the failure fork:
      - attempts < max_attempts → exponential backoff → delayed ZSet
      - attempts >= max_attempts → dead-letter queue

    Called by Worker._handle_failure (handler raised an exception) and
    Monitor.tick (worker died with a job in the PEL).  Both represent
    the same semantic event: "this attempt did not complete successfully."

    Parameters
    ----------
    client : redis.Redis
        Connected Redis client.
    job : Job
        The job to transition.  Modified in place (attempts, status, etc.).
    hash_key : str
        Redis key for the job's hash (e.g. "job:abc123").
    entry_id : str
        Stream entry ID to XACK after the transition.
    queue_name : str
        Stream name for the XACK.
    group_name : str
        Consumer group name for the XACK.
    error_message : str
        Human-readable description of what went wrong.  Stored in
        job.last_error and in the DLQ entry's "reason" field.
    trigger : str
        What caused this failure — "exception" (handler raised) or
        "worker_death" (monitor reclaimed from a dead worker).  Used
        as a label on the distqueue_jobs_failed_total counter so
        dashboards can distinguish application errors from infra failures.
    """
    job.attempts += 1
    job.last_error = error_message
    job.updated_at = time.time()

    if job.attempts >= job.max_attempts:
        # --- Dead letter: all retries exhausted ---
        job.status = "DEAD"

        pipe = client.pipeline(transaction=True)
        pipe.hset(hash_key, mapping=job.to_redis_hash())
        pipe.xadd(
            config.DLQ_STREAM,
            {"job_id": job.id, "reason": job.last_error},
        )
        pipe.xack(queue_name, group_name, entry_id)
        pipe.execute()

        JOBS_FAILED.labels(
            queue=queue_name, outcome="dlq", trigger=trigger
        ).inc()
    else:
        # --- Retry: schedule with exponential backoff + jitter ---
        #
        # delay = min(BASE_BACKOFF_S * 2^attempts + jitter, MAX_BACKOFF_S)
        #
        # The exponential term gives the failing downstream service
        # progressively more breathing room on each retry.  The jitter
        # prevents thundering herd — see config.py § JITTER_MAX_S for
        # the full rationale.
        delay = min(
            config.BASE_BACKOFF_S * (2 ** job.attempts)
            + random.uniform(0, config.JITTER_MAX_S),
            config.MAX_BACKOFF_S,
        )
        job.next_retry_at = time.time() + delay
        job.status = "PENDING"

        pipe = client.pipeline(transaction=True)
        pipe.hset(hash_key, mapping=job.to_redis_hash())
        # ZADD to the delayed set with score = next_retry_at.  The
        # scheduler will poll this set and re-XADD jobs whose score
        # is <= now.
        pipe.zadd(config.DELAYED_ZSET, {job.id: job.next_retry_at})
        pipe.xack(queue_name, group_name, entry_id)
        pipe.execute()

        JOBS_FAILED.labels(
            queue=queue_name, outcome="retried", trigger=trigger
        ).inc()


class Worker:
    """A consumer that pulls jobs from a Redis Stream and executes them.

    Parameters
    ----------
    client : redis.Redis
        A connected Redis client (from distqueue.client.get_redis_client).
    handler : Callable[[dict], None]
        A function that takes a job's payload dict and does the actual work.
        Returns normally on success; raises any exception on failure.
    queue_name : str
        The Redis Stream to consume from.
    group_name : str
        The consumer group name.  All workers sharing a group cooperatively
        split the stream's messages — each message goes to exactly one consumer.
    consumer_name : str | None
        Unique identifier for this consumer within the group.  Auto-generated
        if not provided, combining hostname + PID + random suffix to be unique
        across hosts and processes in a Docker Compose environment.
    stop_event : threading.Event | None
        When set, the run() loop exits cleanly.  Allows tests and callers to
        stop the worker without killing the process.
    block_ms : int
        How long (in milliseconds) XREADGROUP blocks waiting for new messages
        before returning empty.  Default 2000ms balances responsiveness with
        CPU usage.  Tests can set this low (e.g. 100) to avoid slow tests.
    heartbeat_interval_s : float
        How often the heartbeat thread refreshes the worker's liveness key.
        Overridable for testability — tests can set this to 0.1s instead of
        waiting the default 5s.
    heartbeat_ttl_s : int
        TTL on the heartbeat key.  Should be ~3× heartbeat_interval_s.
    """

    def __init__(
        self,
        client: redis.Redis,
        handler: Callable[[dict[str, Any]], None],
        queue_name: str = config.QUEUE_NAME,
        group_name: str = config.CONSUMER_GROUP,
        consumer_name: str | None = None,
        stop_event: threading.Event | None = None,
        block_ms: int = 2000,
        heartbeat_interval_s: float = config.HEARTBEAT_INTERVAL_S,
        heartbeat_ttl_s: int = config.HEARTBEAT_TTL_S,
    ) -> None:
        self._client = client
        self._handler = handler
        self._queue_name = queue_name
        self._group_name = group_name

        # Generate a consumer name that's unique across hosts *and* processes.
        # In a Docker Compose setup, hostname alone isn't unique (containers
        # get random hostnames), and PID alone isn't unique across containers
        # (PID 1 in every container).  Combining hostname + PID + random
        # suffix covers all cases.
        self._consumer_name = consumer_name or (
            f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        )

        self._stop_event = stop_event or threading.Event()
        self._block_ms = block_ms
        self._heartbeat_interval_s = heartbeat_interval_s
        self._heartbeat_ttl_s = heartbeat_ttl_s

        # Create the consumer group eagerly so that process_one() can be
        # called directly (e.g. in tests) without needing to call run() first.
        self._ensure_consumer_group()

    # ------------------------------------------------------------------
    # Consumer group setup
    # ------------------------------------------------------------------

    def _ensure_consumer_group(self) -> None:
        """Create the consumer group if it doesn't already exist.

        Uses MKSTREAM so the stream is also auto-created if absent — this
        avoids a chicken-and-egg problem where the producer can't XADD
        because the stream doesn't exist, and the worker can't XGROUP CREATE
        because there's no stream to attach the group to.

        The "$" start ID means the group will only receive messages added
        AFTER the group was created.  Messages already in the stream are
        ignored — this is the correct behavior for a "start fresh" consumer
        group.

        BUSYGROUP is the error Redis returns when the group already exists.
        We catch and swallow it to make this method idempotent — safe to
        call from every Worker instance without coordination.
        """
        try:
            self._client.xgroup_create(
                self._queue_name,
                self._group_name,
                id="$",
                mkstream=True,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
            # Group already exists — expected when multiple workers start up
            # against the same stream.  Nothing to do.

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        """Continuously refresh this worker's heartbeat key in Redis.

        WHY THIS MUST BE A SEPARATE THREAD:

        The main thread alternates between two potentially long operations:
          1. XREADGROUP with BLOCK — can block for up to block_ms waiting
             for new messages.
          2. handler(payload) — can run for an arbitrary duration (e.g. 30s
             for an ML inference job, minutes for a large file upload).

        If the heartbeat refresh happened in the same loop (e.g. "refresh
        heartbeat between process_one() calls"), it would NOT fire while
        either of those operations is running.  If the handler takes longer
        than HEARTBEAT_TTL_S (default 15 seconds), the heartbeat key would
        expire while the worker is still alive and actively working on the
        job.  The monitor process would then see the expired key, conclude
        the worker is dead, and XAUTOCLAIM the job — re-assigning it to
        another worker while the original worker is still executing it.
        This leads to duplicate processing: two workers working on the
        same job simultaneously, which violates the exactly-once-delivery
        guarantee that consumer groups are supposed to provide.

        Running the heartbeat on a daemon thread ensures it fires on
        schedule regardless of what the main thread is doing.
        """
        key = (
            f"{config.WORKER_HEARTBEAT_KEY_PREFIX}"
            f"{self._consumer_name}"
            f"{config.WORKER_HEARTBEAT_KEY_SUFFIX}"
        )
        while not self._stop_event.is_set():
            self._client.set(key, str(time.time()), ex=self._heartbeat_ttl_s)
            # Use Event.wait() instead of time.sleep() so the thread wakes
            # up immediately when stop_event is set, rather than sleeping
            # through the remaining interval.  This makes shutdown faster.
            self._stop_event.wait(self._heartbeat_interval_s)

    # ------------------------------------------------------------------
    # Job processing
    # ------------------------------------------------------------------

    def process_one(self) -> bool:
        """Read and fully process exactly one job from the stream.

        Returns True if a job was processed (success or failure), False if
        the XREADGROUP call timed out with no message available.

        This method is deliberately separate from the run() loop so that
        tests can call it directly for deterministic, single-step execution
        without dealing with threads or blocking read loops.
        """
        # XREADGROUP: pull the next undelivered message (">") for this
        # consumer.  COUNT=1 so we process one job at a time — simpler to
        # reason about and gives predictable memory/CPU usage.  BLOCK waits
        # up to block_ms for a new entry if none is immediately available,
        # which prevents a busy-spin loop from burning CPU.
        result = self._client.xreadgroup(
            groupname=self._group_name,
            consumername=self._consumer_name,
            streams={self._queue_name: ">"},
            count=1,
            block=self._block_ms,
        )

        # xreadgroup returns [] or None on timeout (no messages available).
        if not result:
            return False

        # Result shape: [[stream_name, [(entry_id, fields_dict)]]]
        _stream_name, entries = result[0]
        entry_id, fields = entries[0]
        job_id = fields["job_id"]

        hash_key = f"{config.JOB_HASH_KEY_PREFIX}{job_id}"

        # --- Edge case: job hash doesn't exist ---
        # This can happen if someone manually DEL'd the key, or if a future
        # TTL policy expires old hashes.  There's nothing useful we can do
        # with a stream entry that points to a nonexistent job, so we XACK
        # it to clear it from the PEL and move on.
        raw = self._client.hgetall(hash_key)
        if not raw:
            logger.warning(
                "Job hash %s not found — job may have been manually deleted "
                "or TTL'd.  Acknowledging stream entry %s to clear it from "
                "the PEL; nothing more can be done.",
                hash_key,
                entry_id,
            )
            self._client.xack(self._queue_name, self._group_name, entry_id)
            return True

        # --- Deserialize and mark as RUNNING ---
        job = Job.from_redis_hash(raw)

        # Deliberate simplification: we skip writing a separate CLAIMED state
        # to the hash.  The moment XREADGROUP delivers an entry to this
        # consumer, Redis adds it to the Pending Entries List (PEL), which
        # *already represents* "this consumer has claimed this message."
        # Writing CLAIMED to the hash and then immediately overwriting it
        # with RUNNING would be a redundant HSET round trip with no
        # observable benefit — no other component reads the CLAIMED state.
        # The monitor checks the PEL (via XPENDING/XAUTOCLAIM), not the hash
        # status field, to find stuck jobs.
        job.status = "RUNNING"
        job.last_worker = self._consumer_name
        job.updated_at = time.time()
        self._client.hset(hash_key, mapping=job.to_redis_hash())

        # --- Execute the user's handler ---
        # Wrap with monotonic clock to measure handler duration regardless
        # of outcome — a failed job still consumed processing time and
        # that's worth measuring for capacity planning.
        t0 = time.monotonic()
        try:
            self._handler(job.payload)
        except Exception as exc:
            elapsed = time.monotonic() - t0
            JOBS_DURATION.labels(queue=self._queue_name).observe(elapsed)
            return self._handle_failure(job, hash_key, entry_id, exc)
        else:
            elapsed = time.monotonic() - t0
            JOBS_DURATION.labels(queue=self._queue_name).observe(elapsed)
            return self._handle_success(job, hash_key, entry_id)

    def _handle_success(
        self, job: Job, hash_key: str, entry_id: str
    ) -> bool:
        """Mark the job as completed and acknowledge the stream entry.

        Both operations are wrapped in a MULTI/EXEC pipeline — same
        discipline as producer.py — so no consumer can observe a
        "completed but un-acked" or "acked but still RUNNING" state.
        """
        job.status = "COMPLETED"
        job.updated_at = time.time()

        pipe = self._client.pipeline(transaction=True)
        pipe.hset(hash_key, mapping=job.to_redis_hash())
        pipe.xack(self._queue_name, self._group_name, entry_id)
        pipe.execute()

        JOBS_COMPLETED.labels(queue=self._queue_name).inc()

        return True

    def _handle_failure(
        self, job: Job, hash_key: str, entry_id: str, exc: Exception
    ) -> bool:
        """Handle a job whose handler raised an exception.

        Delegates to the module-level handle_job_failure() — the shared
        retry/DLQ logic that the monitor also calls when reclaiming jobs
        from dead workers.  See that function's docstring for the full
        state-transition details.
        """
        handle_job_failure(
            self._client, job, hash_key, entry_id,
            self._queue_name, self._group_name,
            error_message=str(exc),
            trigger="exception",
        )
        return True

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the heartbeat thread and consume jobs until stopped.

        Loops calling process_one() until the stop_event is set.  The
        heartbeat thread runs independently on a daemon thread so it
        keeps firing even while process_one() is blocked on XREADGROUP
        or inside the handler.

        To stop cleanly from another thread or a test:
            stop_event.set()
        The run() loop will exit after the current process_one() call
        finishes (at most block_ms delay if idle).
        """
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{self._consumer_name}",
        )
        heartbeat_thread.start()

        try:
            while not self._stop_event.is_set():
                self.process_one()
        finally:
            # Ensure the heartbeat thread exits even if the loop ended
            # due to an unexpected exception (not just stop_event).
            self._stop_event.set()
            heartbeat_thread.join(timeout=5)
