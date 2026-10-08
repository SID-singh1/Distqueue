"""
worker.py — Consumer-side logic for distqueue.

A Worker reads jobs from one queue's stream via a consumer group, runs a
user-supplied handler, and reports the outcome through the transitions in
transitions.py: COMPLETE on success, FAIL (retry or dead-letter) on error.

Delivery guarantee: at-least-once
---------------------------------
Consumer groups deliver each entry to one consumer *at a time*, not
exactly once.  If a worker dies (or stalls past its heartbeat TTL) the
monitor reclaims its job and it runs again elsewhere — possibly after the
first worker already performed some side effects.  Fencing (see
transitions.py) guarantees the job's *state* stays consistent; it cannot
un-send an email.  Handlers must therefore be idempotent.

This module writes INTO the delayed ZSet (via the FAIL transition) but
does not poll it — that's the scheduler's job.  The worker owns "what
happens when a job succeeds or fails"; the scheduler owns "when does a
delayed job become eligible again".
"""

from __future__ import annotations

import logging
import math
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

import redis

from distqueue import config
from distqueue.errors import PermanentError
from distqueue.job import Job
from distqueue.metrics import (
    END_TO_END,
    HEARTBEAT_FAILURES,
    JOBS_COMPLETED,
    JOBS_DURATION,
    JOBS_SKIPPED,
    LEASE_LOST,
    QUEUE_WAIT,
)
from distqueue.runloop import run_until_stopped
from distqueue.transitions import (
    StartStatus,
    complete_job,
    dead_letter_unreadable,
    fail_job,
    format_error,
    start_job,
)

logger = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], None]


class Worker:
    """A consumer that pulls jobs from a Redis Stream and executes them.

    Parameters
    ----------
    client : redis.Redis
        A connected Redis client (from distqueue.client.get_redis_client).
    handler : Callable[[dict], None]
        Takes a job's payload dict and does the work.  Returns normally on
        success; raises on failure.  Raise ``PermanentError`` to skip
        retries and dead-letter immediately.
    queue : str
        Logical queue name to consume (see config.stream_key).
    group_name : str
        Consumer group.  All workers sharing a group split the stream.
    consumer_name : str | None
        Unique identifier within the group.  Auto-generated from hostname +
        PID + random suffix: in Docker every container's PID is 1 and
        hostnames repeat across restarts, so neither alone is unique.
    stop_event : threading.Event | None
        When set, run() exits after the current job finishes.
    block_ms : int
        How long XREADGROUP blocks waiting for a message.  Bounds how long
        shutdown takes when idle.
    heartbeat_interval_s, heartbeat_ttl_s
        Heartbeat cadence and key TTL.  TTL should be ~3× the interval.
    """

    def __init__(
        self,
        client: redis.Redis,
        handler: Handler,
        queue: str = config.DEFAULT_QUEUE,
        group_name: str = config.CONSUMER_GROUP,
        consumer_name: str | None = None,
        stop_event: threading.Event | None = None,
        block_ms: int = 2000,
        heartbeat_interval_s: float = config.HEARTBEAT_INTERVAL_S,
        heartbeat_ttl_s: int = config.HEARTBEAT_TTL_S,
    ) -> None:
        self._client = client
        self._handler = handler
        self._queue = queue
        self._stream = config.stream_key(queue)
        self._group_name = group_name
        self._consumer_name = consumer_name or (
            f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        )
        self._stop_event = stop_event or threading.Event()
        self._block_ms = block_ms
        self._heartbeat_interval_s = heartbeat_interval_s
        self._heartbeat_ttl_s = heartbeat_ttl_s
        self._heartbeat_key = config.heartbeat_key(self._consumer_name)

        # Monotonic time of the last *successful* heartbeat write.  -inf
        # means "never": a worker that has not yet proven it's alive is
        # treated as not alive (see heartbeat_healthy).
        self._last_heartbeat_ok = -math.inf

        # Create the consumer group eagerly so process_one() can be called
        # directly (e.g. in tests) without run().
        self._ensure_consumer_group()

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    @property
    def queue(self) -> str:
        return self._queue

    # ------------------------------------------------------------------
    # Consumer group setup
    # ------------------------------------------------------------------

    def _ensure_consumer_group(self) -> None:
        """Create the consumer group if it doesn't already exist.

        **Start ID "0", not "$".**  "$" means "only entries added after the
        group was created", so any job enqueued before the very first
        worker started would be skipped forever — its hash stuck at
        PENDING, with no error anywhere.  "0" delivers everything already
        in the stream.  Re-delivering old entries is harmless: entries
        whose job is already COMPLETED/DEAD are acknowledged without
        running (see the START transition), and trimming keeps the stream
        short anyway.

        MKSTREAM creates the stream if absent, so a worker can start
        before any producer has written anything.

        BUSYGROUP ("group already exists") is swallowed so every Worker
        instance can call this without coordination.
        """
        try:
            self._client.xgroup_create(
                self._stream, self._group_name, id="0", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _beat(self) -> bool:
        """Refresh the heartbeat key once.  Never raises.

        Why never raise: before this change the heartbeat loop had no
        exception handling, so a single Redis blip during SET killed the
        heartbeat *thread* while the main thread kept consuming jobs.  The
        worker became a zombie — alive and taking work, but invisible to
        the monitor, which then reclaimed (and re-ran) every job it took.
        """
        try:
            self._client.set(
                self._heartbeat_key, str(time.time()), ex=self._heartbeat_ttl_s
            )
        except Exception:  # this thread must not die: see docstring
            HEARTBEAT_FAILURES.inc()
            logger.warning(
                "Heartbeat refresh failed for %s", self._consumer_name, exc_info=True
            )
            return False
        self._last_heartbeat_ok = time.monotonic()
        return True

    def heartbeat_healthy(self) -> bool:
        """True if the last successful heartbeat is younger than the TTL.

        If this is False, the heartbeat key may already have expired and
        the monitor may consider this worker dead — so any job taken now
        could be reclaimed and run twice.  The run loop stops taking new
        work until a heartbeat succeeds again ("self-fencing").
        """
        return time.monotonic() - self._last_heartbeat_ok < self._heartbeat_ttl_s

    def _heartbeat_loop(self) -> None:
        """Refresh the heartbeat key every heartbeat_interval_s.

        WHY A SEPARATE THREAD: the main thread spends its time blocked in
        XREADGROUP or inside the handler, which can run for minutes.  A
        heartbeat refreshed "between jobs" would expire during any job
        longer than the TTL, and the monitor would reclaim a job whose
        worker is alive and making progress.
        """
        while not self._stop_event.wait(self._heartbeat_interval_s):
            self._beat()

    # ------------------------------------------------------------------
    # Job processing
    # ------------------------------------------------------------------

    def process_one(self) -> bool:
        """Read and fully process at most one job from the stream.

        Returns True if a stream entry was consumed (run, skipped, or
        dead-lettered), False if XREADGROUP timed out with nothing to read.

        Separate from run() so tests can drive the worker one deterministic
        step at a time, without threads or blocking loops.
        """
        # COUNT=1: one job at a time per worker keeps memory and failure
        # blast radius predictable.  Prefetching a batch would save round
        # trips but every prefetched entry sits in this worker's PEL — if
        # it dies, all of them wait for reclamation, not just one.
        result = self._client.xreadgroup(
            groupname=self._group_name,
            consumername=self._consumer_name,
            streams={self._stream: ">"},
            count=1,
            block=self._block_ms,
        )
        if not result:
            return False

        # Result shape: [[stream_name, [(entry_id, fields_dict)]]]
        _stream_name, entries = result[0]
        entry_id, fields = entries[0]
        job_id = fields.get("job_id")

        if not job_id:
            # Not produced by distqueue.  Nothing can process it; ack so it
            # doesn't sit in the PEL forever.
            logger.warning("Stream entry %s has no job_id; acknowledging.", entry_id)
            self._client.xack(self._stream, self._group_name, entry_id)
            JOBS_SKIPPED.labels(queue=self._queue, reason="malformed").inc()
            return True

        started = start_job(
            self._client,
            job_id=job_id,
            queue=self._queue,
            entry_id=entry_id,
            group=self._group_name,
            consumer=self._consumer_name,
        )

        if started.status is StartStatus.MISSING:
            logger.warning(
                "Job hash for %s not found; entry %s acked.", job_id, entry_id
            )
            JOBS_SKIPPED.labels(queue=self._queue, reason="missing").inc()
            return True
        if started.status is StartStatus.TERMINAL:
            # A duplicate delivery of a job that already finished.  This is
            # what makes the at-least-once machinery safe to over-deliver.
            logger.info(
                "Job %s already %s; skipping duplicate delivery %s.",
                job_id,
                started.terminal_status,
                entry_id,
            )
            JOBS_SKIPPED.labels(queue=self._queue, reason="terminal").inc()
            return True
        if started.status is StartStatus.LOST:
            # Reclaimed between XREADGROUP and START — only possible if this
            # worker stalled longer than the monitor's idle threshold.
            LEASE_LOST.labels(queue=self._queue, transition="start").inc()
            return True

        if started.queue_wait_s is not None:
            QUEUE_WAIT.labels(queue=self._queue).observe(max(started.queue_wait_s, 0.0))

        try:
            job = Job.from_redis_hash(started.job_data or {})
        except (KeyError, ValueError, TypeError) as exc:
            logger.error(
                "Job %s has an unreadable hash (%s); dead-lettering.", job_id, exc
            )
            dead_letter_unreadable(
                self._client,
                job_id=job_id,
                queue=self._queue,
                entry_id=entry_id,
                group=self._group_name,
                consumer=self._consumer_name,
                error=f"corrupt job record: {format_error(exc)}",
            )
            return True

        self._run_handler(job, entry_id)
        return True

    def _run_handler(self, job: Job, entry_id: str) -> None:
        """Execute the handler and record the outcome.

        Duration is measured on every outcome — a failed job still consumed
        worker time, and that matters for capacity planning.
        """
        t0 = time.monotonic()
        try:
            self._handler(job.payload)
        except PermanentError as exc:
            JOBS_DURATION.labels(queue=self._queue).observe(time.monotonic() - t0)
            fail_job(
                self._client,
                job,
                entry_id=entry_id,
                group=self._group_name,
                consumer=self._consumer_name,
                error=format_error(exc),
                trigger="permanent",
                permanent=True,
            )
        except Exception as exc:  # noqa: BLE001 — user code: any error is a failed attempt
            JOBS_DURATION.labels(queue=self._queue).observe(time.monotonic() - t0)
            fail_job(
                self._client,
                job,
                entry_id=entry_id,
                group=self._group_name,
                consumer=self._consumer_name,
                error=format_error(exc),
                trigger="exception",
            )
        else:
            JOBS_DURATION.labels(queue=self._queue).observe(time.monotonic() - t0)
            done = complete_job(
                self._client,
                job_id=job.id,
                queue=self._queue,
                entry_id=entry_id,
                group=self._group_name,
                consumer=self._consumer_name,
            )
            if done.applied:
                JOBS_COMPLETED.labels(queue=self._queue).inc()
                if done.end_to_end_s is not None:
                    END_TO_END.labels(queue=self._queue).observe(
                        max(done.end_to_end_s, 0.0)
                    )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _tick(self) -> None:
        """One iteration of the run loop."""
        if not self.heartbeat_healthy():
            # Self-fencing: we can't prove we're alive, so don't take work
            # that the monitor might reclaim out from under us.  Try to
            # re-establish the heartbeat directly rather than waiting for
            # the heartbeat thread's next interval.
            if not self._beat():
                self._stop_event.wait(min(self._heartbeat_interval_s, 1.0))
            return
        try:
            self.process_one()
        except redis.ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            # The stream or group vanished: Redis restarted without
            # persistence, or someone ran FLUSHDB.  Recreate and carry on
            # instead of crashing every worker in the fleet.
            logger.warning("Consumer group missing on %s; recreating it.", self._stream)
            self._ensure_consumer_group()

    def run(self) -> None:
        """Heartbeat in the background and consume jobs until stopped.

        To stop cleanly from another thread or a signal handler:
            stop_event.set()
        The loop exits after the current job finishes (at most block_ms
        when idle).
        """
        # Beat once synchronously so the heartbeat key exists before the
        # first job is read — otherwise the monitor could see this
        # worker's first job with no heartbeat behind it.
        self._beat()
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{self._consumer_name}",
        )
        heartbeat_thread.start()

        try:
            run_until_stopped(self._tick, self._stop_event, 0, "worker")
        finally:
            self._stop_event.set()
            heartbeat_thread.join(timeout=5)
            self._deregister()

    def _deregister(self) -> None:
        """Best-effort cleanup on graceful shutdown.

        Deleting the heartbeat key lets the monitor act immediately on
        anything this worker left pending, rather than waiting out the TTL.
        Removing the consumer from the group keeps XINFO CONSUMERS from
        filling up with dead names — but only when it has no pending
        entries, because XGROUP DELCONSUMER silently discards a consumer's
        pending entries, which would orphan those jobs forever.
        """
        try:
            self._client.delete(self._heartbeat_key)
            pending = self._client.xpending_range(
                self._stream,
                self._group_name,
                min="-",
                max="+",
                count=1,
                consumername=self._consumer_name,
            )
            if not pending:
                self._client.xgroup_delconsumer(
                    self._stream, self._group_name, self._consumer_name
                )
        except redis.RedisError:
            logger.warning(
                "Deregistration of %s failed", self._consumer_name, exc_info=True
            )
