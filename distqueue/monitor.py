"""
monitor.py — Dead-worker detection and job reclamation for distqueue.

The monitor periodically scans the Pending Entries List (PEL) for stream
entries that have been idle too long, checks whether the owning worker is
still alive via its heartbeat key, and reclaims orphaned jobs from dead
workers.

This is the "last line of defense" in the failure-detection design:

  1. A healthy worker refreshes its heartbeat key every HEARTBEAT_INTERVAL_S
     (5s) with a TTL of HEARTBEAT_TTL_S (15s).
  2. If a worker crashes or loses connectivity, its heartbeat key expires
     after 15 seconds (no refresh).
  3. The monitor finds the dead worker's PEL entries via XPENDING IDLE,
     confirms the heartbeat is gone, and reclaims the job via XAUTOCLAIM.

Reclaimed jobs go through the same retry-or-DLQ logic as handler exceptions
(via the shared handle_job_failure function in worker.py) — a worker death
counts as a failed attempt, which is the correct semantic: if the job wasn't
completed, the attempt was wasted, and the retry budget should reflect that.
Without this, a poison job that kills its worker would retry forever.
"""

from __future__ import annotations

import logging
import threading
import uuid

import redis

from distqueue import config
from distqueue.job import Job
from distqueue.metrics import JOBS_RECLAIMED, PENDING_ENTRIES
from distqueue.worker import handle_job_failure

logger = logging.getLogger(__name__)


class Monitor:
    """Detects dead workers and reclaims their orphaned jobs.

    Parameters
    ----------
    client : redis.Redis
        A connected Redis client.
    queue_name : str
        The Redis Stream to monitor.
    group_name : str
        The consumer group whose PEL to scan.
    poll_interval_s : float
        Seconds between tick() calls in the run() loop.
    min_idle_ms : int
        Minimum idle time (in milliseconds) a PEL entry must have before
        the monitor considers it.  This is a coarse pre-filter — the
        actual reclaim decision is gated on the heartbeat check.
    stop_event : threading.Event | None
        When set, the run() loop exits cleanly.
    """

    def __init__(
        self,
        client: redis.Redis,
        queue_name: str = config.QUEUE_NAME,
        group_name: str = config.CONSUMER_GROUP,
        poll_interval_s: float = config.MONITOR_POLL_INTERVAL_S,
        min_idle_ms: int = config.MONITOR_MIN_IDLE_MS,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        self._group_name = group_name
        self._poll_interval_s = poll_interval_s
        self._min_idle_ms = min_idle_ms
        self._stop_event = stop_event or threading.Event()

        # The monitor claims orphaned entries under its own consumer name.
        # This name must be unique so that multiple monitor instances don't
        # collide, and distinct from real worker names so it's easy to spot
        # in XPENDING output during debugging.
        self._consumer_name = f"monitor-reclaim-{uuid.uuid4().hex[:6]}"

    def tick(self) -> int:
        """One monitoring pass: find and reclaim jobs from dead workers.

        Returns the number of jobs actually reclaimed.

        Flow:
          1. XPENDING with IDLE filter → list PEL entries idle > min_idle_ms
          2. For each, check if the owning worker's heartbeat key exists
          3. If heartbeat present → skip (worker is alive, just slow)
          4. If heartbeat absent → worker is presumed dead:
             a. XAUTOCLAIM the entry to the monitor's consumer
             b. Apply retry-or-DLQ logic (via handle_job_failure)

        This method is deliberately separate from run() so tests can call
        it directly for deterministic, single-pass assertions.
        """
        # XPENDING with IDLE: returns PEL entries idle for >= min_idle_ms.
        # Each entry includes the owning consumer name, which we need for
        # the heartbeat check.  Count capped at 100 for the same reason as
        # the scheduler's batch_size — prevents one tick from running
        # unboundedly long on a large PEL.
        pending = self._client.xpending_range(
            self._queue_name,
            self._group_name,
            min="-",
            max="+",
            count=100,
            idle=self._min_idle_ms,
        )

        if not pending:
            # Still update the PEL gauge even when no idle entries matched.
            pending_summary = self._client.xpending(
                self._queue_name, self._group_name
            )
            PENDING_ENTRIES.labels(queue=self._queue_name).set(
                pending_summary["pending"]
            )
            return 0

        reclaimed = 0
        for entry in pending:
            consumer_name = entry["consumer"]
            entry_id = entry["message_id"]

            # --- Heartbeat check: is this worker still alive? ---
            heartbeat_key = (
                f"{config.WORKER_HEARTBEAT_KEY_PREFIX}"
                f"{consumer_name}"
                f"{config.WORKER_HEARTBEAT_KEY_SUFFIX}"
            )
            if self._client.exists(heartbeat_key):
                # Worker is alive — it's just slow (e.g. running a long
                # handler, or the XREADGROUP block hasn't timed out yet).
                # Don't touch this entry; the worker will eventually XACK
                # it or fail it normally.
                continue

            # --- Worker is presumed dead: reclaim via XAUTOCLAIM ---
            #
            # Why XAUTOCLAIM instead of XCLAIM?
            #
            # XCLAIM requires you to specify exact entry IDs and will error
            # (or silently do nothing) if an entry no longer exists in the
            # stream (e.g. it was XDEL'd or the stream was trimmed).  You'd
            # have to add your own error handling for this edge case.
            #
            # XAUTOCLAIM handles this gracefully: it scans the PEL from a
            # start ID, claims eligible entries, and returns deleted entries
            # in a separate list — no exception, no silent data loss.  It
            # also re-checks the idle time atomically, so if another monitor
            # instance already reclaimed this entry (resetting its idle time),
            # XAUTOCLAIM simply skips it.  This makes the monitor naturally
            # safe to run as multiple replicas without coordination.
            #
            # We pass start_id=entry_id and count=1 to target the specific
            # entry we identified via XPENDING.
            result = self._client.xautoclaim(
                self._queue_name,
                self._group_name,
                self._consumer_name,
                min_idle_time=self._min_idle_ms,
                start_id=entry_id,
                count=1,
            )

            # xautoclaim returns: (next_start_id, claimed_entries, deleted_ids)
            # claimed_entries: list of (entry_id, {field: value})
            # deleted_ids: entries that no longer exist in the stream
            _next_id, claimed_entries, _deleted_ids = result

            if not claimed_entries:
                # Entry was already reclaimed by another monitor, or its
                # idle time was reset (unlikely given the heartbeat is gone,
                # but defensive coding).
                continue

            for claimed_id, fields in claimed_entries:
                job_id = fields.get("job_id")
                if not job_id:
                    # Malformed stream entry — shouldn't happen, but XACK
                    # it to clear it from the PEL regardless.
                    logger.warning(
                        "Stream entry %s has no job_id field — acking to "
                        "clear from PEL.",
                        claimed_id,
                    )
                    self._client.xack(
                        self._queue_name, self._group_name, claimed_id
                    )
                    continue

                hash_key = f"{config.JOB_HASH_KEY_PREFIX}{job_id}"
                raw = self._client.hgetall(hash_key)

                if not raw:
                    # Job hash doesn't exist — same edge case as worker.py.
                    # XACK to clear the PEL entry.
                    logger.warning(
                        "Job hash %s not found during reclamation — acking "
                        "entry %s to clear PEL.",
                        hash_key,
                        claimed_id,
                    )
                    self._client.xack(
                        self._queue_name, self._group_name, claimed_id
                    )
                    continue

                job = Job.from_redis_hash(raw)

                # A worker death counts as a failed attempt — same logic
                # as a handler exception, just with a different error
                # message so the retry/DLQ history distinguishes
                # "application error" from "infrastructure failure."
                error_msg = (
                    f"worker {consumer_name} presumed dead "
                    f"(heartbeat expired)"
                )

                handle_job_failure(
                    self._client,
                    job,
                    hash_key,
                    claimed_id,
                    self._queue_name,
                    self._group_name,
                    error_message=error_msg,
                    trigger="worker_death",
                )
                reclaimed += 1
                JOBS_RECLAIMED.labels(queue=self._queue_name).inc()

        # --- Update PEL gauge once per tick ---
        # This uses an *unfiltered* XPENDING summary (no IDLE parameter)
        # because we want the total PEL size, not just the idle entries.
        # The idle-filtered query above is for reclaim candidates; this
        # one is for the gauge — they answer different questions.
        pending_summary = self._client.xpending(
            self._queue_name, self._group_name
        )
        PENDING_ENTRIES.labels(queue=self._queue_name).set(
            pending_summary["pending"]
        )

        return reclaimed

    def run(self) -> None:
        """Loop calling tick() until the stop event is set.

        Between ticks, we sleep for poll_interval_s using Event.wait()
        so that stop_event.set() wakes us immediately instead of waiting
        out the full interval.

        No heartbeat thread is needed here.  The monitor doesn't hold jobs
        in the PEL the way workers do — reclaimed entries are immediately
        processed (XACK'd after retry/DLQ).  If the monitor dies, orphaned
        jobs simply wait in the PEL a bit longer until it restarts.  There's
        no data loss, just delayed reclamation.
        """
        while not self._stop_event.is_set():
            self.tick()
            self._stop_event.wait(self._poll_interval_s)
