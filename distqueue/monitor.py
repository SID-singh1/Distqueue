"""
monitor.py — Dead-worker detection, job timeouts, and queue housekeeping.

Each tick, for every known queue, the monitor:

  1. **Reclaims** jobs whose worker is dead (heartbeat key expired) or that
     have run past their ``timeout_s`` even though the worker is alive.
     Reclaimed jobs go through the same FAIL transition as a handler
     exception — a lost attempt is a failed attempt, otherwise a poison
     job that kills its worker would be retried forever.
  2. **Trims** the stream of entries every consumer has finished with.
  3. **Garbage-collects** consumer names left behind by dead workers.
  4. **Publishes** queue-state gauges (backlog, PEL, delayed, DLQ, live
     workers) — the single observer of global queue state.

Safe to run as several replicas: every reclaim is an XCLAIM with a minimum
idle time on one exact entry id (see _reclaim_entry), so two monitors
racing for the same entry cannot both win, and neither can take an entry
it did not mean to.
"""

from __future__ import annotations

import logging
import socket
import threading
import uuid

import redis

from distqueue import config
from distqueue.job import TERMINAL_STATUSES, Job
from distqueue.metrics import (
    DELAYED_JOBS,
    DLQ_DEPTH,
    JOBS_RECLAIMED,
    JOBS_SKIPPED,
    LIVE_WORKERS,
    PENDING_ENTRIES,
    QUEUE_BACKLOG,
    STREAM_LENGTH,
)
from distqueue.runloop import run_until_stopped
from distqueue.stats import known_queues, live_consumers, queue_stats
from distqueue.transitions import (
    FailOutcome,
    LuaScript,
    dead_letter_unreadable,
    fail_job,
    format_error,
)

logger = logging.getLogger(__name__)

# Delete a consumer from the group only if, atomically: it has no heartbeat
# and no pending entries.  As separate commands there's a window where a
# worker could read a job between our check and the delete — and
# XGROUP DELCONSUMER silently discards a consumer's pending entries, so
# that job would be orphaned forever (delivered, never acked, never
# reclaimable).
# KEYS: 1 stream, 2 heartbeat key   ARGV: 1 group, 2 consumer
_GC_CONSUMER = LuaScript(
    "gc_consumer",
    """
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
if #redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]) > 0 then
    return 0
end
redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
return 1
""",
)


def _parse_id(entry_id: str) -> tuple[int, int]:
    """Stream ids compare numerically on (ms, seq), not as strings."""
    ms, _, seq = entry_id.partition("-")
    return int(ms), int(seq or 0)


def _is_nogroup(exc: redis.ResponseError) -> bool:
    return "NOGROUP" in str(exc) or "no such key" in str(exc).lower()


class Monitor:
    """Detects dead and timed-out jobs and reclaims them.

    Parameters
    ----------
    client : redis.Redis
        A connected Redis client.
    queues : list[str] | None
        Queues to watch.  None (the default) means "every queue in the
        jobs:queues registry", re-read each tick so new queues are picked up
        without a restart.
    group_name : str
        The consumer group whose PEL to scan.
    poll_interval_s : float
        Seconds between tick() calls in the run() loop.
    min_idle_ms : int
        Minimum idle time before a PEL entry is considered at all.
    scan_count : int
        Max PEL entries examined per queue per tick.
    consumer_gc_idle_ms : int
        Idle time after which a consumer with no heartbeat and nothing
        pending is removed from the group.
    approximate_trim : bool
        Use ``XTRIM MINID ~`` (cheap; removes whole radix-tree nodes of
        ~100 entries) rather than exact trimming.  Tests turn it off to
        observe trimming on small streams.
    stop_event : threading.Event | None
        When set, the run() loop exits cleanly.
    """

    def __init__(
        self,
        client: redis.Redis,
        queues: list[str] | None = None,
        group_name: str = config.CONSUMER_GROUP,
        poll_interval_s: float = config.MONITOR_POLL_INTERVAL_S,
        min_idle_ms: int = config.MONITOR_MIN_IDLE_MS,
        scan_count: int = config.MONITOR_SCAN_COUNT,
        consumer_gc_idle_ms: int = config.CONSUMER_GC_IDLE_MS,
        approximate_trim: bool = True,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._client = client
        self._queues = list(queues) if queues is not None else None
        self._group_name = group_name
        self._poll_interval_s = poll_interval_s
        self._min_idle_ms = min_idle_ms
        self._scan_count = scan_count
        self._consumer_gc_idle_ms = consumer_gc_idle_ms
        self._approximate_trim = approximate_trim
        self._stop_event = stop_event or threading.Event()

        # Reclaimed entries are XCLAIMed to this consumer name before the
        # FAIL transition acks them.  Unique per monitor so replicas don't
        # share an identity, and prefixed so it's obvious in XPENDING output.
        # It deliberately has no heartbeat: if a monitor dies between XCLAIM
        # and FAIL, another monitor sees a dead owner and reclaims it again.
        self._consumer_name = f"monitor-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"

        # Per-queue PEL scan cursor.  Without it every tick re-read the
        # *first* scan_count idle entries; if those all belonged to live
        # workers running long jobs, dead workers' entries further along
        # were never examined at all.
        self._cursors: dict[str, str] = {}

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    # ------------------------------------------------------------------
    # Tick
    # ------------------------------------------------------------------

    def queues(self) -> list[str]:
        """Queues to watch this tick."""
        if self._queues is not None:
            return self._queues
        return known_queues(self._client)

    def tick(self) -> int:
        """One monitoring pass over every queue.  Returns jobs reclaimed."""
        reclaimed = 0
        live_consumers: set[str] = set()
        for queue in self.queues():
            try:
                reclaimed += self.reclaim(queue)
                live_consumers |= self.collect_consumers(queue)
                self.trim(queue)
                self._update_queue_gauges(queue)
            except redis.ResponseError as exc:
                if not _is_nogroup(exc):
                    raise
                # No group yet: no worker has ever consumed this queue, so
                # there's nothing pending to reclaim and nothing to trim.
                self._update_queue_gauges(queue)

        DELAYED_JOBS.set(self._client.zcard(config.DELAYED_ZSET))
        DLQ_DEPTH.set(self._client.xlen(config.DLQ_STREAM))
        LIVE_WORKERS.set(len(live_consumers))
        return reclaimed

    # ------------------------------------------------------------------
    # 1. Reclamation
    # ------------------------------------------------------------------

    def reclaim(self, queue: str) -> int:
        """Reclaim dead-worker and timed-out jobs on one queue."""
        stream = config.stream_key(queue)
        start = self._cursors.get(queue, "-")
        pending = self._client.xpending_range(
            stream,
            self._group_name,
            min=start,
            max="+",
            count=self._scan_count,
            idle=self._min_idle_ms,
        )
        # Advance the cursor; wrap to the beginning after a short page.
        if len(pending) < self._scan_count:
            self._cursors[queue] = "-"
        else:
            self._cursors[queue] = "(" + pending[-1]["message_id"]
        if not pending:
            return 0

        # One pipelined round trip for every owner's heartbeat.
        owners = sorted({entry["consumer"] for entry in pending})
        pipe = self._client.pipeline(transaction=False)
        for owner in owners:
            pipe.exists(config.heartbeat_key(owner))
        alive = {owner for owner, ok in zip(owners, pipe.execute(), strict=True) if ok}

        reclaimed = 0
        for entry in pending:
            owner = entry["consumer"]
            entry_id = entry["message_id"]
            idle_ms = int(entry["time_since_delivered"])

            if owner not in alive:
                reason = "worker_death"
                claim_idle_ms = self._min_idle_ms
                error = f"worker {owner} presumed dead (heartbeat expired)"
            else:
                # Alive worker: only reclaim if the job has outrun its
                # timeout.  PEL idle time is "time since delivery", which for
                # a running job is exactly its run time so far.
                timeout_s = self._job_timeout(stream, entry_id)
                if not timeout_s or idle_ms < timeout_s * 1000:
                    continue
                reason = "timeout"
                claim_idle_ms = int(timeout_s * 1000)
                error = f"job exceeded timeout of {timeout_s:g}s on worker {owner}"

            if self._reclaim_entry(queue, entry_id, claim_idle_ms, error, reason):
                reclaimed += 1
        return reclaimed

    def _job_timeout(self, stream: str, entry_id: str) -> float | None:
        """Look up the timeout_s of the job an entry points at."""
        entries = self._client.xrange(stream, min=entry_id, max=entry_id, count=1)
        if not entries:
            return None
        job_id = entries[0][1].get("job_id")
        if not job_id:
            return None
        raw = self._client.hget(config.job_key(job_id), "timeout_s")
        try:
            return float(raw) if raw else None
        except ValueError:
            return None

    def _reclaim_entry(
        self, queue: str, entry_id: str, min_idle_ms: int, error: str, reason: str
    ) -> bool:
        """Claim one exact entry and fail its job.  True if we reclaimed it.

        Why XCLAIM on the exact id, not XAUTOCLAIM?
        XAUTOCLAIM(start=entry_id, count=1) claims the first idle entry *at
        or after* entry_id, whoever owns it.  If entry_id was already
        claimed by another monitor (its idle time just reset) or acked, it
        silently claims the *next* idle entry instead — which can belong to
        a live worker partway through a long job.  That job would then run
        twice.  XCLAIM with MIN-IDLE-TIME on a single id either claims
        exactly that entry, still idle, or returns nothing.  The idle check
        is re-evaluated atomically inside Redis, which is what makes
        concurrent monitors safe: whichever claims first resets the idle
        time, so the other's XCLAIM finds it "not idle enough" and gets [].
        """
        stream = config.stream_key(queue)
        claimed = self._client.xclaim(
            stream,
            self._group_name,
            self._consumer_name,
            min_idle_time=min_idle_ms,
            message_ids=[entry_id],
        )
        if not claimed:
            return False
        claimed_id, fields = claimed[0]
        job_id = (fields or {}).get("job_id")

        if not job_id:
            logger.warning("Reclaimed entry %s has no job_id; acking.", claimed_id)
            self._client.xack(stream, self._group_name, claimed_id)
            JOBS_SKIPPED.labels(queue=queue, reason="malformed").inc()
            return False

        raw = self._client.hgetall(config.job_key(job_id))
        if not raw:
            logger.warning("Job hash %s missing during reclaim; acking.", job_id)
            self._client.xack(stream, self._group_name, claimed_id)
            JOBS_SKIPPED.labels(queue=queue, reason="missing").inc()
            return False

        try:
            job = Job.from_redis_hash(raw)
        except (KeyError, ValueError, TypeError) as exc:
            dead_letter_unreadable(
                self._client,
                job_id=job_id,
                queue=queue,
                entry_id=claimed_id,
                group=self._group_name,
                consumer=self._consumer_name,
                error=f"corrupt job record: {format_error(exc)}",
            )
            return False

        if job.status in TERMINAL_STATUSES:
            # A stale duplicate entry for a job that already finished.
            # Failing it would drag a COMPLETED job back to PENDING.
            self._client.xack(stream, self._group_name, claimed_id)
            JOBS_SKIPPED.labels(queue=queue, reason="terminal").inc()
            return False

        outcome = fail_job(
            self._client,
            job,
            entry_id=claimed_id,
            group=self._group_name,
            consumer=self._consumer_name,
            error=error,
            trigger=reason,
        )
        if outcome is FailOutcome.LEASE_LOST:
            return False
        JOBS_RECLAIMED.labels(queue=queue, reason=reason).inc()
        logger.info(
            "Reclaimed job %s (%s): %s -> %s", job.id, reason, error, outcome.value
        )
        return True

    # ------------------------------------------------------------------
    # 2. Consumer garbage collection (+ live-worker discovery)
    # ------------------------------------------------------------------

    def collect_consumers(self, queue: str) -> set[str]:
        """Delete long-idle dead consumers; return the live ones.

        Every worker start gets a fresh random consumer name, so without
        this the group's consumer list grows by one ghost per restart.
        """
        stream = config.stream_key(queue)
        consumers, alive = live_consumers(self._client, queue, self._group_name)
        for c in consumers:
            name = c["name"]
            if name in alive or int(c["pending"]) > 0:
                continue
            if int(c["idle"]) < self._consumer_gc_idle_ms:
                continue
            gc_keys = [stream, config.heartbeat_key(name)]
            if int(_GC_CONSUMER(self._client, gc_keys, [self._group_name, name])):
                logger.info("Removed idle dead consumer %s from %s", name, stream)
        return alive

    # ------------------------------------------------------------------
    # 3. Stream trimming
    # ------------------------------------------------------------------

    def trim(self, queue: str) -> int:
        """Drop stream entries that every consumer group is done with.

        XACK does not delete anything from a stream, so without trimming
        the stream holds every job ever enqueued, forever.

        The safe cut point (XTRIM MINID keeps ids >= the cut) is the older of:
          * just past the group's last-delivered-id — entries up to and
            including it have been delivered; everything after has not;
          * the oldest pending entry — delivered but not acked, and the
            monitor may still need to XCLAIM it.
        Every entry older than the cut has been delivered and acknowledged.

        ``approximate=True`` (``MINID ~``) lets Redis drop only whole
        radix-tree nodes, which is much cheaper; it may keep a few entries
        older than the cut, never fewer — the safe direction.
        """
        stream = config.stream_key(queue)
        groups = self._client.xinfo_groups(stream)
        if not groups:
            return 0
        delivered = min(_parse_id(g["last-delivered-id"]) for g in groups)
        if delivered == (0, 0):
            return 0  # nothing has been delivered yet
        # The smallest id strictly greater than last-delivered-id.
        cut = (delivered[0], delivered[1] + 1)
        for g in groups:
            summary = self._client.xpending(stream, g["name"])
            if summary["pending"] and summary["min"]:
                cut = min(cut, _parse_id(summary["min"]))
        return int(
            self._client.xtrim(
                stream, minid=f"{cut[0]}-{cut[1]}", approximate=self._approximate_trim
            )
        )

    # ------------------------------------------------------------------
    # 4. Gauges
    # ------------------------------------------------------------------

    def _update_queue_gauges(self, queue: str) -> None:
        stats = queue_stats(self._client, queue, self._group_name)
        QUEUE_BACKLOG.labels(queue=queue).set(stats.backlog)
        PENDING_ENTRIES.labels(queue=queue).set(stats.pending)
        STREAM_LENGTH.labels(queue=queue).set(stats.stream_length)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Loop calling tick() until the stop event is set.

        No heartbeat is needed: entries the monitor claims are failed and
        acked within the same tick.  If every monitor dies, orphaned jobs
        wait in the PEL until one restarts — delayed, not lost.
        """
        run_until_stopped(self.tick, self._stop_event, self._poll_interval_s, "monitor")
