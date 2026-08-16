"""
scheduler.py — Delayed-job re-injection for distqueue.

The scheduler is the bridge between the delayed ZSet (where worker.py parks
failed jobs that still have retries remaining) and the main stream (where
workers pick up jobs).  It polls the ZSet for jobs whose next_retry_at
timestamp has passed, and atomically moves them back onto the stream so a
worker can try them again.

Why a separate process instead of having workers do this inline?
  1. Separation of concerns: workers own "process one job," the scheduler
     owns "when should delayed jobs become eligible."  Mixing them would
     mean every worker independently polls the ZSet — wasteful and harder
     to reason about.
  2. Predictable load: one scheduler process issuing one ZRANGEBYSCORE per
     second is much gentler on Redis than N workers each doing the same.
  3. The scheduler can run as a single replica (it's stateless and
     idempotent) whereas workers need to scale horizontally.
"""

from __future__ import annotations

import threading
import time

import redis

from distqueue import config
from distqueue.metrics import DELAYED_JOBS, QUEUE_DEPTH

# ---------------------------------------------------------------------------
# Lua script for atomic move: delayed ZSet → main stream
# ---------------------------------------------------------------------------
#
# Why a Lua script instead of separate ZREM + XADD commands?
#
# Without atomicity, two scheduler instances (or two tick() calls in rapid
# succession) could both see the same job_id in the ZSet via ZRANGEBYSCORE,
# and both try to move it.  If ZREM and XADD were separate commands, the
# race would look like:
#
#   Scheduler A: ZRANGEBYSCORE → sees job_id "abc"
#   Scheduler B: ZRANGEBYSCORE → sees job_id "abc"
#   Scheduler A: ZREM "abc" → success (returns 1)
#   Scheduler A: XADD → job re-injected ✓
#   Scheduler B: ZREM "abc" → fails (returns 0, already removed)
#   Scheduler B: XADD → job re-injected A SECOND TIME ✗
#
# If Scheduler B doesn't check the ZREM return value, the job gets
# double-injected and processed twice.  The Lua script makes ZREM + XADD
# atomic (Redis executes Lua scripts single-threaded with no interleaving),
# and uses the ZREM return value as a gate: if ZREM returns 0, someone else
# already moved this job, so we skip the XADD entirely.
#
# The script also clears next_retry_at on the job hash (back to empty
# string, per our None-as-empty-string convention) because the job is no
# longer "waiting for retry" — it's back on the stream.  Leaving a stale
# next_retry_at in the hash would be misleading to anything reading it
# (dashboards, debugging, the monitor).
#
# KEYS[1] = jobs:delayed        (the delayed ZSet)
# KEYS[2] = jobs:stream:default (the main stream)
# KEYS[3] = job:{id}            (the specific job's hash)
# ARGV[1] = job_id              (the job ID to move)

_LUA_MOVE_DUE_JOB = """
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 1 then
    redis.call('XADD', KEYS[2], '*', 'job_id', ARGV[1])
    redis.call('HSET', KEYS[3], 'next_retry_at', '')
    return 1
end
return 0
"""


class Scheduler:
    """Polls the delayed ZSet and re-injects due jobs into the main stream.

    Parameters
    ----------
    client : redis.Redis
        A connected Redis client.
    poll_interval_s : float
        Seconds between tick() calls in the run() loop.
    batch_size : int
        Max number of due jobs to move per tick() call.
    stop_event : threading.Event | None
        When set, the run() loop exits cleanly.
    """

    def __init__(
        self,
        client: redis.Redis,
        poll_interval_s: float = config.SCHEDULER_POLL_INTERVAL_S,
        batch_size: int = config.SCHEDULER_BATCH_SIZE,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._client = client
        self._poll_interval_s = poll_interval_s
        self._batch_size = batch_size
        self._stop_event = stop_event or threading.Event()

        # Register the Lua script once at init time.
        #
        # Why register_script() instead of eval() on every call?
        #
        # client.eval(script_body, ...) sends the full script text over
        # the wire and forces Redis to parse it on every single invocation.
        # For a script called once per job per tick, that's a lot of
        # redundant bytes and parsing work.
        #
        # register_script() returns a Script object that, on first call,
        # sends the script body and receives back its SHA1 hash.  On all
        # subsequent calls it uses EVALSHA (just the 40-byte hash), so
        # Redis looks up the already-compiled script from its cache.  If
        # the script has been flushed from cache (SCRIPT FLUSH or server
        # restart), the Script object transparently falls back to re-sending
        # the body once — so it's self-healing with no extra code from us.
        self._move_due_job = client.register_script(_LUA_MOVE_DUE_JOB)

    def tick(self) -> int:
        """One polling pass: find due jobs and move them back to the stream.

        Returns the number of jobs actually moved.  A job counts as "moved"
        only if the Lua script's ZREM succeeded (returned 1) — if another
        scheduler instance already moved it, the script returns 0 and we
        don't count it.

        This method is deliberately separate from run() so tests can call
        it directly for deterministic, single-pass assertions without
        threads or timing.
        """
        now = time.time()

        # ZRANGEBYSCORE: get job IDs whose score (next_retry_at) is <= now.
        # The -inf lower bound means "any score at or below now."
        # LIMIT caps the batch so one tick() doesn't run unboundedly long
        # if thousands of jobs all became due at the same moment.
        #
        # Note: we use start=0, num=batch_size.  The 'start' parameter is
        # an offset (not a score), and we always want the first batch_size
        # results, so offset=0.
        candidates: list[str] = self._client.zrangebyscore(
            config.DELAYED_ZSET,
            min="-inf",
            max=now,
            start=0,
            num=self._batch_size,
        )

        if not candidates:
            # --- Update gauge metrics even when nothing to move ---
            QUEUE_DEPTH.labels(queue=config.QUEUE_NAME).set(
                self._client.xlen(config.QUEUE_NAME)
            )
            DELAYED_JOBS.labels(queue=config.QUEUE_NAME).set(
                self._client.zcard(config.DELAYED_ZSET)
            )
            return 0

        moved = 0
        for job_id in candidates:
            # Each call is atomic within Redis (Lua runs single-threaded).
            # The script returns 1 if it moved the job, 0 if someone else
            # already moved it (ZREM returned 0).
            hash_key = f"{config.JOB_HASH_KEY_PREFIX}{job_id}"
            result = self._move_due_job(
                keys=[config.DELAYED_ZSET, config.QUEUE_NAME, hash_key],
                args=[job_id],
            )
            moved += int(result)

        # --- Update gauge metrics once per tick ---
        # These are point-in-time snapshots of queue state.  We update
        # them here (after moving jobs) so the gauges reflect the
        # post-move reality, not the stale pre-move counts.
        QUEUE_DEPTH.labels(queue=config.QUEUE_NAME).set(
            self._client.xlen(config.QUEUE_NAME)
        )
        DELAYED_JOBS.labels(queue=config.QUEUE_NAME).set(
            self._client.zcard(config.DELAYED_ZSET)
        )

        return moved

    def run(self) -> None:
        """Loop calling tick() until the stop event is set.

        Between ticks, we sleep for poll_interval_s using Event.wait()
        so that stop_event.set() wakes us immediately instead of waiting
        out the full interval.

        No heartbeat thread is needed here.  The scheduler is not a
        "worker" in the consumer-group sense — it doesn't hold jobs in
        a PEL that would need reclaiming on death.  If the scheduler dies,
        delayed jobs simply sit in the ZSet a bit longer until it restarts.
        There's no data loss, just delayed retry.
        """
        while not self._stop_event.is_set():
            self.tick()
            self._stop_event.wait(self._poll_interval_s)
