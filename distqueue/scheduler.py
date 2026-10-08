"""
scheduler.py — Delayed-job re-injection for distqueue.

The scheduler is the bridge between the delayed ZSet (where failed jobs
wait out their backoff, and where scheduled jobs wait for their start
time) and the per-queue streams workers consume.  Each tick it finds jobs
whose due time has passed and moves each one back onto *its own* queue's
stream with the MOVE_DUE transition (see transitions.py).

Why a separate process instead of having workers do this inline?
  1. Separation of concerns: workers own "process one job," the scheduler
     owns "when should delayed jobs become eligible."
  2. Predictable load: one ZRANGEBYSCORE per second, instead of N workers
     each polling the same set.

High availability without leader election
------------------------------------------
Run as many scheduler replicas as you like.  MOVE_DUE is gated on ZREM's
return value, so if two replicas both see the same due job, exactly one
moves it and the other's script returns 0.  Correctness never depends on
there being a single scheduler, so there is no leader to elect and no
failover delay when one dies — the others simply keep ticking.  The cost
of running two is a second, mostly-redundant ZRANGEBYSCORE per second.
"""

from __future__ import annotations

import logging
import threading

import redis

from distqueue import config
from distqueue.metrics import SCHEDULER_MOVED, SCHEDULER_ORPHANS
from distqueue.runloop import run_until_stopped
from distqueue.transitions import move_due_job

logger = logging.getLogger(__name__)


class Scheduler:
    """Polls the delayed ZSet and re-injects due jobs into their streams.

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

    def _redis_now(self) -> float:
        """Current time on Redis's clock.

        Due times are written by the FAIL / ENQUEUE scripts using Redis's
        TIME, so "is it due yet?" must be answered on the same clock.  With
        the scheduler's local clock, a scheduler running 30 s fast would
        re-inject every retry 30 s early.  One TIME call per tick is cheap.
        """
        seconds, micros = self._client.time()
        return seconds + micros / 1_000_000

    def tick(self) -> int:
        """One polling pass: move due jobs back onto their streams.

        Returns the number of jobs actually moved by *this* scheduler.

        Round trips per tick, regardless of batch size:
          1. TIME                         (Redis clock)
          2. ZRANGEBYSCORE ... LIMIT      (which jobs are due)
          3. pipelined HGET queue × N     (where each one goes)
          4. pipelined EVALSHA × N        (the atomic moves)
        The previous version did one round trip per job; at 100 due jobs on
        a remote Redis that was 100 RTTs per tick.

        Separate from run() so tests can call it directly for deterministic,
        single-pass assertions.
        """
        candidates: list[str] = self._client.zrangebyscore(
            config.DELAYED_ZSET,
            min="-inf",
            max=self._redis_now(),
            start=0,
            num=self._batch_size,
        )
        if not candidates:
            return 0

        # The queue is immutable after enqueue, so reading it outside the
        # atomic move can't race with anything.  It must be read at all
        # because the delayed set is shared by every queue.
        pipe = self._client.pipeline(transaction=False)
        for job_id in candidates:
            pipe.hget(config.job_key(job_id), "queue")
        queues: list[str | None] = pipe.execute()

        pipe = self._client.pipeline(transaction=False)
        for job_id, queue in zip(candidates, queues, strict=True):
            # A missing hash returns None; the script will notice the hash
            # is gone and drop the orphan, so the stream key is irrelevant.
            move_due_job(pipe, job_id=job_id, queue=queue or config.DEFAULT_QUEUE)
        results = pipe.execute()

        moved = 0
        for job_id, queue, result in zip(candidates, queues, results, strict=True):
            outcome = int(result)
            if outcome == 1:
                moved += 1
                SCHEDULER_MOVED.labels(queue=queue or config.DEFAULT_QUEUE).inc()
            elif outcome == -1:
                SCHEDULER_ORPHANS.inc()
                logger.warning(
                    "Dropped delayed job %s: its hash no longer exists.", job_id
                )
        return moved

    def run(self) -> None:
        """Loop calling tick() until the stop event is set.

        No heartbeat is needed: the scheduler holds no PEL entries that
        would need reclaiming if it died.  If every replica dies, delayed
        jobs simply wait in the ZSet until one comes back — late, not lost.
        """
        run_until_stopped(
            self.tick, self._stop_event, self._poll_interval_s, "scheduler"
        )
