"""
config.py — Central configuration and Redis key layout for distqueue.

All tuning knobs live here so they're easy to find, and every one of them
can be overridden with a ``DISTQUEUE_<NAME>`` environment variable.

Why environment variables (and not a config file)?
  - Every process in this system runs in a container.  Env vars are the
    container-native way to configure a process (twelve-factor style), and
    docker-compose / CI can override them per service without baking a
    new image.
  - The chaos tests and CI want *much* shorter heartbeat TTLs and poll
    intervals than production does.  Without env overrides they'd have to
    monkeypatch module constants inside a running container, which is
    impossible.

Values are read once, at import time.  That's deliberate: a process that
changed its heartbeat TTL halfway through its life would break the
"TTL ≈ 3× interval" invariant the monitor relies on.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import TypeVar

_T = TypeVar("_T")


def _env(name: str, default: _T, cast: Callable[[str], _T]) -> _T:
    """Read ``DISTQUEUE_<name>`` from the environment, falling back to default.

    A malformed value raises immediately at import time rather than being
    silently ignored — a typo in HEARTBEAT_TTL_S that quietly fell back to
    the default would be a miserable thing to debug in production.
    """
    raw = os.environ.get(f"DISTQUEUE_{name}")
    if raw is None or raw == "":
        return default
    return cast(raw)


# ---------------------------------------------------------------------------
# Queues
# ---------------------------------------------------------------------------

# Logical name of the queue used when the caller doesn't pick one.  A queue
# name is a short identifier ("default", "emails"); the Redis *key* that
# backs it is derived by stream_key() below.  Keeping names and keys
# separate means metric labels and CLI arguments stay short and readable.
DEFAULT_QUEUE: str = _env("DEFAULT_QUEUE", "default", str)

# Consumer group name.  All workers on a queue join this group so Redis
# distributes entries across them: each entry is delivered to one consumer
# at a time.  (Note "at a time" — a crashed consumer's entries get
# re-delivered, which is why the system is at-least-once, not exactly-once.)
CONSUMER_GROUP: str = _env("CONSUMER_GROUP", "workers", str)

# ---------------------------------------------------------------------------
# Redis key layout
# ---------------------------------------------------------------------------
#
# | Key                     | Type   | Purpose                                 |
# |-------------------------|--------|-----------------------------------------|
# | jobs:stream:{queue}     | Stream | per-queue work stream (group "workers") |
# | job:{id}                | Hash   | authoritative job state                 |
# | jobs:delayed            | ZSet   | retries + scheduled jobs, score = due   |
# | jobs:dlq                | Stream | permanently failed jobs                 |
# | jobs:queues             | Set    | registry of every queue name ever used  |
# | worker:{id}:heartbeat   | String | liveness key with TTL                   |
# | idem:{key}              | String | idempotency key -> job_id, with TTL     |

STREAM_KEY_PREFIX: str = "jobs:stream:"
JOB_HASH_KEY_PREFIX: str = "job:"
DELAYED_ZSET: str = "jobs:delayed"
DLQ_STREAM: str = "jobs:dlq"

# The monitor and the stats CLI need to know which queues exist, but Redis
# has no cheap "list streams matching a pattern" command (SCAN over the
# whole keyspace is O(N) in *all* keys).  A tiny registry set, written on
# every enqueue with SADD (idempotent, O(1)), answers the question directly.
QUEUES_SET: str = "jobs:queues"

WORKER_HEARTBEAT_KEY_PREFIX: str = "worker:"
WORKER_HEARTBEAT_KEY_SUFFIX: str = ":heartbeat"
IDEMPOTENCY_KEY_PREFIX: str = "idem:"


def stream_key(queue: str) -> str:
    """Redis key of the stream backing a queue name."""
    return f"{STREAM_KEY_PREFIX}{queue}"


def job_key(job_id: str) -> str:
    """Redis key of a job's state hash."""
    return f"{JOB_HASH_KEY_PREFIX}{job_id}"


def heartbeat_key(consumer_name: str) -> str:
    """Redis key of a worker's liveness key."""
    return f"{WORKER_HEARTBEAT_KEY_PREFIX}{consumer_name}{WORKER_HEARTBEAT_KEY_SUFFIX}"


def idempotency_key(key: str) -> str:
    """Redis key that maps a caller-supplied idempotency key to a job id."""
    return f"{IDEMPOTENCY_KEY_PREFIX}{key}"


# Kept for backward compatibility with code written against the original
# single-queue API: the stream key of the default queue.
QUEUE_NAME: str = stream_key(DEFAULT_QUEUE)

# ---------------------------------------------------------------------------
# Retry / backoff
# ---------------------------------------------------------------------------

# Base delay in seconds for exponential backoff.  See backoff.py for the
# full formula and why it uses "equal jitter".
#
# Why 2 seconds?  Small enough that transient errors recover fast on the
# first retry, but large enough to avoid a tight retry loop if the first
# attempt fails instantly.
BASE_BACKOFF_S: float = _env("BASE_BACKOFF_S", 2.0, float)

# Hard ceiling on the exponential term.  Without a cap, a job on its 10th
# attempt would wait 2 * 2^10 = 2048 s ≈ 34 minutes.  5 minutes is long
# enough to let a downstream service recover, short enough that the job
# doesn't look "stuck."
MAX_BACKOFF_S: float = _env("MAX_BACKOFF_S", 300.0, float)

# Default number of times a job will be attempted before it's moved to the
# DLQ.  Individual jobs can override this at enqueue time.
DEFAULT_MAX_ATTEMPTS: int = _env("DEFAULT_MAX_ATTEMPTS", 5, int)

# ---------------------------------------------------------------------------
# Job execution timeout
# ---------------------------------------------------------------------------

# How long a job may run before the monitor treats it as failed, even if
# its worker is still heartbeating.
#
# Why is a heartbeat not enough?  A heartbeat proves the *process* is
# alive, not that the *job* is making progress.  A handler stuck on a
# socket read with no timeout keeps its worker's heartbeat thread happy
# forever, and without this limit the job would sit in the PEL forever.
# This is the same idea as SQS's "visibility timeout".  Per-job override
# at enqueue time; 0 disables the limit for that job.
DEFAULT_JOB_TIMEOUT_S: float = _env("DEFAULT_JOB_TIMEOUT_S", 300.0, float)

# ---------------------------------------------------------------------------
# Heartbeat / worker liveness
# ---------------------------------------------------------------------------

# How often (in seconds) a worker refreshes its heartbeat key in Redis.
HEARTBEAT_INTERVAL_S: float = _env("HEARTBEAT_INTERVAL_S", 5.0, float)

# TTL on the heartbeat key.  If a worker doesn't refresh within this window,
# the monitor considers it dead.  Set to ~3× the interval so a single missed
# heartbeat doesn't trigger a false positive (network blip, GC pause, etc.).
HEARTBEAT_TTL_S: int = _env("HEARTBEAT_TTL_S", 15, int)

# ---------------------------------------------------------------------------
# Scheduler (delayed-job re-injection)
# ---------------------------------------------------------------------------

# How often (in seconds) the scheduler polls the delayed ZSet.  1 second
# keeps retried jobs from sitting idle noticeably longer than their
# computed backoff without hammering Redis on a mostly-empty set.
SCHEDULER_POLL_INTERVAL_S: float = _env("SCHEDULER_POLL_INTERVAL_S", 1.0, float)

# Maximum number of due jobs to move per poll iteration.  Without a cap, a
# sudden avalanche of due jobs (e.g. after an outage where backoffs all
# converge) could make one tick() unboundedly long.  Remaining due jobs are
# picked up on the next tick, one interval later.
SCHEDULER_BATCH_SIZE: int = _env("SCHEDULER_BATCH_SIZE", 100, int)

# ---------------------------------------------------------------------------
# Monitor (dead-worker detection, timeouts, housekeeping)
# ---------------------------------------------------------------------------

# How often (in seconds) the monitor scans the PEL.  A dead worker's jobs
# get reclaimed within roughly HEARTBEAT_TTL_S + MONITOR_POLL_INTERVAL_S.
MONITOR_POLL_INTERVAL_S: float = _env("MONITOR_POLL_INTERVAL_S", 5.0, float)

# Minimum idle time (ms) a PEL entry must have before the monitor even
# considers it.  This is a coarse pre-filter; the actual reclaim decision
# is gated on the heartbeat check (or the job's timeout).
#
# Deliberately BELOW HEARTBEAT_TTL_S: a dead worker's heartbeat expires at
# ~15s, and because entries idle > 10s are already being *checked*, the
# monitor can act on the first tick after expiry instead of waiting for
# idle > 15s *and then* another poll.  An entry whose worker still has a
# valid heartbeat is skipped regardless of idle time.
MONITOR_MIN_IDLE_MS: int = _env("MONITOR_MIN_IDLE_MS", 10_000, int)

# Max PEL entries examined per tick.  The monitor walks the PEL with a
# cursor (see monitor.py), so a large PEL is covered across several ticks
# instead of the first page being re-examined forever.
MONITOR_SCAN_COUNT: int = _env("MONITOR_SCAN_COUNT", 100, int)

# Consumers that have no pending entries, no heartbeat, and have been idle
# this long are deleted from the consumer group.  Every worker restart gets
# a fresh random consumer name, so without this the group's consumer list
# (XINFO CONSUMERS) grows forever with ghosts of dead containers.
CONSUMER_GC_IDLE_MS: int = _env("CONSUMER_GC_IDLE_MS", 3_600_000, int)  # 1 hour

# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------

# Completed job hashes expire after this many seconds (0 = keep forever).
# Without a TTL every job ever run stays in Redis memory forever — fine for
# a demo, a slow-motion OOM in production.  A day is long enough to debug
# "what happened to job X this morning".
COMPLETED_JOB_TTL_S: int = _env("COMPLETED_JOB_TTL_S", 86_400, int)

# Dead jobs are kept longer than completed ones because a human may need
# to inspect and replay them.  0 = keep forever.
DEAD_JOB_TTL_S: int = _env("DEAD_JOB_TTL_S", 7 * 86_400, int)

# Idempotency keys expire after this long.  The window answers "how late
# can a duplicate enqueue arrive and still be recognised as a duplicate?"
IDEMPOTENCY_TTL_S: int = _env("IDEMPOTENCY_TTL_S", 86_400, int)

# ---------------------------------------------------------------------------
# Metrics (Prometheus)
# ---------------------------------------------------------------------------

# The port each process exposes its /metrics endpoint on.  The same number
# for every role is safe because each runs in its own container with its
# own network namespace; Prometheus discovers replicas via Docker DNS.
METRICS_PORT: int = _env("METRICS_PORT", 9100, int)
