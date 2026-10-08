"""
transitions.py — Every job state transition, each one an atomic Lua script.

This module is the job state machine.  Producer, worker, scheduler, monitor
and CLI never write a job hash directly; they call one of the functions
below, and each function runs exactly one Lua script.

Why Lua for every transition (and not MULTI/EXEC pipelines)?
------------------------------------------------------------
MULTI/EXEC gives *isolation* (no other client's commands interleave) but it
cannot make a *decision*: every queued command runs unconditionally.  Most
transitions here need "check, then write":

  * enqueue:   "if this idempotency key is new, create the job"
  * complete:  "if I still own this job, mark it COMPLETED"
  * move_due:  "if I was the one who removed it from the delayed set,
                inject it" (the ZREM gate that stops double-injection)

The only way to do check-then-write in a pipeline is WATCH + optimistic
retry, which turns every contended transition into a retry loop.  A Lua
script runs atomically inside Redis — nothing else executes while it does —
so the check and the write cannot be separated.

Fencing: why worker writes are checked against the PEL
------------------------------------------------------
A worker holds a job from the moment XREADGROUP delivers its stream entry
until it XACKs it.  Redis already records that hold in the consumer group's
Pending Entries List (PEL): entry id → owning consumer.  The monitor
reclaims a job by XCLAIM-ing the entry, which changes the owner.

So the PEL *is* a lease table, and ownership of the entry is the fencing
token.  Every worker-side transition (START, COMPLETE, FAIL) first checks
``XPENDING stream group <id> <id> 1 <consumer>`` and refuses to write if the
caller no longer owns the entry.  The scenario this prevents:

  1. Worker W starts job J, then stalls (GC pause, ``docker pause``, a
     network partition) for longer than its heartbeat TTL.
  2. The monitor sees no heartbeat, XCLAIMs J's entry, counts a failed
     attempt (attempts 0 → 1) and schedules a retry.
  3. W wakes up and finishes.  Without fencing it would write
     ``status=COMPLETED, attempts=0`` from its stale in-memory copy —
     marking a job COMPLETED that is about to run again, and resetting the
     attempt counter so a poison job could dodge the DLQ forever.

With fencing, W's write in step 3 is rejected (``LEASE_LOST`` metric), and
the state the monitor wrote stands.  This is the "fencing token" argument
from Martin Kleppmann's "How to do distributed locking" (2016): a lease
alone can't stop a paused process from acting on a lease it no longer
holds; the *storage* has to reject the stale write.

What fencing can NOT do: stop W's handler from having already performed
its side effects (sent the email, charged the card).  That is why the
system is at-least-once and handlers must be idempotent.

Clocks
------
Timestamps that are *compared across processes* (retry due-times, queue
wait, end-to-end latency) are taken from Redis's clock (``TIME``) inside
the scripts, not from each caller's local clock.  With many hosts, clock
skew between a worker (which computes a due-time) and the scheduler (which
compares it to "now") would otherwise shift every retry by the skew.

Redis Cluster note
------------------
These scripts touch several keys (stream, job hash, delayed set, DLQ) that
would hash to different cluster slots.  That's fine on a single Redis or
with replicas/Sentinel; running on Redis Cluster would require a common
hash tag in every key name (e.g. ``{dq}:job:<id>``).  Documented, not done.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import redis
from redis.commands.core import Script

from distqueue import config
from distqueue.backoff import compute_backoff
from distqueue.job import Job
from distqueue.metrics import JOBS_FAILED, LEASE_LOST

logger = logging.getLogger(__name__)

# Longest error string stored in a hash or DLQ entry.  A handler that
# raises with a 5 MB message (it happens: someone puts a whole response
# body in the exception) shouldn't be able to bloat Redis memory.
MAX_ERROR_LEN = 2000


# ---------------------------------------------------------------------------
# Shared Lua snippets
# ---------------------------------------------------------------------------

# Redis server time as a float epoch.  TIME returns {seconds, microseconds}.
_LUA_NOW = """
local function now()
    local t = redis.call('TIME')
    return tonumber(t[1]) + tonumber(t[2]) / 1000000
end
"""

# True iff `entry_id` is pending in `group` AND owned by `consumer`.  The
# consumer argument filters XPENDING server-side, so the reply is either
# exactly that entry or empty.
_LUA_OWNS = """
local function owns(stream, group, entry_id, consumer)
    local p = redis.call('XPENDING', stream, group, entry_id, entry_id, 1, consumer)
    return #p == 1
end
"""


# ---------------------------------------------------------------------------
# Script registry
# ---------------------------------------------------------------------------


class LuaScript:
    """A Lua script loaded lazily and invoked via EVALSHA.

    Why EVALSHA (via redis-py's Script) instead of EVAL?  EVAL sends the
    whole script body on every call and makes Redis re-parse it.  Script
    computes the SHA1 locally, calls EVALSHA (40 bytes on the wire), and
    on NOSCRIPT (first call, or after a Redis restart / SCRIPT FLUSH) loads
    the body once and retries — self-healing with no code of ours.

    Passing ``client`` per call (instead of binding one client forever)
    lets the same script run on a plain client or queue into a pipeline.
    """

    def __init__(self, name: str, source: str) -> None:
        self.name = name
        self.source = source
        self._script: Script | None = None
        self._lock = threading.Lock()

    def __call__(self, client: redis.Redis, keys: list[str], args: list[Any]) -> Any:
        if self._script is None:
            with self._lock:
                if self._script is None:
                    self._script = client.register_script(self.source)
        return self._script(keys=keys, args=args, client=client)


# ---------------------------------------------------------------------------
# ENQUEUE
# ---------------------------------------------------------------------------
# KEYS: 1 job hash, 2 stream, 3 delayed zset, 4 queues set, [5 idempotency key]
# ARGV: 1 job_id, 2 queue, 3 delay_s or '', 4 run_at epoch or '',
#       5 idempotency ttl, 6.. hash field/value pairs
# Returns {1, job_id} when created, {0, existing_job_id} on a duplicate.
_ENQUEUE = LuaScript(
    "enqueue",
    _LUA_NOW
    + """
if #KEYS >= 5 then
    local existing = redis.call('GET', KEYS[5])
    if existing then
        return {0, existing}
    end
    redis.call('SET', KEYS[5], ARGV[1], 'EX', ARGV[5])
end

redis.call('HSET', KEYS[1], unpack(ARGV, 6))
-- Overwrite the caller's timestamps with Redis's clock: end-to-end latency
-- is later computed inside COMPLETE as (Redis now - created_at), and
-- subtracting two different machines' clocks would measure their skew.
local t = now()
redis.call('HSET', KEYS[1], 'created_at', tostring(t), 'updated_at', tostring(t))
redis.call('SADD', KEYS[4], ARGV[2])

local due = nil
if ARGV[3] ~= '' then due = t + tonumber(ARGV[3]) end
if ARGV[4] ~= '' then due = tonumber(ARGV[4]) end

if due then
    redis.call('HSET', KEYS[1], 'next_retry_at', tostring(due))
    redis.call('ZADD', KEYS[3], due, ARGV[1])
else
    redis.call('XADD', KEYS[2], '*', 'job_id', ARGV[1])
end
return {1, ARGV[1]}
""",
)


@dataclass(frozen=True)
class EnqueueResult:
    job_id: str
    created: bool  # False when an idempotency key matched an existing job


def enqueue_job(
    client: redis.Redis,
    job: Job,
    *,
    delay_s: float | None = None,
    run_at: float | None = None,
    idempotency_key: str | None = None,
    idempotency_ttl_s: int = config.IDEMPOTENCY_TTL_S,
) -> EnqueueResult:
    """Atomically persist a new job and make it runnable (now or later)."""
    keys = [
        config.job_key(job.id),
        config.stream_key(job.queue),
        config.DELAYED_ZSET,
        config.QUEUES_SET,
    ]
    if idempotency_key is not None:
        keys.append(config.idempotency_key(idempotency_key))

    fields: list[str] = []
    for name, value in job.to_redis_hash().items():
        fields.extend((name, value))

    created, job_id = _ENQUEUE(
        client,
        keys,
        [
            job.id,
            job.queue,
            "" if delay_s is None else repr(float(delay_s)),
            "" if run_at is None else repr(float(run_at)),
            idempotency_ttl_s,
            *fields,
        ],
    )
    return EnqueueResult(job_id=job_id, created=bool(int(created)))


# ---------------------------------------------------------------------------
# START  (worker, right after XREADGROUP delivered an entry)
# ---------------------------------------------------------------------------
# KEYS: 1 job hash, 2 stream
# ARGV: 1 entry_id, 2 group, 3 consumer
# Returns one of:
#   {'ok', queue_wait_seconds, {flat hash}}   job is now RUNNING
#   {'missing'}                               hash gone; entry acked
#   {'terminal', status}                      already COMPLETED/DEAD; acked
#   {'lost'}                                  entry no longer ours
_START = LuaScript(
    "start",
    _LUA_NOW
    + _LUA_OWNS
    + """
local entry_id, group, consumer = ARGV[1], ARGV[2], ARGV[3]
if not owns(KEYS[2], group, entry_id, consumer) then
    return {'lost'}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('XACK', KEYS[2], group, entry_id)
    return {'missing'}
end
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'COMPLETED' or status == 'DEAD' then
    redis.call('XACK', KEYS[2], group, entry_id)
    return {'terminal', status}
end
local t = now()
redis.call('HSET', KEYS[1], 'status', 'RUNNING', 'last_worker', consumer,
           'updated_at', tostring(t))
-- Stream entry ids are '<ms since epoch>-<seq>' stamped by Redis's clock,
-- so the entry id itself records when the job became runnable.
local added_ms = tonumber(string.match(entry_id, '^(%d+)'))
local wait = t - added_ms / 1000
return {'ok', tostring(wait), redis.call('HGETALL', KEYS[1])}
""",
)


class StartStatus(StrEnum):
    OK = "ok"
    MISSING = "missing"
    TERMINAL = "terminal"
    LOST = "lost"


@dataclass(frozen=True)
class StartResult:
    status: StartStatus
    job_data: dict[str, str] | None = None
    queue_wait_s: float | None = None
    terminal_status: str | None = None


def start_job(
    client: redis.Redis,
    *,
    job_id: str,
    queue: str,
    entry_id: str,
    group: str,
    consumer: str,
) -> StartResult:
    """Mark a delivered job RUNNING, unless it shouldn't run at all.

    Folding "is the hash missing / is the job already terminal" into the
    same atomic step as "mark RUNNING" means a duplicate delivery (two
    stream entries for one job) can never run a COMPLETED job a second
    time: whichever delivery comes second sees the terminal status.
    """
    reply = _START(
        client,
        [config.job_key(job_id), config.stream_key(queue)],
        [entry_id, group, consumer],
    )
    status = StartStatus(reply[0])
    if status is StartStatus.OK:
        flat = reply[2]
        data = dict(zip(flat[::2], flat[1::2], strict=True))
        return StartResult(status, job_data=data, queue_wait_s=float(reply[1]))
    if status is StartStatus.TERMINAL:
        return StartResult(status, terminal_status=reply[1])
    return StartResult(status)


# ---------------------------------------------------------------------------
# COMPLETE  (worker, handler returned normally)
# ---------------------------------------------------------------------------
# KEYS: 1 job hash, 2 stream
# ARGV: 1 entry_id, 2 group, 3 consumer, 4 completed-job ttl seconds
# Returns {1, end_to_end_seconds or ''} on success, {0} if the lease was lost.
_COMPLETE = LuaScript(
    "complete",
    _LUA_NOW
    + _LUA_OWNS
    + """
local entry_id, group, consumer = ARGV[1], ARGV[2], ARGV[3]
if not owns(KEYS[2], group, entry_id, consumer) then
    return {0}
end
redis.call('XACK', KEYS[2], group, entry_id)
if redis.call('EXISTS', KEYS[1]) == 0 then
    return {1, ''}
end
local t = now()
redis.call('HSET', KEYS[1], 'status', 'COMPLETED', 'updated_at', tostring(t),
           'next_retry_at', '')
local ttl = tonumber(ARGV[4])
if ttl > 0 then
    redis.call('EXPIRE', KEYS[1], ttl)
end
local created = tonumber(redis.call('HGET', KEYS[1], 'created_at'))
if created then
    return {1, tostring(t - created)}
end
return {1, ''}
""",
)


@dataclass(frozen=True)
class CompleteResult:
    applied: bool
    end_to_end_s: float | None = None


def complete_job(
    client: redis.Redis,
    *,
    job_id: str,
    queue: str,
    entry_id: str,
    group: str,
    consumer: str,
    ttl_s: int = config.COMPLETED_JOB_TTL_S,
) -> CompleteResult:
    """Mark a job COMPLETED and XACK it — only if ``consumer`` still owns it."""
    reply = _COMPLETE(
        client,
        [config.job_key(job_id), config.stream_key(queue)],
        [entry_id, group, consumer, ttl_s],
    )
    if not int(reply[0]):
        LEASE_LOST.labels(queue=queue, transition="complete").inc()
        logger.warning(
            "Lease lost: %s no longer owns entry %s of job %s; its COMPLETED "
            "write was rejected (the monitor reclaimed the job).",
            consumer,
            entry_id,
            job_id,
        )
        return CompleteResult(applied=False)
    e2e = float(reply[1]) if reply[1] else None
    return CompleteResult(applied=True, end_to_end_s=e2e)


# ---------------------------------------------------------------------------
# FAIL  (worker on handler exception; monitor on reclaim; corrupt records)
# ---------------------------------------------------------------------------
# KEYS: 1 job hash, 2 stream, 3 delayed zset, 4 dlq stream
# ARGV: 1 entry_id, 2 group, 3 consumer, 4 expected attempts or '',
#       5 new attempts or '', 6 outcome ('retry' | 'dead'), 7 delay seconds,
#       8 error, 9 job_id, 10 queue, 11 dead-job ttl, 12 trigger
# Returns 1 applied, 0 lease lost, -1 attempts changed underneath us.
_FAIL = LuaScript(
    "fail",
    _LUA_NOW
    + _LUA_OWNS
    + """
local entry_id, group, consumer = ARGV[1], ARGV[2], ARGV[3]
if not owns(KEYS[2], group, entry_id, consumer) then
    return 0
end
local exists = redis.call('EXISTS', KEYS[1]) == 1

-- Optimistic version check.  The caller decided retry-vs-DLQ from the
-- attempts value it read; if attempts changed since, that decision is
-- stale.  PEL ownership already makes this nearly impossible; the check
-- is a second, independent guard on the counter that bounds retries.
if exists and ARGV[4] ~= '' then
    if redis.call('HGET', KEYS[1], 'attempts') ~= ARGV[4] then
        return -1
    end
end

local t = now()
local outcome = ARGV[6]
if not exists then
    outcome = 'dead'
end

if outcome == 'retry' then
    local due = t + tonumber(ARGV[7])
    redis.call('HSET', KEYS[1], 'status', 'PENDING', 'attempts', ARGV[5],
               'last_error', ARGV[8], 'updated_at', tostring(t),
               'next_retry_at', tostring(due))
    redis.call('ZADD', KEYS[3], due, ARGV[9])
else
    if exists then
        redis.call('HSET', KEYS[1], 'status', 'DEAD', 'last_error', ARGV[8],
                   'updated_at', tostring(t), 'next_retry_at', '')
        if ARGV[5] ~= '' then
            redis.call('HSET', KEYS[1], 'attempts', ARGV[5])
        end
        local ttl = tonumber(ARGV[11])
        if ttl > 0 then
            redis.call('EXPIRE', KEYS[1], ttl)
        end
    end
    redis.call('XADD', KEYS[4], '*', 'job_id', ARGV[9], 'queue', ARGV[10],
               'reason', ARGV[8], 'attempts', ARGV[5], 'trigger', ARGV[12],
               'failed_at', tostring(t))
end
redis.call('XACK', KEYS[2], group, entry_id)
return 1
""",
)


class FailOutcome(StrEnum):
    RETRIED = "retried"
    DEAD = "dlq"
    LEASE_LOST = "lease_lost"


def format_error(exc: BaseException) -> str:
    """'ExceptionType: message', truncated.

    ``str(exc)`` alone loses the type: ``str(KeyError('user_id'))`` is just
    ``'user_id'``, which tells an operator reading the DLQ nothing.
    """
    text = f"{type(exc).__name__}: {exc}"
    return text if len(text) <= MAX_ERROR_LEN else text[: MAX_ERROR_LEN - 3] + "..."


def fail_job(
    client: redis.Redis,
    job: Job,
    *,
    entry_id: str,
    group: str,
    consumer: str,
    error: str,
    trigger: str,
    permanent: bool = False,
    dead_ttl_s: int = config.DEAD_JOB_TTL_S,
) -> FailOutcome:
    """Apply the retry-or-DLQ transition for one failed attempt.

    The single implementation of the failure fork, shared by the worker
    (handler raised) and the monitor (worker died / job timed out).  Every
    trigger counts as a used attempt — otherwise a poison job that kills
    its worker would be retried forever, since the worker never gets to
    record the failure itself.

    The decision (retry or dead, and the backoff delay) is made here in
    Python, where it's unit-testable; the Lua script only *guards* it
    (ownership + attempts version) and applies it atomically.
    """
    new_attempts = job.attempts + 1
    error = error if len(error) <= MAX_ERROR_LEN else error[: MAX_ERROR_LEN - 3] + "..."
    dead = permanent or new_attempts >= job.max_attempts
    delay = 0.0 if dead else compute_backoff(new_attempts)

    result = int(
        _FAIL(
            client,
            [
                config.job_key(job.id),
                config.stream_key(job.queue),
                config.DELAYED_ZSET,
                config.DLQ_STREAM,
            ],
            [
                entry_id,
                group,
                consumer,
                str(job.attempts),
                str(new_attempts),
                "dead" if dead else "retry",
                repr(delay),
                error,
                job.id,
                job.queue,
                dead_ttl_s,
                trigger,
            ],
        )
    )
    if result != 1:
        LEASE_LOST.labels(queue=job.queue, transition="fail").inc()
        logger.warning(
            "Lease lost: %s's failure write for job %s (entry %s) was rejected (%s).",
            consumer,
            job.id,
            entry_id,
            "entry no longer owned" if result == 0 else "attempts changed",
        )
        return FailOutcome.LEASE_LOST

    outcome = FailOutcome.DEAD if dead else FailOutcome.RETRIED
    JOBS_FAILED.labels(queue=job.queue, outcome=outcome.value, trigger=trigger).inc()
    return outcome


def dead_letter_unreadable(
    client: redis.Redis,
    *,
    job_id: str,
    queue: str,
    entry_id: str,
    group: str,
    consumer: str,
    error: str,
    dead_ttl_s: int = config.DEAD_JOB_TTL_S,
) -> bool:
    """Dead-letter a job whose hash can't be deserialized.

    Retrying can't fix a corrupt record, and crashing on it is worse: the
    entry would stay in the PEL, the monitor would reclaim it, hit the same
    error, crash, restart, and hit it again — a poison pill for the reaper
    itself.  Returns False if the lease was lost.
    """
    result = int(
        _FAIL(
            client,
            [
                config.job_key(job_id),
                config.stream_key(queue),
                config.DELAYED_ZSET,
                config.DLQ_STREAM,
            ],
            [entry_id, group, consumer, "", "", "dead", "0", error[:MAX_ERROR_LEN],
             job_id, queue, dead_ttl_s, "corrupt"],
        )
    )  # fmt: skip
    if result == 1:
        JOBS_FAILED.labels(queue=queue, outcome="dlq", trigger="corrupt").inc()
        return True
    LEASE_LOST.labels(queue=queue, transition="fail").inc()
    return False


# ---------------------------------------------------------------------------
# MOVE_DUE  (scheduler)
# ---------------------------------------------------------------------------
# KEYS: 1 delayed zset, 2 stream, 3 job hash
# ARGV: 1 job_id
# Returns 1 moved, 0 someone else moved it first, -1 orphan dropped.
#
# The ZREM return value is the gate: two schedulers can both *see* a due
# job, but only one ZREM can succeed, and only that one XADDs.  This is
# what makes running several scheduler replicas safe without any leader
# election — the operation is idempotent per job.
_MOVE_DUE = LuaScript(
    "move_due",
    """
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
    return 0
end
if redis.call('EXISTS', KEYS[3]) == 0 then
    -- The job hash is gone (expired or deleted).  Injecting a pointer to
    -- nothing would only make a worker ack an empty entry; and an HSET
    -- here would *create* a stub hash missing every required field,
    -- which crashes deserialization downstream.  Drop it.
    return -1
end
redis.call('XADD', KEYS[2], '*', 'job_id', ARGV[1])
redis.call('HSET', KEYS[3], 'next_retry_at', '')
return 1
""",
)


def move_due_job(
    client: redis.Redis | redis.client.Pipeline, *, job_id: str, queue: str
) -> Any:
    """Move one due job from the delayed set to its queue's stream.

    Accepts a pipeline so the scheduler can batch a whole tick's moves into
    one round trip; in that case the int result arrives from execute().
    """
    return _MOVE_DUE(
        client,
        [config.DELAYED_ZSET, config.stream_key(queue), config.job_key(job_id)],
        [job_id],
    )


# ---------------------------------------------------------------------------
# REPLAY  (operator, via CLI: DLQ → queue)
# ---------------------------------------------------------------------------
# KEYS: 1 dlq stream, 2 job hash, 3 stream
# ARGV: 1 dlq entry id, 2 job_id
# Returns 1 replayed, 0 job hash missing, -1 job is not DEAD.
_REPLAY = LuaScript(
    "replay",
    _LUA_NOW
    + """
local status = redis.call('HGET', KEYS[2], 'status')
if not status then
    return 0
end
if status ~= 'DEAD' then
    return -1
end
redis.call('HSET', KEYS[2], 'status', 'PENDING', 'attempts', '0',
           'last_error', '', 'next_retry_at', '', 'updated_at', tostring(now()))
-- Dead jobs carry a retention TTL; a replayed job is live again.
redis.call('PERSIST', KEYS[2])
redis.call('XADD', KEYS[3], '*', 'job_id', ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[1])
return 1
""",
)


class ReplayResult(StrEnum):
    REPLAYED = "replayed"
    MISSING = "missing"
    NOT_DEAD = "not_dead"


def replay_dead_job(
    client: redis.Redis, *, dlq_entry_id: str, job_id: str, queue: str
) -> ReplayResult:
    """Reset a DEAD job's attempts and put it back on its queue.

    Only DEAD jobs can be replayed: replaying a job that is still pending or
    running would create a second live stream entry for it, breaking the
    "one live delivery per job" invariant that fencing relies on.
    """
    result = int(
        _REPLAY(
            client,
            [config.DLQ_STREAM, config.job_key(job_id), config.stream_key(queue)],
            [dlq_entry_id, job_id],
        )
    )
    return {
        1: ReplayResult.REPLAYED,
        0: ReplayResult.MISSING,
        -1: ReplayResult.NOT_DEAD,
    }[result]


__all__ = [
    "CompleteResult",
    "EnqueueResult",
    "FailOutcome",
    "ReplayResult",
    "StartResult",
    "StartStatus",
    "complete_job",
    "dead_letter_unreadable",
    "enqueue_job",
    "fail_job",
    "format_error",
    "move_due_job",
    "replay_dead_job",
    "start_job",
]
