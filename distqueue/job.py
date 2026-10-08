"""
job.py — The Job dataclass: the fundamental unit of work in distqueue.

Design notes
------------
A Job is a plain dataclass rather than a Pydantic model because:
  1. We don't need validation beyond what we enforce ourselves — the Job is
     an internal data structure, not an API boundary.
  2. Fewer dependencies.  The only serialization target is a Redis hash
     (flat dict of strings), which is simple enough to handle manually.

Redis stores hash values as bytes (or strings if decode_responses=True).
Our to_redis_hash() and from_redis_hash() bridge between typed Python
fields and that flat string representation.  The payload dict is the only
non-scalar field, so it gets JSON-encoded; everything else is just str().
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from distqueue import config


class JobStatus(StrEnum):
    """Every status a job hash can hold.

    A StrEnum (rather than bare string literals) means a typo like
    "COMPLETE" is an AttributeError at import time instead of a silently
    unmatched comparison at runtime.  Because it *is* a str, the values
    serialise into Redis and compare against Lua script results unchanged.

    There is no CLAIMED status: the moment XREADGROUP delivers an entry,
    Redis records the claim in the Pending Entries List (PEL), which is
    the authoritative "who holds this job" table.  Mirroring it into the
    hash would be a second copy that could disagree with the first.
    """

    PENDING = "PENDING"  # waiting in a stream or in the delayed set
    RUNNING = "RUNNING"  # a worker's handler is executing it
    COMPLETED = "COMPLETED"  # terminal: handler returned normally
    DEAD = "DEAD"  # terminal: moved to the dead-letter queue


# Statuses from which a job never moves again on its own.  Workers skip
# stream entries pointing at a job in one of these states (see the START
# transition), which makes duplicate deliveries harmless.
TERMINAL_STATUSES: frozenset[str] = frozenset({JobStatus.COMPLETED, JobStatus.DEAD})


def _now() -> float:
    """Current UTC epoch timestamp.

    Extracted into a helper so we have a single definition of "what clock
    does distqueue use" for informational timestamps.  (Timestamps that are
    *compared across processes*, like retry due-times, use Redis's clock
    instead — see transitions.py.)
    """
    return time.time()


def _new_id() -> str:
    """Generate a new job ID (UUID4 hex string).

    UUID4 is random, so there's no ordering guarantee — which is fine because
    Redis Streams provide ordering via their own auto-generated entry IDs.
    We only need the job ID to be globally unique for the job:<id> hash key.
    """
    return uuid.uuid4().hex


@dataclass
class Job:
    """Represents a unit of work flowing through the queue.

    Fields map 1-to-1 to the job:{id} Redis hash (see config.py for the key
    layout and AGENTS.md for the state machine).
    """

    # ---- Identity ----
    id: str = field(default_factory=_new_id)

    # Which queue the job belongs to.  Stored on the job (not just implied
    # by the stream it sits in) because a retried job leaves its stream,
    # waits in the shared delayed set, and must be re-injected into the
    # *same* queue it came from.  Immutable after enqueue.
    queue: str = config.DEFAULT_QUEUE

    # ---- What to do ----
    # The payload is an arbitrary dict that the handler function receives.
    # It's JSON-encoded when stored in Redis.
    payload: dict[str, Any] = field(default_factory=dict)

    # ---- State machine ----
    status: str = JobStatus.PENDING

    # ---- Retry tracking ----
    # How many attempts have *finished* (failed, timed out, or lost their
    # worker).  Incremented on each failure, so a job on its first run has
    # attempts == 0.
    attempts: int = 0

    # Upper bound on attempts.  After this many failures the job moves to
    # the DLQ instead of being re-scheduled.  Per-job override at enqueue.
    max_attempts: int = config.DEFAULT_MAX_ATTEMPTS

    # Maximum run time before the monitor reclaims the job even though its
    # worker is alive.  0 means "no limit".
    timeout_s: float = config.DEFAULT_JOB_TIMEOUT_S

    # ---- Timestamps (UTC epoch floats) ----
    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    # When the job is next due (retry backoff or scheduled start).
    # None means "not waiting in the delayed set."
    next_retry_at: float | None = None

    # ---- Diagnostics ----
    # Which worker last touched this job.  Useful for "who had it when it
    # died."  Not set until a worker starts it.
    last_worker: str | None = None

    # "ExceptionType: message" from the most recent failure.
    last_error: str | None = None

    # ------------------------------------------------------------------
    # Serialization: Python ↔ Redis hash
    # ------------------------------------------------------------------

    def to_redis_hash(self) -> dict[str, str]:
        """Serialize this Job into a flat dict of strings for HSET.

        Every field is stored, including None values (as empty strings ""),
        so that from_redis_hash() always sees a complete set of keys.  Empty
        string is used instead of the literal "None" to avoid ambiguity — a
        real error message could genuinely be the text "None".
        """
        return {
            "id": self.id,
            "queue": self.queue,
            "payload": json.dumps(self.payload),
            "status": str(self.status),
            "attempts": str(self.attempts),
            "max_attempts": str(self.max_attempts),
            "timeout_s": str(self.timeout_s),
            "created_at": str(self.created_at),
            "updated_at": str(self.updated_at),
            "next_retry_at": (
                str(self.next_retry_at) if self.next_retry_at is not None else ""
            ),
            "last_worker": self.last_worker if self.last_worker is not None else "",
            "last_error": self.last_error if self.last_error is not None else "",
        }

    @classmethod
    def from_redis_hash(cls, data: dict[str, str]) -> Job:
        """Reconstruct a Job from the dict returned by HGETALL.

        Required fields (id, payload, status, attempts, ...) are indexed
        directly, so a hash missing them raises KeyError.  Callers treat
        that as a corrupt record and dead-letter it rather than crash.

        Fields added after the first release (``queue``, ``timeout_s``) use
        .get() with the config default, so job hashes written by an older
        version of the code still deserialize during a rolling upgrade.
        """
        next_retry_at_raw = data.get("next_retry_at", "")
        timeout_raw = data.get("timeout_s", "")
        return cls(
            id=data["id"],
            queue=data.get("queue") or config.DEFAULT_QUEUE,
            payload=json.loads(data["payload"]),
            status=data["status"],
            attempts=int(data["attempts"]),
            max_attempts=int(data["max_attempts"]),
            timeout_s=(
                float(timeout_raw) if timeout_raw else config.DEFAULT_JOB_TIMEOUT_S
            ),
            created_at=float(data["created_at"]),
            updated_at=float(data["updated_at"]),
            next_retry_at=float(next_retry_at_raw) if next_retry_at_raw else None,
            last_worker=data.get("last_worker") or None,
            last_error=data.get("last_error") or None,
        )
