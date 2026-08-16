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
from typing import Any

from distqueue.config import DEFAULT_MAX_ATTEMPTS


def _now() -> float:
    """Current UTC epoch timestamp.

    Extracted into a helper so tests can monkeypatch it if they ever need
    deterministic timestamps, and so we have a single definition of
    "what clock does distqueue use."
    """
    return time.time()


def _new_id() -> str:
    """Generate a new job ID (UUID4 hex string).

    UUID4 is random, so there's no ordering guarantee — which is fine because
    Redis Streams provide ordering via their own auto-generated message IDs.
    We only need the job ID to be globally unique for the job:<id> hash key.
    """
    return uuid.uuid4().hex


@dataclass
class Job:
    """Represents a unit of work flowing through the queue.

    Fields map 1-to-1 to the job:{id} Redis hash described in the
    architecture doc.  See AGENTS.md § "Redis data model" for the full
    key layout.
    """

    # ---- Identity ----
    id: str = field(default_factory=_new_id)

    # ---- What to do ----
    # The payload is an arbitrary dict that the handler function receives.
    # It's JSON-encoded when stored in Redis.
    payload: dict[str, Any] = field(default_factory=dict)

    # ---- State machine ----
    # Valid statuses: PENDING → CLAIMED → RUNNING → COMPLETED | DEAD
    # (see AGENTS.md § "Job state machine").
    status: str = "PENDING"

    # ---- Retry tracking ----
    # How many times this job has been attempted so far (including the
    # current attempt).  Starts at 0; incremented when a worker claims the
    # job or when the monitor reclaims it from a dead worker.
    attempts: int = 0

    # Upper bound on attempts.  After this many failures the job moves to
    # the DLQ instead of being re-scheduled.  Defaults to the global config
    # value but can be overridden per-job at enqueue time.
    max_attempts: int = DEFAULT_MAX_ATTEMPTS

    # ---- Timestamps (UTC epoch floats) ----
    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    # When the job should next be eligible for retry (set by backoff logic).
    # None means "not scheduled for retry."
    next_retry_at: float | None = None

    # ---- Diagnostics ----
    # Which worker last touched this job.  Useful for debugging "who had it
    # when it died."  Not set until a worker claims it.
    last_worker: str | None = None

    # Traceback or error message from the most recent failure.
    last_error: str | None = None

    # ------------------------------------------------------------------
    # Serialization: Python ↔ Redis hash
    # ------------------------------------------------------------------

    def to_redis_hash(self) -> dict[str, str]:
        """Serialize this Job into a flat dict of strings for HSET.

        Redis hashes are {field: bytes} under the hood.  When using
        decode_responses=True (which our client does), they become
        {str: str}.  So every value here must be a plain string.

        We intentionally store *all* fields, including None values
        (as empty strings ""), so that from_redis_hash() always sees
        a complete set of keys.  This avoids subtle KeyError bugs
        when a field was added in a later version of the code but the
        hash was written by an older version.  Empty string is used
        instead of the literal "None" to avoid ambiguity — a real
        error message could genuinely be the text "None".
        """
        return {
            "id": self.id,
            "payload": json.dumps(self.payload),
            "status": self.status,
            "attempts": str(self.attempts),
            "max_attempts": str(self.max_attempts),
            "created_at": str(self.created_at),
            "updated_at": str(self.updated_at),
            "next_retry_at": str(self.next_retry_at) if self.next_retry_at is not None else "",
            "last_worker": self.last_worker if self.last_worker is not None else "",
            "last_error": self.last_error if self.last_error is not None else "",
        }

    @classmethod
    def from_redis_hash(cls, data: dict[str, str]) -> Job:
        """Reconstruct a Job from the dict returned by HGETALL.

        This is the inverse of to_redis_hash().  Because Redis only stores
        strings, we have to cast numeric fields back to their Python types.

        The empty-string convention for None fields (see to_redis_hash)
        means we check `val or None` for optional string fields and
        explicitly handle empty strings for optional floats.
        """
        next_retry_at_raw = data.get("next_retry_at", "")
        return cls(
            id=data["id"],
            payload=json.loads(data["payload"]),
            status=data["status"],
            attempts=int(data["attempts"]),
            max_attempts=int(data["max_attempts"]),
            created_at=float(data["created_at"]),
            updated_at=float(data["updated_at"]),
            next_retry_at=float(next_retry_at_raw) if next_retry_at_raw else None,
            last_worker=data.get("last_worker") or None,
            last_error=data.get("last_error") or None,
        )
