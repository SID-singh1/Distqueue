"""
test_job.py — Unit tests for Job serialization round-trip.

These tests verify that a Job can survive the Python → Redis hash → Python
cycle without data loss or type corruption.  They don't need a running
Redis instance — they just exercise to_redis_hash() and from_redis_hash()
as pure functions.
"""

from __future__ import annotations

import json
import time

import pytest

from distqueue.job import Job


# ---------------------------------------------------------------------------
# Round-trip tests
# ---------------------------------------------------------------------------


class TestJobRoundTrip:
    """Verify that to_redis_hash ↔ from_redis_hash is lossless."""

    def test_minimal_job_round_trips(self) -> None:
        """A default Job (no payload, no error) should survive the round trip."""
        original = Job()
        rebuilt = Job.from_redis_hash(original.to_redis_hash())

        assert rebuilt.id == original.id
        assert rebuilt.payload == original.payload  # empty dict
        assert rebuilt.status == original.status
        assert rebuilt.attempts == original.attempts
        assert rebuilt.max_attempts == original.max_attempts
        assert rebuilt.created_at == original.created_at
        assert rebuilt.updated_at == original.updated_at
        assert rebuilt.next_retry_at is None
        assert rebuilt.last_worker is None
        assert rebuilt.last_error is None

    def test_full_job_round_trips(self) -> None:
        """A Job with every field populated should survive the round trip."""
        now = time.time()
        original = Job(
            id="abc123",
            payload={"url": "https://example.com", "retries": 3},
            status="RUNNING",
            attempts=2,
            max_attempts=10,
            created_at=now - 60,
            updated_at=now,
            next_retry_at=now + 30,
            last_worker="worker-42",
            last_error="ConnectionTimeout: upstream refused",
        )
        rebuilt = Job.from_redis_hash(original.to_redis_hash())

        assert rebuilt.id == original.id
        assert rebuilt.payload == original.payload
        assert rebuilt.status == original.status
        assert rebuilt.attempts == original.attempts
        assert rebuilt.max_attempts == original.max_attempts
        assert rebuilt.created_at == original.created_at
        assert rebuilt.updated_at == original.updated_at
        assert rebuilt.next_retry_at == pytest.approx(original.next_retry_at)
        assert rebuilt.last_worker == original.last_worker
        assert rebuilt.last_error == original.last_error

    def test_nested_payload_round_trips(self) -> None:
        """Payload with nested dicts/lists should JSON-encode cleanly."""
        payload = {
            "user": {"name": "Alice", "tags": ["admin", "beta"]},
            "scores": [1, 2.5, 3],
            "meta": None,
        }
        original = Job(payload=payload)
        rebuilt = Job.from_redis_hash(original.to_redis_hash())
        assert rebuilt.payload == payload


# ---------------------------------------------------------------------------
# to_redis_hash output checks
# ---------------------------------------------------------------------------


class TestToRedisHash:
    """Verify the shape of the dict we hand to Redis."""

    def test_all_values_are_strings(self) -> None:
        """Redis hashes store strings; every value must be str."""
        job = Job(
            payload={"key": "value"},
            next_retry_at=123456.789,
            last_worker="w-1",
            last_error="boom",
        )
        h = job.to_redis_hash()
        for key, value in h.items():
            assert isinstance(value, str), f"Field {key!r} is {type(value)}, expected str"

    def test_none_fields_become_empty_strings(self) -> None:
        """Optional fields that are None should serialize as '' (empty string),
        not the literal string 'None', to avoid ambiguity on deserialization."""
        job = Job()  # next_retry_at, last_worker, last_error are all None
        h = job.to_redis_hash()
        assert h["next_retry_at"] == ""
        assert h["last_worker"] == ""
        assert h["last_error"] == ""

    def test_payload_is_valid_json(self) -> None:
        """The payload field should be a JSON string, not repr()."""
        job = Job(payload={"a": 1})
        h = job.to_redis_hash()
        parsed = json.loads(h["payload"])
        assert parsed == {"a": 1}


# ---------------------------------------------------------------------------
# from_redis_hash edge cases
# ---------------------------------------------------------------------------


class TestFromRedisHash:
    """Cover edge cases in deserialization."""

    def test_empty_optional_strings_become_none(self) -> None:
        """Empty strings for optional fields should deserialize back to None."""
        h = Job().to_redis_hash()
        # Simulate what Redis would return — ensure the empty strings work
        h["next_retry_at"] = ""
        h["last_worker"] = ""
        h["last_error"] = ""

        rebuilt = Job.from_redis_hash(h)
        assert rebuilt.next_retry_at is None
        assert rebuilt.last_worker is None
        assert rebuilt.last_error is None

    def test_numeric_strings_cast_correctly(self) -> None:
        """Integers and floats stored as strings should parse back to the
        correct Python types."""
        h = Job(attempts=7, max_attempts=15, created_at=1000.5).to_redis_hash()
        rebuilt = Job.from_redis_hash(h)
        assert isinstance(rebuilt.attempts, int)
        assert isinstance(rebuilt.max_attempts, int)
        assert isinstance(rebuilt.created_at, float)
        assert rebuilt.attempts == 7
        assert rebuilt.max_attempts == 15
        assert rebuilt.created_at == 1000.5


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------


class TestJobDefaults:
    """Verify that a freshly created Job has sensible defaults."""

    def test_status_is_pending(self) -> None:
        """New jobs should start in PENDING — they haven't been claimed yet."""
        assert Job().status == "PENDING"

    def test_attempts_start_at_zero(self) -> None:
        """No work has been attempted on a brand-new job."""
        assert Job().attempts == 0

    def test_id_is_unique(self) -> None:
        """Each Job should get a distinct UUID."""
        ids = {Job().id for _ in range(100)}
        assert len(ids) == 100, "Job IDs should be unique"

    def test_timestamps_are_recent(self) -> None:
        """created_at and updated_at should be close to 'now'."""
        before = time.time()
        job = Job()
        after = time.time()
        assert before <= job.created_at <= after
        assert before <= job.updated_at <= after
