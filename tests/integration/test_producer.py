"""
test_producer.py — Integration tests for distqueue.producer.enqueue().

These tests require a running Redis instance.  Start one with:

    docker compose -f docker/docker-compose.yml up -d

The redis_client fixture (from conftest.py) connects to Redis using the
same factory the production code uses and flushes the DB before/after
each test.

All tests in this file are marked with @pytest.mark.integration so they
can be excluded from fast unit-test runs:

    pytest -m "not integration"          # unit tests only
    pytest tests/integration -v          # integration tests only
"""

from __future__ import annotations

import pytest

from distqueue import config
from distqueue.job import Job
from distqueue.producer import enqueue


# Apply the integration marker to every test in this module so
# `pytest -m "not integration"` skips them all without needing to
# decorate each function individually.
pytestmark = pytest.mark.integration


class TestEnqueueBasics:
    """Core behaviour: ID generation, hash persistence, stream insertion."""

    def test_returns_non_empty_job_id(self, redis_client) -> None:
        """enqueue() should return a truthy string (the UUID hex)."""
        job_id = enqueue(redis_client, {"task": "noop"})
        assert isinstance(job_id, str)
        assert len(job_id) > 0

    def test_job_hash_round_trips(self, redis_client) -> None:
        """The job:{id} hash should contain the full job state and be
        deserializable back into a Job with the correct payload/status."""
        payload = {"url": "https://example.com", "depth": 3}
        job_id = enqueue(redis_client, payload)

        hash_key = f"{config.JOB_HASH_KEY_PREFIX}{job_id}"
        raw = redis_client.hgetall(hash_key)

        # Sanity: the hash shouldn't be empty.
        assert raw, f"Expected job hash at {hash_key}, got empty dict"

        job = Job.from_redis_hash(raw)
        assert job.id == job_id
        assert job.payload == payload
        assert job.status == "PENDING"
        assert job.attempts == 0

    def test_stream_length_increases_by_one(self, redis_client) -> None:
        """Each enqueue() call should append exactly one entry to the
        main stream."""
        before = redis_client.xlen(config.QUEUE_NAME)
        enqueue(redis_client, {"x": 1})
        after = redis_client.xlen(config.QUEUE_NAME)
        assert after - before == 1

    def test_stream_entry_contains_only_job_id(self, redis_client) -> None:
        """The stream entry should carry *only* the job_id field.

        This locks in the single-source-of-truth design: the payload lives
        exclusively in the job:{id} hash, not duplicated in the stream.
        If someone accidentally adds more fields to the XADD call, this
        test will catch it.
        """
        payload = {"secret": "should_not_appear_in_stream"}
        job_id = enqueue(redis_client, payload)

        # XRANGE returns a list of (message_id, fields_dict) tuples.
        entries = redis_client.xrange(config.QUEUE_NAME)
        assert len(entries) == 1

        _msg_id, fields = entries[0]

        # The only field should be "job_id" pointing to our job's UUID.
        assert fields == {"job_id": job_id}

        # Belt-and-suspenders: make sure no payload data leaked in.
        assert "secret" not in fields
        assert "should_not_appear_in_stream" not in fields.values()


class TestEnqueueMaxAttempts:
    """Verify the max_attempts parameter / default behaviour."""

    def test_custom_max_attempts(self, redis_client) -> None:
        """Passing max_attempts should override the default."""
        job_id = enqueue(redis_client, {"x": 1}, max_attempts=42)
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)
        assert job.max_attempts == 42

    def test_default_max_attempts(self, redis_client) -> None:
        """Omitting max_attempts should fall back to the config default."""
        job_id = enqueue(redis_client, {"x": 1})
        raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
        job = Job.from_redis_hash(raw)
        assert job.max_attempts == config.DEFAULT_MAX_ATTEMPTS


class TestEnqueueMultipleJobs:
    """Verify behaviour when multiple jobs are enqueued."""

    def test_unique_ids_and_independent_reads(self, redis_client) -> None:
        """Enqueueing N jobs should produce N distinct IDs, each backed
        by its own readable hash."""
        n = 5
        ids = [enqueue(redis_client, {"i": i}) for i in range(n)]

        # All IDs should be unique.
        assert len(set(ids)) == n

        # Each job's hash should exist and contain the correct payload.
        for i, job_id in enumerate(ids):
            raw = redis_client.hgetall(f"{config.JOB_HASH_KEY_PREFIX}{job_id}")
            job = Job.from_redis_hash(raw)
            assert job.payload == {"i": i}

        # The stream should have exactly N entries.
        assert redis_client.xlen(config.QUEUE_NAME) == n
