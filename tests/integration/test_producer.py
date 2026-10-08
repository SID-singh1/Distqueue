"""
test_producer.py — Integration tests for distqueue.producer.enqueue().

Requires Redis (see conftest.py).  All tests are marked integration so
`pytest -m "not integration"` skips them.
"""

from __future__ import annotations

import threading

import pytest

from distqueue import config
from distqueue.job import Job
from distqueue.producer import enqueue
from tests.integration.helpers import redis_now, sample

pytestmark = pytest.mark.integration


def _job(client, job_id: str) -> Job:
    return Job.from_redis_hash(client.hgetall(config.job_key(job_id)))


class TestEnqueueBasics:
    """Core behaviour: ID generation, hash persistence, stream insertion."""

    def test_job_hash_round_trips(self, redis_client) -> None:
        payload = {"url": "https://example.com", "depth": 3}
        job_id = enqueue(redis_client, payload)
        job = _job(redis_client, job_id)
        assert job.id == job_id
        assert job.payload == payload
        assert job.status == "PENDING"
        assert job.attempts == 0
        assert job.queue == config.DEFAULT_QUEUE

    def test_stream_entry_contains_only_job_id(self, redis_client) -> None:
        """Locks in the single-source-of-truth design: the payload lives
        only in the hash; the stream entry is a pointer."""
        job_id = enqueue(redis_client, {"secret": "should_not_appear_in_stream"})
        entries = redis_client.xrange(config.QUEUE_NAME)
        assert len(entries) == 1
        assert entries[0][1] == {"job_id": job_id}

    def test_unique_ids_and_independent_reads(self, redis_client) -> None:
        ids = [enqueue(redis_client, {"i": i}) for i in range(5)]
        assert len(set(ids)) == 5
        for i, job_id in enumerate(ids):
            assert _job(redis_client, job_id).payload == {"i": i}
        assert redis_client.xlen(config.QUEUE_NAME) == 5

    def test_timestamps_come_from_redis_clock(self, redis_client) -> None:
        """created_at is stamped inside the script with Redis TIME, so
        end-to-end latency (computed on Redis's clock at completion) never
        mixes two machines' clocks."""
        before = redis_now(redis_client)
        job = _job(redis_client, enqueue(redis_client, {}))
        after = redis_now(redis_client)
        assert before - 0.01 <= job.created_at <= after + 0.01


class TestEnqueueOptions:
    def test_custom_and_default_max_attempts(self, redis_client) -> None:
        assert (
            _job(redis_client, enqueue(redis_client, {}, max_attempts=42)).max_attempts
            == 42
        )
        assert (
            _job(redis_client, enqueue(redis_client, {})).max_attempts
            == config.DEFAULT_MAX_ATTEMPTS
        )

    def test_custom_timeout(self, redis_client) -> None:
        assert (
            _job(redis_client, enqueue(redis_client, {}, timeout_s=7.5)).timeout_s
            == 7.5
        )

    def test_named_queue_uses_its_own_stream_and_is_registered(
        self, redis_client
    ) -> None:
        job_id = enqueue(redis_client, {}, queue="emails")
        assert redis_client.xlen(config.stream_key("emails")) == 1
        assert redis_client.xlen(config.QUEUE_NAME) == 0
        assert _job(redis_client, job_id).queue == "emails"
        assert "emails" in redis_client.smembers(config.QUEUES_SET)

    def test_rejects_both_delay_and_run_at(self, redis_client) -> None:
        with pytest.raises(ValueError):
            enqueue(redis_client, {}, delay_s=1, run_at=1)

    def test_rejects_negative_delay(self, redis_client) -> None:
        with pytest.raises(ValueError):
            enqueue(redis_client, {}, delay_s=-1)


class TestScheduledEnqueue:
    """delay_s / run_at put the job in the delayed set, not the stream."""

    def test_delay_goes_to_delayed_set(self, redis_client) -> None:
        before = redis_now(redis_client)
        job_id = enqueue(redis_client, {}, delay_s=30)
        after = redis_now(redis_client)

        assert redis_client.xlen(config.QUEUE_NAME) == 0
        score = redis_client.zscore(config.DELAYED_ZSET, job_id)
        assert before + 30 - 0.01 <= score <= after + 30 + 0.01
        assert _job(redis_client, job_id).next_retry_at == pytest.approx(
            score, abs=1e-3
        )

    def test_run_at_uses_absolute_time(self, redis_client) -> None:
        job_id = enqueue(redis_client, {}, run_at=4_000_000_000.0)
        assert redis_client.zscore(config.DELAYED_ZSET, job_id) == 4_000_000_000.0


class TestIdempotency:
    def test_same_key_returns_same_job_and_creates_nothing(self, redis_client) -> None:
        dedup_before = sample("distqueue_jobs_deduplicated_total", queue="default")
        first = enqueue(redis_client, {"n": 1}, idempotency_key="order-42")
        second = enqueue(redis_client, {"n": 2}, idempotency_key="order-42")

        assert first == second
        assert redis_client.xlen(config.QUEUE_NAME) == 1
        # The original payload wins; the duplicate's payload is ignored.
        assert _job(redis_client, first).payload == {"n": 1}
        assert (
            sample("distqueue_jobs_deduplicated_total", queue="default") - dedup_before
            == 1
        )

    def test_different_keys_create_different_jobs(self, redis_client) -> None:
        a = enqueue(redis_client, {}, idempotency_key="a")
        b = enqueue(redis_client, {}, idempotency_key="b")
        assert a != b
        assert redis_client.xlen(config.QUEUE_NAME) == 2

    def test_key_has_ttl(self, redis_client) -> None:
        enqueue(redis_client, {}, idempotency_key="ttl-check")
        ttl = redis_client.ttl(config.idempotency_key("ttl-check"))
        assert 0 < ttl <= config.IDEMPOTENCY_TTL_S

    def test_concurrent_duplicates_create_one_job(
        self, redis_client, make_client
    ) -> None:
        """Check-then-create runs inside one Lua script, so racing callers
        cannot both pass the check — the reason it isn't GET-then-SET."""
        results: list[str] = []
        lock = threading.Lock()

        def submit() -> None:
            job_id = enqueue(make_client(), {}, idempotency_key="race")
            with lock:
                results.append(job_id)

        threads = [threading.Thread(target=submit) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(set(results)) == 1
        assert redis_client.xlen(config.QUEUE_NAME) == 1
