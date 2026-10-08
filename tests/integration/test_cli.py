"""
test_cli.py — The operational CLI commands against a real Redis.
"""

from __future__ import annotations

import json

import pytest

from distqueue import config
from distqueue.cli import main
from distqueue.producer import enqueue
from distqueue.worker import Worker

pytestmark = pytest.mark.integration


def _dead_job(client) -> str:
    def fail(payload: dict) -> None:
        raise RuntimeError("nope")

    job_id = enqueue(client, {"x": 1}, max_attempts=1)
    Worker(client, fail, block_ms=200).process_one()
    assert client.hget(config.job_key(job_id), "status") == "DEAD"
    return job_id


def test_enqueue_prints_job_id(redis_client, capsys) -> None:
    assert main(["enqueue", '{"a": 1}', "--queue", "cli"], client=redis_client) == 0
    job_id = capsys.readouterr().out.strip()
    assert redis_client.hget(config.job_key(job_id), "queue") == "cli"


def test_enqueue_rejects_bad_json(redis_client) -> None:
    assert main(["enqueue", "{nope"], client=redis_client) == 2


def test_job_shows_decoded_payload(redis_client, capsys) -> None:
    job_id = enqueue(redis_client, {"hello": "world"})
    assert main(["job", job_id], client=redis_client) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["payload"] == {"hello": "world"}
    assert shown["status"] == "PENDING"


def test_job_missing_returns_1(redis_client) -> None:
    assert main(["job", "does-not-exist"], client=redis_client) == 1


def test_stats_json(redis_client, capsys) -> None:
    enqueue(redis_client, {})
    enqueue(redis_client, {}, delay_s=60)
    assert main(["stats", "--json"], client=redis_client) == 0
    stats = json.loads(capsys.readouterr().out)
    default = next(q for q in stats["queues"] if q["queue"] == "default")
    assert default["backlog"] == 1
    assert stats["delayed"] == 1


def test_dlq_list_and_replay(redis_client, capsys) -> None:
    job_id = _dead_job(redis_client)

    assert main(["dlq", "list"], client=redis_client) == 0
    assert job_id in capsys.readouterr().out

    assert main(["dlq", "replay", job_id], client=redis_client) == 0
    assert f"{job_id}: replayed" in capsys.readouterr().out

    job = redis_client.hgetall(config.job_key(job_id))
    assert (job["status"], job["attempts"], job["last_error"]) == ("PENDING", "0", "")
    assert redis_client.ttl(config.job_key(job_id)) == -1, (
        "replayed job must not expire"
    )
    assert redis_client.xlen(config.DLQ_STREAM) == 0
    # It's runnable again.
    assert Worker(redis_client, lambda p: None, block_ms=200).process_one()
    assert redis_client.hget(config.job_key(job_id), "status") == "COMPLETED"


def test_replay_all(redis_client) -> None:
    ids = {_dead_job(redis_client) for _ in range(3)}
    assert main(["dlq", "replay", "--all"], client=redis_client) == 0
    assert all(redis_client.hget(config.job_key(i), "status") == "PENDING" for i in ids)


def test_replay_unknown_job_fails(redis_client) -> None:
    assert main(["dlq", "replay", "nope"], client=redis_client) == 1


def test_replay_refuses_non_dead_job(redis_client, capsys) -> None:
    """Replaying a live job would create a second live entry for it."""
    job_id = _dead_job(redis_client)
    redis_client.hset(config.job_key(job_id), "status", "RUNNING")
    assert main(["dlq", "replay", job_id], client=redis_client) == 1
    assert "not_dead" in capsys.readouterr().out
