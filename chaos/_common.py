"""
_common.py — Shared plumbing for the chaos experiments.

Each experiment is: enqueue a deterministic probe job, wait for a worker
container to pick it up, do something violent to that container, then
watch the job hash until the system has (or hasn't) recovered.  The
helpers here cover the parts every experiment repeats.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import redis
from docker.models.containers import Container

import docker
from distqueue import config
from distqueue.client import get_redis_client
from distqueue.stats import queue_stats

COMPOSE_PROJECT = "distqueue"


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def connect() -> tuple[redis.Redis, docker.DockerClient]:
    """Redis (published on localhost:6379) and the local Docker daemon."""
    client = get_redis_client()
    try:
        client.ping()
    except redis.RedisError as exc:
        fail(f"cannot reach Redis on localhost:6379 ({exc}). Is the stack up?")
    try:
        engine = docker.from_env()
        engine.ping()
    except docker.errors.DockerException as exc:
        fail(f"cannot reach the Docker daemon ({exc}). Is Docker Desktop running?")
    return client, engine


def fail(message: str) -> None:
    log(f"FAIL: {message}")
    sys.exit(1)


def worker_containers(engine: docker.DockerClient) -> list[Container]:
    return engine.containers.list(
        filters={
            "label": [
                f"com.docker.compose.project={COMPOSE_PROJECT}",
                "com.docker.compose.service=worker",
            ]
        }
    )


def require_quiet_queue(client: redis.Redis, engine: docker.DockerClient) -> None:
    """The probe must not queue behind other work, or timings are meaningless."""
    workers = worker_containers(engine)
    if len(workers) < 2:
        fail(f"need at least 2 running worker containers, found {len(workers)}")
    stats = queue_stats(client, config.DEFAULT_QUEUE)
    if stats.backlog or stats.pending:
        fail(
            f"queue is busy (backlog={stats.backlog}, in flight={stats.pending}). "
            "Stop the demo producer: docker compose -f docker/docker-compose.yml "
            "stop producer"
        )
    log(f"{len(workers)} worker containers up, queue idle")


def container_for_consumer(
    engine: docker.DockerClient, consumer_name: str
) -> Container:
    """Map a consumer name (hostname-pid-suffix) back to its container.

    A container's hostname defaults to its 12-character short id, so the
    first dash-separated field of the consumer name identifies it.
    """
    hostname = consumer_name.split("-", 1)[0]
    for container in worker_containers(engine):
        if container.attrs["Config"]["Hostname"] == hostname:
            return container
    fail(f"no running worker container has hostname {hostname!r}")
    raise AssertionError  # unreachable; fail() exits


def job_state(client: redis.Redis, job_id: str) -> dict[str, str]:
    return client.hgetall(config.job_key(job_id))


def wait_for(
    description: str,
    predicate: Callable[[], bool],
    timeout_s: float,
    poll_s: float = 0.25,
) -> float:
    """Poll until ``predicate`` holds; return seconds waited.  Exit on timeout."""
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        if predicate():
            return time.monotonic() - start
        time.sleep(poll_s)
    fail(f"timed out after {timeout_s:.0f}s waiting for: {description}")
    raise AssertionError  # unreachable


@dataclass
class Report:
    """Collects results and prints them as a table (and optionally JSON)."""

    experiment: str
    rows: list[tuple[str, str]] = field(default_factory=list)
    checks: list[tuple[str, bool]] = field(default_factory=list)
    data: dict[str, object] = field(default_factory=dict)

    def add(self, label: str, value: object, unit: str = "") -> None:
        self.data[label] = value
        shown = f"{value:.1f}{unit}" if isinstance(value, float) else f"{value}{unit}"
        self.rows.append((label, shown))

    def check(self, label: str, ok: bool) -> None:
        self.checks.append((label, ok))

    def finish(self, json_path: str | None = None) -> None:
        width = max(len(label) for label, _ in self.rows + self.checks) + 2
        print("\n" + "=" * 64)
        print(f"CHAOS: {self.experiment}")
        print("=" * 64)
        for label, shown in self.rows:
            print(f"{label:<{width}}{shown}")
        print("-" * 64)
        for label, ok in self.checks:
            print(f"{'PASS' if ok else 'FAIL'}  {label}")
        print("=" * 64)
        passed = all(ok for _, ok in self.checks)
        if json_path:
            with open(json_path, "w", encoding="utf-8") as fh:
                json.dump(
                    {"experiment": self.experiment, "passed": passed, **self.data},
                    fh,
                    indent=2,
                )
        sys.exit(0 if passed else 1)
