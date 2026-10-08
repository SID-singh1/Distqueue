"""
benchmark.py — Throughput and latency of distqueue vs. worker count.

    python -m loadtest.benchmark --workers 1,2,4,8 --jobs 5000

For each worker count K it runs two experiments against a dedicated Redis
database (FLUSHed before each run — never point this at real data):

1. **Drain throughput (saturation).**  Enqueue N jobs with no workers
   running, then start K worker *processes* and time how long they take to
   drain the backlog.  This is the queue's maximum service rate for that
   worker count.  Enqueue throughput is measured on the way in.

2. **Latency at 50% load.**  Start K workers, then enqueue jobs open-loop
   at half the throughput measured in (1).  End-to-end latency is
   COMPLETED time minus created_at — both stamped by Redis's clock inside
   the transition scripts, so no cross-process clock skew.  Measuring at
   50% load matters: at 100% the latency is just "how long is the queue",
   which grows without bound (utilisation ρ ≥ 1) and says nothing about the
   system's own overhead.

Why not Locust (the originally planned loadtest/locustfile.py)?  Locust
models users making request/response calls and reports per-request
latency.  A queue's interesting numbers are asynchronous — drain rate vs.
worker count, and enqueue-to-completion latency — which Locust doesn't
measure directly.  This harness measures them with the standard library,
keeping the project's dependency list unchanged.

Workers are separate processes (not threads) so the numbers reflect what a
deployment of K worker containers would see, not one process's GIL.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import platform
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from distqueue import config
from distqueue.client import get_redis_client
from distqueue.producer import enqueue
from distqueue.stats import queue_stats
from distqueue.worker import Worker

QUEUE = "bench"


# ---------------------------------------------------------------------------
# Child processes (top-level functions: Windows uses spawn, not fork)
# ---------------------------------------------------------------------------


def _worker_process(
    db: int, handler_ms: float, stop: mp.Event, ready: mp.Barrier
) -> None:
    client = get_redis_client(db=db)
    delay = handler_ms / 1000

    def handler(payload: dict) -> None:
        if delay:
            time.sleep(delay)

    halt = threading.Event()
    worker = Worker(client, handler, queue=QUEUE, stop_event=halt, block_ms=100)
    thread = threading.Thread(target=worker.run, daemon=True)
    ready.wait()
    thread.start()
    stop.wait()
    halt.set()
    thread.join(timeout=10)


def _producer_process(db: int, count: int, ready: mp.Barrier) -> None:
    client = get_redis_client(db=db)
    ready.wait()
    for i in range(count):
        enqueue(client, {"i": i}, queue=QUEUE)


# ---------------------------------------------------------------------------
# Experiments
# ---------------------------------------------------------------------------


@dataclass
class RunResult:
    workers: int
    jobs: int
    enqueue_per_s: float
    drain_per_s: float
    latency_rate_per_s: float
    e2e_p50_ms: float
    e2e_p95_ms: float
    e2e_p99_ms: float


def _start_workers(db: int, k: int, handler_ms: float) -> tuple[mp.Event, list]:
    stop = mp.Event()
    ready = mp.Barrier(k + 1)
    procs = [
        mp.Process(
            target=_worker_process, args=(db, handler_ms, stop, ready), daemon=True
        )
        for _ in range(k)
    ]
    for p in procs:
        p.start()
    ready.wait()  # every worker process has imported and connected
    return stop, procs


def _stop_workers(stop: mp.Event, procs: list) -> None:
    stop.set()
    for p in procs:
        p.join(timeout=15)
        if p.is_alive():
            p.terminate()


def _wait_drained(client, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        stats = queue_stats(client, QUEUE)
        if stats.backlog == 0 and stats.pending == 0:
            return
        time.sleep(0.01)
    raise TimeoutError("queue did not drain in time")


def measure_drain(
    db: int, k: int, jobs: int, producers: int, handler_ms: float
) -> tuple[float, float]:
    """Return (enqueue jobs/s, drain jobs/s) for K workers."""
    client = get_redis_client(db=db)
    client.flushdb()

    per = jobs // producers
    ready = mp.Barrier(producers + 1)
    procs = [
        mp.Process(target=_producer_process, args=(db, per, ready))
        for _ in range(producers)
    ]
    for p in procs:
        p.start()
    ready.wait()
    t0 = time.perf_counter()
    for p in procs:
        p.join()
    enqueue_rate = (per * producers) / (time.perf_counter() - t0)

    stop, workers = _start_workers(db, k, handler_ms)
    t0 = time.perf_counter()
    _wait_drained(client, timeout_s=600)
    drain_rate = (per * producers) / (time.perf_counter() - t0)
    _stop_workers(stop, workers)
    return enqueue_rate, drain_rate


def measure_latency(
    db: int, k: int, rate: float, jobs: int, handler_ms: float
) -> tuple[float, list[float]]:
    """Open-loop arrivals at ``rate``/s; return (achieved rate, e2e latencies s)."""
    client = get_redis_client(db=db)
    client.flushdb()
    stop, workers = _start_workers(db, k, handler_ms)

    ids: list[str] = []
    interval = 1.0 / rate
    t0 = time.perf_counter()
    for i in range(jobs):
        # Open loop: each arrival is scheduled from the start time, not from
        # the previous enqueue finishing, so a slow enqueue doesn't quietly
        # lower the offered load (the "coordinated omission" trap).
        target = t0 + i * interval
        now = time.perf_counter()
        if target > now:
            time.sleep(target - now)
        ids.append(enqueue(client, {"i": i}, queue=QUEUE))
    achieved = jobs / (time.perf_counter() - t0)
    _wait_drained(client, timeout_s=300)
    _stop_workers(stop, workers)

    pipe = client.pipeline(transaction=False)
    for job_id in ids:
        pipe.hmget(config.job_key(job_id), "created_at", "updated_at", "status")
    latencies = [
        float(done) - float(created)
        for created, done, status in pipe.execute()
        if status == "COMPLETED"
    ]
    return achieved, latencies


def _pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p / 100 * len(ordered)))] * 1000


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="distqueue throughput/latency benchmark"
    )
    parser.add_argument(
        "--workers", default="1,2,4,8", help="comma-separated worker counts"
    )
    parser.add_argument("--jobs", type=int, default=5000, help="jobs per drain run")
    parser.add_argument("--latency-jobs", type=int, default=2000)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument(
        "--handler-ms", type=float, default=0.0, help="simulated work per job"
    )
    parser.add_argument(
        "--db", type=int, default=14, help="Redis DB to use (it is FLUSHED)"
    )
    parser.add_argument("--out", default="loadtest/results")
    args = parser.parse_args()

    if args.db == 0:
        print(
            "refusing to benchmark on DB 0 (it is flushed); pick another --db",
            file=sys.stderr,
        )
        return 2

    counts = [int(x) for x in args.workers.split(",")]
    redis_info = get_redis_client(db=args.db).info("server")
    env = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "redis": redis_info.get("redis_version"),
        "handler_ms": args.handler_ms,
    }
    print(f"environment: {env}")

    results: list[RunResult] = []
    for k in counts:
        enq, drain = measure_drain(
            args.db, k, args.jobs, args.producers, args.handler_ms
        )
        rate, lat = measure_latency(
            args.db, k, drain * 0.5, args.latency_jobs, args.handler_ms
        )
        result = RunResult(
            workers=k,
            jobs=args.jobs,
            enqueue_per_s=round(enq),
            drain_per_s=round(drain),
            latency_rate_per_s=round(rate),
            e2e_p50_ms=round(_pct(lat, 50), 2),
            e2e_p95_ms=round(_pct(lat, 95), 2),
            e2e_p99_ms=round(_pct(lat, 99), 2),
        )
        results.append(result)
        print(
            f"K={k:>2}  enqueue {result.enqueue_per_s:>6}/s  "
            f"drain {result.drain_per_s:>6}/s  "
            f"@{result.latency_rate_per_s}/s e2e p50 {result.e2e_p50_ms} ms  "
            f"p95 {result.e2e_p95_ms} ms  p99 {result.e2e_p99_ms} ms",
            flush=True,
        )

    base = results[0].drain_per_s / results[0].workers
    table = [
        "| Workers | Drain (jobs/s) | Scaling efficiency "
        "| e2e p50 | e2e p95 | e2e p99 |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        efficiency = r.drain_per_s / (base * r.workers) * 100
        table.append(
            f"| {r.workers} | {r.drain_per_s:,} | {efficiency:.0f}% | "
            f"{r.e2e_p50_ms} ms | {r.e2e_p95_ms} ms | {r.e2e_p99_ms} ms |"
        )
    print("\n" + "\n".join(table))

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    payload = {"environment": env, "results": [asdict(r) for r in results]}
    (out / f"bench-{stamp}.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    (out / f"bench-{stamp}.md").write_text("\n".join(table) + "\n", encoding="utf-8")
    print(f"\nsaved to {out}/bench-{stamp}.json and .md")
    get_redis_client(db=args.db).flushdb()
    return 0


if __name__ == "__main__":
    sys.exit(main())
