"""
cli.py — The ``distqueue`` command.

    distqueue worker [--queue Q] [--handler module:function]
    distqueue scheduler
    distqueue monitor
    distqueue produce [--queue Q] [--rate R] [--count N]
    distqueue enqueue '{"json": "payload"}' [--queue Q] [--delay S] ...
    distqueue job <job_id>
    distqueue stats [--json]
    distqueue dlq list [--limit N]
    distqueue dlq replay <job_id>... | --all

Built on argparse (standard library) rather than click/typer to keep the
project inside its stated dependency budget (Redis + Prometheus client).

The operational commands (stats, job, dlq) exist because a queue you can't
inspect is a queue you can't operate: "why is this job stuck?" and "replay
everything that failed during last night's outage" should be one command,
not a hand-written redis-cli session.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence

import redis

from distqueue import config
from distqueue.client import get_redis_client
from distqueue.stats import known_queues, live_consumers, queue_stats
from distqueue.transitions import ReplayResult, replay_dead_job


def _cmd_worker(args: argparse.Namespace) -> int:
    from distqueue.entrypoints import run_worker

    run_worker(
        queue=args.queue, handler_spec=args.handler, metrics_port=args.metrics_port
    )
    return 0


def _cmd_scheduler(args: argparse.Namespace) -> int:
    from distqueue.entrypoints import run_scheduler

    run_scheduler(metrics_port=args.metrics_port)
    return 0


def _cmd_monitor(args: argparse.Namespace) -> int:
    from distqueue.entrypoints import run_monitor

    run_monitor(metrics_port=args.metrics_port)
    return 0


def _cmd_produce(args: argparse.Namespace) -> int:
    from distqueue.entrypoints import run_producer

    run_producer(
        queue=args.queue,
        rate=args.rate,
        count=args.count,
        metrics_port=args.metrics_port,
    )
    return 0


def _cmd_enqueue(args: argparse.Namespace, client: redis.Redis) -> int:
    from distqueue.producer import enqueue

    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        print(f"payload is not valid JSON: {exc}", file=sys.stderr)
        return 2
    if not isinstance(payload, dict):
        print("payload must be a JSON object", file=sys.stderr)
        return 2
    job_id = enqueue(
        client,
        payload,
        args.max_attempts,
        queue=args.queue,
        timeout_s=args.timeout,
        delay_s=args.delay,
        run_at=args.run_at,
        idempotency_key=args.idempotency_key,
    )
    print(job_id)
    return 0


def _cmd_job(args: argparse.Namespace, client: redis.Redis) -> int:
    raw = client.hgetall(config.job_key(args.job_id))
    if not raw:
        print(
            f"job {args.job_id} not found (never existed, or expired)", file=sys.stderr
        )
        return 1
    if raw.get("payload"):
        try:
            raw["payload"] = json.loads(raw["payload"])
        except json.JSONDecodeError:
            pass
    ttl = client.ttl(config.job_key(args.job_id))
    raw["ttl_s"] = ttl if ttl >= 0 else None
    print(json.dumps(raw, indent=2, sort_keys=True))
    return 0


def _cmd_stats(args: argparse.Namespace, client: redis.Redis) -> int:
    queues = [queue_stats(client, q) for q in known_queues(client)]
    live: set[str] = set()
    for q in queues:
        live |= live_consumers(client, q.queue)[1]
    summary = {
        "queues": [q.as_dict() for q in queues],
        "delayed": int(client.zcard(config.DELAYED_ZSET)),
        "dlq": int(client.xlen(config.DLQ_STREAM)),
        "live_workers": len(live),
    }
    if args.json:
        print(json.dumps(summary, indent=2))
        return 0
    print(
        f"{'QUEUE':<16}{'BACKLOG':>10}{'IN-FLIGHT':>11}"
        f"{'RETAINED':>10}{'CONSUMERS':>11}"
    )
    for q in queues:
        print(
            f"{q.queue:<16}{q.backlog:>10}{q.pending:>11}"
            f"{q.stream_length:>10}{q.consumers:>11}"
        )
    print(
        f"\ndelayed: {summary['delayed']}   dlq: {summary['dlq']}   "
        f"live workers: {summary['live_workers']}"
    )
    return 0


def _dlq_entries(client: redis.Redis) -> list[tuple[str, dict[str, str]]]:
    return client.xrange(config.DLQ_STREAM, min="-", max="+")


def _cmd_dlq_list(args: argparse.Namespace, client: redis.Redis) -> int:
    entries = client.xrevrange(config.DLQ_STREAM, max="+", min="-", count=args.limit)
    if not entries:
        print("DLQ is empty.")
        return 0
    for entry_id, f in entries:
        print(
            f"{entry_id}  job={f.get('job_id')}  queue={f.get('queue', '?')}  "
            f"attempts={f.get('attempts', '?')}  trigger={f.get('trigger', '?')}\n"
            f"    {f.get('reason', '')}"
        )
    return 0


def _cmd_dlq_replay(args: argparse.Namespace, client: redis.Redis) -> int:
    if not args.all and not args.job_ids:
        print("give one or more job ids, or --all", file=sys.stderr)
        return 2
    wanted = set(args.job_ids)
    # Newest entry per job wins: a job that died, was replayed, and died
    # again has two DLQ entries; replaying should use the latest.
    latest: dict[str, tuple[str, str]] = {}
    for entry_id, f in _dlq_entries(client):
        job_id = f.get("job_id", "")
        if args.all or job_id in wanted:
            latest[job_id] = (entry_id, f.get("queue") or config.DEFAULT_QUEUE)

    failures = 0
    for job_id in sorted(wanted - latest.keys()):
        print(f"{job_id}: not in the DLQ", file=sys.stderr)
        failures += 1
    for job_id, (entry_id, queue) in latest.items():
        result = replay_dead_job(
            client, dlq_entry_id=entry_id, job_id=job_id, queue=queue
        )
        print(f"{job_id}: {result.value}")
        failures += result is not ReplayResult.REPLAYED
    return 1 if failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="distqueue", description="Redis-backed distributed job queue."
    )
    parser.add_argument(
        "--log-level", default="INFO", help="DEBUG, INFO, WARNING... (default INFO)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_metrics(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--metrics-port",
            type=int,
            default=config.METRICS_PORT,
            help="port for /metrics (0 disables; default %(default)s)",
        )

    p = sub.add_parser("worker", help="consume and run jobs")
    p.add_argument("--queue", default=config.DEFAULT_QUEUE)
    p.add_argument(
        "--handler",
        default="distqueue.demo:handler",
        help="handler as module:function (default %(default)s)",
    )
    add_metrics(p)
    p.set_defaults(func=_cmd_worker, needs_client=False)

    p = sub.add_parser("scheduler", help="re-inject due delayed jobs")
    add_metrics(p)
    p.set_defaults(func=_cmd_scheduler, needs_client=False)

    p = sub.add_parser("monitor", help="reclaim dead/timed-out jobs, housekeeping")
    add_metrics(p)
    p.set_defaults(func=_cmd_monitor, needs_client=False)

    p = sub.add_parser("produce", help="enqueue demo jobs at a steady rate")
    p.add_argument("--queue", default=config.DEFAULT_QUEUE)
    p.add_argument("--rate", type=float, default=3.0, help="jobs per second")
    p.add_argument("--count", type=int, default=0, help="stop after N jobs (0 = never)")
    add_metrics(p)
    p.set_defaults(func=_cmd_produce, needs_client=False)

    p = sub.add_parser("enqueue", help="enqueue one job")
    p.add_argument("payload", help="JSON object")
    p.add_argument("--queue", default=config.DEFAULT_QUEUE)
    p.add_argument("--max-attempts", type=int, default=None)
    p.add_argument("--timeout", type=float, default=None, help="seconds")
    when = p.add_mutually_exclusive_group()
    when.add_argument("--delay", type=float, default=None, help="seconds from now")
    when.add_argument("--run-at", type=float, default=None, help="epoch seconds")
    p.add_argument("--idempotency-key", default=None)
    p.set_defaults(func=_cmd_enqueue, needs_client=True)

    p = sub.add_parser("job", help="show a job's state")
    p.add_argument("job_id")
    p.set_defaults(func=_cmd_job, needs_client=True)

    p = sub.add_parser("stats", help="queue backlog, in-flight, DLQ, live workers")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=_cmd_stats, needs_client=True)

    dlq = sub.add_parser("dlq", help="inspect and replay dead-lettered jobs")
    dlq_sub = dlq.add_subparsers(dest="dlq_command", required=True)
    p = dlq_sub.add_parser("list", help="newest dead-lettered jobs first")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=_cmd_dlq_list, needs_client=True)
    p = dlq_sub.add_parser("replay", help="reset attempts and re-enqueue DEAD jobs")
    p.add_argument("job_ids", nargs="*")
    p.add_argument("--all", action="store_true", help="replay every job in the DLQ")
    p.set_defaults(func=_cmd_dlq_replay, needs_client=True)

    return parser


def main(argv: Sequence[str] | None = None, client: redis.Redis | None = None) -> int:
    """CLI entry point.  ``client`` is injectable so tests can pass their own."""
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if args.needs_client:
        return int(args.func(args, client or get_redis_client()))
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
