"""
stats.py — Read-only queue statistics, shared by the monitor and the CLI.

Kept separate from monitor.py so ``distqueue stats`` can answer "what is the
queue doing right now?" without constructing a Monitor (which has side
effects: reclaiming, trimming, garbage-collecting consumers).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import redis

from distqueue import config


@dataclass(frozen=True)
class QueueStats:
    queue: str
    backlog: int  # entries not yet delivered to any worker
    pending: int  # delivered, not yet acknowledged (in flight)
    stream_length: int  # entries physically in the stream (XLEN)
    consumers: int  # consumer names registered in the group

    def as_dict(self) -> dict[str, int | str]:
        return asdict(self)


def known_queues(client: redis.Redis) -> list[str]:
    """Every queue that has ever been enqueued to, plus the default."""
    names = set(client.smembers(config.QUEUES_SET))
    names.add(config.DEFAULT_QUEUE)
    return sorted(names)


def queue_stats(
    client: redis.Redis, queue: str, group: str = config.CONSUMER_GROUP
) -> QueueStats:
    """Backlog, in-flight and retained-entry counts for one queue.

    Backlog is the consumer group's ``lag`` (Redis ≥ 7.0): entries added to
    the stream that the group has not yet delivered.  It is NOT XLEN.  XACK
    never removes entries from a stream, so XLEN counts every entry still
    retained — delivered, acknowledged and all — and before trimming
    existed it only ever went up.
    """
    stream = config.stream_key(queue)
    length = int(client.xlen(stream))
    try:
        groups = client.xinfo_groups(stream)
    except redis.ResponseError:  # stream doesn't exist yet
        groups = []
    info = next((g for g in groups if g["name"] == group), None)
    if info is None:
        # Nobody has ever consumed this queue: everything is backlog.
        return QueueStats(queue, length, 0, length, 0)

    backlog = info.get("lag")
    if backlog is None:
        # Redis reports lag as nil when it can't derive it cheaply (e.g.
        # entries were XDEL'd in the undelivered range).  Count the
        # undelivered entries directly, bounded so a huge backlog can't
        # turn one stats call into a giant XRANGE.
        backlog = len(
            client.xrange(
                stream, min="(" + info["last-delivered-id"], max="+", count=10_000
            )
        )
    return QueueStats(
        queue=queue,
        backlog=int(backlog),
        pending=int(info["pending"]),
        stream_length=length,
        consumers=int(info["consumers"]),
    )


def live_consumers(
    client: redis.Redis, queue: str, group: str = config.CONSUMER_GROUP
) -> tuple[list[dict], set[str]]:
    """Return (all consumers in the group, names whose heartbeat exists).

    Live workers are found from the consumer group rather than with
    ``SCAN MATCH worker:*:heartbeat``: SCAN walks the *entire* keyspace,
    and with a day of completed-job hashes retained that's millions of keys
    every monitor tick.  The consumer list is small and bounded by the
    garbage collection in monitor.py.
    """
    try:
        consumers = client.xinfo_consumers(config.stream_key(queue), group)
    except redis.ResponseError:  # no stream or no group yet
        return [], set()
    names = [c["name"] for c in consumers]
    if not names:
        return consumers, set()
    pipe = client.pipeline(transaction=False)
    for name in names:
        pipe.exists(config.heartbeat_key(name))
    alive = {n for n, ok in zip(names, pipe.execute(), strict=True) if ok}
    return consumers, alive
