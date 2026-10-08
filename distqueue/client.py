"""
client.py — Thin wrapper around redis.Redis for distqueue.

Why wrap at all?
  1. Single place to read connection config from environment variables,
     so every module doesn't repeat os.environ.get() boilerplate.
  2. We always want decode_responses=True (str, not bytes) and sane socket
     timeouts, set once rather than hoped-for at every call site.
  3. Later we may add Sentinel support — having a wrapper means that
     change is localised here.
"""

from __future__ import annotations

import os

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry


def get_redis_client(db: int | None = None) -> redis.Redis:
    """Create a Redis client from environment configuration.

    Environment variables
    ---------------------
    REDIS_URL : str, optional
        Full URL (``redis://[:password@]host:port/db``).  Takes precedence
        over the individual settings below; it's the form most hosted
        Redis offerings hand you.
    REDIS_HOST : str, default "localhost"
    REDIS_PORT : int, default 6379
    REDIS_DB   : int, default 0
    REDIS_SOCKET_TIMEOUT_S : float, default 10

    Parameters
    ----------
    db : int | None
        Override the database index (tests use a dedicated one).

    Why socket timeouts matter here
    -------------------------------
    redis-py's default socket timeout is *none*: a read waits forever.  If
    the network drops between worker and Redis without a TCP reset (a
    half-open connection — common with NAT, cloud load balancers, or a
    hard-killed VM), a worker blocked in XREADGROUP would hang forever:
    no exception, so the run loop's retry logic never runs.  A timeout
    turns that silent hang into a TimeoutError the loop can handle.

    It must exceed the longest *intended* block — XREADGROUP's BLOCK
    (2 s by default) — or every idle read would time out.  10 s leaves a
    wide margin.  ``health_check_interval`` makes redis-py PING a pooled
    connection that has sat idle, catching dead connections before a
    real command is sent on them.

    Why automatic retries are disabled
    ----------------------------------
    redis-py ≥ 6 retries every command up to 3 times on connection errors
    by default.  That is unsafe for non-idempotent commands: if an ENQUEUE
    script *ran* but the reply was lost to a dropped connection, the
    library would run it again and create a second job.  It also hides
    outages — one command against a dead Redis blocked for ~50 s here
    before raising.  Retries belong to the layer that knows whether the
    operation is safe to repeat: the run loops (runloop.py), which re-read
    state each tick, and producers, which can retry safely by passing an
    idempotency key.
    """
    timeout = float(os.environ.get("REDIS_SOCKET_TIMEOUT_S", "10"))
    common = {
        "decode_responses": True,
        "socket_timeout": timeout,
        "socket_connect_timeout": 5,
        "health_check_interval": 30,
        "retry": Retry(NoBackoff(), 0),
    }

    url = os.environ.get("REDIS_URL")
    if url:
        if db is not None:
            return redis.Redis.from_url(url, db=db, **common)
        return redis.Redis.from_url(url, **common)

    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        db=db if db is not None else int(os.environ.get("REDIS_DB", "0")),
        **common,
    )
