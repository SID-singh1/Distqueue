"""
client.py — Thin wrapper around redis.Redis for distqueue.

Why wrap at all?
  1. Single place to read connection config from environment variables,
     so every module doesn't repeat os.environ.get() boilerplate.
  2. We always want decode_responses=True (so we get str back, not bytes),
     and we want that set once, not hoped-for in every call site.
  3. Later we may add connection pooling, health checks, or Sentinel
     support — having a wrapper means those changes are localised here
     instead of scattered across the codebase.
"""

from __future__ import annotations

import os

import redis


def get_redis_client() -> redis.Redis:
    """Create and return a Redis client using env-var configuration.

    Environment variables
    ---------------------
    REDIS_HOST : str, default "localhost"
        Hostname or IP of the Redis server.
    REDIS_PORT : int, default 6379
        Port the Redis server is listening on.
    REDIS_DB   : int, default 0
        Redis database index.  We use 0 for everything; tests can
        override to an isolated DB if needed.

    Returns
    -------
    redis.Redis
        A synchronous Redis client with decode_responses=True.

    Notes
    -----
    This function creates a *new* client each time it's called.  For
    long-lived processes (workers, scheduler) that's fine — they call
    this once at startup.  If we ever need to share a connection pool
    across threads, we'll add a module-level singleton or a pool factory.
    """
    host: str = os.environ.get("REDIS_HOST", "localhost")
    port: int = int(os.environ.get("REDIS_PORT", "6379"))
    db: int = int(os.environ.get("REDIS_DB", "0"))

    return redis.Redis(
        host=host,
        port=port,
        db=db,
        # decode_responses=True makes Redis return Python str instead of
        # bytes.  This keeps our serialization code (job.py) simpler —
        # no .decode() calls everywhere.
        decode_responses=True,
    )
