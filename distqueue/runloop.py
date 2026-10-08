"""
runloop.py — The shared "tick until stopped, survive Redis errors" loop.

Every long-running role (worker, scheduler, monitor) is a loop around a
single-step ``tick()``.  This helper owns the part they all need:

* **Redis errors are survived, not fatal.**  A Redis restart, a failover,
  or a brief network partition raises ConnectionError / TimeoutError from
  whatever command happened to be in flight.  Before this helper, that
  exception escaped run(), the process exited, and — with no restart
  policy — the container stayed dead.  Now the loop backs off and retries.

* **Backoff with jitter, reusing backoff.compute_backoff.**  When Redis
  comes back after an outage, every worker, scheduler and monitor is
  retrying at once.  Without jitter they would all reconnect in the same
  instant — a thundering herd against a Redis that just restarted.

* **Non-Redis exceptions still crash.**  An AttributeError in our own code
  is a bug, not a transient condition; retrying it forever would hide it
  behind a log line.  Crashing lets the container restart policy and the
  `up == 0` alert surface it.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

import redis

from distqueue.backoff import compute_backoff
from distqueue.metrics import REDIS_ERRORS

logger = logging.getLogger(__name__)

ERROR_BACKOFF_BASE_S = 0.5
ERROR_BACKOFF_CAP_S = 30.0


def run_until_stopped(
    tick: Callable[[], object],
    stop_event: threading.Event,
    interval_s: float,
    component: str,
) -> None:
    """Call ``tick()`` every ``interval_s`` until ``stop_event`` is set.

    Uses ``stop_event.wait()`` rather than ``time.sleep()`` so a shutdown
    signal interrupts the wait immediately instead of after a full interval.
    """
    error_streak = 0
    while not stop_event.is_set():
        try:
            tick()
        except redis.RedisError as exc:
            error_streak += 1
            REDIS_ERRORS.labels(component=component).inc()
            delay = compute_backoff(
                error_streak, base_s=ERROR_BACKOFF_BASE_S, cap_s=ERROR_BACKOFF_CAP_S
            )
            logger.warning(
                "%s: Redis error (%s: %s); retry %d in %.1fs",
                component,
                type(exc).__name__,
                exc,
                error_streak,
                delay,
            )
            stop_event.wait(delay)
            continue
        error_streak = 0
        if interval_s > 0:
            stop_event.wait(interval_s)
