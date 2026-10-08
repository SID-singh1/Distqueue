"""
errors.py — Exceptions that handlers can raise to steer retry behaviour.

Why does the queue need its own exception type?
  By default every handler exception is treated as *transient*: the job is
  retried with backoff until max_attempts runs out.  That's the right
  default for timeouts, 503s and connection resets — but wrong for errors
  that will fail identically on every attempt, like a payload that fails
  validation or a reference to a row that no longer exists.  Retrying
  those just burns worker time and delays the inevitable DLQ entry by the
  sum of every backoff (minutes, with default settings).

  Raising PermanentError tells the worker "retrying cannot help", so the
  job goes straight to the dead-letter queue on the first failure.
"""

from __future__ import annotations


class PermanentError(Exception):
    """Raise from a handler to dead-letter the job without retrying."""
