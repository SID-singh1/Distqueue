"""
demo.py — The handler the Docker stack runs, built to exercise every path.

It exists to produce interesting, realistic data for Prometheus and Grafana:
variable run times, transient failures that succeed on retry, and the
occasional permanent failure that goes straight to the DLQ.

Knobs (environment variables):
    DEMO_MIN_S / DEMO_MAX_S     uniform run-time range (default 0.1–1.0 s)
    DEMO_FAILURE_RATE           chance of a transient error (default 0.10)
    DEMO_PERMANENT_RATE         chance of a PermanentError (default 0.01)

Payload overrides (used by the chaos tests):
    {"sleep_s": 20}             sleep exactly that long and succeed — no
                                random failures, so a chaos run proves the
                                worker-death recovery path in isolation
                                rather than mixing it with ordinary
                                application failures.
    {"fail": "permanent"}       raise PermanentError
    {"fail": "transient"}       raise RuntimeError
"""

from __future__ import annotations

import os
import random
import time
from typing import Any

from distqueue.errors import PermanentError

MIN_S = float(os.environ.get("DEMO_MIN_S", "0.1"))
MAX_S = float(os.environ.get("DEMO_MAX_S", "1.0"))
FAILURE_RATE = float(os.environ.get("DEMO_FAILURE_RATE", "0.1"))
PERMANENT_RATE = float(os.environ.get("DEMO_PERMANENT_RATE", "0.01"))


def handler(payload: dict[str, Any]) -> None:
    """Simulate a job: sleep a while, sometimes fail."""
    if "sleep_s" in payload:
        time.sleep(float(payload["sleep_s"]))
        return

    forced = payload.get("fail")
    if forced == "permanent":
        raise PermanentError("payload requested a permanent failure")
    if forced == "transient":
        raise RuntimeError("payload requested a transient failure")

    time.sleep(random.uniform(MIN_S, MAX_S))

    roll = random.random()
    if roll < PERMANENT_RATE:
        raise PermanentError("simulated permanent failure (e.g. invalid payload)")
    if roll < PERMANENT_RATE + FAILURE_RATE:
        raise RuntimeError(f"simulated transient failure (rate {FAILURE_RATE:.0%})")


def noop(payload: dict[str, Any]) -> None:
    """Do nothing.  The load test uses this to measure queue overhead alone."""
