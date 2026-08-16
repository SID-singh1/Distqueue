"""
metrics.py — Prometheus metric definitions and server for distqueue.

All metric objects are module-level singletons, created once at import time
and shared across the process.  This is the standard prometheus_client
pattern: metrics are registered in a global CollectorRegistry, and the
/metrics HTTP endpoint serialises that registry on each scrape.

Naming convention: every metric starts with ``distqueue_`` so it's
immediately obvious in Grafana/PromQL which application owns it, even
when mixed with system-level metrics (node_*, process_*, etc.).
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from distqueue import config

# ---------------------------------------------------------------------------
# Counters (monotonically increasing — reset only on process restart)
# ---------------------------------------------------------------------------

# How many jobs have been enqueued.  A Counter because jobs are only ever
# added, never "un-enqueued."
JOBS_ENQUEUED = Counter(
    "distqueue_jobs_enqueued_total",
    "Total number of jobs added to the queue.",
    ["queue"],
)

# How many jobs have completed successfully.  Counter for the same reason:
# completions only go up.
JOBS_COMPLETED = Counter(
    "distqueue_jobs_completed_total",
    "Total number of jobs that completed successfully.",
    ["queue"],
)

# How many jobs have failed (either retried or sent to DLQ).  The "outcome"
# label distinguishes retry from permanent failure; "trigger" distinguishes
# a handler exception from a dead-worker reclaim.  Counter because failures
# only accumulate.
JOBS_FAILED = Counter(
    "distqueue_jobs_failed_total",
    "Total number of job failures, labelled by outcome and trigger.",
    ["queue", "outcome", "trigger"],
)

# How many jobs the monitor has reclaimed from dead workers.  Separate from
# JOBS_FAILED because "reclaimed" and "failed" answer different questions:
# reclaimed measures how often the monitor has to intervene (infrastructure
# health), while failed measures what happens to jobs overall (application
# health).  Counter because reclamations only accumulate.
JOBS_RECLAIMED = Counter(
    "distqueue_jobs_reclaimed_total",
    "Total number of jobs reclaimed from dead workers by the monitor.",
    ["queue"],
)

# ---------------------------------------------------------------------------
# Histogram (captures distribution, not just average)
# ---------------------------------------------------------------------------

# How long each job's handler takes to execute (success or failure).
# A Histogram rather than a simple average because averages hide tail
# latency — a system can have a comfortable p50 of 10ms while its p99
# is 5 seconds, meaning 1 in 100 users waits 500× longer.  As Dean &
# Barroso's "The Tail at Scale" (2013) showed, in fan-out architectures
# the overall latency is dominated by the slowest component, so tracking
# percentiles (p50/p95/p99) via histogram buckets is essential for
# identifying and addressing tail latency before it compounds across
# services.
#
# Buckets span from 10ms (fast synchronous jobs like cache lookups or
# lightweight API calls) through 120s (long-running batch-style jobs
# like ML inference, video transcoding, or large file processing).
JOBS_DURATION = Histogram(
    "distqueue_job_duration_seconds",
    "Time spent executing the job handler, in seconds.",
    ["queue"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120),
)

# ---------------------------------------------------------------------------
# Gauges (point-in-time snapshots — can go up or down)
# ---------------------------------------------------------------------------

# Current length of the main stream.  A Gauge because the stream grows
# (new jobs enqueued) and shrinks (entries trimmed/expired).  Updated by
# the scheduler on each tick().
QUEUE_DEPTH = Gauge(
    "distqueue_queue_depth",
    "Current number of entries in the main job stream.",
    ["queue"],
)

# Current size of the delayed ZSet.  Gauge because jobs enter (on failure
# with retries remaining) and leave (when the scheduler re-injects them).
# Updated by the scheduler on each tick().
DELAYED_JOBS = Gauge(
    "distqueue_delayed_jobs",
    "Current number of jobs in the delayed retry set.",
    ["queue"],
)

# Current total PEL size (unfiltered — all pending entries, not just idle
# ones).  Gauge because entries enter (XREADGROUP delivery) and leave
# (XACK).  Updated by the monitor on each tick().
PENDING_ENTRIES = Gauge(
    "distqueue_pending_entries",
    "Current total number of entries in the consumer group's PEL.",
    ["queue"],
)


# ---------------------------------------------------------------------------
# Metrics HTTP server
# ---------------------------------------------------------------------------


def start_metrics_server(port: int = config.METRICS_PORT) -> None:
    """Start a background HTTP server that serves the /metrics endpoint.

    Prometheus operates on a **pull model**: the monitoring server itself
    scrapes each target's /metrics endpoint on a configurable interval
    (typically 15–30 seconds), rather than the application pushing metrics
    to a central collector.  This design means:

      - The app doesn't need to know Prometheus's address or availability.
      - If Prometheus is down, the app keeps running — metrics are just
        not scraped until it comes back.
      - Each scrape gets a consistent, point-in-time snapshot of all
        metrics from the process's in-memory registry.

    Because of this pull model, this function just opens a passive HTTP
    server on a background daemon thread and returns immediately.  There
    is no ongoing push loop — Prometheus comes to us.
    """
    start_http_server(port)
