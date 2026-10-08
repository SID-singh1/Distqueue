"""
metrics.py — Prometheus metric definitions and server for distqueue.

All metric objects are module-level singletons, created once at import time
and shared across the process.  This is the standard prometheus_client
pattern: metrics are registered in a global CollectorRegistry, and the
/metrics HTTP endpoint serialises that registry on each scrape.

Naming convention: every metric starts with ``distqueue_`` so it's
immediately obvious in Grafana/PromQL which application owns it.

Who emits what
--------------
* **Event counters and latency histograms** are emitted by the process
  where the event happens (producer, worker, scheduler, monitor).
* **Queue-state gauges** (backlog, PEL size, delayed, DLQ, live workers)
  are set only by the monitor, which samples Redis once per tick.  One
  observer means one consistent snapshot per tick instead of N workers
  each racing to overwrite the same gauge with slightly different values.

The ``queue`` label is the logical queue name ("default"), not the Redis
key ("jobs:stream:default") — labels are for humans reading dashboards.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from distqueue import config

# ---------------------------------------------------------------------------
# Counters (monotonically increasing — reset only on process restart)
# ---------------------------------------------------------------------------

JOBS_ENQUEUED = Counter(
    "distqueue_jobs_enqueued_total",
    "Jobs accepted by enqueue() (excludes idempotent duplicates).",
    ["queue"],
)

# Separate from JOBS_ENQUEUED so "how many duplicate submissions are
# clients making?" is answerable on its own — a spike usually means a
# client is retrying requests it thinks timed out.
JOBS_DEDUPLICATED = Counter(
    "distqueue_jobs_deduplicated_total",
    "enqueue() calls that matched an existing idempotency key.",
    ["queue"],
)

JOBS_COMPLETED = Counter(
    "distqueue_jobs_completed_total",
    "Jobs whose handler returned normally.",
    ["queue"],
)

# outcome = what happened to the job (retried | dlq).
# trigger = why the attempt failed:
#   exception    handler raised
#   permanent    handler raised PermanentError (skips retries)
#   worker_death heartbeat expired; monitor reclaimed the job
#   timeout      job ran longer than its timeout_s; monitor reclaimed it
#   corrupt      job hash unreadable; dead-lettered without retry
# Keeping trigger as a label lets a dashboard separate application errors
# (exception) from infrastructure failures (worker_death) at a glance.
JOBS_FAILED = Counter(
    "distqueue_jobs_failed_total",
    "Failed job attempts, by outcome and trigger.",
    ["queue", "outcome", "trigger"],
)

# How often the monitor has to intervene.  Separate from JOBS_FAILED
# because it measures infrastructure health, not job outcomes.
JOBS_RECLAIMED = Counter(
    "distqueue_jobs_reclaimed_total",
    "Jobs reclaimed from workers by the monitor, by reason.",
    ["queue", "reason"],
)

# Deliveries a worker acknowledged without running: the job was already
# COMPLETED/DEAD (a duplicate delivery) or its hash no longer exists.
JOBS_SKIPPED = Counter(
    "distqueue_jobs_skipped_total",
    "Stream deliveries acknowledged without running the handler.",
    ["queue", "reason"],
)

# The fencing metric.  Each increment is a state write that was REJECTED
# because the writer no longer owned the job — e.g. a worker that was
# paused past its heartbeat TTL wakes up and tries to mark a job COMPLETED
# after the monitor already reclaimed it.  Non-zero values are expected
# occasionally (that's the fencing doing its job); a sustained rate means
# workers are routinely outliving their heartbeat (GC pauses, CPU
# starvation, a TTL that's too short).
LEASE_LOST = Counter(
    "distqueue_lease_lost_total",
    "State transitions rejected because the caller no longer owned the job.",
    ["queue", "transition"],
)

SCHEDULER_MOVED = Counter(
    "distqueue_scheduler_jobs_moved_total",
    "Delayed jobs moved back onto their stream by the scheduler.",
    ["queue"],
)

# A delayed-set member whose job hash no longer exists.  The scheduler
# drops it instead of injecting a pointer to nothing.
SCHEDULER_ORPHANS = Counter(
    "distqueue_scheduler_orphans_dropped_total",
    "Delayed-set entries dropped because their job hash was missing.",
)

HEARTBEAT_FAILURES = Counter(
    "distqueue_heartbeat_failures_total",
    "Heartbeat refreshes that failed (Redis unreachable or erroring).",
)

# Redis errors survived by a run loop.  Each increment is one tick that
# backed off instead of crashing the process.
REDIS_ERRORS = Counter(
    "distqueue_redis_errors_total",
    "Redis errors caught and retried by a component's run loop.",
    ["component"],
)

# ---------------------------------------------------------------------------
# Histograms (capture the distribution, not just the average)
# ---------------------------------------------------------------------------
#
# Why histograms rather than averages: averages hide tail latency.  A queue
# can have a comfortable p50 of 10 ms while its p99 is 5 s.  Dean &
# Barroso's "The Tail at Scale" (2013) shows that in fan-out systems the
# slowest component dominates, so p95/p99 are the numbers that matter.
#
# Why histograms rather than Summaries: Summary quantiles are computed
# inside each process and cannot be aggregated — the p99 of three workers'
# p99s is meaningless.  Histogram buckets are counters, so Prometheus can
# sum them across every worker and compute a fleet-wide quantile.

# Handler run time.  Buckets are dense between 0.1 s and 3 s (where the demo
# workload lives, since histogram_quantile interpolates linearly inside a
# bucket and wide buckets make p95 estimates coarse), and extend down to
# 5 ms for the load test's no-op handler and up to 5 min for long jobs.
JOBS_DURATION = Histogram(
    "distqueue_job_duration_seconds",
    "Time spent executing the job handler, in seconds.",
    ["queue"],
    buckets=(
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75,
        1, 1.5, 2, 3, 5, 10, 30, 60, 120, 300,
    ),
)  # fmt: skip

# Time between a stream entry being added and a worker starting it.  This
# is the queue's own contribution to latency (and the first thing that
# grows when workers can't keep up), so it's the SLI to alert on.
QUEUE_WAIT = Histogram(
    "distqueue_job_queue_wait_seconds",
    "Time a job's stream entry waited before a worker started it.",
    ["queue"],
    buckets=(
        0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5,
        10, 30, 60, 120, 300, 600, 1800,
    ),
)  # fmt: skip

# Enqueue → COMPLETED, including every retry and backoff along the way.
# The number a user of the queue actually experiences.
END_TO_END = Histogram(
    "distqueue_job_end_to_end_seconds",
    "Time from enqueue to successful completion, including retries.",
    ["queue"],
    buckets=(
        0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60,
        120, 300, 600, 1800, 3600,
    ),
)  # fmt: skip

# ---------------------------------------------------------------------------
# Gauges (point-in-time snapshots — set by the monitor every tick)
# ---------------------------------------------------------------------------

# Entries no worker has received yet: the consumer group's "lag".  This is
# the real backlog.  (XLEN is NOT the backlog: XACK does not remove entries
# from a stream, so XLEN counts every job ever enqueued until trimming.)
QUEUE_BACKLOG = Gauge(
    "distqueue_queue_backlog",
    "Stream entries not yet delivered to any worker (consumer-group lag).",
    ["queue"],
)

# Delivered but not yet acknowledged: jobs currently in flight.
PENDING_ENTRIES = Gauge(
    "distqueue_pending_entries",
    "Entries in the consumer group's Pending Entries List (in flight).",
    ["queue"],
)

# Entries physically retained in the stream.  Should stay close to
# backlog + pending; if it climbs steadily, trimming has stopped working.
STREAM_LENGTH = Gauge(
    "distqueue_stream_length",
    "Entries retained in the queue's stream (XLEN, after trimming).",
    ["queue"],
)

DELAYED_JOBS = Gauge(
    "distqueue_delayed_jobs",
    "Jobs waiting in the delayed set (retry backoff or scheduled start).",
)

DLQ_DEPTH = Gauge(
    "distqueue_dlq_depth",
    "Entries in the dead-letter stream.",
)

LIVE_WORKERS = Gauge(
    "distqueue_live_workers",
    "Workers with an unexpired heartbeat key.",
)


# ---------------------------------------------------------------------------
# Metrics HTTP server
# ---------------------------------------------------------------------------


def start_metrics_server(port: int = config.METRICS_PORT) -> None:
    """Start a background HTTP server that serves the /metrics endpoint.

    Prometheus operates on a **pull model**: it scrapes each target's
    /metrics endpoint on an interval rather than the application pushing
    metrics to a collector.  So:

      - The app doesn't need to know Prometheus's address or availability.
      - If Prometheus is down, the app keeps running — metrics are just
        not scraped until it comes back.
      - Each scrape gets a consistent snapshot of the in-memory registry.

    This just opens a passive HTTP server on a daemon thread and returns.
    """
    start_http_server(port)
