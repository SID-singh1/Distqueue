"""
config.py — Central configuration constants for distqueue.

All tuning knobs live here so they're easy to find and override later
(e.g. from env vars or a config file).  Having a single source of truth
prevents magic numbers scattered across modules and makes it obvious
what's adjustable vs. hard-coded.
"""

# ---------------------------------------------------------------------------
# Queue / Stream names
# ---------------------------------------------------------------------------

# The Redis Stream that acts as the main job queue.
# Using a prefix like "jobs:stream:" namespaces it clearly if the same Redis
# instance is shared with other applications.
QUEUE_NAME: str = "jobs:stream:default"

# Consumer group name.  All workers join this group so Redis distributes
# messages across them (each message delivered to exactly one consumer in
# the group, which is the whole point of consumer groups).
CONSUMER_GROUP: str = "workers"

# ---------------------------------------------------------------------------
# Dead-letter queue
# ---------------------------------------------------------------------------

# Jobs that exhaust all retry attempts land here for post-mortem inspection.
DLQ_STREAM: str = "jobs:dlq"

# ---------------------------------------------------------------------------
# Retry / backoff
# ---------------------------------------------------------------------------

# Base delay in seconds for exponential backoff.
# Formula: delay = min(BASE_BACKOFF_S * 2^attempts + jitter, MAX_BACKOFF_S)
#
# Why 2 seconds?  Small enough that transient errors recover fast on the
# first retry, but large enough to avoid a tight retry loop if the first
# attempt fails instantly.
BASE_BACKOFF_S: float = 2.0

# Maximum random jitter (in seconds) added to each backoff delay.
# Without jitter, jobs that fail together (e.g. from the same downstream
# outage) would all compute the exact same retry timestamp, wake up in
# lockstep, and re-create the load spike that caused the failure in the
# first place.  Adding uniform random jitter in [0, JITTER_MAX_S] spreads
# retries across a window so the downstream service sees a smooth trickle
# instead of a burst.
JITTER_MAX_S: float = 1.0

# Hard ceiling on backoff delay.  Without a cap, a job on its 10th attempt
# would wait 2 * 2^10 = 2048 s ≈ 34 minutes.  5 minutes is a reasonable
# upper bound for most workloads — long enough to let a downstream service
# recover, short enough that the job doesn't look "stuck."
MAX_BACKOFF_S: float = 300.0  # 5 minutes

# Default number of times a job will be attempted before it's moved to the
# DLQ.  Individual jobs can override this at enqueue time.
DEFAULT_MAX_ATTEMPTS: int = 5

# ---------------------------------------------------------------------------
# Heartbeat / worker liveness
# ---------------------------------------------------------------------------

# How often (in seconds) a worker refreshes its heartbeat key in Redis.
HEARTBEAT_INTERVAL_S: float = 5.0

# TTL on the heartbeat key.  If a worker doesn't refresh within this window,
# the monitor considers it dead.  Set to ~3× the interval so a single missed
# heartbeat doesn't trigger a false positive (network blip, GC pause, etc.).
HEARTBEAT_TTL_S: int = 15

# ---------------------------------------------------------------------------
# Scheduler (delayed-job re-injection)
# ---------------------------------------------------------------------------

# How often (in seconds) the scheduler polls the delayed ZSet for jobs whose
# next_retry_at has passed.  1 second is a good balance: fast enough that
# retried jobs don't sit idle noticeably longer than their computed backoff,
# but not so fast that the scheduler hammers Redis with ZRANGEBYSCORE on a
# mostly-empty set.  If the delayed set is typically large (thousands of
# pending retries), this can be lowered — the Lua script makes each move
# atomic, so overlapping polls from multiple scheduler instances won't
# double-inject.
SCHEDULER_POLL_INTERVAL_S: float = 1.0

# Maximum number of due jobs to move per poll iteration.  Without a cap, a
# sudden avalanche of due jobs (e.g. after a long outage where backoffs all
# converge) could make a single tick() iteration take an unbounded amount of
# time, starving the scheduler's own poll loop and delaying detection of
# newly-due jobs.  100 is large enough to drain normal bursts in a single
# pass, but small enough to keep each iteration predictable.  Any remaining
# due jobs will be picked up on the next tick(), one second later.
SCHEDULER_BATCH_SIZE: int = 100

# ---------------------------------------------------------------------------
# Monitor (dead-worker detection & job reclamation)
# ---------------------------------------------------------------------------

# How often (in seconds) the monitor scans the PEL for idle entries owned
# by dead workers.  5 seconds is a reasonable cadence: frequent enough that
# a dead worker's orphaned jobs get reclaimed within ~20 seconds (5s poll +
# up to 15s heartbeat TTL), but not so frequent that we spam Redis with
# XPENDING queries on an empty PEL.
MONITOR_POLL_INTERVAL_S: float = 5.0

# Minimum idle time (in milliseconds) a PEL entry must have before the
# monitor will even consider it for reclamation.  This is a pre-filter:
# entries idle for less than this are skipped entirely, regardless of
# heartbeat status.
#
# Set to 10 000 ms (10 seconds), which is deliberately BELOW HEARTBEAT_TTL_S
# (15 seconds).  Why not match or exceed the TTL?
#
# The idle-time filter is a coarse first pass that keeps the monitor from
# examining every single PEL entry on every tick.  The *actual* decision to
# reclaim is gated on the heartbeat key check (EXISTS worker:{id}:heartbeat).
# A job idle for 10s whose worker still has a valid heartbeat will NOT be
# reclaimed — the heartbeat check catches it and skips it.  Setting the idle
# floor below the TTL means we start *checking* entries slightly before
# the heartbeat fully expires, which gives us faster reclamation when a
# worker dies abruptly (heartbeat expires at ~15s, monitor can act on the
# next tick after that, rather than waiting for idle > 15s *and* then the
# next poll).
MONITOR_MIN_IDLE_MS: int = 10_000  # 10 seconds

# ---------------------------------------------------------------------------
# Redis key patterns
# ---------------------------------------------------------------------------

# Per-job metadata hash.  `{id}` is replaced with the job's UUID.
JOB_HASH_KEY_PREFIX: str = "job:"

# Sorted set for delayed/retry jobs.  Score = next_retry_at epoch timestamp.
DELAYED_ZSET: str = "jobs:delayed"

# Per-worker heartbeat key.  `{worker_id}` is replaced at runtime.
WORKER_HEARTBEAT_KEY_PREFIX: str = "worker:"
WORKER_HEARTBEAT_KEY_SUFFIX: str = ":heartbeat"

# ---------------------------------------------------------------------------
# Metrics (Prometheus)
# ---------------------------------------------------------------------------

# The port each process exposes its /metrics HTTP endpoint on.
# Using the same port number across worker, scheduler, and monitor processes
# is safe because each runs in its own Docker container (Milestone 7) with
# its own network namespace — no actual port collision, even though the
# integer is identical.  Prometheus's service-discovery config (or a
# docker-compose labels approach) maps each container's port independently.
# Keeping the port constant across roles simplifies the compose file and
# the Prometheus scrape config: one target template, not three.
METRICS_PORT: int = 9100
