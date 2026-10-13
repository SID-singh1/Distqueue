# Project Context — distqueue

Save this as `AGENTS.md` at the project root (Workspace-scoped rule, not
Global) so it applies automatically to every prompt in this project without
needing to be repasted.

## Stop condition (important — auto-continue is likely enabled)

Only implement what the current prompt explicitly asks for, then stop and
report back. Do not chain forward into the next milestone, file, or feature
on your own reasoning, even if it seems like a natural next step or would
be more "efficient" to bundle in. Each prompt corresponds to one commit;
running ahead breaks that structure and skips the checkpoint where the
person reviews and reports results before the next stage begins.

## What this is

`distqueue` is a Redis-backed distributed job queue and task scheduler,
built from scratch in Python as a portfolio project. The person building it
has a strong ML/backend background (FastAPI, Airflow/dbt ETL pipelines,
Qdrant, OAuth, Docker, AWS) but this is their **first from-scratch systems
project** — no prior hands-on experience building distributed primitives
(consumer groups, heartbeat-based failure detection, backoff/retry logic).

Two goals, equally important:
1. A working, demoable system: queue core → distributed workers → horizontal
   scaling → observability → chaos test → load test → CI/CD.
2. The person needs to understand *every* design decision well enough to
   defend it cold in a systems-design interview. Code comments and any
   explanations you give should include the "why," not just the "what."

## Non-negotiable constraints

- **Stack:** Python, Redis, Docker, Prometheus, Grafana, GitHub Actions.
  No additional frameworks (no Celery, no Kafka, no message-queue
  abstraction libraries). If a task seems to need something outside this
  stack, flag it instead of adding a dependency.
- **No scope creep.** Only implement what the current prompt asks for. If a
  prompt says "don't implement X yet," do not implement X yet, even if it
  seems like it would be convenient to bundle it in.
- **Commit granularity matters.** This project is being built prompt-by-prompt
  specifically so the git history reads as a coherent build log (one logical
  unit of work per prompt/commit — e.g. "add Job model," "add producer
  enqueue logic," "add consumer group + ack," not one giant dump). Don't
  pre-build later milestones early even if it'd be more efficient.
- **Every module needs comments that explain *why*, not just *what*.**
  E.g. not just "# increment attempts" but "# a worker crash counts as a
  failed attempt too, otherwise a poison job that kills its worker retries
  forever."
- **Tests ship with the code that needs them**, in the same prompt/commit,
  not deferred to a later cleanup pass.
- If a prompt is ambiguous or you (Cursor) have to make a nontrivial design
  choice not already specified, state the assumption explicitly in the
  response rather than silently picking one — this project doubles as
  interview prep, so undocumented decisions are wasted learning.

## Architecture (do not deviate without discussion)

```
Producer(s) → Redis Stream per queue → Consumer Group ("workers")
   │ delay/run_at        ↑                     ↓ (PEL: pending entries list = lease table)
   └──→ Delayed ZSet ←─ retry/backoff ─────────┘
              ↓ (scheduler replicas poll, ZREM-gated re-inject into the job's own queue)
        DLQ Stream (after max_attempts, or PermanentError)

Heartbeat keys (per worker, TTL) → Monitor → owner dead, or job past timeout_s →
    XCLAIM that exact entry → FAIL transition (attempts++) → retry or DLQ
```

Every job state change is ONE Lua script in `distqueue/transitions.py`
(ENQUEUE, START, COMPLETE, FAIL, MOVE_DUE, REPLAY). Worker-side writes are
fenced: they check `XPENDING ... <consumer>` ownership inside the script and
are rejected if the monitor has reclaimed the entry. See `docs/design.md`.

### Redis data model

| Key | Type | Purpose |
|---|---|---|
| `jobs:stream:{queue}` | Stream | per-queue work stream; entry = `{job_id}` only; group `workers` |
| `job:{id}` | Hash | `id, queue, payload, status, attempts, max_attempts, timeout_s, created_at, updated_at, next_retry_at, last_worker, last_error` |
| `jobs:delayed` | ZSet | retries + scheduled jobs; score = due time (Redis `TIME`), member = `job_id` |
| `jobs:dlq` | Stream | `job_id, queue, reason, attempts, trigger, failed_at` |
| `jobs:queues` | Set | registry of queue names |
| `worker:{id}:heartbeat` | String w/ TTL | refreshed by each worker's heartbeat thread |
| `idem:{key}` | String w/ TTL | idempotency key → job_id |

### Job state machine

```
enqueue → PENDING (stream, or delayed set if scheduled)
PENDING → START (owner check, skip if COMPLETED/DEAD) → RUNNING
RUNNING → success → COMPLETE (owner check, XACK, TTL) → COMPLETED
RUNNING → exception → FAIL (owner + attempts check) → attempts++ →
     attempts < max_attempts → jobs:delayed with backoff → PENDING (later)
     attempts >= max_attempts or PermanentError → jobs:dlq → DEAD
RUNNING → worker dies (heartbeat expires) or exceeds timeout_s →
     monitor XCLAIMs the entry → same FAIL fork (a lost attempt is an attempt)
DEAD → `distqueue dlq replay` → PENDING (attempts reset)
```

There is no CLAIMED status: the PEL is the record of who holds what.

Backoff formula ("equal jitter", see `distqueue/backoff.py`):
`d = min(max_delay, base * 2**attempts); delay = uniform(d/2, d)`.
Jitter is required, not optional — without it, jobs that fail together
retry in lockstep and recreate the load spike that caused the failure.
(The original `min(base * 2**attempts + jitter, max_delay)` discarded the
jitter once the cap was reached, so capped retries ran in lockstep.)

## Repo structure (current)

```
distqueue/
├── distqueue/
│   ├── transitions.py    # the state machine: every transition as one fenced Lua script
│   ├── job.py            # Job dataclass, JobStatus, serialization
│   ├── client.py         # Redis connection (timeouts, no client auto-retry)
│   ├── producer.py       # enqueue(): queues, delay/run_at, idempotency keys
│   ├── worker.py         # consume loop, heartbeat thread, self-fencing
│   ├── scheduler.py      # delayed ZSet -> stream, replica-safe
│   ├── monitor.py        # XCLAIM dead/timed-out jobs, trim, consumer GC, gauges
│   ├── backoff.py        # equal-jitter backoff (pure, unit-tested)
│   ├── runloop.py        # shared tick loop that survives Redis errors
│   ├── stats.py          # queue stats for the monitor and the CLI
│   ├── metrics.py        # prometheus_client instrumentation
│   ├── config.py         # env-overridable settings + key layout
│   ├── cli.py / entrypoints.py / demo.py / errors.py
├── tests/{unit,integration}/
├── chaos/{kill_worker.py, pause_worker.py}
├── loadtest/benchmark.py     # stdlib harness (replaced the planned locustfile)
├── docker/{docker-compose.yml, prometheus.yml, alerts.yml, grafana/}
├── Dockerfile, .dockerignore
├── docs/{design.md, benchmarks.md, images/}
└── .github/workflows/ci.yml
```

## Working process

I (the person) am pasting prompts into you one stage at a time and reporting
results back to a separate planning conversation. Treat each prompt as
scoped and final for that step — implement exactly what's asked, run/verify
what can be verified locally, and tell me clearly:
- what you built,
- how to verify it works,
- any assumptions you made,
- anything that seems off from the architecture above so I can flag it
  upstream before we build on top of it.
