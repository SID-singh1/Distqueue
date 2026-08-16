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
Producer(s) → Redis Stream (queue) → Consumer Group ("workers")
                    ↑                        ↓ (PEL: pending entries list)
              Delayed ZSet ←─ retry/backoff ─┘
                    ↓ (scheduler polls, re-injects when due)
              DLQ Stream (after max_attempts)

Heartbeat keys (per worker, TTL) → Monitor process → detects dead worker →
    XAUTOCLAIM job from dead consumer → increment attempts → re-inject or DLQ
```

### Redis data model

| Key | Type | Purpose |
|---|---|---|
| `jobs:stream:{queue}` | Stream | main queue; consumer group `workers` reads from it |
| `job:{id}` | Hash | `payload, status, attempts, max_attempts, created_at, updated_at, next_retry_at, last_worker, last_error` |
| `jobs:delayed` | ZSet | score = `next_retry_at` epoch, member = `job_id` |
| `jobs:dlq` | Stream | permanently failed jobs, same shape as main stream + failure metadata |
| `worker:{id}:heartbeat` | String w/ TTL | refreshed periodically by each worker |

### Job state machine

```
PENDING → CLAIMED (in PEL) → RUNNING
RUNNING → success → XACK → COMPLETED
RUNNING → exception → attempts++ →
     attempts < max_attempts → scheduled in jobs:delayed with backoff → PENDING (later)
     attempts >= max_attempts → moved to jobs:dlq → DEAD
CLAIMED/RUNNING → worker dies (heartbeat expires) → monitor XAUTOCLAIMs job →
     attempts++ → same fork as above
```

Backoff formula: `delay = min(base * 2**attempts + jitter, max_delay)`.
Jitter is required, not optional — without it, jobs that fail together
retry in lockstep and recreate the load spike that caused the failure.

## Repo structure (target — build toward this, don't jump ahead)

```
distqueue/
├── distqueue/
│   ├── job.py            # Job dataclass, serialization
│   ├── client.py          # Redis connection wrapper
│   ├── producer.py        # enqueue()
│   ├── worker.py           # consume loop, heartbeat emit, ack/nack
│   ├── scheduler.py       # polls delayed ZSet, re-injects due jobs
│   ├── monitor.py          # heartbeat reaper, XAUTOCLAIM dead-worker jobs
│   ├── config.py          # backoff params, timeouts, queue names
│   └── metrics.py         # prometheus_client instrumentation
├── tests/{unit,integration}/
├── chaos/kill_worker.py
├── loadtest/locustfile.py
├── docker/{docker-compose.yml, Dockerfile, prometheus.yml}
├── grafana/dashboards/queue.json
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
