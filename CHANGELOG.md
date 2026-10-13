# Changelog

## 0.2.0: correctness, operability, proof

The 0.1.0 architecture (Redis Streams, consumer groups, a delayed ZSet,
heartbeat-gated reclamation) was sound. This release fixes the bugs that
appeared under failure, makes every state change atomic and fenced, adds the
operational features a real queue needs, and backs every claim with tests,
chaos experiments and benchmarks.

Commits: `1051616` (core), `3493d8c` (Docker stack), `1cb38e9` (chaos),
`e6d7f4b` (load test), `cef608a` (CI), and the docs commit.

### Bugs fixed

Each bug was **reproduced against 0.1.0** with a script, and each now has a
**regression test** that fails on 0.1.0 and passes on 0.2.0.

| # | Bug | Observed on 0.1.0 | Fix | Regression test |
|---|---|---|---|---|
| 1 | **Jobs lost on cold start.** The consumer group was created at `$` ("new entries only") | Jobs enqueued before the first worker started stayed PENDING forever: `process_one()` returned `False` with three jobs waiting | Group created at `0`; duplicate deliveries of finished jobs are skipped atomically | `test_jobs_enqueued_before_first_worker_are_processed` |
| 2 | **Monitor stole live workers' jobs.** `XAUTOCLAIM(start=id, count=1)` claims the *next* idle entry if `id` is gone | Monitor A asked for entry e1 (already reclaimed by monitor B) and claimed **e2, a live worker's job** | `XCLAIM` on the exact id with `MIN-IDLE-TIME`; safe with multiple monitors | `test_already_claimed_entry_does_not_spill_onto_neighbour`, `test_concurrent_monitors_reclaim_each_job_once` |
| 3 | **Stalled worker overwrote reclaimed jobs (no fencing).** Workers wrote the whole hash from a stale in-memory copy | After the reclaim, the job was `PENDING, attempts=1`. When the stalled worker woke it became **`COMPLETED, attempts=0`** while a retry was still queued. A poison job could reset its attempt counter forever | Every transition is a Lua script; worker writes check PEL ownership (`XPENDING … consumer`) and an attempts version, and are rejected if stale | `test_fencing.py` (8 tests), `chaos/pause_worker.py` |
| 4 | **Heartbeat thread died on one Redis error**, leaving a zombie worker | Heartbeat thread dead, key gone, but the worker **kept taking jobs** (which the monitor would then re-run) | The heartbeat never raises; failures are counted; a worker whose heartbeat is stale stops taking work (self-fencing) | `test_heartbeat_survives_redis_errors`, `test_worker_without_heartbeat_takes_no_work` |
| 5 | **Jitter discarded at the backoff cap.** `min(base·2ⁿ + jitter, cap)` | 1,000 samples at attempts=9 gave **exactly one distinct value: 300.0** (lockstep retries) | Equal jitter, `uniform(d/2, d)` applied after the cap; the spread scales with the delay | `test_jitter_survives_the_cap` and 7 more in `test_backoff.py` |
| 6 | **Retries leaked into the default queue.** The scheduler always re-injected into `jobs:stream:default` | A failed job from `emails` came back on `default` | `queue` stored on the job; the scheduler re-injects into the job's own stream | `test_job_returns_to_its_own_queue` |
| 7 | **Orphaned delayed job → stub hash → crash loop.** The scheduler's `HSET` created `{next_retry_at: ''}` for a missing job | `process_one()` raised `KeyError('id')`; the monitor would crash on the same entry after every restart | The move script drops orphans; unreadable hashes are dead-lettered (`trigger=corrupt`) instead of crashing | `test_orphan_is_dropped_without_creating_stub_hash`, `test_corrupt_hash_is_dead_lettered_*` |
| 8 | **"Queue depth" could only go up.** It was `XLEN`, and XACK doesn't delete entries | 5 jobs all COMPLETED, yet `distqueue_queue_depth = 5` | Backlog = consumer-group `lag`; streams trimmed with `XTRIM MINID ~` up to the oldest pending entry | `test_backlog_stays_correct_after_trimming`, `test_trim_drops_only_finished_entries` |
| 9 | **Any Redis error killed a role permanently** (no error handling, no restart policy) | After a FLUSHDB or restart without persistence, the worker thread died with `NOGROUP` | Shared run loop with jittered backoff; workers recreate a missing group; `restart: unless-stopped`; Redis AOF | `test_run_loop_survives_lost_consumer_group` |

Also fixed:

- **Unsafe client auto-retry.** redis-py ≥ 6 retries commands 3 times by
  default, so an ENQUEUE script whose reply was lost could run twice and
  create a duplicate job. One ping against a dead Redis also blocked for
  about 49 s. Automatic retries are now off and socket timeouts are set.
- **Integration tests wiped the live stack.** Tests ran FLUSHDB on DB 0,
  the stack's database, which deleted the consumer group under running
  workers, and those workers raced the tests for jobs. Tests now use DB 15,
  skip cleanly without Redis, and fail in CI if Redis is missing.
- **Failing unit test.** The histogram-bucket test checked private
  attributes; metric tests now assert on the scraped exposition text.
- **The monitor's PEL scan always restarted from the beginning**, so dead
  workers' entries behind a page of long-running jobs were never examined.
  It now uses a cursor.
- **Error messages lost the exception type** (`str(KeyError('x'))` is just
  `'x'`). They're now `ExceptionType: message`, truncated.
- **The dashboard showed one series per worker** with duplicate legends
  (no `sum by`). The producer's metrics were never scraped.

### Added

- **Atomic, fenced state machine** (`transitions.py`): ENQUEUE, START,
  COMPLETE, FAIL, MOVE_DUE, REPLAY.
- **Per-job timeouts**: the monitor reclaims jobs that run longer than
  `timeout_s`, even when their worker is alive.
- **Scheduled jobs** (`delay_s`, `run_at`), **idempotency keys**,
  **`PermanentError`** (skip retries), **named queues** end to end.
- **`distqueue` CLI**: `worker`, `scheduler`, `monitor`, `produce`,
  `enqueue`, `job`, `stats`, `dlq list`, `dlq replay`.
- **Retention**: TTLs on completed and dead jobs, stream trimming, cleanup of
  dead consumer names.
- **Metrics**: queue wait, end-to-end latency, backlog (lag), live workers,
  DLQ depth, lease lost (fencing), reclaims by reason, skipped deliveries,
  Redis errors. Cross-process durations use Redis's clock.
- **7 Prometheus alert rules**; a rebuilt Grafana dashboard.
- **Environment-variable configuration** for every tuning knob.
- **Chaos experiments** with pass/fail checks: SIGKILL a worker, freeze a
  worker past its TTL.
- **Load test** (`loadtest/benchmark.py`): drain throughput and latency at
  50% load against worker count.
- **CI**: lint, tests on 3.11 and 3.13, promtool, image build, and both chaos
  experiments.
- **Docs**: `docs/design.md`, `docs/benchmarks.md`, this changelog.

### Changed

- **Docker:** `.dockerignore` (the image no longer ships `.venv`; 45 MB of
  content), a cached dependency layer, a non-root user, and
  `ENTRYPOINT ["distqueue"]`. `scripts/run_*.py` removed.
- **Compose:** pinned images, restart policies, AOF persistence, 2 scheduler
  replicas, a 30 s stop grace period, the producer as a `demo`-profile
  service, Grafana anonymous role reduced to Viewer.
- **Backoff formula** changed to equal jitter (bug 5).
- **`Worker(queue_name=<stream key>)` → `Worker(queue=<name>)`**; metric
  label `queue` is now the queue name (`default`), not the key.
- **`distqueue_queue_depth` replaced** by `distqueue_queue_backlog`, plus
  `distqueue_stream_length`.
- **Job hash** gained `queue` and `timeout_s`. Hashes written by 0.1.0 still
  load, with defaults.

### Verified

| Check | Result |
|---|---|
| Test suite | 157 passed (69 unit, 88 integration), 4 consecutive runs, 92% line coverage |
| CI | All 5 jobs green (lint, tests 3.11 and 3.13, config, docker, chaos) |
| Chaos: SIGKILL | detected 19.0 s, retry running 22.0 s, completed 42.0 s; 1 attempt charged |
| Chaos: freeze | reclaimed 13.9 s; stale write rejected; completed once elsewhere |
| Load: 20 ms jobs | 44 → 342 jobs/s, 1 → 8 workers (97% efficiency), +1–3 ms overhead |
| Load: no-op | ceiling about 4.5k jobs/s (single Redis); p50 overhead 0.4–0.6 ms |
| Live stack | 7/7 Prometheus targets up; dashboard values match expected rates |

## 0.1.0

Initial build: Job model, MULTI/EXEC enqueue, consumer-group worker with
heartbeat, retry/DLQ, Lua-gated scheduler, XAUTOCLAIM monitor, Prometheus
metrics, Docker Compose with DNS service discovery, Grafana dashboard.
