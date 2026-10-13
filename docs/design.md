# distqueue design

How distqueue works, why each decision was made, and what it does when
things break. Code comments explain the local "why"; this document explains
how the pieces fit together.

## 1. Guarantees, stated precisely

| Property | Guarantee | Mechanism |
|---|---|---|
| Delivery | **At-least-once** | Consumer-group PEL + explicit `XACK`; un-acked entries are reclaimed |
| Job state | **Linearizable per job** | Every transition is one Lua script; worker writes are fenced on PEL ownership |
| Enqueue | **Exactly-once with an idempotency key** | Key check and job creation happen in the same script |
| Retries | Bounded by `max_attempts` | Crashes, timeouts and exceptions each consume one attempt |
| Ordering | FIFO delivery per queue; **no** completion order | One stream per queue; workers run in parallel |

**Not guaranteed:** exactly-once *execution*. A worker that stalls past its
heartbeat TTL may finish a job the monitor has already handed to someone
else. Fencing stops the stale worker from corrupting the job's **state**, but
it cannot undo the handler's **side effects**. Handlers must be idempotent.
This is the same contract SQS, Celery (`acks_late`) and Sidekiq offer.

## 2. Architecture

```
            enqueue (Lua)                     XREADGROUP
Producer ───────────────▶ jobs:stream:{q} ─────────────────▶ Worker × N
   │                          ▲    │                           │  heartbeat thread
   │ delay/run_at             │    │ PEL (who holds what)      │  SET worker:{id}:heartbeat EX ttl
   ▼                          │    ▼                           │
jobs:delayed (ZSet) ──────────┘  Monitor ── XCLAIM dead/timed-out entries
   ▲      Scheduler × M            │   ── trim streams, GC consumers, publish gauges
   │      (ZREM-gated move)        │
   └──── FAIL: retry with backoff ─┴──── FAIL: max attempts ──▶ jobs:dlq
```

| Key | Type | Purpose |
|---|---|---|
| `jobs:stream:{queue}` | Stream | Work queue; entry = `{job_id}` pointer only |
| `job:{id}` | Hash | Authoritative job state (single source of truth) |
| `jobs:delayed` | ZSet | Retries and scheduled jobs; score = due time (Redis clock) |
| `jobs:dlq` | Stream | Dead jobs + `queue, reason, attempts, trigger, failed_at` |
| `jobs:queues` | Set | Registry of queue names (avoids keyspace `SCAN`) |
| `worker:{id}:heartbeat` | String + TTL | Liveness |
| `idem:{key}` | String + TTL | Idempotency key → job id |

**Why the stream carries only a pointer.** If the entry duplicated the
payload or attempt count, every retry would leave two copies that disagree.
The hash is the only copy, and the entry just says which job to look up.

## 3. State machine

```
            ┌──────────── scheduler (due) ─────────────┐
            ▼                                          │
enqueue → PENDING ──START──▶ RUNNING ──COMPLETE──▶ COMPLETED (TTL 1 d)
 (or delayed)                  │
                               ├── FAIL, attempts < max ──▶ PENDING in jobs:delayed
                               └── FAIL, attempts ≥ max  ──▶ DEAD + jobs:dlq (TTL 7 d)
                                     ▲
           monitor: heartbeat expired │ or ran past timeout_s (XCLAIM, then FAIL)
DEAD ──dlq replay──▶ PENDING (attempts reset)
```

Every arrow is one Lua script in [`transitions.py`](../distqueue/transitions.py).
There is no `CLAIMED` status. The PEL already records which consumer holds
which entry, and copying that into the hash would create a second source of
truth that could disagree with the first.

## 4. Atomic transitions and fencing

`MULTI/EXEC` makes a batch of commands *isolated*: no other client's commands
interleave with them. But it can't *decide* anything, because every queued
command runs unconditionally. Each transition here needs to check something
first and then write:

| Transition | Check before writing |
|---|---|
| ENQUEUE | idempotency key unused |
| START | caller owns the entry; job not already COMPLETED/DEAD; hash exists |
| COMPLETE | caller owns the entry |
| FAIL | caller owns the entry; `attempts` unchanged since the caller read it |
| MOVE_DUE | this caller's `ZREM` removed the member; hash exists |
| REPLAY | job is DEAD |

A Lua script runs atomically inside Redis, so nothing can happen between the
check and the write.

**Fencing.** The question "does this caller still own the job?" is answered
by `XPENDING stream group id id 1 consumer`. In effect the PEL *is* a lease
table, and the monitor's `XCLAIM` transfers the lease. A worker that paused
for 20 s and then wakes up finds that it no longer owns the entry, so its
COMPLETE or FAIL is rejected. That rejection is counted in
`distqueue_lease_lost_total`. This is the "fencing token" argument from
Kleppmann's *How to do distributed locking*: a process can't reliably know
its own lease has expired, so the storage layer has to refuse its writes.

`chaos/pause_worker.py` demonstrates this live: it freezes a worker with
`docker pause`, lets the monitor reclaim its job, unfreezes the worker, and
checks that the stale write was rejected.

## 5. Failure detection

| Signal | Proves | Doesn't prove |
|---|---|---|
| Heartbeat key (TTL 15 s, refreshed every 5 s) | the worker *process* is alive | the job is making progress |
| `timeout_s` vs PEL idle time | the job has run too long | the worker is dead |

The monitor reclaims an entry when it has been idle for at least 10 s **and**
either the owner's heartbeat is gone or the job has run longer than its
`timeout_s`. PEL idle time is "time since delivery", which for a running job
is exactly its run time so far.

**Why XCLAIM, not XAUTOCLAIM.** `XAUTOCLAIM(start=id, count=1)` claims the
first idle entry *at or after* `id`, whatever consumer owns it. If `id` was
claimed a moment earlier by another monitor (which resets its idle time),
XAUTOCLAIM moves on and takes the *next* entry, which may belong to a healthy
worker partway through a job. `XCLAIM id MIN-IDLE-TIME t` either claims
exactly that entry or returns nothing, and Redis re-checks the idle time
atomically. That is what makes running several monitors safe.

**Heartbeat thread.** The heartbeat runs on its own thread so that a long
handler doesn't let the key expire. It catches every exception, because
before this fix a single Redis blip killed the thread silently and left a
zombie worker that kept taking jobs the monitor would then re-run. A worker
whose last successful heartbeat is older than its TTL also **stops taking new
jobs** (self-fencing). Anything it took could be reclaimed out from under it.

**Pending-list scan cursor.** Each tick the monitor examines at most 100
entries and then resumes where it stopped. Previously it always restarted
from the beginning, so a page full of live workers' long jobs could hide
dead workers' entries indefinitely.

## 6. Retries and backoff

```
d     = min(300 s, 2 s × 2^attempts)
delay = uniform(d/2, d)            # "equal jitter"
```

- **Jitter is applied after the cap.** The original
  `min(base·2ⁿ + jitter, cap)` returned exactly `cap` once the exponential
  term passed it, so every capped retry fired at the same instant.
- **The jitter scales with the delay.** A fixed 1 s of jitter spreads a 4 s
  retry but does nothing for a 128 s one.
- **Equal jitter rather than full jitter.** Full jitter, `uniform(0, d)`,
  spreads load slightly better but can retry roughly 0 s after a failure,
  right back into a downstream service that just signalled trouble. Both
  schemes come from Brooker, *Exponential Backoff And Jitter* (AWS, 2015).
- **Raise `PermanentError`** to skip retries entirely, for failures that will
  happen again identically such as validation errors.

Due times are computed with Redis's `TIME` inside the FAIL script, and the
scheduler compares them against Redis's `TIME` too. One clock means clock
skew between hosts can't shift retries earlier or later.

## 7. Scheduler

The scheduler runs a `ZRANGEBYSCORE ≤ now LIMIT 100` and then, for each due
job, a script that moves it only if `ZREM` returned 1. Two schedulers can see
the same due job, but only one `ZREM` can succeed. So scheduler replicas need
**no leader election**: the compose file runs two, and losing one has no
failover delay. Each tick takes four round trips (TIME, ZRANGEBYSCORE, a
pipelined HGET of each job's queue, and pipelined EVALSHAs) no matter how
many jobs are due.

## 8. Retention

| Data | Kept for | Why |
|---|---|---|
| Stream entries | Until every group has acked them; the monitor then runs `XTRIM MINID ~` | `XACK` doesn't delete; without trimming the stream grows forever |
| COMPLETED hashes | 24 h | Enough to answer "what happened to job X?" |
| DEAD hashes | 7 days | Enough time to inspect and replay |
| Idempotency keys | 24 h | The dedupe window |
| Dead consumers | Removed after 1 h idle with nothing pending | Every restart creates a new random consumer name |

The trim cut point is the older of "just past the group's last-delivered-id"
and "the oldest pending entry", so an undelivered or unacked entry is never
removed.

## 9. Observability

- **Backlog is `XINFO GROUPS` lag, not `XLEN`.** XLEN counts every retained
  entry, acked or not, so before trimming it could only ever go up.
- **Three latencies:** queue wait (the SLI: are workers keeping up?),
  handler duration (is the work slow?), and end-to-end (what users
  experience, retries included). Queue wait and end-to-end are computed on
  Redis's clock: from the stream entry id and from `created_at`.
- **Histograms, not Summaries**, because histogram buckets can be summed
  across workers and summary quantiles cannot.
- **Gauges come from a single observer (the monitor)**, giving one consistent
  snapshot per tick.
- **Alerts** (`docker/alerts.yml`) cover p95 queue wait, a growing backlog,
  no live workers, dead-lettering, rejected stale writes, Redis errors, and
  components being down.

## 10. Failure modes

| What fails | What happens | Recovery |
|---|---|---|
| Worker SIGKILLed mid-job | Heartbeat expires; the monitor reclaims within about 20 s (measured: 19 s); 1 attempt charged | Automatic (`chaos/kill_worker.py`) |
| Worker paused past its TTL | Reclaimed as above; its late write is rejected by fencing | Automatic (`chaos/pause_worker.py`) |
| Handler hangs forever | Reclaimed once `timeout_s` passes | Automatic; the stuck thread stays stuck |
| Handler keeps crashing the worker (poison job) | Each crash costs an attempt, then the job goes to the DLQ | `distqueue dlq replay` once fixed |
| Corrupt job hash | Dead-lettered as `trigger=corrupt`; no crash loop | Manual |
| All schedulers down | Retries wait in the ZSet; nothing is lost | Resumes on restart |
| All monitors down | Dead workers' jobs wait in the PEL; nothing is lost | Resumes on restart |
| Redis restarts | AOF replays at most about 1 s of lost writes; run loops back off and reconnect; a missing group is recreated | Automatic |
| Redis unreachable mid-COMPLETE | Job stays pending; reclaimed after `timeout_s`; runs again | At-least-once |
| Client retries an enqueue that timed out | A duplicate job, unless an idempotency key is used | Use `idempotency_key` |

## 11. Known limitations and next steps

- **Redis is a single point of failure.** Production needs replicas plus
  Sentinel, or a managed Redis. With asynchronous replication, a failover can
  lose about 1 s of acknowledged writes.
- **Not Redis Cluster ready.** Each script touches several keys that would
  hash to different slots. The fix is a shared hash tag in every key name
  (`{dq}:job:<id>`).
- **One job at a time per worker process.** You scale by adding processes or
  containers. A thread pool with a prefetch window would help I/O-bound
  handlers, at the cost of more in-flight work lost per crash.
- **No priorities or per-tenant fairness.** These could be built with one
  stream per priority and weighted reads.
- **No periodic (cron) jobs.** Scheduled jobs exist (`delay_s` / `run_at`);
  recurring ones would need a schedule table and next-run computation.
