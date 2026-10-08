# Benchmarks

Measured with [`loadtest/benchmark.py`](../loadtest/benchmark.py), run inside
the Docker network so every Redis call is container-to-container:

```bash
docker compose -f docker/docker-compose.yml up -d redis
docker run --rm --network distqueue_default -e REDIS_HOST=redis -v "${PWD}:/src" -w /src \
  --entrypoint python distqueue:local -m loadtest.benchmark --workers 1,2,4,8 --jobs 10000 --latency-jobs 3000
```

**Environment:** laptop, 8 vCPUs (Docker Desktop / WSL2), Redis 7.4.11,
Python 3.13. Workers are separate processes, so these numbers reflect K
worker containers rather than threads sharing one GIL. Absolute numbers are
machine-specific; the shape of the curves is the point.

For each worker count K the harness measures:

- **Drain throughput.** N jobs are enqueued first, then K workers drain
  them. That's the maximum service rate for K workers.
- **End-to-end latency at 50% load.** Jobs arrive open-loop at half the
  measured drain rate. Latency is COMPLETED time minus `created_at`, both
  stamped by Redis's clock inside the transition scripts, so there is no
  cross-process clock skew.

## Realistic jobs (20 ms of simulated I/O per job)

| Workers | Drain (jobs/s) | Scaling efficiency | e2e p50 | e2e p95 | e2e p99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 44 | 100% | 23.0 ms | 23.9 ms | 24.6 ms |
| 2 | 87 | 99% | 23.0 ms | 23.8 ms | 24.4 ms |
| 4 | 173 | 98% | 22.2 ms | 23.2 ms | 23.6 ms |
| 8 | 342 | 97% | 21.3 ms | 21.8 ms | 22.1 ms |

**Throughput scales almost linearly with workers (97% efficiency at 8).**
End-to-end latency is the 20 ms of work plus about **1–3 ms of queue
overhead**: enqueue, delivery, START and COMPLETE.

## Queue overhead alone (no-op handler)

| Workers | Drain (jobs/s) | Scaling efficiency | e2e p50 | e2e p95 | e2e p99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 2,065 | 100% | 0.4 ms | 0.5 ms | 1.8 ms |
| 2 | 2,636 | 64% | 0.5 ms | 0.6 ms | 0.9 ms |
| 4 | 3,750 | 45% | 0.6 ms | 1.1 ms | 1.7 ms |
| 8 | 4,518 | 27% | 0.6 ms | 1.0 ms | 1.4 ms |

Enqueue throughput with 4 producer processes: about 9–14k jobs/s.

**This run finds the ceiling.** With no work per job, every worker spends
all its time talking to Redis. Each job costs 3 round trips (XREADGROUP,
START, COMPLETE), and START and COMPLETE are Lua scripts that each run
several commands, including the XPENDING ownership check behind fencing.
Redis executes commands on a single core, so adding workers stops helping
at roughly **4.5k jobs/s on this machine**. Workers and Redis also share
the same 8 vCPUs here.

## What this means

- For real workloads, where the handler takes milliseconds or more, the
  queue is not the bottleneck: workers scale out linearly and add about 1–3
  ms of latency.
- The single-Redis ceiling (about 4.5k jobs/s here) is the scaling limit of
  this design. Ways past it, roughly in order of effort:
  1. **Batch delivery.** `XREADGROUP COUNT n` with a small prefetch window
     cuts round trips per job, at the cost of more in-flight work being
     reclaimed when a worker dies.
  2. **Shard queues across Redis instances.** Queues are independent
     streams, so different queues can live on different Redis servers.
  3. **Redis Cluster.** This needs a shared hash tag in every key so each
     script's keys land in one slot (see design.md, section 11).
- Fencing isn't free: the ownership check adds one XPENDING lookup inside
  each worker-side script. That's the cost of guaranteeing a stalled worker
  can never overwrite a reclaimed job, and at under 1 ms p50 it's cheap.
