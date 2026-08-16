# distqueue

A Redis-backed distributed job queue and task scheduler, built from scratch in Python.

## Testing

### Prerequisites

Start a local Redis instance (required for integration tests):

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Running tests

```bash
# Unit tests only (fast, no Redis required)
.venv/Scripts/pytest -m "not integration" -v

# Integration tests only (requires Redis)
.venv/Scripts/pytest tests/integration -v

# All tests
.venv/Scripts/pytest -v
```

## Running with Docker Compose

To spin up the entire multi-worker stack with Prometheus scraping metrics:

```bash
docker compose -f docker/docker-compose.yml up --build --scale worker=5 -d
```

This will launch Redis, a single scheduler, a single monitor, 5 scaled worker replicas, a Prometheus instance, and Grafana.

To verify that Prometheus successfully discovered all scaled worker IPs dynamically via DNS, visit the targets page in your browser:
[http://localhost:9090/targets](http://localhost:9090/targets)

You should see 5 endpoints under `distqueue_worker`, plus `distqueue_scheduler` and `distqueue_monitor` all listed as **UP**.

To view the live metrics dashboard:
1. Open [http://localhost:3000](http://localhost:3000) in your browser.
2. Log in with `admin` / `admin` (or skip login, since anonymous access is enabled for this demo).
3. Navigate to Dashboards — the `Distqueue Metrics` dashboard is automatically provisioned and loaded for you (no manual JSON import required).

