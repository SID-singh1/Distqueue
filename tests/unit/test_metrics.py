"""
test_metrics.py — Unit tests for distqueue.metrics.

These assert on what Prometheus actually scrapes — the text exposition
format produced by generate_latest() — rather than on prometheus_client
private attributes (_name, _labelnames, _upper_bounds).  The previous
version of this file checked _upper_bounds directly, so changing the
buckets broke a test that wasn't testing anything a scraper could see.
"""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY, generate_latest

from distqueue import metrics


def _exposition() -> str:
    return generate_latest(REGISTRY).decode()


EXPECTED_TYPES = {
    "distqueue_jobs_enqueued_total": "counter",
    "distqueue_jobs_deduplicated_total": "counter",
    "distqueue_jobs_completed_total": "counter",
    "distqueue_jobs_failed_total": "counter",
    "distqueue_jobs_reclaimed_total": "counter",
    "distqueue_jobs_skipped_total": "counter",
    "distqueue_lease_lost_total": "counter",
    "distqueue_scheduler_jobs_moved_total": "counter",
    "distqueue_scheduler_orphans_dropped_total": "counter",
    "distqueue_heartbeat_failures_total": "counter",
    "distqueue_redis_errors_total": "counter",
    "distqueue_job_duration_seconds": "histogram",
    "distqueue_job_queue_wait_seconds": "histogram",
    "distqueue_job_end_to_end_seconds": "histogram",
    "distqueue_queue_backlog": "gauge",
    "distqueue_pending_entries": "gauge",
    "distqueue_stream_length": "gauge",
    "distqueue_delayed_jobs": "gauge",
    "distqueue_dlq_depth": "gauge",
    "distqueue_live_workers": "gauge",
}


@pytest.mark.parametrize(("name", "kind"), sorted(EXPECTED_TYPES.items()))
def test_metric_is_exposed_with_type(name: str, kind: str) -> None:
    assert f"# TYPE {name} {kind}" in _exposition()


def test_failed_counter_labels() -> None:
    metrics.JOBS_FAILED.labels(queue="t", outcome="retried", trigger="exception").inc(0)
    assert (
        REGISTRY.get_sample_value(
            "distqueue_jobs_failed_total",
            {"queue": "t", "outcome": "retried", "trigger": "exception"},
        )
        is not None
    )


def test_duration_buckets_cover_demo_and_load_test_ranges() -> None:
    """Buckets must resolve both the load test's ~ms no-op jobs and the
    demo's 0.1–1 s jobs; histogram_quantile can't see inside a bucket."""
    metrics.JOBS_DURATION.labels(queue="buckets").observe(0.0)
    text = _exposition()
    for le in ("0.005", "0.1", "0.5", "1.0", "300.0", "+Inf"):
        assert (
            f'distqueue_job_duration_seconds_bucket{{le="{le}",queue="buckets"}}'
            in text
        )


def test_start_metrics_server_serves_scrapes() -> None:
    """The server binds and answers a real HTTP scrape."""
    import socket
    import urllib.request

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    metrics.start_metrics_server(port=port)
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read()
    assert b"distqueue_jobs_enqueued_total" in body
