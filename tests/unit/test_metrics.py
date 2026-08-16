"""
test_metrics.py — Unit tests for distqueue.metrics.

These tests verify that all metric objects are correctly defined with the
expected names, types, and label sets.  No Redis needed — these test the
metrics module in isolation against the prometheus_client registry.
"""

from __future__ import annotations

import pytest
from prometheus_client import Counter, Gauge, Histogram

from distqueue import metrics


class TestMetricDefinitions:
    """Verify each metric object exists with the expected name and type."""

    def test_jobs_enqueued_is_counter(self) -> None:
        assert isinstance(metrics.JOBS_ENQUEUED, Counter)
        # _name is the internal prometheus_client attribute.  For counters,
        # the library stores it WITHOUT the _total suffix — _total is
        # auto-appended during serialisation to the /metrics endpoint.
        assert metrics.JOBS_ENQUEUED._name == "distqueue_jobs_enqueued"

    def test_jobs_completed_is_counter(self) -> None:
        assert isinstance(metrics.JOBS_COMPLETED, Counter)
        assert metrics.JOBS_COMPLETED._name == "distqueue_jobs_completed"

    def test_jobs_failed_is_counter(self) -> None:
        assert isinstance(metrics.JOBS_FAILED, Counter)
        assert metrics.JOBS_FAILED._name == "distqueue_jobs_failed"

    def test_jobs_failed_has_expected_labels(self) -> None:
        assert metrics.JOBS_FAILED._labelnames == ("queue", "outcome", "trigger")

    def test_jobs_reclaimed_is_counter(self) -> None:
        assert isinstance(metrics.JOBS_RECLAIMED, Counter)
        assert metrics.JOBS_RECLAIMED._name == "distqueue_jobs_reclaimed"

    def test_job_duration_is_histogram(self) -> None:
        assert isinstance(metrics.JOBS_DURATION, Histogram)
        assert metrics.JOBS_DURATION._name == "distqueue_job_duration_seconds"

    def test_job_duration_has_expected_buckets(self) -> None:
        # _upper_bounds is a list that includes the implicit +Inf bucket.
        expected = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, float("inf")]
        assert metrics.JOBS_DURATION._upper_bounds == expected

    def test_queue_depth_is_gauge(self) -> None:
        assert isinstance(metrics.QUEUE_DEPTH, Gauge)
        assert metrics.QUEUE_DEPTH._name == "distqueue_queue_depth"

    def test_delayed_jobs_is_gauge(self) -> None:
        assert isinstance(metrics.DELAYED_JOBS, Gauge)
        assert metrics.DELAYED_JOBS._name == "distqueue_delayed_jobs"

    def test_pending_entries_is_gauge(self) -> None:
        assert isinstance(metrics.PENDING_ENTRIES, Gauge)
        assert metrics.PENDING_ENTRIES._name == "distqueue_pending_entries"


class TestMetricsServer:
    """Verify start_metrics_server doesn't raise on a valid port."""

    def test_start_metrics_server_callable(self) -> None:
        """start_metrics_server should be callable without raising.

        We use a high, unlikely-to-collide test port (39187) to avoid
        binding conflicts with other services.  However, if a previous
        test run in the same pytest session already bound this port
        (prometheus_client reuses the global registry and its HTTP server
        is a singleton per port), we'll get an "address already in use"
        OSError.  We treat this specific error as acceptable rather than
        silently swallowing all exceptions — the test still proves the
        function is importable and callable.
        """
        try:
            metrics.start_metrics_server(port=39187)
        except OSError as exc:
            if "address already in use" in str(exc).lower():
                # Already bound by a previous test run in the same session.
                # This is expected and acceptable.
                pytest.skip("Metrics server port already bound from prior test")
            else:
                raise
