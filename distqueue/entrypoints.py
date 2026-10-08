"""
entrypoints.py — Process wiring for each distqueue role.

Each run_* function turns a library object (Worker, Scheduler, Monitor) into
a long-running process: it starts the /metrics server, connects to Redis,
installs signal handlers, and blocks until shutdown.  The CLI (cli.py)
parses arguments and calls these.

Graceful shutdown: SIGTERM (what `docker stop` sends) and SIGINT (Ctrl-C)
set a stop event.  The worker finishes its current job before exiting, so
a deploy doesn't turn every in-flight job into a "dead worker" reclaim.
Docker's default stop grace period is 10 s; docker-compose.yml raises it
for workers so a typical job can finish.
"""

from __future__ import annotations

import importlib
import logging
import signal
import threading
from typing import Any

from distqueue import config
from distqueue.client import get_redis_client
from distqueue.metrics import start_metrics_server
from distqueue.monitor import Monitor
from distqueue.producer import enqueue
from distqueue.runloop import run_until_stopped
from distqueue.scheduler import Scheduler
from distqueue.worker import Handler, Worker

logger = logging.getLogger(__name__)


def install_signal_handlers() -> threading.Event:
    """Set the returned event on SIGTERM / SIGINT."""
    stop_event = threading.Event()

    def handler(signum: int, _frame: Any) -> None:
        logger.info(
            "Received %s; shutting down gracefully.", signal.Signals(signum).name
        )
        stop_event.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    return stop_event


def load_handler(spec: str) -> Handler:
    """Import a handler from a ``"package.module:function"`` string.

    This is what makes the worker reusable beyond the demo: point it at any
    importable function, e.g. ``distqueue worker --handler myapp.jobs:run``.
    """
    module_name, sep, attr = spec.partition(":")
    if not sep or not attr:
        raise ValueError(f"handler must look like 'module:function', got {spec!r}")
    handler = getattr(importlib.import_module(module_name), attr)
    if not callable(handler):
        raise TypeError(f"{spec} is not callable")
    return handler


def _start_metrics(port: int) -> None:
    if port > 0:
        start_metrics_server(port)
        logger.info("Serving Prometheus metrics on :%d/metrics", port)


def run_worker(
    queue: str = config.DEFAULT_QUEUE,
    handler_spec: str = "distqueue.demo:handler",
    metrics_port: int = config.METRICS_PORT,
) -> None:
    _start_metrics(metrics_port)
    worker = Worker(
        client=get_redis_client(),
        handler=load_handler(handler_spec),
        queue=queue,
        stop_event=install_signal_handlers(),
    )
    logger.info(
        "Worker %s consuming queue %r with %s",
        worker.consumer_name,
        queue,
        handler_spec,
    )
    worker.run()
    logger.info("Worker exited cleanly.")


def run_scheduler(metrics_port: int = config.METRICS_PORT) -> None:
    _start_metrics(metrics_port)
    scheduler = Scheduler(
        client=get_redis_client(), stop_event=install_signal_handlers()
    )
    logger.info("Scheduler started.")
    scheduler.run()
    logger.info("Scheduler exited cleanly.")


def run_monitor(metrics_port: int = config.METRICS_PORT) -> None:
    _start_metrics(metrics_port)
    monitor = Monitor(client=get_redis_client(), stop_event=install_signal_handlers())
    logger.info("Monitor %s started.", monitor.consumer_name)
    monitor.run()
    logger.info("Monitor exited cleanly.")


def run_producer(
    queue: str = config.DEFAULT_QUEUE,
    rate: float = 3.0,
    count: int = 0,
    metrics_port: int = config.METRICS_PORT,
) -> None:
    """Enqueue demo jobs at a steady ``rate`` per second (forever if count=0).

    Runs as its own container so its metrics (enqueue rate) are scraped
    like every other role's — when it ran on the host, the enqueue counter
    was never exported anywhere.
    """
    _start_metrics(metrics_port)
    client = get_redis_client()
    stop_event = install_signal_handlers()
    sent = 0

    def tick() -> None:
        nonlocal sent
        enqueue(client, {"task": f"demo-{sent}"}, queue=queue)
        sent += 1
        if sent % 100 == 0:
            logger.info("Enqueued %d jobs", sent)
        if count and sent >= count:
            stop_event.set()

    logger.info("Producer enqueuing %.1f jobs/s to %r", rate, queue)
    run_until_stopped(tick, stop_event, 1.0 / rate, "producer")
    logger.info("Producer stopped after %d jobs.", sent)
