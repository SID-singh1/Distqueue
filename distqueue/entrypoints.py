"""
entrypoints.py — Entry points for running distqueue processes.
"""

import os
import signal
import threading
import time
import random
import logging

from distqueue.metrics import start_metrics_server
from distqueue.client import get_redis_client
from distqueue.worker import Worker
from distqueue.scheduler import Scheduler
from distqueue.monitor import Monitor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def _setup_signal_handler() -> threading.Event:
    """Setup SIGTERM and SIGINT handler for graceful shutdown."""
    stop_event = threading.Event()

    def handler(signum, frame):
        signame = signal.Signals(signum).name
        logger.info(f"Received {signame}, initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    return stop_event


def run_worker() -> None:
    start_metrics_server()
    client = get_redis_client()
    stop_event = _setup_signal_handler()

    # Read failure rate from environment, default to 10%
    failure_rate = float(os.environ.get("DEMO_FAILURE_RATE", "0.1"))

    def demo_handler(payload: dict) -> None:
        """Simulate variable-duration work with a configurable failure rate.
        
        This exists to provide interesting data for Prometheus and Grafana.
        """
        duration = random.uniform(0.1, 2.0)
        time.sleep(duration)
        
        if random.random() < failure_rate:
            raise RuntimeError(f"Simulated failure (rate {failure_rate:.1%})")

    worker = Worker(
        client=client,
        handler=demo_handler,
        stop_event=stop_event,
    )
    logger.info("Worker started.")
    worker.run()
    logger.info("Worker exited cleanly.")


def run_scheduler() -> None:
    start_metrics_server()
    client = get_redis_client()
    stop_event = _setup_signal_handler()

    scheduler = Scheduler(
        client=client,
        stop_event=stop_event,
    )
    logger.info("Scheduler started.")
    scheduler.run()
    logger.info("Scheduler exited cleanly.")


def run_monitor() -> None:
    start_metrics_server()
    client = get_redis_client()
    stop_event = _setup_signal_handler()

    monitor = Monitor(
        client=client,
        stop_event=stop_event,
    )
    logger.info("Monitor started.")
    monitor.run()
    logger.info("Monitor exited cleanly.")
