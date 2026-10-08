"""distqueue — a Redis-backed distributed job queue, built from scratch.

Public API::

    from distqueue import enqueue, Worker, PermanentError, get_redis_client

    client = get_redis_client()
    job_id = enqueue(client, {"to": "a@example.com"}, queue="emails")

    def send_email(payload: dict) -> None: ...

    Worker(client, send_email, queue="emails").run()
"""

from distqueue.client import get_redis_client
from distqueue.errors import PermanentError
from distqueue.job import Job, JobStatus
from distqueue.monitor import Monitor
from distqueue.producer import enqueue
from distqueue.scheduler import Scheduler
from distqueue.worker import Worker

__version__ = "0.2.0"

__all__ = [
    "Job",
    "JobStatus",
    "Monitor",
    "PermanentError",
    "Scheduler",
    "Worker",
    "enqueue",
    "get_redis_client",
]
