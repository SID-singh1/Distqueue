"""
pause_worker.py — Freeze a worker past its heartbeat TTL; prove fencing.

    python -m chaos.pause_worker [--sleep 30] [--json-out results.json]

What it proves
--------------
The hardest failure for a heartbeat-based system is not a dead worker but
a *paused* one: a long GC pause, a VM migration, CPU starvation — or here,
`docker pause`, which freezes every thread in the container (including
the heartbeat thread) via the cgroup freezer.

  1. Probe job {"sleep_s": N} starts on worker Z.
  2. Z is frozen.  Its heartbeat stops; the key expires.
  3. The monitor reclaims the job (attempts 0 -> 1) and schedules a retry.
     As far as the system can tell, Z is dead.
  4. Z is unfrozen.  It has no idea any time passed: it finishes the
     handler and tries to mark the job COMPLETED.

Without fencing, step 4 overwrote the job with status=COMPLETED and
attempts=0 from Z's stale in-memory copy — while the retry was queued or
already running elsewhere.  With fencing (every worker write is checked
against PEL ownership inside a Lua script), Z's write is rejected and Z
logs "Lease lost".

Checks: Z's stale write was rejected (seen in its logs); at no point did the
job show COMPLETED by Z; the retry completed on another worker with exactly
one attempt charged.

Note that the probe job *did* run twice — Z finished its sleep too.  That
is at-least-once delivery, and why handlers must be idempotent; fencing
protects the job's state, not the outside world.
"""

from __future__ import annotations

import argparse
import threading
import time

from chaos._common import (
    Report,
    connect,
    container_for_consumer,
    job_state,
    log,
    require_quiet_queue,
    wait_for,
)
from distqueue import config
from distqueue.producer import enqueue


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument(
        "--sleep", type=float, default=30.0, help="probe job length (s)"
    )
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()

    client, engine = connect()
    require_quiet_queue(client, engine)

    job_id = enqueue(client, {"sleep_s": args.sleep}, max_attempts=3)
    log(f"enqueued probe job {job_id} (sleep_s={args.sleep:g})")

    wait_for(
        "probe job to start",
        lambda: job_state(client, job_id).get("status") == "RUNNING",
        timeout_s=30,
    )
    zombie_consumer = job_state(client, job_id)["last_worker"]
    zombie = container_for_consumer(engine, zombie_consumer)
    log(f"running on {zombie.name} (consumer {zombie_consumer})")

    # Watch the hash continuously from here on: the bug this guards against
    # is a *transient* bad state (COMPLETED by the zombie while the retry
    # is still running), which a final-state check alone could miss.
    violations: list[dict[str, str]] = []
    stop_watch = threading.Event()

    def watch() -> None:
        while not stop_watch.is_set():
            state = job_state(client, job_id)
            if (
                state.get("status") == "COMPLETED"
                and state.get("last_worker") == zombie_consumer
            ):
                violations.append(state)
            time.sleep(0.05)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()

    time.sleep(1)
    zombie.pause()
    paused_at = time.monotonic()
    log(f"froze {zombie.name}; its heartbeat stops now")

    budget = config.HEARTBEAT_TTL_S + 2 * config.MONITOR_POLL_INTERVAL_S + 15
    try:
        detection = wait_for(
            "monitor to reclaim the frozen worker's job",
            lambda: job_state(client, job_id).get("attempts") == "1",
            timeout_s=budget,
        )
    finally:
        zombie.unpause()  # never leave a container frozen, even on failure
    frozen_for = time.monotonic() - paused_at
    log(f"reclaimed after {detection:.1f}s; unfroze after {frozen_for:.1f}s")

    log("waiting for the zombie to finish and attempt its stale write")
    lease_lost_logged = False

    def zombie_was_fenced() -> bool:
        nonlocal lease_lost_logged
        logs = zombie.logs(since=int(time.time() - 600)).decode(errors="replace")
        lease_lost_logged = any(
            "Lease lost" in line and job_id in line for line in logs.splitlines()
        )
        return lease_lost_logged

    wait_for(
        "zombie's write to be rejected", zombie_was_fenced, timeout_s=args.sleep + 20
    )
    log("zombie's COMPLETED write was rejected (Lease lost)")

    wait_for(
        "retry to complete on another worker",
        lambda: job_state(client, job_id).get("status") == "COMPLETED",
        timeout_s=args.sleep + 45,
    )
    stop_watch.set()
    watcher.join(timeout=2)
    final = job_state(client, job_id)

    report = Report("Freeze a worker past its heartbeat TTL (fencing)")
    report.add("Probe job", job_id)
    report.add("Frozen container", zombie.name)
    report.add("Frozen for", frozen_for, " s")
    report.add("Detection (freeze -> reclaimed)", detection, " s")
    report.add("Completed by", final.get("last_worker"))
    report.add("Final attempts", final.get("attempts"))
    report.check("zombie's stale COMPLETED write was rejected", lease_lost_logged)
    report.check("job never showed COMPLETED by the zombie", not violations)
    report.check(
        "retry completed on a different worker",
        final.get("last_worker") != zombie_consumer,
    )
    report.check("exactly one attempt charged", final.get("attempts") == "1")
    report.finish(args.json_out)


if __name__ == "__main__":
    main()
