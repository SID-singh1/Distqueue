"""
kill_worker.py — SIGKILL a worker mid-job; prove the job still completes.

    python -m chaos.kill_worker [--sleep 20] [--json-out results.json]

What it proves
--------------
Heartbeat-gated failure detection works end to end with real processes:

  1. A probe job {"sleep_s": N} starts on some worker container W.
  2. W is SIGKILLed (`docker kill`) — no SIGTERM, no graceful shutdown, no
     chance to ack or clean up.  This is what an OOM kill or a lost VM
     looks like.  (`docker stop` would test the graceful path instead.)
  3. W's heartbeat key expires (HEARTBEAT_TTL_S).  The monitor sees an
     idle PEL entry with no heartbeat behind it, XCLAIMs it, and records
     a failed attempt (attempts 0 -> 1) with a retry backoff.
  4. The scheduler re-injects the job; another worker runs it to COMPLETED.

The probe uses sleep_s so the demo handler never fails it randomly: the
only failure on the job's record is the one this experiment caused.

Checks: job COMPLETED; exactly one attempt charged; finished by a different
worker; failure attributed to the dead worker.
"""

from __future__ import annotations

import argparse
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
        "--sleep", type=float, default=20.0, help="probe job length (s)"
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
    victim_consumer = job_state(client, job_id)["last_worker"]
    victim = container_for_consumer(engine, victim_consumer)
    log(f"running on {victim.name} (consumer {victim_consumer})")

    time.sleep(1)  # let it get properly into the handler
    victim.kill()  # SIGKILL
    killed_at = time.monotonic()
    log(f"SIGKILLed {victim.name}; waiting for the monitor")

    budget = config.HEARTBEAT_TTL_S + 2 * config.MONITOR_POLL_INTERVAL_S + 15
    detection = wait_for(
        "monitor to reclaim the job",
        lambda: job_state(client, job_id).get("attempts") == "1",
        timeout_s=budget,
    )
    reclaimed = job_state(client, job_id)
    log(f"reclaimed after {detection:.1f}s: {reclaimed.get('last_error')}")

    def rerun_started() -> bool:
        state = job_state(client, job_id)
        return (
            state.get("status") == "RUNNING"
            and state.get("last_worker") != victim_consumer
        )

    wait_for("retry to start on another worker", rerun_started, timeout_s=30)
    requeued = time.monotonic() - killed_at
    rescuer = job_state(client, job_id)["last_worker"]
    log(f"retry started on {rescuer}")

    wait_for(
        "retry to complete",
        lambda: job_state(client, job_id).get("status") == "COMPLETED",
        timeout_s=args.sleep + 30,
    )
    total = time.monotonic() - killed_at
    final = job_state(client, job_id)

    log(f"restarting {victim.name} to restore the fleet")
    victim.start()

    report = Report("SIGKILL a worker mid-job")
    report.add("Probe job", job_id)
    report.add("Killed container", victim.name)
    report.add("Detection (kill -> reclaimed)", detection, " s")
    report.add("Requeue (kill -> retry running)", requeued, " s")
    report.add("Recovery (kill -> COMPLETED)", total, " s")
    report.add(
        "Heartbeat TTL / monitor poll",
        f"{config.HEARTBEAT_TTL_S} s / {config.MONITOR_POLL_INTERVAL_S:g} s",
    )
    report.add("Completed by", final.get("last_worker"))
    report.check("job reached COMPLETED", final.get("status") == "COMPLETED")
    report.check(
        "exactly one attempt charged for the crash", final.get("attempts") == "1"
    )
    report.check(
        "finished by a different worker", final.get("last_worker") != victim_consumer
    )
    report.check(
        "failure attributed to the dead worker",
        victim_consumer in (reclaimed.get("last_error") or ""),
    )
    report.finish(args.json_out)


if __name__ == "__main__":
    main()
