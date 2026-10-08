"""
test_cli.py — Unit tests for CLI argument parsing and handler loading.

Commands that talk to Redis are covered in tests/integration/test_cli.py.
"""

from __future__ import annotations

import pytest

from distqueue import demo
from distqueue.cli import build_parser
from distqueue.entrypoints import load_handler


def test_worker_defaults() -> None:
    args = build_parser().parse_args(["worker"])
    assert args.queue == "default"
    assert args.handler == "distqueue.demo:handler"


def test_enqueue_delay_and_run_at_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["enqueue", "{}", "--delay", "5", "--run-at", "1"])


def test_dlq_replay_accepts_ids_or_all() -> None:
    args = build_parser().parse_args(["dlq", "replay", "a", "b"])
    assert args.job_ids == ["a", "b"]
    assert build_parser().parse_args(["dlq", "replay", "--all"]).all is True


def test_load_handler_imports_function() -> None:
    assert load_handler("distqueue.demo:noop") is demo.noop


@pytest.mark.parametrize("spec", ["distqueue.demo", "distqueue.demo:", ":noop"])
def test_load_handler_rejects_malformed_spec(spec: str) -> None:
    with pytest.raises((ValueError, ModuleNotFoundError)):
        load_handler(spec)


def test_load_handler_rejects_non_callable() -> None:
    with pytest.raises(TypeError):
        load_handler("distqueue.demo:MIN_S")


def test_demo_handler_payload_overrides() -> None:
    from distqueue.errors import PermanentError

    demo.handler({"sleep_s": 0})  # deterministic success
    with pytest.raises(PermanentError):
        demo.handler({"fail": "permanent"})
    with pytest.raises(RuntimeError):
        demo.handler({"fail": "transient"})
