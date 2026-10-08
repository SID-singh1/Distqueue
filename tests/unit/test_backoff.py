"""
test_backoff.py — Unit tests for distqueue.backoff.compute_backoff.

The properties under test are the ones the retry design depends on:
  * delays grow exponentially, then stop growing at the cap;
  * jitter is never thrown away — not even at the cap (the original
    formula clipped jitter there, so capped retries ran in lockstep);
  * the spread scales with the delay, so large delays still de-synchronise
    jobs that failed together.
"""

from __future__ import annotations

import random

import pytest

from distqueue.backoff import compute_backoff

BASE = 2.0
CAP = 300.0


def _samples(attempts: int, n: int = 2000, seed: int = 7) -> list[float]:
    rng = random.Random(seed)
    return [
        compute_backoff(attempts, base_s=BASE, cap_s=CAP, rng=rng) for _ in range(n)
    ]


@pytest.mark.parametrize("attempts", [1, 2, 3, 4, 5, 6, 7])
def test_delay_within_equal_jitter_bounds(attempts: int) -> None:
    """Every delay lies in [d/2, d] with d = min(cap, base * 2**attempts)."""
    d = min(CAP, BASE * 2**attempts)
    for delay in _samples(attempts):
        assert d / 2 <= delay <= d


def test_first_retry_waits_two_to_four_seconds() -> None:
    """With the default base of 2 s, the first retry waits 2–4 s."""
    delays = _samples(1)
    assert min(delays) >= 2.0
    assert max(delays) <= 4.0


def test_never_exceeds_cap() -> None:
    for attempts in (8, 9, 20, 63, 64, 1000):
        assert max(_samples(attempts, n=200)) <= CAP


def test_jitter_survives_the_cap() -> None:
    """Regression test for the lockstep bug.

    The original formula, min(base * 2**n + jitter, cap), returned exactly
    `cap` for every job once base * 2**n > cap — zero spread.  Here capped
    delays must still be spread across [cap/2, cap].
    """
    delays = _samples(20)
    assert len({round(d, 3) for d in delays}) > 100, "capped delays must not collapse"
    assert max(delays) - min(delays) > CAP / 4


def test_spread_scales_with_delay() -> None:
    """A fixed 1 s jitter would be noise on a 128 s retry; equal jitter isn't."""
    small = _samples(1)
    large = _samples(6)
    assert (max(large) - min(large)) > 10 * (max(small) - min(small))


def test_huge_attempt_counts_do_not_overflow() -> None:
    """2**10_000 as a float would raise OverflowError; the exponent is capped."""
    assert compute_backoff(10_000, base_s=BASE, cap_s=CAP) <= CAP


def test_zero_or_negative_attempts_are_clamped() -> None:
    for attempts in (0, -5):
        delay = compute_backoff(attempts, base_s=BASE, cap_s=CAP, rng=random.Random(1))
        assert BASE / 2 <= delay <= BASE


def test_rng_makes_it_deterministic() -> None:
    a = compute_backoff(3, rng=random.Random(42))
    b = compute_backoff(3, rng=random.Random(42))
    assert a == b
