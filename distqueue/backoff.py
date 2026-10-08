"""
backoff.py — Retry delay computation (capped exponential backoff + jitter).

This is a pure function in its own module so it can be unit-tested
exhaustively without Redis, and so there is exactly one definition of
"how long does a failed job wait" for both the worker and the monitor.

The formula: "equal jitter"
---------------------------
    d     = min(MAX_BACKOFF_S, BASE_BACKOFF_S * 2**attempts)
    delay = d/2 + uniform(0, d/2)          # i.e. uniform in [d/2, d]

Why not the original ``min(base * 2**attempts + uniform(0, 1), cap)``?
That formula has two problems:

1. **Jitter vanishes at the cap.**  Once ``base * 2**attempts`` exceeds the
   cap, ``min(...)`` throws the jitter away and *every* job waits exactly
   ``cap`` seconds.  Jobs that failed together then retry in perfect
   lockstep — the exact thundering herd jitter is supposed to prevent.
   Here jitter is applied *after* the cap, so it can never be clipped.

2. **Fixed-size jitter doesn't scale.**  One second of jitter spreads a 4 s
   retry nicely but is noise on a 128 s retry: 1,000 jobs that failed
   together still land inside the same one-second window.  Making the
   jitter proportional to the delay (half of it) keeps the spread
   meaningful at every attempt count.

Why "equal" jitter rather than "full" jitter (uniform in [0, d])?  Both come
from Marc Brooker's AWS Architecture Blog post "Exponential Backoff And
Jitter" (2015).  Full jitter spreads load slightly better, but it can
schedule a retry ~0 s after a failure, re-hitting a downstream service
that just told us it's struggling.  Equal jitter guarantees at least half
the exponential delay while still spreading retries over a window as wide
as that half.
"""

from __future__ import annotations

import random

from distqueue import config

# 2**63 is already ~9e18 seconds; capping the exponent keeps the float
# arithmetic from raising OverflowError if someone sets max_attempts=10_000.
_MAX_EXPONENT = 63


def compute_backoff(
    attempts: int,
    *,
    base_s: float = config.BASE_BACKOFF_S,
    cap_s: float = config.MAX_BACKOFF_S,
    rng: random.Random | None = None,
) -> float:
    """Return the delay (seconds) before retry number ``attempts`` runs.

    Parameters
    ----------
    attempts : int
        How many attempts have been made so far, *including* the one that
        just failed.  The first retry therefore uses ``attempts == 1``.
    base_s, cap_s : float
        Base delay and ceiling of the exponential term.
    rng : random.Random | None
        Source of randomness.  Injectable so tests can make the jitter
        deterministic; defaults to the module-level ``random`` functions.

    Returns
    -------
    float
        A delay in ``[d/2, d]`` where ``d = min(cap_s, base_s * 2**attempts)``.
    """
    exponent = min(max(attempts, 0), _MAX_EXPONENT)
    ceiling = min(cap_s, base_s * (2**exponent))
    half = ceiling / 2
    uniform = rng.uniform if rng is not None else random.uniform
    return half + uniform(0, half)
