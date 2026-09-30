"""Seeded randomness.

Every random draw anywhere in the simulation must come from an instance
created here, never from the `random` or `numpy.random` module-level globals,
so that a given seed always reproduces the same output (spec's determinism
principle).

Components get their own named *stream* so that tuning one of them never
shifts the draws of another: changing a downtime rate in the plant model must
not change which parts the job generator picked.
"""

import random


def make_rng(seed: int, stream: str | None = None) -> random.Random:
    """Return a generator for `seed`, optionally on a named sub-stream.

    Python seeds strings through SHA-512, so `"42/plant"` is stable across
    runs and machines (unlike `hash()`, which is randomized per process).
    """
    if stream is None:
        return random.Random(seed)
    return random.Random(f"{seed}/{stream}")
