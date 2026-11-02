"""Device clocks that wander (spec §4.3, `machines.clock_drift_seconds`).

A brake controller or a gateway with no time sync keeps its own clock, and
that clock is wrong by an amount that changes slowly: seconds for a gateway
someone set by hand a while ago, minutes for a PLC nobody has touched in
years. The offset is modelled as a slowly reverting random walk sampled
every hour and read off by linear interpolation, so it never jumps. The
knob is the typical (RMS) size of the offset; the path itself is drawn
standardized and scaled by the knob, so 0 means exactly true time and
halving the knob halves the same path rather than drawing another.

Only the clocks that stamp a payload are modelled: `ts` on PB-01, the
Sparkplug timestamps on LSR-01, and `gw_ts` on GW02 (shared by PB-02 and
PEM-01). `published_at` is the broker's clock and never drifts.
"""

import math
import random
from datetime import datetime, timedelta

KNOT = timedelta(hours=1)
REVERSION_HOURS = 24 * 7  # how long the offset takes to forget where it was


class DriftingClock:
    def __init__(
        self, rms_seconds: float, start: datetime, end: datetime, rng: random.Random
    ) -> None:
        knots = int((end - start) / KNOT) + 2
        keep = math.exp(-1 / REVERSION_HOURS)
        noise = math.sqrt(1 - keep * keep)  # keeps the path's variance at 1
        x = rng.gauss(0, 1)
        path = [x]
        for _ in range(knots - 1):
            x = keep * x + noise * rng.gauss(0, 1)
            path.append(x)
        self.start = start
        self.offsets = [v * rms_seconds for v in path]

    def offset(self, at: datetime) -> float:
        """Seconds the clock is ahead of true time at `at`."""
        hours = (at - self.start) / KNOT
        i = min(max(int(hours), 0), len(self.offsets) - 2)
        frac = hours - i
        return self.offsets[i] + frac * (self.offsets[i + 1] - self.offsets[i])

    def device_time(self, at: datetime) -> datetime:
        return at + timedelta(seconds=self.offset(at))
