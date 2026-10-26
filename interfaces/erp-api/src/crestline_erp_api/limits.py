"""Throttling: how many requests a client may make before the ERP says wait.

One window per API process, on the wall clock, like a gateway in front of an
old ERP: up to `per_second` requests in any second, then `429` with a
`Retry-After` until the oldest one is a second old.
"""

import math
import threading
from collections import deque


class RateLimit:
    def __init__(self, per_second: float) -> None:
        self._per_second = per_second
        self._recent: deque[float] = deque()
        self._lock = threading.Lock()

    def admit(self, now: float) -> int | None:
        """None if the request may go ahead (and counts); else whole seconds to wait."""
        with self._lock:
            while self._recent and self._recent[0] <= now - 1:
                self._recent.popleft()
            if len(self._recent) >= self._per_second:
                return max(1, math.ceil(self._recent[0] + 1 - now))
            self._recent.append(now)
            return None
