"""A warning written at most every `FAILURE_LOG_INTERVAL`, the ones between counted.

Shared by the notifier (`notifier.py`) and each phone's push endpoint
(`push.py`): a dead endpoint fails on every notification, and the log should say
so once in a while, not on every one.
"""
from __future__ import annotations

import logging
from collections.abc import Callable

log = logging.getLogger(__name__)

#: How often a failure is written to the log; the ones in between are counted.
FAILURE_LOG_INTERVAL = 300.0


class FailureLog:
    """Logs `"{label}: {message}"` at most every `FAILURE_LOG_INTERVAL`."""

    def __init__(self, clock: Callable[[], float], label: str = "notify") -> None:
        self._clock = clock
        self._label = label
        self._last = float("-inf")
        self._suppressed = 0

    def __call__(self, message: str) -> None:
        now = self._clock()
        if now - self._last < FAILURE_LOG_INTERVAL:
            self._suppressed += 1
            return
        more = f" ({self._suppressed} more since the last report)" if self._suppressed else ""
        log.warning("%s: %s%s", self._label, message, more)
        self._last = now
        self._suppressed = 0
