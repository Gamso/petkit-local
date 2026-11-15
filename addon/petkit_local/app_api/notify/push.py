"""The second sink: a small JSON message to every phone that registered for push.

A phone registers its own UnifiedPush endpoint with its session
(`PUT /app/v1/push`, stored by `auth.py`). For every notification that got
through the notifier's gates — the kind switch, the `event_uid` dedupe, the
rate limit, the bounded queue, shared with the ntfy topic — `dispatch` POSTs the
same message to every live registration whose session's `kinds` allow it.

The message (`message`, `PUSH_VERSION` 1) is stable keys only, so the app writes
the notification itself, in the phone's language, and nothing in it is worth
stealing::

    {"v":1,"kind":"toilet","device_id":100,"pet_id":2,"code":"10","ts":1763150000}

No title, no free text (a device's own fault text never leaves), no token, no
URL. A few hundred bytes at most, far under UnifiedPush's 4096.

The endpoint is a capability URL, so it is treated as a secret: a log line or a
failure reason names only its host. And it is chosen by the phone, so the POST
is fenced in: no redirect is ever followed, TLS is verified, the time is
bounded, and `GuardedResolver` refuses — at connect time, on the address
actually dialled — this machine's own addresses (`app_api/push.py`).

Nothing here waits on an endpoint. Each registration has its own lane — a
queue of at most `LANE_SIZE` messages, sent in order, one at a time, by a task
of its own — so a dead phone delays neither the others nor the ntfy publisher
nor the event path, and a burst to a healthy one is delivered, not dropped. A
lane that is full (its endpoint is not keeping up) drops what does not fit, and
says so. Lanes are bounded by the sessions (`auth.MAX_SESSIONS`).
An endpoint answering 404 or 410 (UnifiedPush's "gone") `GONE_LIMIT` times in a
row is unregistered.
"""
from __future__ import annotations

import asyncio
import json
import logging
import socket
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult
from aiohttp.resolver import DefaultResolver
from yarl import URL

from petkit_local.app_api import push as registration
from petkit_local.app_api.notify.failurelog import FailureLog
from petkit_local.app_api.notify.rules import PRIORITY_HIGH, Notification
from petkit_local.utils.coerce import to_int

log = logging.getLogger(__name__)

#: The message format's version, its `v`. A new key is a compatible change; a
#: removed or renamed one is a new version.
PUSH_VERSION = 1
#: UnifiedPush's limit on a message body. Ours stays far below it.
MAX_MESSAGE_BYTES = 4096
#: Longest `code` sent. Every code is one `events/codes.py` or `rules.py` knows.
MAX_CODE = 64
#: One POST, connect to response.
PUSH_TIMEOUT = aiohttp.ClientTimeout(total=10.0, connect=5.0)
#: RFC 8030's `TTL`: how long the push server may hold the message for a phone
#: that is offline. A day; the app can judge staleness from `ts`.
PUSH_TTL = 86400
#: Consecutive 404/410 answers after which an endpoint is unregistered. More
#: than one, so a push server restarting with an empty table for a moment does
#: not cost the phone its registration.
GONE_LIMIT = 3
GONE_STATUSES = frozenset({404, 410})
#: Messages waiting for one endpoint. A full minute of the busiest device's
#: rate limit, times a few kinds; beyond it the endpoint is not keeping up.
LANE_SIZE = 16


def message(notification: Notification, now: float) -> dict[str, Any]:
    """The push message for `notification`: exactly these six keys."""
    code = notification.code
    return {
        "v": PUSH_VERSION,
        "kind": notification.kind,
        "device_id": to_int(notification.device_id, None),
        "pet_id": to_int(notification.pet_id, None),
        "code": str(code)[:MAX_CODE] if code is not None else None,
        "ts": int(notification.ts if notification.ts is not None else now),
    }


def encode(msg: dict[str, Any]) -> bytes:
    return json.dumps(msg, separators=(",", ":"), ensure_ascii=True).encode("ascii")


class GuardedResolver(AbstractResolver):
    """The default resolver, minus every address `refused_address` forbids.

    The check is on what the name resolves to NOW, at connect time, so a name
    that pointed somewhere harmless at registration and at 127.0.0.1 later is
    still refused.
    """

    def __init__(self) -> None:
        self._inner = DefaultResolver()

    async def resolve(self, host: str, port: int = 0,
                      family: socket.AddressFamily = socket.AF_INET) -> list[ResolveResult]:
        results = await self._inner.resolve(host, port, family)
        allowed = [r for r in results if not registration.refused_address(r["host"])]
        if not allowed:
            raise OSError("the name resolves only to addresses pushes are never sent to")
        return allowed

    async def close(self) -> None:
        await self._inner.close()


@dataclass
class _Lane:
    """One registration's queue and the task draining it."""

    queue: deque[tuple[bytes, str, str]] = field(default_factory=deque)
    task: asyncio.Task[None] | None = None


class PushSink:
    """Fans each notification out to the registered endpoints. See the module docstring."""

    def __init__(self, auth: Any, *, clock: Callable[[], float] = time.monotonic,
                 wall_clock: Callable[[], float] = time.time) -> None:
        self._auth = auth
        self._clock = clock
        self._wall = wall_clock
        self._session: aiohttp.ClientSession | None = None
        self._resolver: GuardedResolver | None = None
        # (session id, endpoint) -> its lane; removed once drained.
        self._lanes: dict[tuple[str, str], _Lane] = {}
        # (session id, endpoint) -> consecutive "gone" answers.
        self._gone: dict[tuple[str, str], int] = {}
        self._failures: dict[str, FailureLog] = {}

    def wants(self, kind: str) -> bool:
        """Whether any live registration would receive `kind`."""
        return any(s.allows(kind) for s in self._auth.push_targets())

    # --- fan-out --------------------------------------------------------------

    def dispatch(self, notification: Notification) -> int:
        """Queue the message on every lane whose registration allows the kind.

        Returns at once with how many lanes took it: the POSTs run in each
        lane's own task. Never raises.
        """
        targets = self._auth.push_targets()
        self._forget_all_but({s.id for s in targets})
        body = encode(message(notification, self._wall()))
        urgency = "high" if notification.priority >= PRIORITY_HIGH else "normal"
        queued = 0
        for session in targets:
            reg = session.push
            if not session.allows(notification.kind):
                continue
            key = (session.id, reg.endpoint)
            lane = self._lanes.setdefault(key, _Lane())
            if len(lane.queue) >= LANE_SIZE:
                self._failure(session.id)(
                    f"{reg.host} (session {session.id}) is not keeping up; dropped a "
                    f"{notification.kind} push")
                continue
            lane.queue.append((body, urgency, notification.kind))
            if lane.task is None:
                lane.task = asyncio.create_task(self._run_lane(key, session.name),
                                                name="app-push")
            queued += 1
        return queued

    async def _run_lane(self, key: tuple[str, str], name: str) -> None:
        """Send a lane's messages in order until it is empty, then retire it."""
        lane = self._lanes[key]
        try:
            while lane.queue:
                body, urgency, kind = lane.queue.popleft()
                await self._deliver(key[0], name, key[1], body, urgency, kind)
        finally:
            # No await between the emptiness check above and this: a message
            # queued meanwhile would have found the task still set.
            if self._lanes.get(key) is lane:
                del self._lanes[key]

    def _failure(self, session_id: str) -> FailureLog:
        failure = self._failures.get(session_id)
        if failure is None:
            failure = self._failures[session_id] = FailureLog(self._clock, "push")
        return failure

    def _forget_all_but(self, live: set[str]) -> None:
        for sid in [s for s in self._failures if s not in live]:
            del self._failures[sid]
        for key in [k for k in self._gone if k[0] not in live]:
            del self._gone[key]

    async def _deliver(self, session_id: str, name: str, endpoint: str, body: bytes,
                       urgency: str, kind: str) -> bool:
        """POST to one endpoint and act on the answer. Never raises."""
        # Re-checked here, not only at fan-out: a phone revoked while this
        # message was queued receives nothing.
        if not self._auth.push_current(session_id, endpoint):
            return False
        host = registration.endpoint_host(endpoint)
        status, reason = await self.post(endpoint, body, urgency)
        key = (session_id, endpoint)
        if status is not None and 200 <= status < 300:
            self._gone.pop(key, None)
            return True
        if status in GONE_STATUSES:
            count = self._gone.get(key, 0) + 1
            self._gone[key] = count
            if count >= GONE_LIMIT:
                self._gone.pop(key, None)
                try:
                    dropped = await self._auth.clear_push(session_id, endpoint)
                except Exception as e:  # noqa: BLE001 - a failed save must not end the sink
                    log.warning("push: could not unregister the endpoint on %s (%s)", host,
                                type(e).__name__)
                    return False
                if dropped:
                    log.warning("push: unregistered the endpoint on %s of %r (session %s): "
                                "it answered HTTP %d %d times in a row", host, name,
                                session_id, status, count)
                return False
        else:
            self._gone.pop(key, None)
        self._failure(session_id)(f"a {kind} push to {name!r} failed: {reason}")
        return False

    async def post(self, endpoint: str, body: bytes, urgency: str = "normal") -> tuple[int | None, str]:
        """POST `body` to `endpoint`: `(status or None, reason)`. Never raises.

        The reason names the host only, never the endpoint, and never quotes
        the server's answer, which may echo the URL.
        """
        host = registration.endpoint_host(endpoint)
        literal = registration.literal_address(host)
        if literal is not None and registration.refused_address(literal):
            return None, f"{host} is an address pushes are never sent to"
        headers = {"Content-Type": "application/json", "TTL": str(PUSH_TTL),
                   "Urgency": urgency}
        try:
            if self._session is None or self._session.closed:
                self._resolver = GuardedResolver()
                self._session = aiohttp.ClientSession(
                    timeout=PUSH_TIMEOUT,
                    connector=aiohttp.TCPConnector(resolver=self._resolver))
            # `encoded=True`: sent exactly as registered (a query token must
            # not be re-quoted). No redirects: the phone chose this URL, not
            # wherever it points next. TLS is verified (aiohttp's default).
            async with self._session.post(URL(endpoint, encoded=True), data=body,
                                          headers=headers, allow_redirects=False) as r:
                if 200 <= r.status < 300:
                    return r.status, ""
                if 300 <= r.status < 400:
                    return r.status, f"{host} answered a redirect (HTTP {r.status}), not followed"
                return r.status, f"{host} answered HTTP {r.status}"
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            return None, f"{host} did not answer within {PUSH_TIMEOUT.total:g}s"
        except (aiohttp.ClientError, OSError, ValueError) as e:
            # The type only: an exception's text can carry the whole URL.
            return None, f"could not reach {host} ({type(e).__name__})"
        except Exception as e:  # noqa: BLE001 - a push must never take the sink down
            return None, f"pushing to {host} failed ({type(e).__name__})"

    # --- lifecycle ------------------------------------------------------------

    def _lane_tasks(self) -> list[asyncio.Task[None]]:
        return [lane.task for lane in self._lanes.values()
                if lane.task is not None and not lane.task.done()]

    async def drain(self) -> None:
        """Wait until every lane is empty (tests, and an orderly close)."""
        while pending := self._lane_tasks():
            await asyncio.wait(pending)

    async def close(self) -> None:
        pending = self._lane_tasks()
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending)
        self._lanes.clear()
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._resolver is not None:
            await self._resolver.close()
            self._resolver = None
