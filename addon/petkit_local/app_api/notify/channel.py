"""The third sink: notifications over the app API's own WebSocket, with catch-up.

`GET /app/v1/notifications/ws` (`handlers/notifications.py`) is what lets the
phone app do without ntfy: it keeps this one socket open in the background, as
the ntfy app keeps one to its server, and the server writes each notification
to it. It carries notifications only — never device state, which is `/ws`'s —
so a socket open all day is silent until something happens, apart from the
protocol ping every `HEARTBEAT_SEC` that keeps a tunnel from dropping it.

What travels is the push message itself (`notify/push.py`, the `PushMessage`
of `openapi.yaml`), wrapped with an id::

    {"type":"notification","id":42,"replay":false,"message":{"v":1,"kind":...}}

so both channels carry one payload and the app parses one shape.

**One decision point.** Nothing here decides whether a notification is sent:
`deliver` is called by the notifier's worker for exactly what passed its gates
(the panel's kind switches, the `event_uid` dedupe, the rate limit), the same
notifications the ntfy topic and the UnifiedPush endpoints get. What this sink
adds is only WHICH phones: each live session that has opened the socket, whose
`kinds` (`AppSession.kinds`, the same choice its push endpoint obeys) allow it.

**Catch-up.** Every notification delivered here is appended to ONE log, shared
by every phone, with a monotonic id, and persisted to
`{data_dir}/app_notifications.json`. A phone reconnecting with `?after=<the
last id it got>` is sent, before anything live, every entry after it that its
kinds allow and that is not older than its session. The log is bounded twice over —
`LOG_CAPACITY` entries and `LOG_RETENTION` seconds, whichever is reached first —
and when entries the phone had not seen were dropped by those bounds its
`hello` says so (`gap`), so the app can say "some alerts may be missing" rather
than pretend. Ids are not reused while the file lasts: the next id is persisted
with the log. If the file is lost they start again from 1, so a client naming an
id beyond the last one holds an id from a log that no longer exists, and is
replayed everything retained, with `gap`.

A session is owed catch-up from the moment it first opens the socket
(`AppSession.notify_ws`, persisted), connected or not: that is what makes a
notification wanted (`wants`) while the phone is in a dead zone.

**Session rules**, those of `/ws` and of live video: one socket per session (a
new one replaces the old with `CLOSE_REPLACED`); a session that ends — logout,
revocation, password change, expiry — is noticed within `SESSION_CHECK_SEC` and
its socket closed with `CLOSE_SESSION_ENDED`; shutdown closes every socket with
`CLOSE_GOING_AWAY`; a phone whose queue fills is cut off (`CLOSE_TOO_SLOW`) and
catches up on reconnection. The session watch runs only while a socket is open.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from petkit_local.app_api.notify.push import message
from petkit_local.app_api.notify.settings import KINDS
from petkit_local.app_api.updates import CLOSE_GOING_AWAY as CLOSE_GOING_AWAY
from petkit_local.app_api.updates import CLOSE_SESSION_ENDED as CLOSE_SESSION_ENDED
from petkit_local.app_api.updates import CLOSE_TOO_SLOW as CLOSE_TOO_SLOW
from petkit_local.app_api.updates import Subscriber
from petkit_local.utils.jsonio import atomic_write_text, read_json

if TYPE_CHECKING:
    import os

    from petkit_local.app_api.auth import AppAuth, AppSession
    from petkit_local.app_api.notify.rules import Notification

log = logging.getLogger(__name__)

FILENAME = "app_notifications.json"

#: Entries kept for catch-up. Notifications are rate-limited per device and
#: kind (`notifier.RATE_LIMITS`), so a few hundred is days of a busy household.
LOG_CAPACITY = 500
#: ... and for at most this long. A phone off for longer than a long weekend
#: has its history in `/timeline`; an alert that old is not worth ringing for.
LOG_RETENTION = 3 * 86400
#: A burst of notifications is written to disk once, this long after the first.
SAVE_DELAY = 1.0
#: Protocol ping interval. Cloudflare drops a WebSocket idle for ~100 s; 45 s
#: (ntfy's own keepalive) stays well inside it while waking the phone's radio
#: about as often as the ntfy app would. The app should treat two missed pings
#: as a dead connection and reconnect.
HEARTBEAT_SEC = 45.0
#: How soon a session that ended has its socket closed.
SESSION_CHECK_SEC = 1.0
#: Live messages waiting for one phone, beyond a full replay of the log.
LIVE_QUEUE = 64
#: The same session opened another notification socket (as live video's 4409).
CLOSE_REPLACED = 4409


@dataclass(frozen=True)
class LogEntry:
    """One logged notification: its id, when it was logged, its push message."""

    id: int
    at: float
    message: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        return {"id": self.id, "at": self.at, "message": self.message}

    @classmethod
    def from_json(cls, raw: Any) -> LogEntry | None:
        if not isinstance(raw, dict):
            return None
        entry_id, at, msg = raw.get("id"), raw.get("at"), raw.get("message")
        if (not isinstance(entry_id, int) or isinstance(entry_id, bool) or entry_id < 1
                or not isinstance(at, (int, float)) or isinstance(at, bool)
                or not isinstance(msg, dict) or not isinstance(msg.get("kind"), str)):
            return None
        return cls(entry_id, float(at), msg)


class NotifyLog:
    """The bounded catch-up log, ids monotonic across restarts."""

    def __init__(self, capacity: int = LOG_CAPACITY, retention: float = LOG_RETENTION) -> None:
        self._capacity = capacity
        self._retention = retention
        self._entries: deque[LogEntry] = deque()
        self._next_id = 1

    @property
    def last_id(self) -> int:
        """The id of the newest notification ever logged; 0 before the first."""
        return self._next_id - 1

    @property
    def floor(self) -> int:
        """The oldest id still held (the next one when none is)."""
        return self._entries[0].id if self._entries else self._next_id

    def __len__(self) -> int:
        return len(self._entries)

    def append(self, msg: dict[str, Any], now: float) -> LogEntry:
        entry = LogEntry(self._next_id, now, msg)
        self._next_id += 1
        self._entries.append(entry)
        self.prune(now)
        return entry

    def prune(self, now: float) -> None:
        while len(self._entries) > self._capacity:
            self._entries.popleft()
        while self._entries and now - self._entries[0].at > self._retention:
            self._entries.popleft()

    def after(self, after: int) -> list[LogEntry]:
        """Every entry with an id above `after`, oldest first."""
        return [e for e in self._entries if e.id > after]

    def snapshot(self) -> str:
        return json.dumps({"version": 1, "next_id": self._next_id,
                           "entries": [e.to_json() for e in self._entries]},
                          separators=(",", ":"))

    def load(self, raw: Any, now: float) -> None:
        """Take what a stored snapshot holds; whatever is malformed is dropped."""
        if not isinstance(raw, dict):
            return
        entries = sorted((e for e in map(LogEntry.from_json, raw.get("entries") or [])
                          if e is not None), key=lambda e: e.id)
        unique: dict[int, LogEntry] = {e.id: e for e in entries}
        self._entries = deque(unique.values())
        stored_next = raw.get("next_id")
        if not isinstance(stored_next, int) or isinstance(stored_next, bool):
            stored_next = 1
        self._next_id = max(stored_next, (entries[-1].id + 1) if entries else 1)
        self.prune(now)


def _encode(msg: dict[str, Any]) -> str:
    return json.dumps(msg, separators=(",", ":"), ensure_ascii=True)


def notification_text(entry: LogEntry, *, replay: bool) -> str:
    """The socket message carrying one logged notification."""
    return _encode({"type": "notification", "id": entry.id, "replay": replay,
                    "message": entry.message})


class NotifyChannel:
    """Every phone's notification socket, and the log they catch up from."""

    def __init__(self, auth: AppAuth, path: str | os.PathLike[str] | None = None, *,
                 enabled: Callable[[str], bool] | None = None,
                 wall_clock: Callable[[], float] | None = None,
                 capacity: int = LOG_CAPACITY, retention: float = LOG_RETENTION,
                 check_interval: float = SESSION_CHECK_SEC,
                 save_delay: float = SAVE_DELAY) -> None:
        self._auth = auth
        self._path = Path(path) if path is not None else None
        self._enabled = enabled
        # The session store's clock by default: a log entry's time is compared
        # with when a session was created.
        self._wall = wall_clock if wall_clock is not None else auth.now
        self._check_interval = check_interval
        self._save_delay = save_delay
        self._log = NotifyLog(capacity, retention)
        if self._path is not None:
            self._log.load(read_json(self._path, {}), self._wall())
        self._sockets: dict[str, Subscriber] = {}  # session id -> its one socket
        self._opened: dict[str, float] = {}         # session id -> when that socket opened
        self._last_seen: dict[str, float] = {}      # session id -> when its socket last closed
        self._lock = asyncio.Lock()
        self._watch_task: asyncio.Task[None] | None = None
        self._save_lock = asyncio.Lock()
        self._saver: asyncio.Task[None] | None = None
        self._dirty = False

    @property
    def last_id(self) -> int:
        return self._log.last_id

    @property
    def sockets(self) -> int:
        return len(self._sockets)

    def connected(self, session_id: str) -> bool:
        return session_id in self._sockets

    def server_kinds(self) -> list[dict[str, Any]]:
        """Each kind the server knows, and whether the panel's switch lets it through."""
        return [{"kind": k, "enabled": self._enabled(k) if self._enabled is not None else True}
                for k in KINDS]

    def status(self, session: AppSession) -> dict[str, Any] | None:
        """For the panel: None if the session never used the socket."""
        if session.id in self._sockets:
            return {"connected": True, "since": self._opened.get(session.id)}
        if session.notify_ws is None:
            return None
        return {"connected": False, "since": self._last_seen.get(session.id)}

    # --- the sink -------------------------------------------------------------

    def wants(self, kind: str) -> bool:
        """Whether a phone using the socket — connected or owed catch-up — wants `kind`."""
        return any(s.allows(kind) for s in self._auth.notify_targets())

    def deliver(self, notification: Notification) -> int:
        """Log one notification that passed the notifier's gates and send it live.

        Synchronous, so nothing can come between the log and the sockets: a
        phone connecting meanwhile is replayed it or sent it, never neither,
        never both. Returns how many sockets it was written to. Never raises.
        """
        try:
            targets = [s for s in self._auth.notify_targets() if s.allows(notification.kind)]
            if not targets:
                return 0
            now = self._wall()
            entry = self._log.append(message(notification, now), now)
            self._schedule_save()
            text = notification_text(entry, replay=False)
            sent = 0
            for session in targets:
                sub = self._sockets.get(session.id)
                if sub is not None and sub.push(text):
                    sent += 1
            return sent
        except Exception as e:  # noqa: BLE001 - one sink must not end the others
            log.warning("app notifications: could not deliver a %s notification (%s)",
                        notification.kind, type(e).__name__)
            return 0

    # --- sockets --------------------------------------------------------------

    async def attach(self, session: AppSession, after: int | None) -> Subscriber:
        """Open a socket for `session`: `hello`, then what it missed, then live.

        A socket the session already had is closed with `CLOSE_REPLACED`.
        """
        if not await self._auth.mark_notify_ws(session.id):
            # Signed out between the upgrade's check and here.
            sub = Subscriber(session, 1)
            sub.end(CLOSE_SESSION_ENDED, "session ended")
            return sub
        async with self._lock:
            # From here to the end, no await: `deliver` cannot run in between,
            # so the replay and the live messages meet without a gap.
            now = self._wall()
            self._log.prune(now)
            previous = self._sockets.get(session.id)
            if previous is not None:
                previous.end(CLOSE_REPLACED, "replaced by a newer notification socket")
            sub = Subscriber(session, len(self._log) + LIVE_QUEUE + 1)
            replay: list[LogEntry] = []
            gap = False
            if after is not None:
                if after > self._log.last_id:
                    # An id from a log that no longer exists.
                    after, gap = 0, True
                gap = gap or after < self._log.floor - 1
                replay = [e for e in self._log.after(after)
                          if e.at >= session.created
                          and session.allows(str(e.message.get("kind")))]
            sub.push(_encode({"type": "hello", "server_time": now,
                              "heartbeat_sec": HEARTBEAT_SEC, "last_id": self._log.last_id,
                              "replayed": len(replay), "gap": gap}))
            for entry in replay:
                sub.push(notification_text(entry, replay=True))
            self._sockets[session.id] = sub
            self._opened[session.id] = now
            if self._watch_task is None:
                self._watch_task = asyncio.create_task(self._watch(),
                                                       name="app-notify-sessions")
            return sub

    async def detach(self, sub: Subscriber) -> None:
        """Forget one socket (unless already replaced); the last one out stops the watch."""
        async with self._lock:
            sid = sub.session.id
            if self._sockets.get(sid) is sub:
                del self._sockets[sid]
                self._opened.pop(sid, None)
                self._last_seen[sid] = self._wall()
                live = {s.id for s in self._auth.sessions()}
                for gone in [k for k in self._last_seen if k not in live]:
                    del self._last_seen[gone]
            if not self._sockets:
                await self._stop_watch()

    async def _watch(self) -> None:
        while True:
            await asyncio.sleep(self._check_interval)
            for sub in list(self._sockets.values()):
                if not self._auth.is_live(sub.session):
                    sub.end(CLOSE_SESSION_ENDED, "session ended")

    async def _stop_watch(self) -> None:
        task, self._watch_task = self._watch_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def shutdown(self) -> None:
        """Close every socket (`1001`), stop, write the log. The app's `on_shutdown` hook."""
        for sub in list(self._sockets.values()):
            sub.end(CLOSE_GOING_AWAY, "server shutting down")
        async with self._lock:
            await self._stop_watch()
        await self.flush()

    # --- persistence ----------------------------------------------------------

    def _schedule_save(self) -> None:
        self._dirty = True
        if self._path is None or (self._saver is not None and not self._saver.done()):
            return
        self._saver = asyncio.create_task(self._save_soon(), name="app-notify-log")

    async def _save_soon(self) -> None:
        while self._dirty:
            await asyncio.sleep(self._save_delay)
            # Shielded: cancelled mid-write (`flush`), the write still finishes,
            # holding the lock, so it can never land after a newer one.
            await asyncio.shield(self._write())

    async def _write(self) -> None:
        if self._path is None:
            return
        async with self._save_lock:
            if not self._dirty:
                return
            self._dirty = False
            text = self._log.snapshot()
            try:
                await asyncio.to_thread(atomic_write_text, self._path, text)
            except OSError as e:
                log.warning("app notifications: could not save the catch-up log (%s)", e)

    async def flush(self) -> None:
        """Write the log now if anything is unsaved. Idempotent."""
        saver, self._saver = self._saver, None
        if saver is not None and not saver.done():
            saver.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await saver
        # Through the lock even when nothing looks unsaved: a write the
        # cancelled saver started may still be running, and this waits for it.
        await self._write()
