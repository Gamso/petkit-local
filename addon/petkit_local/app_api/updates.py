"""The app's live updates: ONE EventHub subscription, fanned out to every open socket.

`GET /app/v1/ws` (`handlers/updates.py`) attaches a `Subscriber` here. What it is
sent is built once per change and shared by every phone, so ten phones cost
what one does:

* `hello` — on connect: every device as `DeviceDetail`, so the app starts from
  a known state with no window between a GET and the socket;
* `state` — the entities of one device whose value changed, and only those;
* `device` — a device's summary when anything but `last_seen` changed
  (online/offline, transport, firmware), or when a device first appears;
* `device_removed`;
* `timeline` — a timeline card that is new or changed (its media arrived, its
  cleaning cycle attached), with the ids of cards it `replaces`: an episode
  still in progress is anchored on its newest report until the summary that
  closes it arrives, so its id can move on (`events/sessions.py`).

What drives it is the panel's `EventHub`: every HTTP request, MQTT frame,
stored event and finished media file is published there with its device id,
which marks that device to be re-read. Re-read, not trusted — the hub's events
are log lines, not state — and DIFFED against what was last sent, which is
what keeps a chatty device from flooding a phone: a device is looked at once
per `COALESCE_SEC` however many frames it sent, and if nothing a phone can see
changed, nothing is sent. A sweep every `SWEEP_SEC` re-reads every device
regardless, because the hub drops events for a slow reader and a few state
changes (the offline watchdog's) are not narrated at all.

A `Subscriber`'s queue is bounded. A phone that stops reading is DISCONNECTED
(`CLOSE_TOO_SLOW`) when it fills, never allowed to grow memory — it reconnects
and gets a fresh `hello`. A session that ends (logout, revocation in the panel,
a password change, the password removed) is noticed within `COALESCE_SEC` and
its sockets are closed with `CLOSE_SESSION_ENDED`.

Nothing runs while no phone is connected: the hub subscription and both tasks
start with the first socket and stop with the last.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import TYPE_CHECKING, Any

from petkit_local.app_api import serializers
from petkit_local.events import ingest
from petkit_local.events.sessions import SUB_EVENT_WINDOW_SEC
from petkit_local.ha.categories import get_entities_for_device
from petkit_local.ha.state_doc import entity_state_doc
from petkit_local.utils.coerce import to_int

if TYPE_CHECKING:
    from petkit_local.ai.pets import PetRegistry
    from petkit_local.app_api.auth import AppAuth, AppSession
    from petkit_local.devices.base import Device
    from petkit_local.devices.registry import DeviceRegistry
    from petkit_local.events.store import EventStore

log = logging.getLogger(__name__)

#: Protocol-level ping interval. Cloudflare closes a WebSocket idle for 100 s,
#: and most reverse proxies default to 60 s.
HEARTBEAT_SEC = 25.0
#: A device's changes are looked for, and sent, at most once per this.
COALESCE_SEC = 1.0
#: Every device is re-read this often even if the hub said nothing about it.
SWEEP_SEC = 30.0
#: Messages waiting for one phone before it is cut off as too slow.
QUEUE_LIMIT = 256
#: How far back a timeline card is still watched for changes. A card changes
#: while its cleaning cycle attaches and its video is assembled — minutes.
TIMELINE_WINDOW_SEC = 6 * SUB_EVENT_WINDOW_SEC
TIMELINE_ROW_LIMIT = 500
#: Hub event kinds that can mean a timeline card appeared or changed.
TIMELINE_KINDS = frozenset({"event", "media"})

#: WebSocket close codes, the same as the live-video socket's (`video.py`) so an
#: app handles both one way: 1001 server stopping, 1013 the app fell behind
#: (the standard "try again later"), 4401 the session ended (the HTTP 401 of
#: the app's own 4000-4999 range).
CLOSE_GOING_AWAY = 1001
CLOSE_SESSION_ENDED = 4401
CLOSE_TOO_SLOW = 1013

_CLOSE = object()


class Subscriber:
    """One open socket's outbox. Holds encoded messages, at most `limit`."""

    def __init__(self, session: AppSession, limit: int = QUEUE_LIMIT) -> None:
        self.session = session
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=limit)
        self.close_code: int | None = None
        self.close_reason = ""

    def push(self, text: str) -> bool:
        """Queue one message; False (and the socket is ended) if it is full."""
        if self.close_code is not None:
            return False
        try:
            self.queue.put_nowait(text)
            return True
        except asyncio.QueueFull:
            self.end(CLOSE_TOO_SLOW, "too slow: messages were not being read")
            return False

    def end(self, code: int, reason: str) -> None:
        """Close this socket: drop what is queued, then hand the writer the close."""
        if self.close_code is not None:
            return
        self.close_code, self.close_reason = code, reason
        while not self.queue.empty():
            self.queue.get_nowait()
        self.queue.put_nowait(_CLOSE)

    @staticmethod
    def is_close(item: Any) -> bool:
        return item is _CLOSE


class UpdateFeed:
    """The shared half of `/app/v1/ws`: what changed, worked out once."""

    def __init__(self, auth: AppAuth, registry: DeviceRegistry, hub: Any = None, *,
                 event_store: EventStore | None = None,
                 pet_registry: PetRegistry | None = None, media_root: str = "",
                 coalesce: float = COALESCE_SEC, sweep: float = SWEEP_SEC,
                 queue_limit: int = QUEUE_LIMIT) -> None:
        self._auth = auth
        self._registry = registry
        self._hub = hub
        self._store = event_store
        self._pets = pet_registry
        self._media_root = media_root
        self._coalesce = coalesce
        self._sweep = sweep
        self._queue_limit = queue_limit
        self._lock = asyncio.Lock()
        self._subs: set[Subscriber] = set()
        self._hub_queue: asyncio.Queue | None = None
        self._tasks: list[asyncio.Task] = []
        # What was last sent, per device: summary (minus last_seen) and
        # `{entity key: EntityChange}`.
        self._summaries: dict[int, dict[str, Any]] = {}
        self._values: dict[int, dict[str, dict[str, Any]]] = {}
        # Timeline card id -> (fingerprint of what was sent, card ts).
        self._cards: dict[int, tuple[str, float]] = {}
        self._dirty: set[int] = set()
        self._dirty_timeline: set[int] = set()

    @property
    def subscribers(self) -> int:
        return len(self._subs)

    # --- connections ----------------------------------------------------

    async def attach(self, session: AppSession) -> Subscriber:
        """Open a feed for one socket; its first message is the `hello`."""
        async with self._lock:
            if not self._tasks:
                await self._start()
            # Bring everyone already connected up to date first, so the
            # snapshot below and the deltas after it start from one point.
            self._dirty.update(d.petkit_id for d in self._registry.all())
            self._flush_states()
            sub = Subscriber(session, self._queue_limit)
            self._subs.add(sub)
            sub.push(_encode({
                "type": "hello",
                "server_time": time.time(),
                "heartbeat_sec": HEARTBEAT_SEC,
                "devices": [serializers.device_detail(d, get_entities_for_device(d),
                                                      entity_state_doc(d))
                            for d in sorted(self._registry.all(), key=lambda d: d.petkit_id)],
            }))
            return sub

    async def detach(self, sub: Subscriber) -> None:
        """Forget one socket; the last one out stops the feed."""
        async with self._lock:
            self._subs.discard(sub)
            if not self._subs and self._tasks:
                await self._stop()

    async def shutdown(self) -> None:
        """Close every socket (`1001`) and stop. The app's `on_shutdown` hook."""
        for sub in list(self._subs):
            sub.end(CLOSE_GOING_AWAY, "server shutting down")
        async with self._lock:
            if self._tasks:
                await self._stop()

    # --- lifecycle ------------------------------------------------------

    async def _start(self) -> None:
        if self._hub is not None:
            self._hub_queue = self._hub.subscribe()
        # Every card already there is known, not new: the app fetched those
        # with `GET /timeline`, and a burst of them on connect is noise.
        await self._scan_timeline({d.petkit_id for d in self._registry.all()}, announce=False)
        self._tasks = [asyncio.create_task(self._run(), name="app-updates-flush")]
        if self._hub_queue is not None:
            self._tasks.append(asyncio.create_task(self._pump(), name="app-updates-hub"))

    async def _stop(self) -> None:
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._hub is not None and self._hub_queue is not None:
            self._hub.unsubscribe(self._hub_queue)
        self._hub_queue = None
        self._summaries.clear()
        self._values.clear()
        self._cards.clear()
        self._dirty.clear()
        self._dirty_timeline.clear()

    async def _pump(self) -> None:
        """Mark the device each hub event names; cheap, so the hub never waits."""
        assert self._hub_queue is not None
        while True:
            event = await self._hub_queue.get()
            device_id = to_int(event.get("device_id"), None) if isinstance(event, dict) else None
            if device_id is None:
                continue
            self._dirty.add(device_id)
            if event.get("kind") in TIMELINE_KINDS:
                self._dirty_timeline.add(device_id)

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        last_sweep = loop.time()
        while True:
            await asyncio.sleep(self._coalesce)
            try:
                self._check_sessions()
                if loop.time() - last_sweep >= self._sweep:
                    last_sweep = loop.time()
                    self._dirty.update(d.petkit_id for d in self._registry.all())
                    self._dirty.update(self._summaries)
                self._flush_states()
                if self._dirty_timeline:
                    ids, self._dirty_timeline = self._dirty_timeline, set()
                    await self._scan_timeline(ids, announce=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                # One bad device must not end every phone's live updates.
                log.exception("app API: live update pass failed")

    def _check_sessions(self) -> None:
        for sub in list(self._subs):
            if not self._auth.is_live(sub.session):
                sub.end(CLOSE_SESSION_ENDED, "session ended")

    # --- what changed ---------------------------------------------------

    def _broadcast(self, message: dict[str, Any]) -> None:
        if not self._subs:
            return
        text = _encode(message)
        for sub in list(self._subs):
            sub.push(text)

    def _flush_states(self) -> None:
        dirty, self._dirty = self._dirty, set()
        for device_id in sorted(dirty):
            device = self._registry.get(device_id)
            if device is None:
                if self._summaries.pop(device_id, None) is not None:
                    self._values.pop(device_id, None)
                    self._broadcast({"type": "device_removed", "device_id": device_id})
                continue
            self._diff_device(device)

    def _diff_device(self, device: Device) -> None:
        summary = serializers.device_summary(device)
        compared = {k: v for k, v in summary.items() if k != "last_seen"}
        values = {e["key"]: serializers.entity_change(e)
                  for e in serializers.device_entities(get_entities_for_device(device),
                                                       entity_state_doc(device))}
        known = self._summaries.get(device.petkit_id)
        if known != compared:
            self._summaries[device.petkit_id] = compared
            self._broadcast({"type": "device", "device": summary})
        if known is None:
            # New to this feed: `hello` (or the app's re-fetch after the
            # `device` message) carries its values; deltas start from here.
            self._values[device.petkit_id] = values
            return
        before = self._values.get(device.petkit_id, {})
        changed = [v for key, v in values.items() if before.get(key) != v]
        self._values[device.petkit_id] = values
        if changed:
            self._broadcast({"type": "state", "device_id": device.petkit_id,
                             "ts": time.time(), "entities": changed})

    async def _scan_timeline(self, device_ids: set[int], *, announce: bool) -> None:
        """Send each card of these devices' recent history that is new or changed."""
        if self._store is None:
            return
        since = time.time() - TIMELINE_WINDOW_SEC
        pets = {p["id"]: p for p in await self._pets.all()} if self._pets is not None else {}
        for device_id in sorted(device_ids):
            device = self._registry.get(device_id)
            if device is None:
                continue
            rows = await self._store.query_timeline(device_id=device_id, start_ts=since,
                                                    limit=TIMELINE_ROW_LIMIT)
            for session in sorted(ingest.group_sessions(rows), key=lambda s: s.get("ts") or 0):
                item = serializers.timeline_item(
                    session, session.get("device_type") or device.device_type,
                    pets, self._media_root)
                fingerprint = _encode(item)
                if self._cards.get(item["id"], ("",))[0] == fingerprint:
                    continue
                replaces = sorted(step["id"] for step in item["steps"]
                                  if step["id"] != item["id"] and step["id"] in self._cards)
                for old in replaces:
                    del self._cards[old]
                self._cards[item["id"]] = (fingerprint, item["ts"] or time.time())
                if announce:
                    self._broadcast({"type": "timeline", "item": item, "replaces": replaces})
        # Forget cards nobody will change any more.
        horizon = since - TIMELINE_WINDOW_SEC
        for card_id in [i for i, (_, ts) in self._cards.items() if ts < horizon]:
            del self._cards[card_id]


def _encode(message: dict[str, Any]) -> str:
    return json.dumps(message, separators=(",", ":"), sort_keys=True)
