"""The ntfy publisher: listens to the event hub, publishes from a bounded queue.

Nothing here polls. The notifier holds one subscription to the event hub — the
same live feed the panel's log reads — and reacts to three things on it:

* an `event` (a stored event row, either transport) -> `rules.from_event`;
* the offline watchdog's `connect` entry (`detail.online is False`) — the
  watchdog's decision, never a missed heartbeat on its own;
* any other entry naming a device -> its `device.state` is re-read for the
  conditions in `rules.CONDITIONS`, and a condition that has STARTED (and kept
  holding for its `hold` in a later report) is announced once.

Between a classification and the network, four gates, in order: the kind is
switched on; the notification is not a repeat (`event_uid`, the
`event_id + event_type` key, so the reports of one session are each their own
event but a retried report is not, and an update of an existing row
(`is_new` False) is never announced); the device has not used up its allowance
for that kind (`RATE_LIMITS`); and the queue has room. Publishing happens in a
background worker, so a slow or dead ntfy server never holds up the hub, and
nothing in this module can raise into the event path.

Two sinks share those gates: the panel's ntfy topic (`enabled`, a server, a
topic) and every phone that registered its own UnifiedPush endpoint with its
session (`push.py`). The kind switches are the server's, and apply to both; a
notification is classified and counted when at least one sink would take it.
Dedupe and the rate limit are therefore shared too: a notification the topic
would not have sent is not sent to a phone either.

Only ever the configured ntfy server and the endpoints the phones registered
are called. Never PetKit's cloud.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import time
from collections import OrderedDict, deque
from collections.abc import Callable
from typing import Any, Protocol
from urllib.parse import urlsplit

import aiohttp

from petkit_local.app_api.notify import rules
from petkit_local.app_api.notify.failurelog import FAILURE_LOG_INTERVAL as FAILURE_LOG_INTERVAL
from petkit_local.app_api.notify.failurelog import FailureLog
from petkit_local.app_api.notify.push import PushSink
from petkit_local.app_api.notify.rules import Notification
from petkit_local.app_api.notify.settings import (
    KIND_CONSUMABLE, KIND_ERROR, KIND_FEED_FAILED, KIND_FEED_SERVED, KIND_FOOD_LOW,
    KIND_OFFLINE, KIND_TEST, KIND_TOILET, NtfyConfig, NtfySettings,
)

log = logging.getLogger(__name__)

#: Per device and kind: at most `count` notifications in `window` seconds.
#: Errors are one per five minutes because one fault arrives twice — as an
#: `error_start` event and as the state's `errorMsg` — and a flapping fault
#: would otherwise ring the phone on every report.
RATE_LIMITS: dict[str, tuple[int, float]] = {
    KIND_TOILET: (6, 600.0),
    KIND_FEED_SERVED: (6, 600.0),
    KIND_FEED_FAILED: (3, 600.0),
    KIND_ERROR: (1, 300.0),
    KIND_FOOD_LOW: (1, 3600.0),
    KIND_CONSUMABLE: (4, 3600.0),
    KIND_OFFLINE: (1, 1800.0),
}

#: Notifications waiting for the worker. Beyond it they are dropped (and said so).
QUEUE_SIZE = 64
#: `event_uid`s remembered for dedupe.
DEDUPE_MEMORY = 4096
#: Devices whose rate-limit and condition state is kept. A device id can come
#: from an unauthenticated header, so the dicts keyed by it are bounded.
MAX_DEVICES = 64
#: One publish, connect to response.
PUBLISH_TIMEOUT = 10.0

#: Hub kinds that never concern a device's state.
_NOT_STATE = frozenset({"cmd", "patcher", "media", "devlog", "blocked", "redact"})


class Hub(Protocol):
    """The two calls of the event hub this module uses (`main/` hands it in)."""

    def subscribe(self) -> asyncio.Queue: ...

    def unsubscribe(self, q: asyncio.Queue) -> None: ...


def encode_header(text: str) -> str:
    """`text` as an HTTP header value ntfy accepts.

    Plain ASCII passes through; anything else (a pet called Félix) is sent as
    an RFC 2047 encoded word, which ntfy decodes — a raw UTF-8 header value is
    not something every hop preserves. Control characters are dropped so a
    name can never start a second header.
    """
    text = "".join(c for c in text if c.isprintable())
    if text.isascii():
        return text
    return "=?UTF-8?B?" + base64.b64encode(text.encode("utf-8")).decode("ascii") + "?="


class Notifier:
    """Turns the hub's feed into ntfy notifications. See the module docstring."""

    def __init__(self, config: NtfyConfig, registry: Any, *, pet_registry: Any = None,
                 hub: Hub | None = None, clock: Callable[[], float] = time.monotonic,
                 queue_size: int = QUEUE_SIZE, app_auth: Any = None) -> None:
        self._config = config
        self._registry = registry
        self._pets = pet_registry
        self._hub = hub
        self._clock = clock
        self._queue: asyncio.Queue[Notification] = asyncio.Queue(maxsize=queue_size)
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._sent: OrderedDict[tuple[Any, str], deque[float]] = OrderedDict()
        # Per device: condition id -> when it was first seen holding; and the
        # ids already announced (cleared once the condition stops holding).
        self._holding: OrderedDict[Any, dict[str, float]] = OrderedDict()
        self._announced: dict[Any, set[str]] = {}
        self._session: aiohttp.ClientSession | None = None
        self._failure = FailureLog(clock)
        # The phones' own endpoints live with their sessions in `AppAuth`.
        self._push = PushSink(app_auth, clock=clock) if app_auth is not None else None

    @property
    def config(self) -> NtfyConfig:
        return self._config

    @property
    def push(self) -> PushSink | None:
        return self._push

    # --- lifecycle ------------------------------------------------------------

    async def run(self) -> None:
        """Consume the hub until cancelled. One task; the publisher is its child."""
        if self._hub is None:
            return
        self.prime()
        feed = self._hub.subscribe()
        worker = asyncio.create_task(self.publish_forever(), name="ntfy-publisher")
        try:
            while True:
                event = await feed.get()
                try:
                    await self.handle(event)
                except Exception as e:  # noqa: BLE001 - the feed must outlive any one entry
                    self._failure(f"could not classify a hub entry ({type(e).__name__})")
        finally:
            self._hub.unsubscribe(feed)
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
            await self.close()

    async def close(self) -> None:
        if self._push is not None:
            await self._push.close()
        if self._session is not None:
            await self._session.close()
            self._session = None

    def prime(self) -> None:
        """Take every condition holding now as already announced.

        Without this a restart would re-announce every full bin and spent
        cartridge the owner already knows about.
        """
        for device in self._registry.all():
            current = rules.active_conditions(device.petkit_id, device.device_type, device.state)
            self._holding[device.petkit_id] = dict.fromkeys(current, float("-inf"))
            self._announced[device.petkit_id] = set(current)
        self._trim_devices()

    # --- classification -------------------------------------------------------

    async def handle(self, event: dict[str, Any]) -> None:
        """React to one hub entry. Public so tests can drive it without a hub."""
        kind = event.get("kind")
        device_id = event.get("device_id")
        detail = event.get("detail")
        if kind == "event" and isinstance(detail, dict):
            await self._on_event_row(detail)
        elif kind == "connect" and isinstance(detail, dict) and detail.get("online") is False:
            device = self._device(device_id)
            self.submit(rules.offline(device_id, device.device_type if device else None))
            return
        if device_id is not None and kind not in _NOT_STATE:
            device = self._device(device_id)
            if device is not None:
                self.evaluate_state(device)

    async def _on_event_row(self, row: dict[str, Any]) -> None:
        if row.get("is_new") is False:
            return  # a retried report updating a stored row: announced the first time
        pet_name = None
        pet_id = row.get("pet_id")
        if pet_id is not None and self._pets is not None:
            pet = await self._pets.get(pet_id)
            if pet:
                pet_name = pet.get("name") or None
        notification = rules.from_event(row, pet_name)
        if notification is not None:
            self.submit(notification)

    def evaluate_state(self, device: Any) -> None:
        """Announce each condition that started holding and has held long enough."""
        now = self._clock()
        did = device.petkit_id
        current = rules.active_conditions(did, device.device_type, device.state)
        before = self._holding.get(did, {})
        holding = {key: before.get(key, now) for key in current}
        self._holding[did] = holding
        self._holding.move_to_end(did)
        announced = self._announced.setdefault(did, set())
        announced &= set(current)  # a condition that stopped may fire again later
        for key, cond in current.items():
            if key not in announced and now - holding[key] >= cond.hold:
                announced.add(key)
                self.submit(cond.notification)
        self._trim_devices()

    def _device(self, device_id: Any) -> Any:
        try:
            return self._registry.get(int(device_id))
        except (TypeError, ValueError):
            return None

    def _trim_devices(self) -> None:
        while len(self._holding) > MAX_DEVICES:
            did, _ = self._holding.popitem(last=False)
            self._announced.pop(did, None)
        while len(self._sent) > MAX_DEVICES * len(RATE_LIMITS):
            self._sent.popitem(last=False)

    # --- gates ----------------------------------------------------------------

    def submit(self, notification: Notification) -> bool:
        """Queue `notification` if every gate lets it through; True if queued."""
        settings = self._config.settings
        if not settings.wants(notification.kind):
            return False
        if not (settings.ready or (self._push is not None
                                   and self._push.wants(notification.kind))):
            return False  # no sink would take it
        key = notification.dedupe_key
        if key:
            if key in self._seen:
                return False
            self._seen[key] = None
            while len(self._seen) > DEDUPE_MEMORY:
                self._seen.popitem(last=False)
        if not self._allow(notification.device_id, notification.kind):
            log.debug("ntfy: %s for device %s rate-limited", notification.kind,
                      notification.device_id)
            return False
        try:
            self._queue.put_nowait(notification)
        except asyncio.QueueFull:
            self._failure("the publish queue is full; dropping notifications")
            return False
        return True

    def _allow(self, device_id: Any, kind: str) -> bool:
        limit = RATE_LIMITS.get(kind)
        if limit is None:
            return True
        count, window = limit
        now = self._clock()
        sent = self._sent.setdefault((device_id, kind), deque())
        self._sent.move_to_end((device_id, kind))
        while sent and now - sent[0] >= window:
            sent.popleft()
        if len(sent) >= count:
            return False
        sent.append(now)
        return True

    # --- publishing -----------------------------------------------------------

    async def publish_forever(self) -> None:
        """Drain the queue until cancelled: pushes started, then one ntfy publish.

        The pushes run as their own tasks (`PushSink.dispatch` returns at
        once), so neither sink waits for the other.
        """
        while True:
            notification = await self._queue.get()
            settings = self._config.settings
            if not settings.wants(notification.kind):
                continue
            if self._push is not None:
                try:
                    self._push.dispatch(notification)
                except Exception as e:  # noqa: BLE001 - one sink must not end the other
                    self._failure(f"could not queue a push ({type(e).__name__})")
            if settings.ready:
                await self.publish(notification, settings)

    async def publish(self, notification: Notification,
                      settings: NtfySettings) -> tuple[bool, str]:
        """POST one notification to `{server}/{topic}`; `(ok, reason)`. Never raises.

        The reason never contains the topic or the token: both are secrets,
        and the reason is logged and shown in the panel.
        """
        headers = {
            "Title": encode_header(notification.title),
            "Priority": str(notification.priority),
            "Content-Type": "text/plain; charset=utf-8",
        }
        if notification.tags:
            headers["Tags"] = ",".join(notification.tags)
        if settings.token:
            headers["Authorization"] = f"Bearer {settings.token}"
        host = urlsplit(settings.server).hostname or "?"
        try:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=PUBLISH_TIMEOUT))
            # No redirects: one would carry the Authorization header to an
            # address nobody configured. TLS is verified (aiohttp's default).
            async with self._session.post(settings.publish_url,
                                          data=notification.message.encode("utf-8"),
                                          headers=headers, allow_redirects=False) as r:
                if 200 <= r.status < 300:
                    return True, ""
                body = (await r.text(errors="replace"))[:200]
                reason = f"{host} answered HTTP {r.status}: {body.strip()}"
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            reason = f"{host} did not answer within {PUBLISH_TIMEOUT:g}s"
        except (aiohttp.ClientError, OSError, ValueError) as e:
            reason = f"could not reach {host} ({type(e).__name__}: {e})"
        except Exception as e:  # noqa: BLE001 - a publish must never take the worker down
            reason = f"publishing to {host} failed ({type(e).__name__})"
        reason = _scrub(reason, settings)
        self._failure(f"publishing a {notification.kind} notification failed: {reason}")
        return False, reason

    async def send_test(self) -> tuple[bool, str]:
        """Publish a test notification now, bypassing the queue; `(ok, reason)`."""
        settings = self._config.settings
        if not settings.server or not settings.topic:
            return False, "set the server and enable notifications first"
        return await self.publish(Notification(
            kind=KIND_TEST, device_id=None, title="petkit-local",
            message="Test notification: petkit-local can reach this phone.",
            priority=rules.PRIORITY_DEFAULT, tags=("white_check_mark", KIND_TEST)), settings)


def _scrub(text: str, settings: NtfySettings) -> str:
    """`text` with the token and the topic replaced, wherever they appear."""
    for secret, mask in ((settings.token, "<token>"), (settings.topic, "<topic>")):
        if secret:
            text = text.replace(secret, mask)
    return text
