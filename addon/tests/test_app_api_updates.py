"""The phone app API's WebSocket (`GET /app/v1/ws`, `app_api/updates.py`).

What is held here:

* no socket opens without a session — the upgrade is checked like any request;
* the first message is a `hello` with every device, and after it only CHANGES
  are sent: the entities whose value moved, a device's summary when it went
  online or offline, a timeline card that is new or changed;
* a burst of reports is one message per device per coalescing window;
* a session that ends (logout, revocation) has its socket closed within a few
  seconds, a client that stops reading is cut off rather than buffered for, and
  shutdown closes every socket with 1001;
* nothing runs once the last socket is gone;
* every message has exactly its schema's keys.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import WSMsgType

from petkit_local.app_api.appkeys import UPDATE_FEED
from petkit_local.app_api.updates import (
    CLOSE_GOING_AWAY, CLOSE_SESSION_ENDED, CLOSE_TOO_SLOW, Subscriber, UpdateFeed,
)
from petkit_local.app_api.server import PREFIX
from petkit_local.web.hub import EventHub
from tests.test_app_api import PASSWORD, Env, _Schemas

#: Short enough for a test, long enough that a burst lands in one window.
COALESCE = 0.2
SWEEP = 0.6


class FeedEnv(Env):
    def __init__(self, tmp: Path, store: Any, pet_registry: Any) -> None:
        super().__init__(tmp, store, pet_registry)
        self.hub = EventHub()
        self.feed = UpdateFeed(self.auth, self.registry, self.hub, event_store=store,
                             pet_registry=pet_registry, media_root=str(self.media_root),
                             coalesce=COALESCE, sweep=SWEEP)
        self.app[UPDATE_FEED] = self.feed
        self.token = ""

    async def connect(self, token: str | None = None) -> aiohttp.ClientWebSocketResponse:
        return await self.client.ws_connect(
            f"{PREFIX}/ws", headers={"Authorization": f"Bearer {token or self.token}"})


@pytest.fixture
async def feed_env(tmp_path: Path, event_store, pet_registry):
    e = FeedEnv(tmp_path, event_store, pet_registry)
    await e.start()
    await e.auth.set_password(PASSWORD)
    e.token = await e.login()
    yield e
    await e.client.close()


async def _next(ws: aiohttp.ClientWebSocketResponse, kind: str | None = None,
                timeout: float = 3.0) -> dict[str, Any]:
    """The next message (of `kind`, skipping others), as JSON."""
    deadline = time.monotonic() + timeout
    while True:
        msg = await ws.receive(timeout=max(0.01, deadline - time.monotonic()))
        assert msg.type == WSMsgType.TEXT, (msg.type, msg.data, msg.extra)
        data = json.loads(msg.data)
        if kind is None or data["type"] == kind:
            return data


async def _quiet(ws: aiohttp.ClientWebSocketResponse, seconds: float) -> list[dict[str, Any]]:
    """Everything that arrives within `seconds`."""
    got = []
    deadline = time.monotonic() + seconds
    while (left := deadline - time.monotonic()) > 0:
        try:
            msg = await ws.receive(timeout=left)
        except asyncio.TimeoutError:
            break
        if msg.type == WSMsgType.TEXT:
            got.append(json.loads(msg.data))
    return got


async def _closed(ws: aiohttp.ClientWebSocketResponse, timeout: float = 3.0) -> int:
    deadline = time.monotonic() + timeout
    while True:
        msg = await ws.receive(timeout=max(0.01, deadline - time.monotonic()))
        if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING):
            return ws.close_code


# --- opening --------------------------------------------------------------------------

async def test_no_socket_opens_without_a_session(feed_env: FeedEnv):
    for headers in ({}, {"Authorization": "Bearer nope"}):
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            await feed_env.client.ws_connect(f"{PREFIX}/ws", headers=headers)
        assert exc.value.status == 401
    assert feed_env.feed.subscribers == 0


async def test_a_plain_get_is_refused_as_json(feed_env: FeedEnv):
    r = await feed_env.client.get(f"{PREFIX}/ws", headers={"Authorization": f"Bearer {feed_env.token}"})
    assert r.status == 400
    assert "error" in await r.json()


async def test_the_first_message_is_hello_with_every_device(feed_env: FeedEnv):
    ws = await feed_env.connect()
    hello = await _next(ws)
    assert hello["type"] == "hello"
    assert [d["id"] for d in hello["devices"]] == [100]
    assert hello["heartbeat_sec"] > 0
    schemas = _Schemas()
    schemas.check(hello, "UpdateHello")
    await ws.close()


async def test_ping_is_answered(feed_env: FeedEnv):
    ws = await feed_env.connect()
    await _next(ws, "hello")
    await ws.send_str('{"type": "ping"}')
    await ws.send_str("not json")  # ignored
    assert await _next(ws) == {"type": "pong"}
    await ws.close()


# --- what is sent -------------------------------------------------------------------

async def test_only_changed_entities_are_sent_once_per_window(feed_env: FeedEnv):
    ws = await feed_env.connect()
    await _next(ws, "hello")
    settings = feed_env.device.config.setdefault("settings", {})
    # A burst within one window: three frames, two writes to the same value.
    settings["manualLock"] = 1
    feed_env.hub.publish("mqtt", 100, "property/post")
    settings["volume"] = 3
    feed_env.hub.publish("mqtt", 100, "property/post")
    settings["volume"] = 4
    feed_env.hub.publish("http", 100, "dev_state_report")
    states = [m for m in await _quiet(ws, COALESCE * 4) if m["type"] == "state"]
    assert len(states) == 1, states
    changed = {e["key"]: e["value"] for e in states[0]["entities"]}
    assert changed == {"child_lock": True, "volume": 4}
    _Schemas().check(states[0], "UpdateState")

    # Nothing changed: nothing is sent, however much the device talks.
    for _ in range(20):
        feed_env.hub.publish("mqtt", 100, "property/post")
    assert not [m for m in await _quiet(ws, COALESCE * 3) if m["type"] == "state"]
    await ws.close()


async def test_going_offline_is_sent_without_any_report(feed_env: FeedEnv):
    feed_env.device.online = True
    ws = await feed_env.connect()
    hello = await _next(ws, "hello")
    assert hello["devices"][0]["online"] is True
    # The offline watchdog flips this without anything reaching the hub; the
    # sweep is what notices.
    feed_env.device.online = False
    message = await _next(ws, "device", timeout=SWEEP * 4)
    assert message["device"]["id"] == 100 and message["device"]["online"] is False
    _Schemas().check(message, "UpdateDevice")
    await ws.close()


async def test_a_new_and_a_removed_device_are_announced(feed_env: FeedEnv):
    ws = await feed_env.connect()
    await _next(ws, "hello")
    feed_env.registry.get_or_create(petkit_id=300, device_type="d4h", serial_number="SN300")
    feed_env.hub.publish("http", 300, "dev_signup")
    added = await _next(ws, "device")
    assert added["device"]["id"] == 300
    feed_env.registry.remove(300) if hasattr(feed_env.registry, "remove") else \
        feed_env.registry._devices.pop(300)
    removed = await _next(ws, "device_removed", timeout=SWEEP * 4)
    assert removed == {"type": "device_removed", "device_id": 300}
    await ws.close()


async def test_a_new_timeline_card_is_sent_and_history_is_not(feed_env: FeedEnv):
    now = time.time()
    await feed_env.store.upsert_event({
        "device_id": 100, "device_type": "t5", "event_type": "error_start",
        "event_kind": "error", "ts": now - 60, "related_event": "old"})
    ws = await feed_env.connect()
    await _next(ws, "hello")
    event_id = await feed_env.store.upsert_event({
        "device_id": 100, "device_type": "t5", "event_type": "pet_out",
        "event_kind": "toilet_visit", "ts": now, "related_event": "visit1",
        "content_json": json.dumps({"pet_weight": 4300})})
    feed_env.hub.publish("event", 100, "pet_out")
    message = await _next(ws, "timeline")
    assert message["item"]["id"] == event_id and message["replaces"] == []
    _Schemas().check(message, "UpdateTimeline")
    # The card already sent is not sent again until it changes.
    feed_env.hub.publish("event", 100, "again")
    assert not [m for m in await _quiet(ws, COALESCE * 3) if m["type"] == "timeline"]
    await ws.close()


# --- closing ------------------------------------------------------------------------

async def test_logging_out_closes_that_sessions_socket(feed_env: FeedEnv):
    other = await feed_env.login(name="Other phone")
    ws = await feed_env.connect()
    ws_other = await feed_env.connect(other)
    await _next(ws, "hello")
    await _next(ws_other, "hello")
    r = await feed_env.client.post(f"{PREFIX}/auth/logout",
                               headers={"Authorization": f"Bearer {feed_env.token}"})
    assert r.status == 200
    assert await _closed(ws) == CLOSE_SESSION_ENDED
    # The other phone's session is untouched.
    await ws_other.send_str('{"type": "ping"}')
    assert await _next(ws_other, "pong")
    await ws_other.close()


async def test_revoking_from_the_panel_or_removing_the_password_closes_sockets(feed_env: FeedEnv):
    ws = await feed_env.connect()
    await _next(ws, "hello")
    session_id = feed_env.auth.sessions()[0].id
    assert await feed_env.auth.revoke(session_id)
    assert await _closed(ws) == CLOSE_SESSION_ENDED

    token = await feed_env.login()
    ws = await feed_env.connect(token)
    await _next(ws, "hello")
    await feed_env.auth.clear_password()
    assert await _closed(ws) == CLOSE_SESSION_ENDED


async def test_shutdown_closes_every_socket_with_going_away(feed_env: FeedEnv):
    ws = await feed_env.connect()
    await _next(ws, "hello")
    await feed_env.app.shutdown()  # the runner's first step: the app's on_shutdown hook
    assert await _closed(ws) == CLOSE_GOING_AWAY


async def test_the_last_socket_out_stops_the_feed(feed_env: FeedEnv):
    first, second = await feed_env.connect(), await feed_env.connect()
    await _next(first, "hello")
    await _next(second, "hello")
    assert feed_env.feed.subscribers == 2 and feed_env.hub._subs
    await first.close()
    await second.close()
    for _ in range(50):
        if feed_env.feed.subscribers == 0 and not feed_env.hub._subs:
            break
        await asyncio.sleep(0.02)
    assert feed_env.feed.subscribers == 0
    assert not feed_env.hub._subs, "the hub subscription outlived the last socket"


async def test_a_client_that_stops_reading_is_cut_off_not_buffered():
    sub = Subscriber(session=None, limit=3)  # type: ignore[arg-type]
    assert all(sub.push(f"m{i}") for i in range(3))
    assert not sub.push("one too many")
    assert sub.close_code == CLOSE_TOO_SLOW
    # Everything queued is dropped; the writer is handed only the close.
    assert sub.queue.qsize() == 1 and Subscriber.is_close(sub.queue.get_nowait())
    assert not sub.push("after the end")


async def test_a_slow_socket_is_closed_with_too_slow(feed_env: FeedEnv):
    feed_env.feed._queue_limit = 2
    ws = await feed_env.connect()
    await _next(ws, "hello")
    # Fill its outbox faster than its writer can drain it: a burst in one step.
    assert feed_env.feed.subscribers == 1
    for sub in list(feed_env.feed._subs):
        for i in range(5):
            sub.push(f'{{"type":"filler","n":{i}}}')
    assert await _closed(ws) == CLOSE_TOO_SLOW


def test_both_app_websockets_close_with_the_same_codes():
    """An app handles the live-updates and the live-video socket with one
    close handler, so the shared situations must carry the same code."""
    from petkit_local.app_api import updates, video

    assert updates.CLOSE_SESSION_ENDED == video.CLOSE_SESSION_ENDED == 4401
    assert updates.CLOSE_TOO_SLOW == video.CLOSE_TOO_SLOW == 1013
    assert updates.CLOSE_GOING_AWAY == video.CLOSE_SHUTDOWN == 1001
