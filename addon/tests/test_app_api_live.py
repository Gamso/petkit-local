"""The phone app API's WebSocket (`GET /app/v1/ws`, `app_api/live.py`).

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

from petkit_local.app_api.appkeys import LIVE
from petkit_local.app_api.live import (
    CLOSE_GOING_AWAY, CLOSE_SESSION_ENDED, CLOSE_TOO_SLOW, LiveFeed, Subscriber,
)
from petkit_local.app_api.server import PREFIX
from petkit_local.web.hub import EventHub
from tests.test_app_api import PASSWORD, Env, _Schemas

#: Short enough for a test, long enough that a burst lands in one window.
COALESCE = 0.2
SWEEP = 0.6


class LiveEnv(Env):
    def __init__(self, tmp: Path, store: Any, pet_registry: Any) -> None:
        super().__init__(tmp, store, pet_registry)
        self.hub = EventHub()
        self.feed = LiveFeed(self.auth, self.registry, self.hub, event_store=store,
                             pet_registry=pet_registry, media_root=str(self.media_root),
                             coalesce=COALESCE, sweep=SWEEP)
        self.app[LIVE] = self.feed
        self.token = ""

    async def connect(self, token: str | None = None) -> aiohttp.ClientWebSocketResponse:
        return await self.client.ws_connect(
            f"{PREFIX}/ws", headers={"Authorization": f"Bearer {token or self.token}"})


@pytest.fixture
async def live(tmp_path: Path, event_store, pet_registry):
    e = LiveEnv(tmp_path, event_store, pet_registry)
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

async def test_no_socket_opens_without_a_session(live: LiveEnv):
    for headers in ({}, {"Authorization": "Bearer nope"}):
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            await live.client.ws_connect(f"{PREFIX}/ws", headers=headers)
        assert exc.value.status == 401
    assert live.feed.subscribers == 0


async def test_a_plain_get_is_refused_as_json(live: LiveEnv):
    r = await live.client.get(f"{PREFIX}/ws", headers={"Authorization": f"Bearer {live.token}"})
    assert r.status == 400
    assert "error" in await r.json()


async def test_the_first_message_is_hello_with_every_device(live: LiveEnv):
    ws = await live.connect()
    hello = await _next(ws)
    assert hello["type"] == "hello"
    assert [d["id"] for d in hello["devices"]] == [100]
    assert hello["heartbeat_sec"] > 0
    schemas = _Schemas()
    schemas.check(hello, "LiveHello")
    await ws.close()


async def test_ping_is_answered(live: LiveEnv):
    ws = await live.connect()
    await _next(ws, "hello")
    await ws.send_str('{"type": "ping"}')
    await ws.send_str("not json")  # ignored
    assert await _next(ws) == {"type": "pong"}
    await ws.close()


# --- what is sent -------------------------------------------------------------------

async def test_only_changed_entities_are_sent_once_per_window(live: LiveEnv):
    ws = await live.connect()
    await _next(ws, "hello")
    settings = live.device.config.setdefault("settings", {})
    # A burst within one window: three frames, two writes to the same value.
    settings["manualLock"] = 1
    live.hub.publish("mqtt", 100, "property/post")
    settings["volume"] = 3
    live.hub.publish("mqtt", 100, "property/post")
    settings["volume"] = 4
    live.hub.publish("http", 100, "dev_state_report")
    states = [m for m in await _quiet(ws, COALESCE * 4) if m["type"] == "state"]
    assert len(states) == 1, states
    changed = {e["key"]: e["value"] for e in states[0]["entities"]}
    assert changed == {"child_lock": True, "volume": 4}
    _Schemas().check(states[0], "LiveState")

    # Nothing changed: nothing is sent, however much the device talks.
    for _ in range(20):
        live.hub.publish("mqtt", 100, "property/post")
    assert not [m for m in await _quiet(ws, COALESCE * 3) if m["type"] == "state"]
    await ws.close()


async def test_going_offline_is_sent_without_any_report(live: LiveEnv):
    live.device.online = True
    ws = await live.connect()
    hello = await _next(ws, "hello")
    assert hello["devices"][0]["online"] is True
    # The offline watchdog flips this without anything reaching the hub; the
    # sweep is what notices.
    live.device.online = False
    message = await _next(ws, "device", timeout=SWEEP * 4)
    assert message["device"]["id"] == 100 and message["device"]["online"] is False
    _Schemas().check(message, "LiveDevice")
    await ws.close()


async def test_a_new_and_a_removed_device_are_announced(live: LiveEnv):
    ws = await live.connect()
    await _next(ws, "hello")
    live.registry.get_or_create(petkit_id=300, device_type="d4h", serial_number="SN300")
    live.hub.publish("http", 300, "dev_signup")
    added = await _next(ws, "device")
    assert added["device"]["id"] == 300
    live.registry.remove(300) if hasattr(live.registry, "remove") else \
        live.registry._devices.pop(300)
    removed = await _next(ws, "device_removed", timeout=SWEEP * 4)
    assert removed == {"type": "device_removed", "device_id": 300}
    await ws.close()


async def test_a_new_timeline_card_is_sent_and_history_is_not(live: LiveEnv):
    now = time.time()
    await live.store.upsert_event({
        "device_id": 100, "device_type": "t5", "event_type": "error_start",
        "event_kind": "error", "ts": now - 60, "related_event": "old"})
    ws = await live.connect()
    await _next(ws, "hello")
    event_id = await live.store.upsert_event({
        "device_id": 100, "device_type": "t5", "event_type": "pet_out",
        "event_kind": "toilet_visit", "ts": now, "related_event": "visit1",
        "content_json": json.dumps({"pet_weight": 4300})})
    live.hub.publish("event", 100, "pet_out")
    message = await _next(ws, "timeline")
    assert message["item"]["id"] == event_id and message["replaces"] == []
    _Schemas().check(message, "LiveTimeline")
    # The card already sent is not sent again until it changes.
    live.hub.publish("event", 100, "again")
    assert not [m for m in await _quiet(ws, COALESCE * 3) if m["type"] == "timeline"]
    await ws.close()


# --- closing ------------------------------------------------------------------------

async def test_logging_out_closes_that_sessions_socket(live: LiveEnv):
    other = await live.login(name="Other phone")
    ws = await live.connect()
    ws_other = await live.connect(other)
    await _next(ws, "hello")
    await _next(ws_other, "hello")
    r = await live.client.post(f"{PREFIX}/auth/logout",
                               headers={"Authorization": f"Bearer {live.token}"})
    assert r.status == 200
    assert await _closed(ws) == CLOSE_SESSION_ENDED
    # The other phone's session is untouched.
    await ws_other.send_str('{"type": "ping"}')
    assert await _next(ws_other, "pong")
    await ws_other.close()


async def test_revoking_from_the_panel_or_removing_the_password_closes_sockets(live: LiveEnv):
    ws = await live.connect()
    await _next(ws, "hello")
    session_id = live.auth.sessions()[0].id
    assert await live.auth.revoke(session_id)
    assert await _closed(ws) == CLOSE_SESSION_ENDED

    token = await live.login()
    ws = await live.connect(token)
    await _next(ws, "hello")
    await live.auth.clear_password()
    assert await _closed(ws) == CLOSE_SESSION_ENDED


async def test_shutdown_closes_every_socket_with_going_away(live: LiveEnv):
    ws = await live.connect()
    await _next(ws, "hello")
    await live.app.shutdown()  # the runner's first step: the app's on_shutdown hook
    assert await _closed(ws) == CLOSE_GOING_AWAY


async def test_the_last_socket_out_stops_the_feed(live: LiveEnv):
    first, second = await live.connect(), await live.connect()
    await _next(first, "hello")
    await _next(second, "hello")
    assert live.feed.subscribers == 2 and live.hub._subs
    await first.close()
    await second.close()
    for _ in range(50):
        if live.feed.subscribers == 0 and not live.hub._subs:
            break
        await asyncio.sleep(0.02)
    assert live.feed.subscribers == 0
    assert not live.hub._subs, "the hub subscription outlived the last socket"


async def test_a_client_that_stops_reading_is_cut_off_not_buffered():
    sub = Subscriber(session=None, limit=3)  # type: ignore[arg-type]
    assert all(sub.push(f"m{i}") for i in range(3))
    assert not sub.push("one too many")
    assert sub.close_code == CLOSE_TOO_SLOW
    # Everything queued is dropped; the writer is handed only the close.
    assert sub.queue.qsize() == 1 and Subscriber.is_close(sub.queue.get_nowait())
    assert not sub.push("after the end")


async def test_a_slow_socket_is_closed_with_too_slow(live: LiveEnv):
    live.feed._queue_limit = 2
    ws = await live.connect()
    await _next(ws, "hello")
    # Fill its outbox faster than its writer can drain it: a burst in one step.
    assert live.feed.subscribers == 1
    for sub in list(live.feed._subs):
        for i in range(5):
            sub.push(f'{{"type":"filler","n":{i}}}')
    assert await _closed(ws) == CLOSE_TOO_SLOW
