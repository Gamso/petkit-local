"""Notifications over the app API's own WebSocket (`/app/v1/notifications/ws`).

What is held here, in the order issue 89 asks:

* no socket opens without a session, and a bad `after` is refused before the
  upgrade;
* each notification arrives as the push message itself, with a monotonic id;
* the server's kind switches, the dedupe and the rate limit are decided ONCE
  for the UnifiedPush endpoints and the sockets, and each
  session's own `kinds` — settable without any push endpoint — filter after;
* catch-up: a phone that comes back with `after` is replayed what it missed
  (its kinds, nothing from before its session), then goes live with nothing
  lost or repeated; the bounds drop the oldest and say so (`gap`); the log and
  its ids survive a restart;
* one socket per session (4409 for the replaced one), 4401 for whatever ends a
  session, 1001 on shutdown, 1013 for a phone that stops reading;
* the panel shows whether each phone holds its socket;
* the OpenAPI file describes the routes and every message exactly.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import aiohttp
import pytest
import yaml
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.app_api import push
from petkit_local.app_api.appkeys import NOTIFY_CHANNEL
from petkit_local.app_api.auth import FILENAME, SESSION_IDLE_LIFETIME, AppAuth
from petkit_local.app_api.handlers.meta import OPENAPI_PATH
from petkit_local.app_api.notify import channel as channel_module
from petkit_local.app_api.notify.channel import (
    CLOSE_GOING_AWAY, CLOSE_REPLACED, CLOSE_SESSION_ENDED, CLOSE_TOO_SLOW, FILENAME as LOG_FILENAME,
    HEARTBEAT_SEC, NotifyChannel,
)
from petkit_local.app_api.notify.notifier import RATE_LIMITS, Notifier
from petkit_local.app_api.notify.rules import Notification
from petkit_local.app_api.notify.settings import (
    FILENAME as KINDS_FILENAME, KIND_ERROR, KIND_FEED_SERVED, KIND_TOILET, KINDS, NotifyConfig,
)
from petkit_local.app_api.push import PushRegistration
from petkit_local.app_api.server import PREFIX, _close_notify_sockets
from petkit_local.app_api.updates import Subscriber
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.web.appkeys import NOTIFIER
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app
from tests.test_app_api import FAST, PASSWORD, START, Clock, Env, _Schemas
from tests.test_app_api_updates import _next, _quiet
from tests.test_app_notify import _row
from tests.test_app_push import FakePush

TS = 1_763_150_000
WS = f"{PREFIX}/notifications/ws"


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _until(condition: Callable[[], bool], timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition never held"
        await asyncio.sleep(0.01)


async def _closed(ws: aiohttp.ClientWebSocketResponse, timeout: float = 3.0) -> int:
    """The code of the close frame the server sent.

    Read from the frame itself, not `ws.close_code`: the server drops the
    connection once its frame is out, and the client's own reply failing on
    that dropped connection rewrites `close_code` to 1006 after the fact.
    """
    deadline = time.monotonic() + timeout
    while True:
        msg = await ws.receive(timeout=max(0.01, deadline - time.monotonic()))
        if msg.type == aiohttp.WSMsgType.CLOSE:
            return msg.data
        assert msg.type not in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.ERROR), (msg.type, ws.exception())


class NotifyEnv(Env):
    """An app API whose notification channel is the notifier's."""

    def __init__(self, tmp: Path, store: Any, pet_registry: Any) -> None:
        super().__init__(tmp, store, pet_registry)
        self.kinds_config = NotifyConfig(tmp / KINDS_FILENAME)
        self.notifier = Notifier(self.kinds_config, self.registry, clock=Clock(1000.0),
                                 app_auth=self.auth, notify_log=tmp / LOG_FILENAME)
        self.channel = self.notifier.channel
        self.channel._check_interval = 0.05
        self.channel._save_delay = 0.01
        self.app[NOTIFY_CHANNEL] = self.channel
        self.token = ""

    async def connect(self, token: str | None = None,
                      after: Any = None) -> aiohttp.ClientWebSocketResponse:
        params = {"after": str(after)} if after is not None else None
        return await self.client.ws_connect(WS, params=params,
                                            headers=_bearer(token or self.token))

    async def event(self, row: dict[str, Any]) -> None:
        await self.notifier.handle({"kind": "event", "device_id": row["device_id"],
                                    "detail": {**row, "is_new": True}})

    async def settle(self) -> None:
        # Handing over is synchronous; this lets the sockets' writers run.
        for _ in range(3):
            await asyncio.sleep(0)

    def session_id(self) -> str:
        return next(s.id for s in self.auth.sessions() if s.name == "Pixel")


@pytest.fixture
async def nenv(tmp_path: Path, event_store, pet_registry):
    e = NotifyEnv(tmp_path, event_store, pet_registry)
    await e.start()
    await e.auth.set_password(PASSWORD)
    e.token = await e.login()
    yield e
    await e.client.close()
    await e.notifier.close()


@pytest.fixture
def allow_loopback(monkeypatch):
    """Let pushes reach a fake server on 127.0.0.1; every other rule stands."""
    real = push.refused_address

    def refused(value: Any) -> bool:
        try:
            if ipaddress.ip_address(str(value)).is_loopback:
                return False
        except ValueError:
            pass
        return real(value)

    monkeypatch.setattr(push, "refused_address", refused)


def _note(i: int, kind: str = KIND_ERROR) -> Notification:
    return Notification(kind=kind, device_id=100 + i, title="t", message="m", code="1", ts=TS)


def _drain(sub: Subscriber) -> list[dict[str, Any]]:
    """What a socket's writer would send, decoded (the close marker skipped)."""
    out = []
    while not sub.queue.empty():
        item = sub.queue.get_nowait()
        if not Subscriber.is_close(item):
            out.append(json.loads(item))
    return out


@pytest.fixture
async def unit(tmp_path: Path):
    """An account with one session, for driving a channel without sockets."""
    clock = Clock()
    auth = AppAuth(tmp_path / FILENAME, clock=clock, scrypt_params=FAST)
    await auth.set_password(PASSWORD)
    _, session = await auth.create_session("Pixel")
    return clock, auth, session


# --- opening ------------------------------------------------------------------------

async def test_no_socket_without_a_session_and_after_is_checked_before_the_upgrade(
        nenv: NotifyEnv):
    for headers in ({}, _bearer("nope")):
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            await nenv.client.ws_connect(WS, headers=headers)
        assert exc.value.status == 401
    for bad in ("-1", "x", "1.5", "1e3", "9" * 19, "١"):  # the last an Arabic-Indic 1
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            await nenv.connect(after=bad)
        assert exc.value.status == 400, bad
    r = await nenv.client.get(WS, headers=_bearer(nenv.token))
    assert r.status == 400 and "error" in await r.json()
    assert nenv.channel.sockets == 0
    # None of that counts as having used the socket.
    assert nenv.auth.notify_targets() == []


async def test_hello_then_each_notification_as_the_push_message_with_an_id(nenv: NotifyEnv):
    schemas = _Schemas()
    ws = await nenv.connect()
    hello = await _next(ws)
    schemas.check(hello, "NotificationHello")
    assert {k: v for k, v in hello.items() if k != "server_time"} == {
        "type": "hello", "heartbeat_sec": HEARTBEAT_SEC, "last_id": 0, "replayed": 0,
        "gap": False}
    assert HEARTBEAT_SEC < 100 / 2  # inside Cloudflare's idle timeout, twice over

    await nenv.event(_row("10", "t5", content={"is_shit": 1, "shit_weight": 32}, pet_id=2,
                          ts=TS + 0.7))
    got = await _next(ws)
    schemas.check(got, "NotificationDelivery")
    assert got == {"type": "notification", "id": 1, "replay": False,
                   "message": {"v": 1, "kind": KIND_TOILET, "device_id": 100, "pet_id": 2,
                               "code": "10", "ts": TS}}
    await nenv.event(_row("1", "t5", event_id="e2"))
    assert (await _next(ws))["id"] == 2

    await ws.send_str('{"type": "ping"}')
    await ws.send_str("not json")  # ignored
    assert await _next(ws) == {"type": "pong"}
    # Nothing else: the socket carries notifications only.
    assert await _quiet(ws, 0.2) == []
    await ws.close()
    await _until(lambda: nenv.channel.sockets == 0)
    assert nenv.channel._watch_task is None  # nothing runs once the last socket is gone


# --- one decision, then each phone's kinds ------------------------------------------

async def test_the_server_switches_and_each_sessions_kinds_filter(nenv: NotifyEnv):
    await nenv.kinds_config.set_kinds({KIND_FEED_SERVED: False})
    other = await nenv.login(name="errors only")
    r = await nenv.client.put(f"{PREFIX}/notifications",
                              json={"kinds": [KIND_ERROR, KIND_FEED_SERVED, "future_kind"]},
                              headers=_bearer(other))
    assert r.status == 200
    body = await r.json()
    _Schemas().check(body, "NotificationSettings")
    assert body["kinds"] == [KIND_FEED_SERVED, KIND_ERROR]  # known ones, server order
    assert {k["kind"]: k["enabled"] for k in body["server_kinds"]} == {
        k: k != KIND_FEED_SERVED for k in KINDS}

    everything = await nenv.connect()
    errors_only = await nenv.connect(other)
    await _next(everything, "hello")
    await _next(errors_only, "hello")
    await nenv.event(_row("10", "t5", event_id="v"))   # toilet
    await nenv.event(_row("1", "t5", event_id="e"))    # error
    await nenv.event(_row("2", "d4h", event_id="f"))   # feed_served: off on the server
    assert [(m["id"], m["message"]["kind"]) for m in
            (await _next(everything), await _next(everything))] == [
        (1, KIND_TOILET), (2, KIND_ERROR)]
    assert [(m["id"], m["message"]["kind"]) for m in (await _next(errors_only),)] == [
        (2, KIND_ERROR)]
    assert await _quiet(everything, 0.2) == [] and await _quiet(errors_only, 0.01) == []
    await everything.close()
    await errors_only.close()


async def test_one_decision_for_the_endpoints_and_the_sockets(nenv: NotifyEnv,
                                                             allow_loopback):
    endpoint = FakePush()
    await endpoint.start()
    try:
        await nenv.auth.set_push(nenv.session_id(), PushRegistration(endpoint.url("/a"), 0.0))
        ws = await nenv.connect()
        await _next(ws, "hello")
        await nenv.event(_row("10", "t5", event_id="same"))
        await nenv.event(_row("10", "t5", event_id="same"))  # a retried report
        count, _ = RATE_LIMITS[KIND_TOILET]
        for i in range(count + 3):
            await nenv.event(_row("10", "t5", event_id=f"v{i}"))
        await nenv.settle()
        await endpoint.wait(count)
        got = [await _next(ws) for _ in range(count)]
        assert await _quiet(ws, 0.2) == []
        assert len(endpoint.to("/a")) == len(got) == count
        assert [m["id"] for m in got] == list(range(1, count + 1))
        # The very same payload on both channels.
        assert [json.loads(r["body"]) for r in endpoint.to("/a")] == [m["message"] for m in got]
        await ws.close()
    finally:
        await nenv.notifier.push.close()
        await endpoint.server.close()


async def test_a_phone_is_owed_notifications_from_its_first_socket_on(nenv: NotifyEnv):
    note = Notification(kind=KIND_TOILET, device_id=1, title="t", message="m", dedupe_key="k")
    # No endpoint and a session that never opened the socket: no sink at all,
    # so nothing is classified or counted.
    assert nenv.notifier.submit(note) is False
    assert "k" not in nenv.notifier._seen
    ws = await nenv.connect()
    await _next(ws, "hello")
    await ws.close()
    await _until(lambda: nenv.channel.sockets == 0)
    # Disconnected now, but owed catch-up: wanted, and logged.
    assert nenv.notifier.submit(note) is True
    await nenv.settle()
    assert nenv.channel.last_id == 1
    # Choosing no kind at all stops it.
    r = await nenv.client.put(f"{PREFIX}/notifications", json={"kinds": []},
                              headers=_bearer(nenv.token))
    assert (await r.json())["kinds"] == []
    assert nenv.notifier.submit(Notification(kind=KIND_TOILET, device_id=2, title="t",
                                             message="m", dedupe_key="k2")) is False


# --- catch-up -----------------------------------------------------------------------

async def test_a_phone_back_from_a_dead_zone_gets_what_it_missed_then_live(nenv: NotifyEnv):
    ws = await nenv.connect()
    await _next(ws, "hello")
    await nenv.event(_row("10", "t5", event_id="seen"))
    seen = await _next(ws)
    await ws.close()
    await _until(lambda: nenv.channel.sockets == 0)

    # Missed while away: an error on another device, then a visit.
    await nenv.event(_row("1", "t5", event_id="m1", device_id=101))
    await nenv.event(_row("10", "t5", event_id="m2"))
    await nenv.settle()
    assert nenv.channel.last_id == 3

    ws = await nenv.connect(after=seen["id"])
    hello = await _next(ws)
    assert (hello["last_id"], hello["replayed"], hello["gap"]) == (3, 2, False)
    replayed = [await _next(ws) for _ in range(2)]
    assert [(m["id"], m["replay"], m["message"]["kind"]) for m in replayed] == [
        (2, True, KIND_ERROR), (3, True, KIND_TOILET)]
    await nenv.event(_row("1", "t5", event_id="live", device_id=102))
    live = await _next(ws)
    assert (live["id"], live["replay"]) == (4, False)
    await ws.close()

    # Up to date: nothing to replay.
    ws = await nenv.connect(after=4)
    assert (await _next(ws))["replayed"] == 0
    assert await _quiet(ws, 0.1) == []
    await ws.close()

    # The replay follows the kinds the phone holds NOW.
    await nenv.client.put(f"{PREFIX}/notifications", json={"kinds": [KIND_ERROR]},
                          headers=_bearer(nenv.token))
    ws = await nenv.connect(after=0)
    hello = await _next(ws)
    assert hello["replayed"] == 2
    assert [(await _next(ws))["id"] for _ in range(2)] == [2, 4]
    await ws.close()


async def test_a_new_session_is_never_replayed_what_came_before_it(nenv: NotifyEnv):
    ws = await nenv.connect()
    await _next(ws, "hello")
    await nenv.event(_row("10", "t5"))
    await _next(ws, "notification")
    await ws.close()
    nenv.clock.advance(60)
    newcomer = await nenv.login(name="new phone")
    ws = await nenv.connect(newcomer, after=0)
    hello = await _next(ws)
    assert (hello["last_id"], hello["replayed"], hello["gap"]) == (1, 0, False)
    assert await _quiet(ws, 0.1) == []
    await ws.close()


async def test_what_the_bounds_dropped_is_reported_as_a_gap(unit):
    clock, auth, session = unit
    channel = NotifyChannel(auth, capacity=3, retention=100.0)
    await channel.attach(session, None)
    for i in range(5):
        channel.deliver(_note(i))  # ids 1-5; 3-5 kept

    def hello_and_ids(sub: Subscriber) -> tuple[dict[str, Any], list[int]]:
        messages = _drain(sub)
        return messages[0], [m["id"] for m in messages[1:]]

    hello, ids = hello_and_ids(await channel.attach(session, 1))
    assert (hello["gap"], hello["replayed"], ids) == (True, 3, [3, 4, 5])  # 2 is gone
    hello, ids = hello_and_ids(await channel.attach(session, 2))
    assert (hello["gap"], ids) == (False, [3, 4, 5])
    # An id beyond the last: from a log that no longer exists. Everything, flagged.
    hello, ids = hello_and_ids(await channel.attach(session, 99))
    assert (hello["gap"], hello["last_id"], ids) == (True, 5, [3, 4, 5])
    # Retention: three days in production, 100 s here.
    clock.advance(101)
    hello, ids = hello_and_ids(await channel.attach(session, 5))
    assert (hello["gap"], ids) == (False, [])
    hello, ids = hello_and_ids(await channel.attach(session, 4))
    assert (hello["gap"], ids) == (True, [])
    # The ids go on from where they were, never reused.
    channel.deliver(_note(9))
    assert channel.last_id == 6
    await channel.shutdown()


async def test_the_log_and_its_ids_survive_a_restart(unit, tmp_path: Path):
    _, auth, session = unit
    path = tmp_path / LOG_FILENAME
    channel = NotifyChannel(auth, path, save_delay=0.01)
    await channel.attach(session, None)
    for i in range(3):
        channel.deliver(_note(i))
    # Written on its own, shortly after the burst.
    await _until(lambda: path.exists() and len(json.loads(path.read_text())["entries"]) == 3)
    channel.deliver(_note(3))
    await channel.shutdown()  # ... and on shutdown, whatever is still pending
    stored = json.loads(path.read_text())
    assert stored["next_id"] == 5 and [e["id"] for e in stored["entries"]] == [1, 2, 3, 4]
    assert stored["entries"][0]["message"] == {"v": 1, "kind": KIND_ERROR, "device_id": 100,
                                               "pet_id": None, "code": "1", "ts": TS}

    again = NotifyChannel(auth, path)
    assert again.last_id == 4
    messages = _drain(await again.attach(session, 2))
    assert [m["id"] for m in messages[1:]] == [3, 4]
    again.deliver(_note(4))
    assert again.last_id == 5
    await again.shutdown()

    # A damaged file costs the catch-up, never the start; what can be read is kept.
    path.write_text("{not json", encoding="utf-8")
    assert NotifyChannel(auth, path).last_id == 0
    path.write_text(json.dumps({"next_id": 2, "entries": [
        {"id": "x"}, 5, {"id": 7, "at": START, "message": {"kind": KIND_TOILET}},
        {"id": 8, "at": START, "message": "nope"}]}), encoding="utf-8")
    damaged = NotifyChannel(auth, path)
    assert damaged.last_id == 7
    messages = _drain(await damaged.attach(session, 0))
    assert [m["id"] for m in messages[1:]] == [7]
    await damaged.shutdown()


# --- session rules and close codes --------------------------------------------------

async def test_a_second_socket_replaces_the_first_with_4409(nenv: NotifyEnv):
    first = await nenv.connect()
    await _next(first, "hello")
    second = await nenv.connect()
    await _next(second, "hello")
    assert await _closed(first) == CLOSE_REPLACED
    await nenv.event(_row("10", "t5"))
    assert (await _next(second))["id"] == 1
    assert nenv.channel.sockets == 1 and nenv.channel.connected(nenv.session_id())
    await second.close()
    await _until(lambda: nenv.channel.sockets == 0)


@pytest.mark.parametrize("how", ["logout", "revoked", "password changed", "password removed",
                                 "expired"])
async def test_whatever_ends_the_session_closes_its_socket_with_4401(nenv: NotifyEnv, how: str):
    ws = await nenv.connect()
    await _next(ws, "hello")
    if how == "logout":
        await nenv.client.post(f"{PREFIX}/auth/logout", headers=_bearer(nenv.token))
    elif how == "revoked":
        assert await nenv.auth.revoke(nenv.session_id())
    elif how == "password changed":
        await nenv.auth.set_password("another long password")
    elif how == "password removed":
        await nenv.auth.clear_password()
    else:
        nenv.clock.advance(SESSION_IDLE_LIFETIME)
    assert await _closed(ws) == CLOSE_SESSION_ENDED
    # And it is owed nothing any more.
    assert nenv.auth.notify_targets() == []
    assert not nenv.channel.wants(KIND_TOILET)


async def test_shutdown_closes_every_socket_with_1001(nenv: NotifyEnv):
    ws = await nenv.connect()
    await _next(ws, "hello")
    await _close_notify_sockets(nenv.app)
    assert await _closed(ws) == CLOSE_GOING_AWAY


async def test_a_phone_that_stops_reading_is_cut_off_not_buffered_for(unit, monkeypatch):
    _, auth, session = unit
    monkeypatch.setattr(channel_module, "LIVE_QUEUE", 1)
    channel = NotifyChannel(auth)
    sub = await channel.attach(session, None)  # room for the hello and one more
    assert channel.deliver(_note(0)) == 1
    assert channel.deliver(_note(1)) == 0
    assert sub.close_code == CLOSE_TOO_SLOW
    # Nothing is lost to it: the log has both, for its reconnection.
    assert channel.last_id == 2
    await channel.detach(sub)
    assert channel._watch_task is None


# --- kinds without a push endpoint --------------------------------------------------

async def test_kinds_are_set_without_a_push_endpoint_and_shared_with_it(nenv: NotifyEnv,
                                                                        tmp_path: Path):
    schemas = _Schemas()
    headers = _bearer(nenv.token)
    r = await nenv.client.get(f"{PREFIX}/notifications", headers=headers)
    body = await r.json()
    schemas.check(body, "NotificationSettings")
    assert body == {"kinds": None, "connected": False, "last_id": 0,
                    "server_kinds": [{"kind": k, "enabled": True} for k in KINDS]}

    r = await nenv.client.put(f"{PREFIX}/notifications",
                              json={"kinds": [KIND_ERROR, "future_kind"]}, headers=headers)
    assert (await r.json())["kinds"] == [KIND_ERROR]
    info = await (await nenv.get("/info", nenv.token)).json()
    schemas.check(info, "Info")
    assert info["session"]["kinds"] == [KIND_ERROR]

    # A push registration without kinds keeps the session's; with them, replaces them.
    r = await nenv.client.put(f"{PREFIX}/push", json={"endpoint": "https://push.example.org/upX"},
                              headers=headers)
    assert (await r.json())["push"]["kinds"] == [KIND_ERROR]
    r = await nenv.client.put(f"{PREFIX}/push", json={"endpoint": "https://push.example.org/upX",
                                                      "kinds": [KIND_TOILET]}, headers=headers)
    assert (await r.json())["push"]["kinds"] == [KIND_TOILET]
    r = await nenv.client.get(f"{PREFIX}/notifications", headers=headers)
    assert (await r.json())["kinds"] == [KIND_TOILET]
    # Unregistering the endpoint keeps them: they choose what the socket gets too.
    await nenv.client.delete(f"{PREFIX}/push", headers=headers)
    r = await nenv.client.get(f"{PREFIX}/notifications", headers=headers)
    assert (await r.json())["kinds"] == [KIND_TOILET]
    r = await nenv.client.put(f"{PREFIX}/notifications", json={"kinds": None}, headers=headers)
    assert (await r.json())["kinds"] is None

    for body, status, code in (({}, 400, "bad_parameter"),
                               ({"kinds": "toilet"}, 400, "bad_parameter"),
                               ({"kinds": [1]}, 400, "bad_parameter"),
                               ({"kinds": ["x"] * 33}, 400, "bad_parameter"),
                               ([], 400, "bad_json")):
        r = await nenv.client.put(f"{PREFIX}/notifications", json=body, headers=headers)
        assert r.status == status and (await r.json())["error"] == code, body
    r = await nenv.client.put(f"{PREFIX}/notifications", data="kinds=x", headers=headers)
    assert r.status == 415
    r = await nenv.client.put(f"{PREFIX}/notifications", json={"kinds": None})
    assert r.status == 401
    r = await nenv.client.get(f"{PREFIX}/notifications")
    assert r.status == 401

    # Stored with the session.
    await nenv.client.put(f"{PREFIX}/notifications", json={"kinds": []}, headers=headers)
    again = AppAuth(tmp_path / FILENAME, clock=nenv.clock, scrypt_params=FAST)
    assert next(s for s in again.sessions() if s.name == "Pixel").kinds == ()


def test_kinds_stored_in_an_old_push_registration_become_the_sessions(tmp_path: Path):
    path = tmp_path / FILENAME
    path.write_text(json.dumps({"version": 1, "password": None, "sessions": [{
        "id": "abc", "token_hash": "0" * 64, "name": "Pixel", "created": START,
        "last_used": START, "push": {"endpoint": "https://push.example.org/upX",
                                     "kinds": [KIND_ERROR], "registered": START}}]}))
    auth = AppAuth(path, clock=Clock(), scrypt_params=FAST)
    session = auth.sessions()[0]
    assert session.kinds == (KIND_ERROR,) and session.notify_ws is None
    assert session.push.endpoint == "https://push.example.org/upX"
    stored = session.to_json()
    # Written both ways, so a downgrade still reads them from the registration.
    assert stored["kinds"] == stored["push"]["kinds"] == [KIND_ERROR]


# --- the panel ----------------------------------------------------------------------

async def test_the_panel_shows_whether_each_phone_holds_its_socket(nenv: NotifyEnv):
    await nenv.login(name="Tablet")
    panel = create_panel_app(DeviceRegistry(), None, EventHub(), {}, app_auth=nenv.auth)
    panel[NOTIFIER] = nenv.notifier
    client = TestClient(TestServer(panel))
    await client.start_server()

    async def notify() -> dict[str, Any]:
        sessions = (await (await client.get("/api/app-access")).json())["sessions"]
        return {s["name"]: s["notify"] for s in sessions}

    try:
        assert await notify() == {"Pixel": None, "Tablet": None}
        ws = await nenv.connect()
        await _next(ws, "hello")
        assert await notify() == {"Pixel": {"connected": True, "since": START}, "Tablet": None}
        nenv.clock.advance(30)
        await ws.close()
        await _until(lambda: nenv.channel.sockets == 0)
        assert await notify() == {"Pixel": {"connected": False, "since": START + 30},
                                  "Tablet": None}
    finally:
        await client.close()


# --- OpenAPI ------------------------------------------------------------------------

def test_the_openapi_file_documents_the_socket_and_its_messages():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    schemas = spec["components"]["schemas"]
    assert set(spec["paths"]["/notifications"]) == {"get", "put"}
    route = spec["paths"]["/notifications/ws"]["get"]
    assert [p["name"] for p in route["parameters"]] == ["after"]
    assert schemas["NotificationKind"]["enum"] == list(KINDS)
    assert schemas["NotificationDelivery"]["properties"]["message"] == {
        "$ref": "#/components/schemas/PushMessage"}
    assert set(schemas["NotificationSocketMessage"]["discriminator"]["mapping"]) == {
        "hello", "notification", "pong"}
    for name in ("NotificationHello", "NotificationDelivery", "NotificationSettings"):
        assert set(schemas[name]["required"]) == set(schemas[name]["properties"]), name
    assert "kinds" in schemas["Session"]["required"]
