"""The opt-in keep-online heartbeat (`http/presence.py`).

A device on local MQTT stops sending its HTTP heartbeat, and PetKit's app then
shows it offline. With `proxy_keep_online` on, its heartbeat is sent to PetKit
in its name — paced by the device's own MQTT packets, signed as the device,
and with PetKit's reply recorded and never applied. Everything below is one of
those promises, or one of the conditions it is gated on.
"""
from __future__ import annotations

import asyncio
import json
from urllib.parse import parse_qs

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from amqtt.contexts import BaseContext

from petkit_local.devices.base import Device
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.http.cloud_fetch import signature
from petkit_local.http.presence import (
    MIN_INTERVAL_SECONDS, CloudPresence, blocker, heartbeat_request,
    remember_heartbeat, withheld_commands,
)
from petkit_local.http.proxy import close_proxy_session
from petkit_local.http.redact import RedactionPolicy
from petkit_local.http.server import create_app
from petkit_local.mqtt.auth import AliyunAuthPlugin
from petkit_local.web.hub import EventHub

SECRET = "0123456789abcdef"

#: What PetKit might answer a heartbeat with: a queued command, an OTA push
#: and a settings write. None of it may ever reach the device.
LOADED_REPLY = {"result": [
    {"time": 1, "timestamp": 1, "content": json.dumps(
        {"msgType": 2, "type": "start", "payload": {"start_action": 0}})},
    {"time": 2, "timestamp": 2, "content": json.dumps(
        {"msgType": 1, "payload": {"lightMode": 0}})},
    {"time": 3, "timestamp": 3, "content": json.dumps(
        {"msgType": 0, "type": "ota", "payload": {"url": "http://evil/fw.bin"}})},
]}


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class _Upstream:
    """A stand-in for PetKit's API server that records every request."""

    def __init__(self, reply: dict | None = None, status: int = 200,
                 delay: float = 0.0) -> None:
        self.reply = reply if reply is not None else {"result": [{"time": 1}]}
        self.status = status
        self.delay = delay
        self.requests: list[dict] = []
        self.client: TestClient | None = None

    async def _handle(self, request: web.Request) -> web.Response:
        self.requests.append({
            "method": request.method,
            "path": request.path,
            "query": request.query_string,
            "body": await request.read(),
            "headers": dict(request.headers),
        })
        if self.delay:
            await asyncio.sleep(self.delay)
        return web.json_response(self.reply, status=self.status)

    async def __aenter__(self) -> str:
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", self._handle)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        return str(self.client.make_url("")).rstrip("/")

    async def __aexit__(self, *exc) -> None:
        await self.client.close()


def _device(**over) -> Device:
    d = Device(device_type="t4", petkit_id=100, serial_number="SN100")
    d.api_secret = SECRET
    d.wire_type = "T4"
    d.mqtt_connected = True
    for k, v in over.items():
        setattr(d, k, v)
    return d


def _live(upstream: str = "http://127.0.0.1:9", **over) -> dict:
    live = {"proxy_mode": True, "proxy_keep_online": True, "proxy_upstream": upstream,
            "proxy_dns": "", "capture": False}
    live.update(over)
    return live


def _presence(live: dict, hub: EventHub | None = None,
              clock: _Clock | None = None) -> tuple[CloudPresence, web.Application]:
    app = web.Application()
    presence = CloudPresence(app, live, policy_factory=lambda d: RedactionPolicy(device=d),
                             hub=hub, clock=clock or _Clock())
    return presence, app


async def _settle(presence: CloudPresence) -> None:
    """Wait for whatever `note_activity` scheduled."""
    tasks = list(presence._inflight.values())
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# --- gating -----------------------------------------------------------------

@pytest.mark.parametrize("live_over, device_over, reason", [
    ({"proxy_keep_online": False}, {}, "switched off"),
    ({"proxy_mode": False}, {}, "proxy mode is off"),
    ({}, {"mqtt_connected": False}, "not on local MQTT"),
    ({}, {"api_secret": ""}, "real API secret not adopted yet"),
])
async def test_nothing_is_sent_unless_every_condition_holds(live_over, device_over, reason):
    """Off by default and off whenever proxy mode is; a device on HTTP still
    heartbeats for itself (forwarded by proxy mode); and without the adopted
    real secret PetKit would answer 704 to every signature."""
    async with (upstream := _Upstream()) as url:
        live = _live(url, **live_over)
        device = _device(**device_over)
        presence, app = _presence(live)
        assert blocker(live, device) == reason
        presence.note_activity(device)
        await _settle(presence)
        assert upstream.requests == []
        await close_proxy_session(app)


async def test_the_switch_is_off_by_default():
    assert blocker({"proxy_mode": True}, _device()) == "switched off"


async def test_at_most_one_heartbeat_per_interval_however_chatty_the_device():
    """Device-paced, but a burst of `property/post`s is not a burst of
    requests: one per `MIN_INTERVAL_SECONDS`, and the next only on the next
    packet after that."""
    async with (upstream := _Upstream()) as url:
        clock = _Clock()
        presence, app = _presence(_live(url), clock=clock)
        device = _device()

        for _ in range(20):
            presence.note_activity(device)
            await _settle(presence)
        assert len(upstream.requests) == 1

        clock.now += MIN_INTERVAL_SECONDS - 1
        presence.note_activity(device)
        await _settle(presence)
        assert len(upstream.requests) == 1

        clock.now += 1
        presence.note_activity(device)
        await _settle(presence)
        assert len(upstream.requests) == 2
        await close_proxy_session(app)


async def test_a_device_still_heartbeating_for_itself_is_left_to_it():
    """For ~40s after its CONNECT a device still polls over HTTP, and proxy mode
    forwards those; a heartbeat of ours on top would outpace the device."""
    import time

    async with (upstream := _Upstream()) as url:
        presence, app = _presence(_live(url))
        presence.note_activity(_device(last_heartbeat=time.time()))
        await _settle(presence)
        assert upstream.requests == []
        presence.note_activity(_device(last_heartbeat=time.time() - MIN_INTERVAL_SECONDS))
        await _settle(presence)
        assert len(upstream.requests) == 1
        await close_proxy_session(app)


async def test_without_device_activity_nothing_is_ever_sent():
    """Not a poller: there is no timer, so time passing sends nothing."""
    async with (upstream := _Upstream()) as url:
        clock = _Clock()
        presence, app = _presence(_live(url), clock=clock)
        presence.note_activity(_device())
        await _settle(presence)
        clock.now += 10 * MIN_INTERVAL_SECONDS
        await asyncio.sleep(0.05)
        assert len(upstream.requests) == 1
        await close_proxy_session(app)


async def test_turning_the_switch_off_stops_a_send_already_scheduled():
    async with (upstream := _Upstream()) as url:
        live = _live(url)
        presence, app = _presence(live)
        presence.note_activity(_device())
        live["proxy_keep_online"] = False      # before the task gets to run
        await _settle(presence)
        assert upstream.requests == []
        await close_proxy_session(app)


# --- the request ------------------------------------------------------------

async def _record_through_the_real_handler(device: Device, path: str, query: str,
                                           body: dict) -> DeviceRegistry:
    """Let the device's own heartbeat land on our handler, as it does on HTTP."""
    reg = DeviceRegistry()
    reg._devices[device.petkit_id] = device
    client = TestClient(TestServer(create_app(reg, {"api_url": "http://x/6/"})))
    await client.start_server()
    try:
        r = await client.post(f"{path}?{query}", data=body, headers={
            "X-Device": "id=100&nonce=n&timestamp=1&type=T4&sign=s",
            "User-Agent": "petkit-fw/1.652",
        })
        assert r.status == 200
    finally:
        await client.close()
    return reg


async def test_the_request_is_the_device_s_own_heartbeat_signed_as_the_device():
    """Path, query and body exactly as the device last sent them over HTTP,
    with a fresh X-Device built from its wire type and real secret."""
    device = _device()
    device.mqtt_connected = False
    await _record_through_the_real_handler(
        device, "/6/poll/t4/heartbeat", "iotStatus=0&ver=1.652", {"id": "100", "a": "b"})
    assert device.heartbeat_request["path"] == "/6/poll/t4/heartbeat"
    # It then reaches the broker and goes quiet over HTTP.
    device.mqtt_connected = True
    device.last_heartbeat = 0.0

    async with (upstream := _Upstream()) as url:
        presence, app = _presence(_live(url))
        presence.note_activity(device)
        await _settle(presence)
        await close_proxy_session(app)

    [sent] = upstream.requests
    assert sent["method"] == "POST"
    assert sent["path"] == "/6/poll/t4/heartbeat"
    # The device's own query, except that it is on a broker now.
    assert parse_qs(sent["query"]) == {"iotStatus": ["1"], "ver": ["1.652"]}
    assert parse_qs(sent["body"].decode()) == {"id": ["100"], "a": ["b"]}
    assert sent["headers"]["User-Agent"] == "petkit-fw/1.652"
    assert sent["headers"]["Content-Type"] == "application/x-www-form-urlencoded"

    fields = dict(p.split("=", 1) for p in sent["headers"]["X-Device"].split("&"))
    assert fields["id"] == "100" and fields["type"] == "T4"
    assert fields["sign"] == signature(100, fields["nonce"], int(fields["timestamp"]),
                                       "T4", SECRET)


def test_a_device_never_seen_heartbeating_gets_the_default_shape_labelled_as_such():
    req = heartbeat_request(_device())
    assert req["shape"] == "default"
    assert (req["method"], req["path"], req["query"], req["body"]) == (
        "POST", "/6/poll/t4/heartbeat", "", "")


async def test_the_recorded_heartbeat_survives_a_restart():
    """A device on MQTT does not heartbeat over HTTP again until it reboots."""
    device = _device()
    await _record_through_the_real_handler(device, "/6/t4/heartbeat", "iotStatus=0", {})
    restored = Device.from_dict(json.loads(json.dumps(device.to_dict())))
    assert restored.heartbeat_request == device.heartbeat_request
    assert heartbeat_request(restored)["shape"] == "device"
    assert heartbeat_request(restored)["path"] == "/6/t4/heartbeat"


def test_only_a_change_of_shape_asks_for_a_write_to_disk():
    class _Req:
        method = "POST"
        path = "/6/poll/t4/heartbeat"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        def __init__(self, query):
            self.query_string = query

    device = _device()
    assert remember_heartbeat(device, _Req("iotStatus=0"), b"") is True
    assert remember_heartbeat(device, _Req("iotStatus=1"), b"") is False
    assert device.heartbeat_request["query"] == "iotStatus=1"
    assert remember_heartbeat(device, _Req("iotStatus=1&x=2"), b"") is True


# --- the reply --------------------------------------------------------------

async def test_the_reply_is_recorded_and_never_applied():
    """PetKit's commands, settings and firmware stay in PetKit's reply: the
    device's queue, state, settings and credentials are exactly as before."""
    hub = EventHub()
    device = _device()
    device.state = {"sandPercent": 40}
    device.config = {"settings": {"lightMode": 1}}
    before = (json.dumps(device.state), json.dumps(device.config), device.api_secret,
              device.mqtt_device_secret, device.mqtt_product_key)

    async with _Upstream(reply=LOADED_REPLY) as url:
        presence, app = _presence(_live(url), hub=hub)
        presence.note_activity(device)
        await _settle(presence)
        await close_proxy_session(app)

    assert device.command_queue == []
    assert (json.dumps(device.state), json.dumps(device.config), device.api_secret,
            device.mqtt_device_secret, device.mqtt_product_key) == before

    stats = hub.presence_stats()[100]
    assert stats["count"] == 1
    assert stats["last_status"] == 200 and stats["last_outcome"] == "ok"
    assert stats["withheld"] == 3
    assert any("not delivered" in e["summary"] for e in hub.recent())


async def test_a_refusal_is_counted_with_its_code():
    hub = EventHub()
    async with _Upstream(reply={"error": {"code": 704, "msg": "x"}}) as url:
        presence, app = _presence(_live(url), hub=hub)
        presence.note_activity(_device())
        await _settle(presence)
        await close_proxy_session(app)
    stats = hub.presence_stats()[100]
    assert (stats["last_status"], stats["last_outcome"], stats["last_error"]) == (
        200, "error_704", 704)


def test_the_idle_answer_withholds_nothing():
    assert withheld_commands(b'{"result": [{"time": 1}]}') == 0
    assert withheld_commands(json.dumps(LOADED_REPLY).encode()) == 3
    assert withheld_commands(b"not json") == 0


# --- failure ----------------------------------------------------------------

async def test_an_unreachable_upstream_raises_nothing_and_is_counted():
    hub = EventHub()
    # Port 9 (discard) on loopback: refused at once.
    presence, app = _presence(_live("http://127.0.0.1:9"), hub=hub)
    presence.note_activity(_device())
    await _settle(presence)
    await close_proxy_session(app)
    stats = hub.presence_stats()[100]
    assert stats["last_status"] is None
    assert stats["last_outcome"] != "ok"


async def test_a_slow_upstream_is_abandoned(monkeypatch):
    monkeypatch.setattr("petkit_local.http.presence.SEND_TIMEOUT_SECONDS", 0.05)
    hub = EventHub()
    async with _Upstream(delay=1.0) as url:
        presence, app = _presence(_live(url), hub=hub)
        presence.note_activity(_device())
        await _settle(presence)
        await close_proxy_session(app)
    assert hub.presence_stats()[100]["last_outcome"] == "timeout"


async def test_note_activity_never_raises():
    presence, app = _presence(_live())
    presence.note_activity(object())   # not even a device
    await close_proxy_session(app)


async def test_close_cancels_a_send_in_flight():
    async with _Upstream(delay=5.0) as url:
        presence, app = _presence(_live(url))
        presence.note_activity(_device())
        await asyncio.sleep(0.05)
        await presence.close()
        assert presence._inflight == {}
        await close_proxy_session(app)


# --- the broker drives it ---------------------------------------------------

async def test_every_packet_of_a_live_session_is_reported_as_activity():
    """PINGREQ included: the T4's keep-alive is the pace an idle box keeps."""
    reg = DeviceRegistry()
    dev = reg.get_or_create(100, "t4", serial_number="SN100")
    plugin = AliyunAuthPlugin(BaseContext())
    plugin.set_registry(reg)

    seen = []

    class _Presence:
        def note_activity(self, device):
            seen.append(device.petkit_id)

    plugin.set_presence(_Presence())

    class _Session:
        pass

    session = _Session()
    plugin._live_sessions[100] = session
    await plugin.on_mqtt_packet_received(packet=object(), session=session)
    await plugin.on_mqtt_packet_received(packet=object(), session=_Session())
    assert seen == [dev.petkit_id]


# --- the panel --------------------------------------------------------------

def _panel_app(tmp_path, reg: DeviceRegistry, hub: EventHub, live: dict):
    from petkit_local.devices.ble import BLERegistry
    from petkit_local.web.panel import create_panel_app

    cfg = {"api_url": "http://x/6/", "capture": False, "capture_dir": "/nope",
           "settings_path": str(tmp_path / "overrides.json")}
    return create_panel_app(reg, BLERegistry(), hub, cfg, None, live_config=live)


async def test_the_panel_switch_is_saved_live_and_persisted(tmp_path):
    """Panel-only, like every proxy setting: it lands in the live config the
    sender reads, and in the overrides file the next start reads back."""
    live = {"proxy_mode": True}
    client = TestClient(TestServer(_panel_app(tmp_path, DeviceRegistry(), EventHub(), live)))
    await client.start_server()
    try:
        settings = (await (await client.get("/api/settings")).json())["settings"]
        assert settings["proxy_keep_online"] is False

        r = await client.post("/api/settings", json={"proxy_keep_online": True})
        assert r.status == 200
        assert live["proxy_keep_online"] is True
        stored = json.loads((tmp_path / "overrides.json").read_text())
        assert stored["proxy_keep_online"] is True

        r = await client.post("/api/settings", json={"proxy_keep_online": False})
        assert r.status == 200 and live["proxy_keep_online"] is False
    finally:
        await client.close()


async def test_the_panel_shows_what_was_sent_and_what_is_missing(tmp_path):
    reg = DeviceRegistry()
    sent = reg.get_or_create(100, "t4", serial_number="SN100")
    waiting = reg.get_or_create(200, "t5", serial_number="SN200")
    waiting.mqtt_connected = True              # on MQTT, no real secret yet
    reg.get_or_create(300, "d4", serial_number="SN300")   # on HTTP: not listed
    hub = EventHub()
    hub.record_presence(sent.petkit_id, status=200, outcome="ok", shape="device")
    live = {"proxy_mode": True, "proxy_keep_online": True}

    client = TestClient(TestServer(_panel_app(tmp_path, reg, hub, live)))
    await client.start_server()
    try:
        info = await (await client.get("/api/info")).json()
    finally:
        await client.close()

    rows = {r["device_id"]: r for r in info["presence"]}
    assert set(rows) == {100, 200}
    assert rows[100]["count"] == 1 and rows[100]["last_status"] == 200
    assert rows[100]["name"] == "Pura Max"
    assert rows[200]["count"] == 0
    assert rows[200]["waiting"] == "real API secret not adopted yet"
    assert rows[200]["shape"] == "default"
    assert info["presence_interval"] == MIN_INTERVAL_SECONDS
