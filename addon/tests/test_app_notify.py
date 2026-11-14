"""Phone notifications through ntfy (`app_api/notify/`).

What is held here:

* which events become which notification, per device CATEGORY — HTTP codes
  mean different things on a litter box and a feeder, so each case passes the
  codename it would arrive with;
* one notification per event: `event_uid` (`event_id + event_type`) is the
  dedupe key, so the reports of one session are distinct but a retried report
  is not, and an update of a stored row (`is_new` False) is never announced;
* the per-device, per-kind rate limit; switched-off kinds; disabled settings;
* state conditions fire on their START only, after their hold, gated on the
  key being present, and not for what was already true at startup;
* the publish request a real ntfy server would receive (a fake one, here);
* a dead or failing ntfy server never raises into the event path, and the
  failures it causes are logged at a bounded rate;
* the token and the topic appear in no log line, and the token in no panel
  response.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import stat
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, unused_port

from petkit_local.app_api.notify import rules
from petkit_local.app_api.notify.notifier import (
    FAILURE_LOG_INTERVAL, RATE_LIMITS, Notifier, encode_header,
)
from petkit_local.app_api.notify.rules import Notification
from petkit_local.app_api.notify.settings import (
    FILENAME, KIND_CONSUMABLE, KIND_ERROR, KIND_FEED_FAILED, KIND_FEED_SERVED, KIND_FOOD_LOW,
    KIND_OFFLINE, KIND_TOILET, KINDS, NtfyConfig, SettingsRejected, check_server,
    generate_topic,
)
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events.store import EventStore
from petkit_local.http import appkeys as device_keys
from petkit_local.http.server import create_app
from petkit_local.web.appkeys import NOTIFIER
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app

TOKEN = "tk_s3cretT0kenValueNobodyMaySee"


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeNtfy:
    """An aiohttp server answering like ntfy, recording what it was sent."""

    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.requests: list[dict[str, Any]] = []
        self.received = asyncio.Event()
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", self._handle)
        self.server = TestServer(app)

    async def _handle(self, request: web.Request) -> web.Response:
        self.requests.append({"method": request.method, "path": request.path,
                              "query": request.query_string,
                              "headers": dict(request.headers),
                              "body": (await request.read()).decode("utf-8")})
        self.received.set()
        if self.status != 200:
            return web.json_response({"code": 40101, "http": self.status,
                                      "error": "unauthorized"}, status=self.status)
        return web.json_response({"id": "abc", "event": "message"})

    async def start(self) -> str:
        await self.server.start_server()
        return str(self.server.make_url("")).rstrip("/")

    async def wait(self, count: int = 1, timeout: float = 5.0) -> None:
        async def _until():
            while len(self.requests) < count:
                self.received.clear()
                await self.received.wait()
        await asyncio.wait_for(_until(), timeout)


@pytest.fixture
async def ntfy():
    fake = FakeNtfy()
    yield fake
    await fake.server.close()


async def _config(tmp_path: Path, server: str = "https://ntfy.example.org",
                  **kw: Any) -> NtfyConfig:
    config = NtfyConfig(tmp_path / FILENAME)
    await config.update({"server": server, "token": TOKEN, "enabled": True, **kw})
    return config


class Env:
    """A notifier over a real registry, with the queue read directly."""

    def __init__(self, config: NtfyConfig, registry: DeviceRegistry | None = None,
                 pets: Any = None) -> None:
        self.clock = Clock()
        self.registry = registry or DeviceRegistry()
        self.notifier = Notifier(config, self.registry, pet_registry=pets, clock=self.clock)

    def queued(self) -> list[Notification]:
        out = []
        q = self.notifier._queue
        while not q.empty():
            out.append(q.get_nowait())
        return out


def _row(event_type: str, device_type: str, *, event_id: str = "e1", device_id: int = 100,
         content: dict | None = None, **kw: Any) -> dict[str, Any]:
    return {"event_uid": f"{event_id}:{event_type}", "related_event": event_id,
            "device_id": device_id, "device_type": device_type, "event_type": event_type,
            "content_json": json.dumps(content) if content else None, **kw}


async def _event(env: Env, row: dict[str, Any], is_new: bool = True) -> None:
    await env.notifier.handle({"kind": "event", "device_id": row["device_id"],
                               "detail": {**row, "is_new": is_new}})


# --- classification, per device category ------------------------------------------

def test_a_litter_box_visit_summary_is_a_toilet_notification_naming_the_pet():
    n = rules.from_event(_row("10", "t5", content={"is_shit": 1, "shit_weight": 32}), "Félix")
    assert n.kind == KIND_TOILET
    assert "Félix" in n.message and "waste" in n.message
    assert n.dedupe_key == "e1:10"
    # Unattributed: still a notification, naming nobody.
    assert "Visit to the" in rules.from_event(_row("10", "t5")).message
    # The mid-visit weight check shares the visit's kind but is not the visit.
    assert rules.from_event(_row("9", "t5")) is None
    # The MQTT name of the same event.
    assert rules.from_event(_row("pet_out", "t5")).kind == KIND_TOILET


def test_code_2_is_a_cleared_fault_on_a_litter_box_and_a_meal_on_a_feeder():
    assert rules.from_event(_row("2", "t5")) is None
    assert rules.from_event(_row("2", "d4h")).kind == KIND_FEED_SERVED
    assert rules.from_event(_row("1", "t5")).kind == KIND_ERROR
    # A cleaning cycle completing is not worth a notification.
    assert rules.from_event(_row("5", "t5", content={"result": 0})) is None


def test_feed_outcomes_split_into_served_skipped_and_failed():
    served = rules.from_event(_row("4", "d4sh", content={"result": 0}))
    assert served.kind == KIND_FEED_SERVED and served.title == "Meal served"
    failed = rules.from_event(_row("4", "d4sh", content={"result": 8}))
    assert failed.kind == KIND_FEED_FAILED and failed.priority == rules.PRIORITY_HIGH
    assert "nothing dispensed" in failed.message
    # Hoppers empty: the cat did not eat, so it is a failure.
    assert rules.from_event(_row("feed_over", "d4h", content={"result": 9})).kind \
        == KIND_FEED_FAILED
    # (8, err_code 10) is the surplus-control skip, not a failed dispense.
    skipped = rules.from_event(_row("feed_over", "d4h", content={"result": 8, "err_code": 10}))
    assert skipped.kind == KIND_FEED_SERVED and skipped.title == "Meal skipped"
    # A cat finishing its meal is not a feed.
    assert rules.from_event(_row("6", "d4sh", content={"result": 0})) is None
    assert rules.from_event(_row("3", "d4sh")) is None


def test_a_fountain_error_and_an_mqtt_error():
    assert rules.from_event(_row("1", "w7h")).kind == KIND_ERROR
    assert rules.from_event(_row("2", "w7h")) is None
    error = rules.from_event(_row("error_start", "d4h"))
    assert error.kind == KIND_ERROR and error.message.startswith("YumShare Solo: Error")
    assert rules.from_event(_row("error_over", "d4h")) is None


def test_an_unknown_code_or_a_malformed_row_is_no_notification():
    assert rules.from_event(_row("999", "t5")) is None
    assert rules.from_event({"event_type": None}) is None
    assert rules.from_event(_row("10", "t5") | {"content_json": "{not json"}).kind \
        == KIND_TOILET


def test_payloads_carry_no_identifier_or_url():
    n = rules.from_event(_row("10", "t5", event_id="10000001_1784741818"), "Félix")
    assert "10000001" not in n.message + n.title
    assert "http" not in n.message


# --- dedupe -----------------------------------------------------------------------

async def test_one_notification_per_event_and_none_for_a_retried_report(tmp_path):
    env = Env(await _config(tmp_path))
    await _event(env, _row("10", "t5"))
    await _event(env, _row("10", "t5"))  # the same report delivered twice
    await _event(env, _row("10", "t5", event_id="e2"), is_new=False)  # updates a stored row
    assert [n.dedupe_key for n in env.queued()] == ["e1:10"]


async def test_reports_of_one_session_are_each_their_own_event(tmp_path):
    """Sessions share `event_id`; deduping on it alone would drop all but one."""
    env = Env(await _config(tmp_path))
    await _event(env, _row("1", "t5", event_id="s"))
    env.clock.advance(RATE_LIMITS[KIND_ERROR][1])
    await _event(env, _row("10", "t5", event_id="s"))
    assert [n.kind for n in env.queued()] == [KIND_ERROR, KIND_TOILET]


# --- rate limit, kinds, disabled --------------------------------------------------

async def test_a_flapping_device_is_rate_limited_per_kind(tmp_path):
    env = Env(await _config(tmp_path))
    count, window = RATE_LIMITS[KIND_TOILET]
    for i in range(count + 5):
        await _event(env, _row("10", "t5", event_id=f"v{i}"))
    # Another device, and another kind on the same device, are unaffected.
    await _event(env, _row("10", "t5", event_id="other", device_id=200))
    await _event(env, _row("1", "t5", event_id="err"))
    queued = env.queued()
    assert sum(n.device_id == 100 and n.kind == KIND_TOILET for n in queued) == count
    assert sum(n.device_id == 200 for n in queued) == 1
    assert sum(n.kind == KIND_ERROR for n in queued) == 1

    env.clock.advance(window)
    await _event(env, _row("10", "t5", event_id="later"))
    assert len(env.queued()) == 1


async def test_a_switched_off_kind_and_disabled_settings_send_nothing(tmp_path):
    config = await _config(tmp_path, kinds={KIND_TOILET: False})
    env = Env(config)
    await _event(env, _row("10", "t5"))
    await _event(env, _row("2", "d4h", event_id="f"))
    assert [n.kind for n in env.queued()] == [KIND_FEED_SERVED]

    await config.update({"enabled": False})
    await _event(env, _row("2", "d4h", event_id="g"))
    assert env.queued() == []


async def test_the_pet_name_comes_from_the_pet_registry(tmp_path, pet_registry):
    pet = await pet_registry.create("Mochi")
    env = Env(await _config(tmp_path), pets=pet_registry)
    await _event(env, _row("10", "t5", pet_id=pet["id"]))
    assert "Mochi used the" in env.queued()[0].message


# --- state conditions -------------------------------------------------------------

async def test_a_condition_fires_once_when_it_starts_and_again_only_after_clearing(tmp_path):
    env = Env(await _config(tmp_path))
    device = env.registry.get_or_create(petkit_id=100, device_type="t5")
    device.state.update({"boxFull": 1})
    env.notifier.prime()  # already full at startup: the owner knows
    hub_line = {"kind": "http", "device_id": 100}
    await env.notifier.handle(hub_line)
    assert env.queued() == []

    device.state["boxFull"] = 0
    await env.notifier.handle(hub_line)
    device.state["boxFull"] = 1
    await env.notifier.handle(hub_line)  # starts holding
    assert env.queued() == []
    env.clock.advance(next(c.hold for c in rules.CONDITIONS if c.key == "boxFull"))
    await env.notifier.handle(hub_line)  # still holding in a later report
    await env.notifier.handle(hub_line)
    queued = env.queued()
    assert [(n.kind, n.title) for n in queued] == [(KIND_CONSUMABLE, "Waste bin full")]


async def test_food_low_must_outlast_a_dispense(tmp_path):
    """`food` reads 0 while a meal moves through the hopper."""
    env = Env(await _config(tmp_path))
    device = env.registry.get_or_create(petkit_id=7, device_type="d4h")
    line = {"kind": "mqtt", "device_id": 7}
    device.state["foodLow"] = 1
    await env.notifier.handle(line)
    env.clock.advance(30)
    device.state["foodLow"] = 0  # the next report: there is food after all
    await env.notifier.handle(line)
    env.clock.advance(rules.FOOD_HOLD)
    await env.notifier.handle(line)
    assert env.queued() == []

    device.state["foodLow"] = 1
    await env.notifier.handle(line)
    env.clock.advance(rules.FOOD_HOLD)
    await env.notifier.handle(line)
    assert [n.kind for n in env.queued()] == [KIND_FOOD_LOW]


async def test_an_absent_key_is_not_a_condition(tmp_path):
    env = Env(await _config(tmp_path))
    device = env.registry.get_or_create(petkit_id=8, device_type="d4sh")
    device.state.update({"food1": 2})  # no `food2`, no `foodLow`, no days
    assert rules.active_conditions(8, "d4sh", device.state) == {}
    device.state["food2"] = 0
    assert list(rules.active_conditions(8, "d4sh", device.state)) == ["food2"]


async def test_spent_consumables_and_a_changed_fault_message(tmp_path):
    env = Env(await _config(tmp_path))
    device = env.registry.get_or_create(petkit_id=9, device_type="t5")
    line = {"kind": "http", "device_id": 9}
    await env.notifier.handle(line)
    device.state.update({"sprayLeftDays": 0, "errorMsg": "Waste tray missing"})
    await env.notifier.handle(line)  # days: no hold; the fault: held
    env.clock.advance(rules.ERROR_HOLD)
    await env.notifier.handle(line)
    device.state["errorMsg"] = "Motor stalled"
    env.clock.advance(RATE_LIMITS[KIND_ERROR][1])
    await env.notifier.handle(line)
    env.clock.advance(rules.ERROR_HOLD)
    await env.notifier.handle(line)
    assert [n.title for n in env.queued()] == [
        "N60 spray to replace", "Device error", "Device error"]


async def test_the_offline_watchdog_decision_and_nothing_else_is_an_offline(tmp_path):
    env = Env(await _config(tmp_path))
    env.registry.get_or_create(petkit_id=100, device_type="t5")
    await env.notifier.handle({"kind": "connect", "device_id": 100,
                               "summary": "MQTT session ended"})
    await env.notifier.handle({"kind": "connect", "device_id": 100,
                               "summary": "offline (no contact for >180s)",
                               "detail": {"online": False, "timeout": 180}})
    queued = env.queued()
    assert [n.kind for n in queued] == [KIND_OFFLINE]
    assert "stopped responding" in queued[0].message


async def test_the_watchdog_says_online_false(tmp_path):
    from petkit_local.ha.publisher import HAPublisher

    registry = DeviceRegistry()
    device = registry.get_or_create(petkit_id=100, device_type="t5")
    device.online = True
    device.last_seen = 1.0
    hub = EventHub()
    publisher = HAPublisher(registry, {}, hub=hub)
    task = asyncio.create_task(publisher.availability_watchdog(timeout=1, interval=0.01))
    try:
        for _ in range(200):
            if not device.online:
                break
            await asyncio.sleep(0.01)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    lines = [e for e in hub.recent() if e["kind"] == "connect"]
    assert lines and lines[-1]["detail"] == {"online": False, "timeout": 1}


# --- the publish request ----------------------------------------------------------

async def test_the_request_is_what_ntfy_expects(tmp_path, ntfy):
    server = await ntfy.start()
    config = await _config(tmp_path, server=server)
    env = Env(config)
    note = rules.from_event(_row("10", "t5"), "Félix")
    ok, reason = await env.notifier.publish(note, config.settings)
    await env.notifier.close()
    assert ok and reason == ""
    req = ntfy.requests[0]
    assert req["method"] == "POST" and req["path"] == f"/{config.settings.topic}"
    assert req["query"] == ""
    headers = req["headers"]
    assert headers["Authorization"] == f"Bearer {TOKEN}"
    assert headers["Title"] == "Toilet visit"
    assert headers["Priority"] == str(rules.PRIORITY_LOW)
    assert headers["Tags"] == "cat,toilet"
    assert req["body"] == note.message and "Félix" in req["body"]


def test_a_non_ascii_title_is_an_rfc_2047_word_and_controls_are_dropped():
    assert encode_header("Meal served") == "Meal served"
    word = encode_header("Félix\r\nX-Evil: 1")
    assert word.startswith("=?UTF-8?B?") and word.endswith("?=")
    assert base64.b64decode(word[10:-2]).decode() == "FélixX-Evil: 1"


async def test_no_token_means_no_authorization_header(tmp_path, ntfy):
    server = await ntfy.start()
    config = await _config(tmp_path, server=server)
    await config.update({"token": ""})
    env = Env(config)
    assert (await env.notifier.send_test())[0]
    await env.notifier.close()
    assert "Authorization" not in ntfy.requests[0]["headers"]


async def test_the_hub_feeds_the_publisher_end_to_end(tmp_path, ntfy):
    """A retried dev_event_report rings the phone once, over the real hub."""
    server = await ntfy.start()
    registry = DeviceRegistry()
    registry.get_or_create(petkit_id=100, device_type="t5", serial_number="SN100")
    store = EventStore(tmp_path / "petkit.db")
    hub = EventHub()
    device_app = create_app(registry, {"api_url": "http://server/6/", "mqtt_port": 1883,
                                       "proxy_mode": False, "capture": False})
    device_app[device_keys.EVENT_STORE] = store
    device_app[device_keys.EVENT_HUB] = hub
    client = TestClient(TestServer(device_app))
    await client.start_server()
    notifier = Notifier(await _config(tmp_path, server=server), registry, hub=hub)
    task = asyncio.create_task(notifier.run())
    try:
        await asyncio.sleep(0)  # let it subscribe
        for _ in range(2):
            r = await client.post("/6/t5/dev_event_report", data="eventType=10&eventId=v1",
                                  headers={"X-Device": "id=100&sn=SN100"})
            assert r.status == 200
        await ntfy.wait(1)
        await asyncio.sleep(0.1)
        assert len(ntfy.requests) == 1
        assert ntfy.requests[0]["headers"]["Title"] == "Toilet visit"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await client.close()
        await store.close()


# --- failure isolation and secrecy ------------------------------------------------

async def test_a_refusing_server_is_reported_without_the_token_or_topic(tmp_path, caplog):
    fake = FakeNtfy(status=401)
    server = await fake.start()
    try:
        config = await _config(tmp_path, server=server)
        env = Env(config)
        with caplog.at_level(logging.DEBUG):
            ok, reason = await env.notifier.send_test()
        await env.notifier.close()
        assert not ok and "401" in reason
        # Ours, not the fake server's own access log, which names the path it served.
        ours = " ".join(r.getMessage() for r in caplog.records
                        if r.name.startswith("petkit_local"))
        assert "401" in ours
        for text in (reason, ours):
            assert TOKEN not in text
            assert config.settings.topic not in text
    finally:
        await fake.server.close()


async def test_an_unreachable_server_never_raises_and_the_log_is_rate_limited(
        tmp_path, caplog):
    port = unused_port()  # nothing listens there
    config = await _config(tmp_path, server=f"http://127.0.0.1:{port}")
    env = Env(config)
    note = Notification(kind=KIND_TOILET, device_id=1, title="t", message="m")
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            ok, _ = await env.notifier.publish(note, config.settings)
            assert not ok
        assert caplog.text.count("ntfy:") == 1
        env.clock.advance(FAILURE_LOG_INTERVAL)
        await env.notifier.publish(note, config.settings)
        assert caplog.text.count("ntfy:") == 2
        assert "2 more since the last report" in caplog.text
    await env.notifier.close()
    assert TOKEN not in caplog.text
    assert config.settings.topic not in caplog.text


async def test_a_failing_publish_does_not_stop_the_worker_or_block_the_feed(tmp_path, ntfy):
    server = await ntfy.start()
    config = await _config(tmp_path, server=server)
    env = Env(config)
    worker = asyncio.create_task(env.notifier.publish_forever())
    try:
        ntfy.status = 500
        await _event(env, _row("10", "t5", event_id="a"))
        await ntfy.wait(1)
        ntfy.status = 200
        await _event(env, _row("10", "t5", event_id="b"))
        await ntfy.wait(2)
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        await env.notifier.close()


async def test_the_queue_is_bounded_and_overflow_is_dropped(tmp_path):
    config = await _config(tmp_path)
    env = Env(config)
    env.notifier = Notifier(config, env.registry, clock=env.clock, queue_size=2)
    accepted = [env.notifier.submit(Notification(kind=KIND_TOILET, device_id=i,
                                                 title="t", message="m"))
                for i in range(4)]
    assert accepted == [True, True, False, False]


async def test_a_bad_hub_entry_does_not_end_the_feed(tmp_path, monkeypatch):
    real = rules.from_event

    def flaky(row, pet_name=None):
        if row.get("event_type") == "boom":
            raise RuntimeError("classification bug")
        return real(row, pet_name)

    monkeypatch.setattr(rules, "from_event", flaky)
    hub = EventHub()
    config = await _config(tmp_path)
    notifier = Notifier(config, DeviceRegistry(), hub=hub)
    published: list[Notification] = []

    async def record(notification, settings):
        published.append(notification)
        return True, ""

    monkeypatch.setattr(notifier, "publish", record)
    task = asyncio.create_task(notifier.run())
    try:
        await asyncio.sleep(0)
        hub.publish("event", 1, "weird", detail={"event_type": "boom"})
        hub.publish("event", 1, "fine", detail=_row("10", "t5", device_id=1))
        for _ in range(100):
            if published:
                break
            await asyncio.sleep(0.01)
        assert [n.kind for n in published] == [KIND_TOILET]
        assert not task.done()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# --- settings ---------------------------------------------------------------------

def test_a_server_url_must_be_plain_http_or_https_without_credentials():
    assert check_server("https://ntfy.example.org/") == "https://ntfy.example.org"
    assert check_server("http://10.0.0.2:8080/ntfy") == "http://10.0.0.2:8080/ntfy"
    for bad in ("ftp://x", "https://user:pw@ntfy.example.org", "https://x?auth=1",
                "https://x#f", "ntfy.example.org", "https://x:99999", 5):
        with pytest.raises(SettingsRejected):
            check_server(bad)


async def test_settings_persist_and_a_damaged_file_fails_closed(tmp_path):
    config = await _config(tmp_path)
    topic = config.settings.topic
    assert topic.startswith("petkit-") and len(topic) <= 64
    assert generate_topic() != generate_topic()
    again = NtfyConfig(tmp_path / FILENAME)
    assert again.settings == config.settings
    if os.name == "posix":
        assert stat.S_IMODE(os.stat(tmp_path / FILENAME).st_mode) == 0o600

    (tmp_path / FILENAME).write_text(json.dumps({"enabled": True, "server": "ftp://x",
                                                 "topic": "t"}))
    assert NtfyConfig(tmp_path / FILENAME).settings.ready is False


async def test_enabling_needs_a_server_and_generates_a_topic(tmp_path):
    config = NtfyConfig(tmp_path / FILENAME)
    with pytest.raises(SettingsRejected):
        await config.update({"enabled": True})
    s = await config.update({"server": "https://n.example", "enabled": True})
    assert s.topic and s.ready and all(s.wants(k) for k in KINDS)
    old = s.topic
    assert (await config.regenerate_topic()).topic != old
    with pytest.raises(SettingsRejected):
        await config.update({"topic": "../admin"})
    with pytest.raises(SettingsRejected):
        await config.update({"token": "has space"})
    with pytest.raises(SettingsRejected):
        await config.update({"kinds": {"nope": True}})


# --- the panel --------------------------------------------------------------------

@pytest.fixture
async def panel(tmp_path):
    registry = DeviceRegistry()
    app = create_panel_app(registry, None, EventHub(), {})
    notifier = Notifier(NtfyConfig(tmp_path / FILENAME), registry)
    app[NOTIFIER] = notifier
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client, notifier
    await client.close()
    await notifier.close()


async def test_the_panel_never_returns_the_token(panel, ntfy):
    client, notifier = panel
    server = await ntfy.start()
    r = await client.post("/api/app-notify", json={"server": server, "token": TOKEN,
                                                   "enabled": True,
                                                   "kinds": {KIND_OFFLINE: False}})
    assert r.status == 200
    text = await r.text()
    body = json.loads(text)
    assert TOKEN not in text and body["token_set"] is True
    assert body["token_masked"].endswith(TOKEN[-4:])
    assert body["enabled"] is True and body["topic"]
    assert {k["kind"]: k["enabled"] for k in body["kinds"]}[KIND_OFFLINE] is False
    assert TOKEN not in await (await client.get("/api/app-notify")).text()

    # Leaving the token out keeps it; the test button uses it.
    r = await client.post("/api/app-notify", json={"server": server})
    assert (await r.json())["token_set"] is True
    r = await client.post("/api/app-notify/test", json={})
    assert (await r.json()) == {"ok": True, "error": None}
    assert ntfy.requests[0]["headers"]["Authorization"] == f"Bearer {TOKEN}"

    old = body["topic"]
    r = await client.post("/api/app-notify/topic", json={})
    assert (await r.json())["topic"] != old

    r = await client.post("/api/app-notify", json={"server": "https://u:p@x"})
    assert r.status == 400
    r = await client.post("/api/app-notify", json={"token": ""})
    assert (await r.json())["token_set"] is False


async def test_the_panel_without_a_notifier_says_so():
    app = create_panel_app(DeviceRegistry(), None, EventHub(), {})
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        assert (await client.get("/api/app-notify")).status == 503
    finally:
        await client.close()
