"""Phone notifications: what is announced, and the gates before the phones (`app_api/notify/`).

What is held here:

* which events become which notification, per device CATEGORY — HTTP codes
  mean different things on a litter box and a feeder, so each case passes the
  codename it would arrive with;
* one notification per event: `event_uid` (`event_id + event_type`) is the
  dedupe key, so the reports of one session are distinct but a retried report
  is not, and an update of a stored row (`is_new` False) is never announced;
* the per-device, per-kind rate limit; switched-off kinds;
* state conditions fire on their START only, after their hold, gated on the
  key being present, and not for what was already true at startup;
* a bad hub entry never ends the feed;
* the kind switches persist, and an `ntfy.json` from before keeps its kinds and
  loses everything else — its access token above all;
* the panel sets the switches on its Phone app access card, and nothing of ntfy
  is left there.

The sinks themselves (the notification socket and the push endpoints) are
`test_app_notify_ws.py` and `test_app_push.py`; here a recorder stands in for
them, so what passed the gates is read directly.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.app_api.auth import FILENAME as AUTH_FILENAME
from petkit_local.app_api.auth import AppAuth, ScryptParams
from petkit_local.app_api.notify import rules
from petkit_local.app_api.notify.notifier import RATE_LIMITS, Notifier
from petkit_local.app_api.notify.rules import Notification
from petkit_local.app_api.notify.settings import (
    FILENAME, KIND_CONSUMABLE, KIND_ERROR, KIND_FEED_SERVED, KIND_FOOD_LOW, KIND_OFFLINE,
    KIND_TOILET, KINDS, LEGACY_FILENAME, NotifyConfig, SettingsRejected,
)
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events.store import EventStore
from petkit_local.http import appkeys as device_keys
from petkit_local.http.server import create_app
from petkit_local.web.appkeys import NOTIFIER
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app

#: What an old `ntfy.json` held: none of it may survive the migration.
TOKEN = "tk_s3cretT0kenValueNobodyMaySee"
TOPIC = "petkit-Aj4LZxpuM68UPghQY5MeN6bs"
FAST = ScryptParams(n=2 ** 10, r=8, p=1)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Recorder:
    """Stands in for the phones: wants every kind, keeps what it is handed."""

    def __init__(self) -> None:
        self.got: list[Notification] = []
        self.arrived = asyncio.Event()

    def wants(self, kind: str) -> bool:
        return True

    def deliver(self, notification: Notification) -> int:
        self.got.append(notification)
        self.arrived.set()
        return 1

    async def flush(self) -> None:
        pass

    async def wait(self, count: int = 1, timeout: float = 5.0) -> None:
        async def _until():
            while len(self.got) < count:
                self.arrived.clear()
                await self.arrived.wait()
        await asyncio.wait_for(_until(), timeout)


def _recorded(notifier: Notifier) -> Recorder:
    """`notifier` with a recorder as its socket sink; returns the recorder."""
    recorder = Recorder()
    notifier._channel = recorder
    return recorder


async def _config(tmp_path: Path, **kinds: bool) -> NotifyConfig:
    config = NotifyConfig(tmp_path / FILENAME)
    if kinds:
        await config.set_kinds(kinds)
    return config


class Env:
    """A notifier over a real registry, with what passed its gates read directly."""

    def __init__(self, config: NotifyConfig, registry: DeviceRegistry | None = None,
                 pets: Any = None) -> None:
        self.clock = Clock()
        self.registry = registry or DeviceRegistry()
        self.notifier = Notifier(config, self.registry, pet_registry=pets, clock=self.clock)
        self.recorder = _recorded(self.notifier)

    def queued(self) -> list[Notification]:
        out, self.recorder.got = self.recorder.got, []
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
    assert failed.kind == "feed_failed" and failed.priority == rules.PRIORITY_HIGH
    assert "nothing dispensed" in failed.message
    # Hoppers empty: the cat did not eat, so it is a failure.
    assert rules.from_event(_row("feed_over", "d4h", content={"result": 9})).kind \
        == "feed_failed"
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


# --- rate limit, kinds ------------------------------------------------------------

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


async def test_a_switched_off_kind_sends_nothing(tmp_path):
    config = await _config(tmp_path, **{KIND_TOILET: False})
    env = Env(config)
    await _event(env, _row("10", "t5"))
    await _event(env, _row("2", "d4h", event_id="f"))
    assert [n.kind for n in env.queued()] == [KIND_FEED_SERVED]

    await config.set_kinds({KIND_FEED_SERVED: False})
    await _event(env, _row("2", "d4h", event_id="g"))
    assert env.queued() == []
    # Switched back on: counted from now, never what was skipped meanwhile.
    await config.set_kinds({KIND_TOILET: True})
    await _event(env, _row("10", "t5", event_id="h"))
    assert [n.dedupe_key for n in env.queued()] == ["h:10"]


async def test_with_no_phone_wanting_it_nothing_is_counted(tmp_path):
    """No sink, no notification — and no dedupe entry, so a later phone still gets it."""
    env = Env(await _config(tmp_path))
    env.notifier._channel = None
    note = Notification(kind=KIND_TOILET, device_id=1, title="t", message="m", dedupe_key="k")
    assert env.notifier.submit(note) is False
    assert "k" not in env.notifier._seen


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


async def test_without_home_assistant_a_silent_device_still_reaches_the_phone(tmp_path):
    """`--no-ha`: no publisher, yet the watchdog runs and "Device offline" is sent."""
    from petkit_local.main.lifecycle import _start_offline_watchdog

    registry = DeviceRegistry()
    device = registry.get_or_create(petkit_id=100, device_type="t5")
    device.online = True
    device.mqtt_connected = True
    device.last_seen = 1.0  # long ago
    hub = EventHub()
    notifier = Notifier(await _config(tmp_path), registry, hub=hub)
    phone = _recorded(notifier)
    notifier_task = asyncio.create_task(notifier.run())
    host = web.Application()
    host[device_keys.BACKGROUND_TASKS] = []
    services = SimpleNamespace(registry=registry, hub=hub, ha_publisher=None,
                               config=SimpleNamespace(offline_timeout=1))
    try:
        await asyncio.sleep(0)  # let the notifier subscribe
        watchdog = _start_offline_watchdog(services, host, interval=0.01)
        assert host[device_keys.BACKGROUND_TASKS] == [watchdog]
        await phone.wait(1)
        assert not device.online and not device.mqtt_connected
        assert [(n.kind, n.title) for n in phone.got] == [(KIND_OFFLINE, "Device offline")]
    finally:
        for task in (*host[device_keys.BACKGROUND_TASKS], notifier_task):
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


async def test_the_hub_feeds_the_phones_end_to_end(tmp_path):
    """A retried dev_event_report rings the phone once, over the real hub."""
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
    notifier = Notifier(await _config(tmp_path), registry, hub=hub)
    phone = _recorded(notifier)
    task = asyncio.create_task(notifier.run())
    try:
        await asyncio.sleep(0)  # let it subscribe
        for _ in range(2):
            r = await client.post("/6/t5/dev_event_report", data="eventType=10&eventId=v1",
                                  headers={"X-Device": "id=100&sn=SN100"})
            assert r.status == 200
        await phone.wait(1)
        await asyncio.sleep(0.1)
        assert [n.title for n in phone.got] == ["Toilet visit"]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await client.close()
        await store.close()


async def test_a_bad_hub_entry_does_not_end_the_feed(tmp_path, monkeypatch):
    real = rules.from_event

    def flaky(row, pet_name=None):
        if row.get("event_type") == "boom":
            raise RuntimeError("classification bug")
        return real(row, pet_name)

    monkeypatch.setattr(rules, "from_event", flaky)
    hub = EventHub()
    notifier = Notifier(await _config(tmp_path), DeviceRegistry(), hub=hub)
    phone = _recorded(notifier)
    task = asyncio.create_task(notifier.run())
    try:
        await asyncio.sleep(0)
        hub.publish("event", 1, "weird", detail={"event_type": "boom"})
        hub.publish("event", 1, "fine", detail=_row("10", "t5", device_id=1))
        await phone.wait(1)
        assert [n.kind for n in phone.got] == [KIND_TOILET]
        assert not task.done()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# --- the kind switches and their file ---------------------------------------------

async def test_the_switches_persist_and_a_bad_value_changes_nothing(tmp_path):
    config = NotifyConfig(tmp_path / FILENAME)
    assert all(config.settings.wants(k) for k in KINDS)  # a first start: all on
    await config.set_kinds({KIND_OFFLINE: False})
    await config.set_kinds({KIND_TOILET: False})  # the others are left alone
    again = NotifyConfig(tmp_path / FILENAME)
    assert again.settings == config.settings
    assert {k for k in KINDS if not again.settings.wants(k)} == {KIND_OFFLINE, KIND_TOILET}
    stored = json.loads((tmp_path / FILENAME).read_text())
    assert stored == {"version": 1, "kinds": dict(config.settings.kinds)}

    for bad in ({"nope": True}, {KIND_TOILET: "yes"}, [KIND_TOILET], None):
        with pytest.raises(SettingsRejected):
            await config.set_kinds(bad)
    assert NotifyConfig(tmp_path / FILENAME).settings == again.settings

    # A damaged file turns every kind back on rather than stopping the add-on.
    (tmp_path / FILENAME).write_text("{not json")
    assert all(NotifyConfig(tmp_path / FILENAME).settings.wants(k) for k in KINDS)


def _write_legacy(tmp_path: Path, kinds: dict[str, bool]) -> Path:
    legacy = tmp_path / LEGACY_FILENAME
    legacy.write_text(json.dumps({"version": 1, "enabled": True,
                                  "server": "https://ntfy.example.org", "token": TOKEN,
                                  "topic": TOPIC, "kinds": kinds}, indent=2))
    return legacy


def _nothing_of_ntfy_left(tmp_path: Path) -> None:
    for path in tmp_path.rglob("*"):
        if path.is_file():
            text = path.read_text(errors="replace")
            for secret in (TOKEN, TOPIC, "ntfy.example.org"):
                assert secret not in text, (path.name, secret)


async def test_an_old_ntfy_json_keeps_its_kinds_and_loses_its_token(tmp_path):
    legacy = _write_legacy(tmp_path, {KIND_TOILET: False, KIND_FOOD_LOW: False})
    config = NotifyConfig(tmp_path / FILENAME)
    assert {k for k in KINDS if not config.settings.wants(k)} == {KIND_TOILET, KIND_FOOD_LOW}
    # The old file is gone, and the new one holds the switches and nothing else.
    assert not legacy.exists()
    stored = json.loads((tmp_path / FILENAME).read_text())
    assert stored == {"version": 1, "kinds": {k: k not in (KIND_TOILET, KIND_FOOD_LOW)
                                              for k in KINDS}}
    _nothing_of_ntfy_left(tmp_path)
    # Read once: the next start reads the new file alone.
    assert NotifyConfig(tmp_path / FILENAME).settings == config.settings


async def test_a_leftover_ntfy_json_is_deleted_and_the_new_file_wins(tmp_path):
    """A migration cut short between its two steps, or a downgrade that wrote one again."""
    config = await _config(tmp_path, **{KIND_ERROR: False})
    legacy = _write_legacy(tmp_path, {KIND_TOILET: False})
    again = NotifyConfig(tmp_path / FILENAME)
    assert again.settings == config.settings
    assert not legacy.exists()
    _nothing_of_ntfy_left(tmp_path)


async def test_a_damaged_ntfy_json_is_deleted_with_every_kind_on(tmp_path):
    legacy = tmp_path / LEGACY_FILENAME
    legacy.write_text('{"token": "' + TOKEN + '", "kinds": ')
    config = NotifyConfig(tmp_path / FILENAME)
    assert all(config.settings.wants(k) for k in KINDS)
    assert not legacy.exists()
    _nothing_of_ntfy_left(tmp_path)


async def test_an_old_ntfy_json_is_kept_until_the_new_file_can_be_written(tmp_path, monkeypatch):
    """The switches are not lost to a full disk: the migration runs again next start."""
    import petkit_local.app_api.notify.settings as settings_module

    legacy = _write_legacy(tmp_path, {KIND_TOILET: False})

    def refuse(*_a, **_k):
        raise OSError("no space left on device")

    monkeypatch.setattr(settings_module, "atomic_write_text", refuse)
    assert not NotifyConfig(tmp_path / FILENAME).settings.wants(KIND_TOILET)
    assert legacy.exists() and not (tmp_path / FILENAME).exists()
    monkeypatch.undo()
    assert not NotifyConfig(tmp_path / FILENAME).settings.wants(KIND_TOILET)
    assert not legacy.exists()


# --- the panel --------------------------------------------------------------------

@pytest.fixture
async def panel(tmp_path):
    registry = DeviceRegistry()
    auth = AppAuth(tmp_path / AUTH_FILENAME, scrypt_params=FAST)
    app = create_panel_app(registry, None, EventHub(), {"app_port": 8097}, app_auth=auth)
    notifier = Notifier(NotifyConfig(tmp_path / FILENAME), registry, app_auth=auth)
    app[NOTIFIER] = notifier
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client, notifier
    await client.close()
    await notifier.close()


async def test_the_switches_are_on_the_phone_app_access_card(panel):
    client, notifier = panel
    body = await (await client.get("/api/app-access")).json()
    assert [k["kind"] for k in body["notify_kinds"]] == list(KINDS)
    assert all(k["enabled"] and k["label"] for k in body["notify_kinds"])

    r = await client.post("/api/app-access/notify-kinds", json={"kinds": {KIND_OFFLINE: False}})
    assert r.status == 200
    body = await r.json()
    assert body["ok"] is True and "sessions" in body  # the whole card, re-rendered
    assert {k["kind"]: k["enabled"] for k in body["notify_kinds"]}[KIND_OFFLINE] is False
    assert not notifier.config.settings.wants(KIND_OFFLINE)

    for bad in ({"kinds": {"nope": True}}, {"kinds": {KIND_TOILET: 1}}, {}):
        r = await client.post("/api/app-access/notify-kinds", json=bad)
        assert r.status == 400
    assert notifier.config.settings.wants(KIND_TOILET)


async def test_the_panel_has_nothing_of_ntfy_left(panel):
    client, _ = panel
    for path in ("/api/app-notify", "/api/app-notify/topic", "/api/app-notify/test"):
        r = await client.post(path, json={"server": "https://ntfy.example.org"})
        assert r.status == 404, path
    text = await (await client.get("/api/app-access")).text()
    assert "ntfy" not in text and "token" not in text and "topic" not in text
    js_dir = Path(__file__).resolve().parents[1] / "petkit_local" / "web" / "static" / "js"
    assert not (js_dir / "appnotify.js").exists()
    assert "ntfy" not in "\n".join(p.read_text(encoding="utf-8") for p in js_dir.glob("*.js"))


async def test_the_panel_without_a_notifier_shows_no_switches(tmp_path):
    auth = AppAuth(tmp_path / AUTH_FILENAME, scrypt_params=FAST)
    app = create_panel_app(DeviceRegistry(), None, EventHub(), {}, app_auth=auth)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        assert (await (await client.get("/api/app-access")).json())["notify_kinds"] is None
        r = await client.post("/api/app-access/notify-kinds", json={"kinds": {}})
        assert r.status == 503
    finally:
        await client.close()
