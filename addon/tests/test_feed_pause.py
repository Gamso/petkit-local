"""Pausing the feeding plan keeps it stored and tells the device it has no meals.

The firmware has no "schedule enabled" field a server can write, so a pause is
an answer, not a setting: every place that hands a feeder its plan has to leave
the recurring meals out, and nothing may write the device's now-empty plan back
over the stored one. Each of those places is pinned here, because one that
forgets either feeds a cat nobody meant to feed or deletes the meals the pause
exists to keep.
"""
import json
import time

from aiohttp.test_utils import TestClient, TestServer

from petkit_local.devices import defaults
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.ha.categories import get_entities_for_device
from petkit_local.ha.commands import (
    FEED_SCHEDULE_SWITCH, PROPERTY_SET_SUFFIX, handle_ha_command,
)
from petkit_local.http.handlers.feed import feed_push
from petkit_local.http.server import create_app
from petkit_local.mqtt.bridge import MQTTBridge
from petkit_local.utils.const import DEVICE_TYPES_FEEDER
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app

MEAL = {"id": "n_25200", "t": 25200, "a1": 2, "a2": 0}
#: Every day, so `latest` always holds an instance of it whatever day this runs.
PLAN = {"schedule": [{"re": "1,2,3,4,5,6,7", "it": [MEAL]}], "v": 2}
EMPTY_GROUP = {"re": "1,2,3,4,5,6,7", "it": [], "itemJsonString": "[]"}


def _registry(device_type="d4sh"):
    reg = DeviceRegistry()
    d = reg.get_or_create(petkit_id=1, device_type=device_type, serial_number="SN")
    d.config["feed_schedule"] = json.loads(json.dumps(PLAN))
    return reg, d


def _entity(d, key):
    return next(e for e in get_entities_for_device(d) if e.key == key)


def _pause(d):
    return handle_ha_command(d, _entity(d, FEED_SCHEDULE_SWITCH), "OFF")


def _stored_meals(d):
    return d.config["feed_schedule"]["schedule"][0]["it"]


# --- the switch --------------------------------------------------------------

def test_every_feeder_has_the_switch_and_it_starts_on():
    for device_type in sorted(DEVICE_TYPES_FEEDER):
        _reg, d = _registry(device_type)
        assert _entity(d, FEED_SCHEDULE_SWITCH).component == "switch"
        assert d.feed_schedule_active is True


def test_pausing_keeps_the_plan_and_hands_the_device_one_without_meals():
    _reg, d = _registry()
    suffix, envelope = _pause(d)

    assert d.feed_schedule_active is False
    assert _stored_meals(d) == [MEAL], "the meals must survive a pause"
    # Our intent, never a device setting: `to_device_info` serves
    # `config["settings"]` straight to the feeder.
    assert d.config["local"]["feedScheduleActive"] is False
    assert "feedScheduleActive" not in d.config.get("settings", {})

    assert suffix == PROPERTY_SET_SUFFIX
    wire = json.loads(envelope["params"]["feed"])
    assert wire == {"schedule": [], "nextTick": 86340, "latest": []}
    # ...and a device on HTTP is told to fetch the plan again.
    assert d.command_queue[-1]["payload"] == {"feed_get": "1"}


def test_resuming_hands_the_stored_plan_back():
    _reg, d = _registry()
    _pause(d)
    suffix, envelope = handle_ha_command(d, _entity(d, FEED_SCHEDULE_SWITCH), "ON")

    assert d.feed_schedule_active is True
    wire = json.loads(envelope["params"]["feed"])
    assert wire["schedule"] == [{"re": "1,2,3,4,5,6,7", "it": [MEAL]}]
    assert wire["latest"], "a daily meal is due today or tomorrow"
    assert d.command_queue[-1]["payload"] == {"feed_get": "1"}


def test_the_pause_survives_a_restart():
    _reg, d = _registry()
    _pause(d)
    restored = type(d).from_dict(json.loads(json.dumps(d.to_dict())))
    assert restored.feed_schedule_active is False
    assert _stored_meals(restored) == [MEAL]


def test_a_one_off_feed_still_fires_while_the_plan_is_paused():
    """A deferred feed is a dated instruction of its own. Withheld with the
    plan it would expire unfed, and nothing would ever say so."""
    _reg, d = _registry()
    d.config["feed_schedule"]["deferred"] = [
        {"id": "d_20260813_61500", "a1": 0, "a2": 1, "fire_at": time.time() + 3600}]
    _pause(d)

    wire = feed_push(d)
    assert wire["schedule"] == []
    assert [entry["id"] for entry in wire["latest"]] == ["d_20260813_61500"]
    assert wire["nextTick"] == wire["latest"][0]["t"]


def test_the_raw_json_entity_saves_a_plan_without_resuming_it():
    _reg, d = _registry()
    _pause(d)
    edited = {"schedule": [{"re": "1,7", "it": [
        {"id": "n_3600", "t": 3600, "a1": 1, "a2": 1}]}]}
    _suffix, envelope = handle_ha_command(
        d, _entity(d, "feeding_schedule"), json.dumps(edited))

    assert d.config["feed_schedule"]["schedule"] == edited["schedule"]
    assert json.loads(envelope["params"]["feed"])["schedule"] == []


def test_the_editor_is_told_whether_the_plan_is_paused():
    _reg, d = _registry()
    target = next(t for t in defaults.schedule_targets(d) if t["kind"] == "feed")
    assert target["active"] is True
    _pause(d)
    target = next(t for t in defaults.schedule_targets(d) if t["kind"] == "feed")
    assert target["active"] is False
    assert target["value"]["schedule"][0]["it"] == [MEAL], "the editor still shows the meals"


# --- dev_feed_get over HTTP --------------------------------------------------

async def _http_feed(reg):
    app = create_app(reg, {"api_url": "http://localhost:8080",
                           "bucket_endpoint": "https://localhost:9000"})
    async with TestClient(TestServer(app)) as c:
        r = await c.get("/6/d4sh/dev_feed_get",
                        headers={"X-Device": "id=1&nonce=x&timestamp=1&type=d4sh&sign=x"})
        return (await r.json())["result"]


async def test_a_paused_plan_is_served_as_no_plan():
    reg, d = _registry()
    assert (await _http_feed(reg))["latest"], "served while active"

    _pause(d)
    result = await _http_feed(reg)
    assert result == {"schedule": [EMPTY_GROUP], "nextTick": 86340, "latest": []}
    assert _stored_meals(d) == [MEAL], "serving a paused plan must not touch the stored meals"


async def test_a_paused_plan_still_serves_its_one_off_feed():
    reg, d = _registry()
    d.config["feed_schedule"]["deferred"] = [
        {"id": "d_20260813_61500", "a1": 0, "a2": 1, "fire_at": time.time() + 3600}]
    _pause(d)
    result = await _http_feed(reg)
    assert result["schedule"] == [EMPTY_GROUP]
    assert [entry["id"] for entry in result["latest"]] == ["d_20260813_61500"]


# --- the MQTT side -----------------------------------------------------------

def _bridge(reg):
    return MQTTBridge(reg, None, BLERegistry())


def test_a_paused_plan_is_withheld_from_an_mqtt_data_get_too():
    reg, d = _registry()
    bridge = _bridge(reg)
    assert bridge._user_get_payload(d, "dev_feed_get")["result"]["schedule"][0]["it"] == [MEAL]

    _pause(d)
    assert bridge._user_get_payload(d, "dev_feed_get")["result"]["schedule"] == []
    assert _stored_meals(d) == [MEAL]


def test_the_devices_empty_report_does_not_overwrite_a_paused_plan():
    """Told it has no meals, a device that reports its plan reports none. The
    bridge stores a reported plan -- which, paused, would be the deletion."""
    reg, d = _registry()
    bridge = _bridge(reg)
    _pause(d)
    bridge._sync_settings_from_device(d, {"feed": {"schedule": []}})
    assert _stored_meals(d) == [MEAL]

    # Active, a reported plan is still what gets stored, as before.
    handle_ha_command(d, _entity(d, FEED_SCHEDULE_SWITCH), "ON")
    bridge._sync_settings_from_device(d, {"feed": {"schedule": []}})
    assert d.config["feed_schedule"]["schedule"] == []


# --- the panel ---------------------------------------------------------------

class _FakeBridge:
    def __init__(self):
        self._client = object()
        self.sent = []

    async def publish_to_device(self, device, suffix, payload):
        self.sent.append((device.petkit_id, suffix, payload))


async def test_the_panel_toggles_the_same_switch_and_saves_without_resuming():
    reg, d = _registry()
    d.mqtt_connected = True
    bridge = _FakeBridge()
    app = create_panel_app(reg, BLERegistry(), EventHub(),
                           {"api_url": "http://x/6/", "mqtt_tls": True,
                            "mqtt_tls_port": 443, "capture": False,
                            "capture_dir": "/nope"},
                           bridge)
    async with TestClient(TestServer(app)) as c:
        r = await c.post("/api/devices/1/command", json=(
            {"entity": FEED_SCHEDULE_SWITCH, "value": "OFF"}))
        assert r.status == 200 and (await r.json())["ok"]
        assert d.feed_schedule_active is False
        assert json.loads(bridge.sent[-1][2]["params"]["feed"])["schedule"] == []

        # The switch reads back in the Controls card, and the editor is told.
        detail = await (await c.get("/api/devices/1")).json()
        switch = next(e for e in detail["entities"] if e["key"] == FEED_SCHEDULE_SWITCH)
        assert switch["value"] is False
        target = next(t for t in detail["schedules"] if t["kind"] == "feed")
        assert target["active"] is False

        # Saving the plan from the editor stores it and still withholds it.
        r = await c.post("/api/devices/1/schedule", json=(
            {"target": "feed_schedule", "value": {"schedule": [
                {"re": "1,7", "it": [{"id": "n_3600", "t": 3600, "a1": 1, "a2": 1}]}]}}))
        assert r.status == 200
        assert d.config["feed_schedule"]["schedule"][0]["re"] == "1,7"
        assert json.loads(bridge.sent[-1][2]["params"]["feed"])["schedule"] == []
