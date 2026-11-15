"""The phone app API's Bluetooth accessories (`app_api/inventory.py`).

What is held here:

* a fountain or a K3 is a device of the app's, under its own id, with the
  entities Home Assistant gives it and `connection: ble`;
* the link is visible from both ends: the accessory names its `parent_id`, and
  its parent lists it in `accessories` — which is how a litter box knows it has
  a Pura Air (K3), the one accessory never served in the relay list;
* an accessory's write goes as a frame through its parent while that parent is
  on MQTT, and is refused (409, nothing queued) when it is not;
* the routes about hardware an accessory lacks (feeding, live video) stay 404;
* `/ws` carries accessories in `hello` and their changes as `state`;
* every answer has exactly its schema's keys.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from petkit_local.app_api.appkeys import UPDATE_FEED
from petkit_local.app_api.server import PREFIX, create_app_api
from petkit_local.app_api.updates import UpdateFeed
from petkit_local.devices.ble import CMD_SET_MODE, BLERegistry
from petkit_local.web.hub import EventHub
from tests.test_app_api import PASSWORD, Env, _Schemas
from tests.test_app_api_updates import _next, _quiet

K3_ID = 900
CTW3_ID = 901
ORPHAN_ID = 902


class FakeSink:
    """The MQTT bridge, as far as an accessory's write uses it."""

    def __init__(self) -> None:
        self.frames: list[tuple[int, int, int, bytes]] = []
        self.sent = True

    async def publish_to_device(self, device, suffix, envelope) -> None:
        raise AssertionError("an accessory's write is a frame, not a device command")

    async def publish_ble_command(self, parent, ble, cmd, payload) -> bool:
        if self.sent:
            self.frames.append((parent.petkit_id, ble.petkit_id, cmd, payload))
        return self.sent


class FakePublisher:
    def __init__(self) -> None:
        self.ble_states: list[int] = []

    async def publish_state(self, device) -> None:
        pass

    async def publish_ble_state(self, ble) -> None:
        self.ble_states.append(ble.petkit_id)


class AccessoryEnv(Env):
    """A T4 relaying a K3 and a CTW3, and a W5 linked to nothing."""

    def __init__(self, tmp: Path, store: Any, pet_registry: Any) -> None:
        super().__init__(tmp, store, pet_registry)
        self.t4 = self.registry.get_or_create(petkit_id=300, device_type="t4",
                                              serial_number="SN300")
        self.ble = BLERegistry()
        self.k3 = self.ble.register(ble_type="k3", petkit_id=K3_ID, link_with=300,
                                    mac="AABBCCDDEE01", serial_number="K3SN")
        self.k3.state = {"consumables": {"liquid": 60, "battery": 88}}
        self.ctw3 = self.ble.register(ble_type="ctw3", petkit_id=CTW3_ID, link_with=300,
                                      mac="AABBCCDDEE02")
        self.ble.register(ble_type="w5", petkit_id=ORPHAN_ID, mac="AABBCCDDEE03")
        self.sink = FakeSink()
        self.publisher = FakePublisher()
        self.hub = EventHub()
        self.app = create_app_api(self.auth, self.registry, ble_registry=self.ble,
                                  event_store=store, pet_registry=pet_registry,
                                  media_root=str(self.media_root), data_dir=str(tmp),
                                  throttle=self.throttle, command_sink=self.sink,
                                  ha_publisher=self.publisher, hub=self.hub)
        # Quick enough for a test; set before the app starts and freezes.
        self.feed = UpdateFeed(self.auth, self.registry, self.hub, event_store=store,
                               pet_registry=pet_registry, media_root=str(self.media_root),
                               ble_registry=self.ble, coalesce=0.2, sweep=0.6)
        self.app[UPDATE_FEED] = self.feed
        self.token = ""

    async def call(self, method: str, path: str, body: Any = None):
        kw: dict[str, Any] = {"headers": {"Authorization": f"Bearer {self.token}"}}
        if body is not None:
            kw["json"] = body
        return await self.client.request(method, PREFIX + path, **kw)

    async def json(self, path: str) -> Any:
        r = await self.call("GET", path)
        assert r.status == 200, await r.text()
        return await r.json()


@pytest.fixture
async def a(tmp_path: Path, event_store, pet_registry):
    e = AccessoryEnv(tmp_path, event_store, pet_registry)
    await e.start()
    await e.auth.set_password(PASSWORD)
    e.token = await e.login()
    yield e
    await e.client.close()


async def _refused(r, status: int, code: str) -> None:
    assert r.status == status, await r.text()
    assert (await r.json())["error"] == code


# --- listing ------------------------------------------------------------------------

async def test_accessories_are_listed_as_devices_linked_both_ways(a: AccessoryEnv):
    devices = {d["id"]: d for d in (await a.json("/devices"))["devices"]}
    assert sorted(devices) == [100, 300, K3_ID, CTW3_ID, ORPHAN_ID]
    schemas = _Schemas()
    for d in devices.values():
        schemas.check(d, "DeviceSummary")

    t4 = devices[300]
    assert t4["connection"] == "wifi" and t4["parent_id"] is None
    assert t4["accessories"] == [{"id": K3_ID, "type": "k3", "kind": "purifier"},
                                 {"id": CTW3_ID, "type": "ctw3", "kind": "fountain"}]
    assert devices[100]["accessories"] == []

    k3 = devices[K3_ID]
    assert (k3["connection"], k3["parent_id"], k3["kind"], k3["type"]) == \
        ("ble", 300, "purifier", "k3")
    assert k3["model"] == "Pura Air K3"
    assert k3["firmware"] is None and k3["is_camera"] is False and k3["accessories"] == []
    # A K3's readings ride on its parent's reports: no frame, no last_seen.
    assert k3["last_seen"] is None

    orphan = devices[ORPHAN_ID]
    assert orphan["parent_id"] is None and orphan["online"] is False


async def test_an_accessory_is_online_and_on_mqtt_while_its_parent_is(a: AccessoryEnv):
    a.t4.online = True
    a.t4.mqtt_connected = True
    k3 = (await a.json(f"/devices/{K3_ID}"))["device"]
    assert k3["online"] is True and k3["transport"] == "mqtt"


async def test_a_k3_detail_carries_its_cartridge_level_and_no_actions(a: AccessoryEnv):
    detail = (await a.json(f"/devices/{K3_ID}"))["device"]
    _Schemas().check(detail, "DeviceDetail")
    values = {e["key"]: e["value"] for e in detail["entities"]}
    assert values == {"k3_liquid": 60, "k3_battery": 88}
    assert detail["actions"] == []
    assert detail["serial"] == "K3SN"


async def test_a_fountain_detail_offers_its_filter_reset_as_destructive(a: AccessoryEnv):
    detail = (await a.json(f"/devices/{CTW3_ID}"))["device"]
    _Schemas().check(detail, "DeviceDetail")
    assert detail["actions"] == [{"key": "ctw3_reset_filter", "name": "Reset Filter",
                                  "destructive": True}]
    assert {"ctw3_mode", "ctw3_power"} <= {e["key"] for e in detail["entities"]
                                           if e["settable"]}


async def test_the_parent_detail_lists_its_accessories(a: AccessoryEnv):
    detail = (await a.json("/devices/300"))["device"]
    _Schemas().check(detail, "DeviceDetail")
    assert [x["id"] for x in detail["accessories"]] == [K3_ID, CTW3_ID]


async def test_a_device_wins_an_id_it_shares_with_an_accessory(a: AccessoryEnv):
    a.ble.register(ble_type="w5", petkit_id=300, mac="AABBCCDDEE04")
    detail = (await a.json("/devices/300"))["device"]
    assert detail["type"] == "t4"
    assert [d["id"] for d in (await a.json("/devices"))["devices"]].count(300) == 1


async def test_routes_for_hardware_an_accessory_lacks_stay_404(a: AccessoryEnv):
    for path in (f"/devices/{K3_ID}/schedule", f"/devices/{CTW3_ID}/deferred-feeds"):
        await _refused(await a.call("GET", path), 404, "device_not_found")


async def test_without_a_ble_registry_nothing_extra_is_listed(tmp_path, event_store,
                                                             pet_registry):
    e = Env(tmp_path, event_store, pet_registry)
    await e.start()
    try:
        await e.auth.set_password(PASSWORD)
        token = await e.login()
        devices = (await (await e.get("/devices", token)).json())["devices"]
        assert [(d["id"], d["accessories"]) for d in devices] == [(100, [])]
        assert (await e.get(f"/devices/{K3_ID}", token)).status == 404
    finally:
        await e.client.close()


# --- writes ---------------------------------------------------------------------------

async def test_a_setting_is_relayed_as_a_frame_while_the_parent_is_on_mqtt(a: AccessoryEnv):
    a.t4.mqtt_connected = True
    r = await a.call("PUT", f"/devices/{CTW3_ID}/entities/ctw3_mode", {"value": "smart"})
    assert r.status == 200, await r.text()
    body = await r.json()
    _Schemas().check(body, "EntityWriteResult")
    assert body["delivered"] == "mqtt"
    assert body["entity"]["value"] == 2 and body["entity"]["display"] == "smart"
    assert a.sink.frames == [(300, CTW3_ID, CMD_SET_MODE, bytes([1, 1, 2]))]
    assert a.ctw3.state["states"]["mode"] == 2
    assert a.publisher.ble_states == [CTW3_ID]
    assert any("app: ctw3_mode" in e["summary"] for e in a.hub.recent(kinds={"cmd"}))
    assert not a.t4.command_queue


async def test_a_write_is_refused_not_queued_while_the_parent_is_off_mqtt(a: AccessoryEnv):
    r = await a.call("PUT", f"/devices/{CTW3_ID}/entities/ctw3_mode", {"value": "smart"})
    await _refused(r, 409, "accessory_unreachable")
    r = await a.call("POST", f"/devices/{CTW3_ID}/actions/ctw3_reset_filter",
                     {"confirm": True})
    await _refused(r, 409, "accessory_unreachable")
    assert not a.sink.frames and not a.t4.command_queue

    a.t4.mqtt_connected = True
    a.sink.sent = False   # the bridge lost its broker
    r = await a.call("PUT", f"/devices/{CTW3_ID}/entities/ctw3_mode", {"value": "smart"})
    await _refused(r, 409, "accessory_unreachable")
    assert "mode" not in a.ctw3.state.get("states", {})


async def test_an_accessory_with_no_parent_is_unreachable(a: AccessoryEnv):
    r = await a.call("POST", f"/devices/{ORPHAN_ID}/actions/w5_reset_filter",
                     {"confirm": True})
    await _refused(r, 409, "accessory_unreachable")


async def test_an_accessory_write_is_checked_like_any_other(a: AccessoryEnv):
    a.t4.mqtt_connected = True
    # Typed values, as for a device.
    r = await a.call("PUT", f"/devices/{CTW3_ID}/entities/ctw3_mode", {"value": 2})
    await _refused(r, 422, "refused")
    # A block the fountain has never reported in full cannot be restated.
    r = await a.call("PUT", f"/devices/{CTW3_ID}/entities/ctw3_brightness",
                     {"value": "high"})
    await _refused(r, 422, "refused")
    # A reading is not a setting, and a K3 has nothing to press.
    r = await a.call("PUT", f"/devices/{K3_ID}/entities/k3_liquid", {"value": 50})
    await _refused(r, 422, "not_settable")
    r = await a.call("POST", f"/devices/{K3_ID}/actions/reset_n60", {"confirm": True})
    await _refused(r, 404, "action_not_found")
    # A filter reset needs the confirmation the panel's red buttons need.
    r = await a.call("POST", f"/devices/{CTW3_ID}/actions/ctw3_reset_filter")
    await _refused(r, 422, "confirmation_required")
    assert not a.sink.frames


async def test_a_confirmed_filter_reset_is_sent(a: AccessoryEnv):
    a.t4.mqtt_connected = True
    r = await a.call("POST", f"/devices/{CTW3_ID}/actions/ctw3_reset_filter",
                     {"confirm": True})
    assert r.status == 200, await r.text()
    body = await r.json()
    _Schemas().check(body, "ActionResult")
    assert body == {"ok": True, "action": "ctw3_reset_filter", "delivered": "mqtt"}
    assert [(p, b) for p, b, _, _ in a.sink.frames] == [(300, CTW3_ID)]


# --- live updates ---------------------------------------------------------------------

@pytest.fixture
async def live(a: AccessoryEnv):
    ws = await a.client.ws_connect(f"{PREFIX}/ws",
                                   headers={"Authorization": f"Bearer {a.token}"})
    yield a, ws
    await ws.close()


async def test_hello_carries_the_accessories(live):
    a, ws = live
    hello = await _next(ws, "hello")
    _Schemas().check(hello, "UpdateHello")
    ids = [d["id"] for d in hello["devices"]]
    assert ids == [100, 300, K3_ID, CTW3_ID, ORPHAN_ID]


async def test_a_reading_relayed_by_the_parent_is_sent_as_the_accessory_s_state(live):
    a, ws = live
    await _next(ws, "hello")
    # A K3's reading arrives on its parent's report: the hub names the parent.
    a.k3.state["consumables"]["liquid"] = 40
    a.hub.publish("http", 300, "dev_state_report")
    states = [m for m in await _quiet(ws, 0.8) if m["type"] == "state"]
    assert [(m["device_id"], m["entities"]) for m in states] == [
        (K3_ID, [{"key": "k3_liquid", "value": 40, "display": None}])]


async def test_unpairing_an_accessory_is_announced(live):
    a, ws = live
    await _next(ws, "hello")
    a.ble.remove(CTW3_ID)
    got = await _quiet(ws, 1.2)   # past one sweep
    assert {"type": "device_removed", "device_id": CTW3_ID} in got
    parent = [m["device"] for m in got if m["type"] == "device" and m["device"]["id"] == 300]
    assert parent and [x["id"] for x in parent[-1]["accessories"]] == [K3_ID]
