"""Telling the generations of a Bluetooth accessory apart (issue #115).

A CTW3 is an EverSweet Max Cordless or an EverSweet Max 2: PetKit keeps the one
codename, as it does for the Pura Max and the YumShares (issue #107). Unlike a
WiFi device, nothing on the wire says which -- the relay list's `type: 24` is
the scan number for every CTW3 -- so the owner's choice in the panel is the
only source today. What is held here:

* the table names both generations, graded, and the first is the codename's name;
* an accessory resolves its generation with a device's precedence (owner, then
  learned, then nothing), and ignores a stored value the table does not list;
* the choice persists in `ble_devices.json`, survives a reload and a re-pairing;
* the panel's Model card and `/api/ble/{id}/model` set it and go back to Automatic;
* the panel's BLE list, Home Assistant's device model and the app API all
  use the name.
"""
import json
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from petkit_local.app_api import serializers
from petkit_local.devices.base import Device
from petkit_local.devices.ble import BLEDevice, BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events import codes
from petkit_local.ha.publisher import HAPublisher
from petkit_local.utils.const import DEVICE_NAMES, MODEL_GENERATIONS, device_display_name
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app
from tests._fakes import FakeMqttClient

CTW3_ID = 900001

# --- the table -------------------------------------------------------------------


def test_the_ctw3_has_two_named_generations():
    rows = MODEL_GENERATIONS["ctw3"]
    assert {gen: row.name for gen, row in rows.items()} == {
        1: "EverSweet Max Cordless", 2: "EverSweet Max 2"}
    assert rows[1].name == DEVICE_NAMES["ctw3"]
    # From an owner's word that PetKit's app names his CTW3 so, not a wire value.
    assert rows[2].grade == codes.INFERRED and "#115" in rows[2].note
    assert rows[1].grade == codes.INFERRED
    assert device_display_name("ctw3", 2) == "EverSweet Max 2"
    assert device_display_name("ctw3") == "EverSweet Max Cordless"


# --- the accessory ---------------------------------------------------------------


def test_the_owner_outranks_what_was_learned_which_outranks_nothing():
    a = BLEDevice(ble_type="ctw3", petkit_id=CTW3_ID)
    assert a.generation_and_source == (None, "")
    assert a.model_name == "EverSweet Max Cordless"
    assert a.record_generation("typeCode", 2)
    assert a.generation_and_source == (2, "typeCode")
    assert a.model_name == "EverSweet Max 2"
    a.generation_choice = 1
    assert a.generation_and_source == (1, "owner")
    assert a.model_name == "EverSweet Max Cordless"
    # Learning never touches the owner's choice.
    assert a.record_generation("typeCode", 1) and a.generation_choice == 1
    a.generation_choice = None
    assert a.generation_and_source == (1, "typeCode")


def test_a_stored_value_the_table_does_not_list_is_ignored():
    a = BLEDevice(ble_type="ctw3", petkit_id=CTW3_ID)
    for bad in (3, 0, True, 2.0, "x", [2], {"g": 2}):
        a.generation_choice = bad
        assert a.generation is None, bad
    a.generation_learned = {"typeCode": 7, "hardware": "two", "elsewhere": 2}
    assert a.generation is None and a.model_name == "EverSweet Max Cordless"
    a.generation_learned = "garbage"  # type: ignore[assignment]
    assert a.generation is None
    assert not a.record_generation("typeCode", 9)
    assert not a.record_generation("somewhere", 2)
    # A codename sold in one generation has none to choose.
    k3 = BLEDevice(ble_type="k3", petkit_id=2, generation_choice=2)
    assert k3.generation is None and k3.model_name == "Pura Air K3"


def test_the_choice_persists_in_the_ble_record_and_survives_a_reload(tmp_path: Path):
    path = tmp_path / "ble_devices.json"
    ble = BLERegistry(path)
    a = ble.register(ble_type="ctw3", petkit_id=CTW3_ID, mac="AABBCCDDEE02", link_with=1)
    a.generation_choice = 2
    a.record_generation("typeCode", 1)
    ble.save()

    stored = json.loads(path.read_text(encoding="utf-8"))[str(CTW3_ID)]
    assert (stored["generation_choice"], stored["generation_learned"]) == (2, {"typeCode": 1})
    again = BLERegistry(path).get(CTW3_ID)
    assert again.generation_and_source == (2, "owner")
    assert again.model_name == "EverSweet Max 2"

    # The parent re-reporting the accessory, or an import from PetKit, carries
    # no generation and must not erase the owner's.
    ble.register(ble_type="ctw3", petkit_id=CTW3_ID, mac="AABBCCDDEE02", link_with=1,
                 secret="s3cret")
    ble.apply_cloud_binding({"ble_type": "ctw3", "petkit_id": CTW3_ID,
                             "mac": "AABBCCDDEE02", "secret": "s3cret", "link_with": 1,
                             "interval": 240, "scan_type": 24})
    assert ble.get(CTW3_ID).generation_choice == 2


def test_a_record_from_before_generations_or_a_garbled_one_still_loads(tmp_path: Path):
    path = tmp_path / "ble_devices.json"
    path.write_text(json.dumps({
        "1": {"ble_type": "ctw3", "petkit_id": 1, "mac": "AABBCCDDEE01"},
        "2": {"ble_type": "ctw3", "petkit_id": 2, "mac": "AABBCCDDEE02",
              "generation_choice": "nine", "generation_learned": [2]},
    }), encoding="utf-8")
    ble = BLERegistry(path)
    assert ble.get(1).generation is None and ble.get(1).model_name == "EverSweet Max Cordless"
    assert ble.get(2).generation is None and ble.get(2).model_name == "EverSweet Max Cordless"


# --- the panel -------------------------------------------------------------------


def _panel(reg: DeviceRegistry, ble: BLERegistry, publisher=None):
    cfg = {"api_url": "http://x/6/", "capture": False, "capture_dir": "/nope"}
    return create_panel_app(reg, ble, EventHub(), cfg, None, ha_publisher=publisher)


async def test_the_panel_model_card_names_the_accessory_and_goes_back_to_automatic(
        tmp_path: Path):
    path = tmp_path / "ble_devices.json"
    reg = DeviceRegistry()
    reg.get_or_create(petkit_id=1, device_type="t4", serial_number="SN1")
    ble = BLERegistry(path)
    ble.register(ble_type="ctw3", petkit_id=CTW3_ID, mac="AABBCCDDEE02", link_with=1)
    client = TestClient(TestServer(_panel(reg, ble)))
    await client.start_server()
    try:
        row = (await (await client.get("/api/ble")).json())["accessories"][0]
        assert (row["name"], row["generation"]) == ("EverSweet Max Cordless", None)
        info = row["modelInfo"]
        assert (info["source"], info["override"], info["learned"]) == ("", None, {})
        assert [c["name"] for c in info["choices"]] == ["EverSweet Max Cordless",
                                                       "EverSweet Max 2"]
        assert (await (await client.get(f"/api/ble/{CTW3_ID}/model")).json()) == info

        r = await client.post(f"/api/ble/{CTW3_ID}/model", json={"generation": 2})
        body = await r.json()
        assert r.status == 200 and body["ok"]
        assert (body["name"], body["source"], body["override"]) == ("EverSweet Max 2",
                                                                    "owner", 2)
        assert BLERegistry(path).get(CTW3_ID).generation_choice == 2
        row = (await (await client.get("/api/ble")).json())["accessories"][0]
        assert (row["name"], row["generation"]) == ("EverSweet Max 2", 2)

        r = await client.post(f"/api/ble/{CTW3_ID}/model", json={"generation": None})
        body = await r.json()
        assert (body["name"], body["source"], body["override"]) == (
            "EverSweet Max Cordless", "", None)
        assert BLERegistry(path).get(CTW3_ID).generation_choice is None
    finally:
        await client.close()


async def test_automatic_falls_back_to_what_was_learned():
    reg = DeviceRegistry()
    ble = BLERegistry()
    a = ble.register(ble_type="ctw3", petkit_id=CTW3_ID, mac="AABBCCDDEE02")
    a.record_generation("typeCode", 2)
    client = TestClient(TestServer(_panel(reg, ble)))
    await client.start_server()
    try:
        r = await client.post(f"/api/ble/{CTW3_ID}/model", json={"generation": "auto"})
        body = await r.json()
        assert (body["name"], body["source"], body["learned"]) == (
            "EverSweet Max 2", "typeCode", {"typeCode": 2})
    finally:
        await client.close()


async def test_the_panel_refuses_a_model_the_accessory_does_not_have():
    reg = DeviceRegistry()
    ble = BLERegistry()
    ble.register(ble_type="ctw3", petkit_id=CTW3_ID, mac="AABBCCDDEE02")
    ble.register(ble_type="k3", petkit_id=CTW3_ID + 1, mac="AABBCCDDEE03")
    client = TestClient(TestServer(_panel(reg, ble)))
    await client.start_server()
    try:
        for body in ({"generation": 3}, {"generation": True}, {"generation": "two"}, {}):
            r = await client.post(f"/api/ble/{CTW3_ID}/model", json=body)
            assert r.status == 400, body
        assert ble.get(CTW3_ID).generation_choice is None
        # A codename sold in one generation has no Model card at all.
        rows = {a["petkit_id"]: a for a in (await (await client.get("/api/ble")).json())[
            "accessories"]}
        assert rows[CTW3_ID + 1]["modelInfo"] is None
        r = await client.post(f"/api/ble/{CTW3_ID + 1}/model", json={"generation": 1})
        assert r.status == 400
        assert (await client.post("/api/ble/424242/model", json={"generation": 2})).status \
            == 404
    finally:
        await client.close()


def test_the_panel_script_shows_the_model_card_on_an_accessory():
    js = (Path(__file__).parent.parent / "petkit_local/web/static/js/devices.js").read_text(
        encoding="utf-8")
    assert "modelCard(a.modelInfo, a.petkit_id, 'ble')" in js
    assert "(ble ? 'ble/' : 'devices/') + id + '/model'" in js


# --- Home Assistant --------------------------------------------------------------


def _models(client: FakeMqttClient) -> set[str]:
    return {json.loads(p)["device"]["model"] for t, p, _ in client.published
            if t.endswith("/config") and p}


async def test_the_ha_device_model_follows_the_choice():
    reg = DeviceRegistry()
    ble = BLERegistry()
    a = ble.register(ble_type="ctw3", petkit_id=CTW3_ID, mac="AABBCCDDEE02",
                     serial_number="SN")
    pub = HAPublisher(reg, {}, ble_registry=ble)
    pub._client = FakeMqttClient()
    pub._connected = True
    await pub.publish_ble_discovery(a)
    assert _models(pub._client) == {"EverSweet Max Cordless"}

    pub._client.published.clear()
    await pub.publish_ble_state(a)
    assert _models(pub._client) == set(), "no re-announce while nothing changed"

    a.generation_choice = 2
    await pub.publish_ble_state(a)
    assert _models(pub._client) == {"EverSweet Max 2"}
    # The HA device name and the entity ids are user state, and do not move.
    names = {json.loads(p)["device"]["name"] for t, p, _ in pub._client.published
             if t.endswith("/config") and p}
    assert names == {"PetKit CTW3 SN"}


async def test_the_panel_choice_re_announces_the_accessory_to_home_assistant():
    reg = DeviceRegistry()
    ble = BLERegistry()
    ble.register(ble_type="ctw3", petkit_id=CTW3_ID, mac="AABBCCDDEE02", serial_number="SN")
    pub = HAPublisher(reg, {}, ble_registry=ble)
    pub._client = FakeMqttClient()
    pub._connected = True
    client = TestClient(TestServer(_panel(reg, ble, pub)))
    await client.start_server()
    try:
        r = await client.post(f"/api/ble/{CTW3_ID}/model", json={"generation": 2})
        assert r.status == 200
        assert _models(pub._client) == {"EverSweet Max 2"}
    finally:
        await client.close()


# --- the app API -----------------------------------------------------------------


def test_the_app_api_names_the_accessory_by_its_generation():
    parent = Device(device_type="t4", petkit_id=1)
    a = BLEDevice(ble_type="ctw3", petkit_id=CTW3_ID, link_with=1)
    s = serializers.accessory_summary(a, parent)
    assert (s["model"], s["model_name"], s["generation"]) == (
        "EverSweet Max Cordless", "EverSweet Max Cordless", None)
    a.generation_choice = 2
    s = serializers.accessory_summary(a, parent)
    # `model` keeps the codename's own name, which released apps already read.
    assert (s["model"], s["model_name"], s["generation"]) == (
        "EverSweet Max Cordless", "EverSweet Max 2", 2)
