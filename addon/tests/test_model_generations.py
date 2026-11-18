"""Telling the generations of one codename apart (issue #107).

PetKit keeps one codename across generations -- a Pura Max 2 is still a `t4`,
a Dual-Hopper 2 still a `d4sh` -- so the generation has to come from somewhere
else: PetKit's own `dev_device_info.typeCode`, seen in proxy mode; the device's
own `hardware` where that is known to mean it; or the owner, in the panel, whose
choice always wins. What is held here:

* the graded table names each generation, and an unknown one keeps today's name;
* a proxied reply teaches `typeCode` without changing what is relayed, and
  nothing is learned with proxy mode off;
* a D4SH's own `hardware: 2` teaches it, a T4's `hardware: 1` does not;
* Home Assistant's device model, the panel and the app API all use the name;
* a gen-2 YumShare supports recognition before it has asked for anything.
"""
import json
from pathlib import Path

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.app_api import serializers
from petkit_local.devices.base import GENERATION_KEY, GENERATION_LEARNED_KEY, Device
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events import codes
from petkit_local.events.normalize import apply_state_snapshot
from petkit_local.ha.discovery import build_discovery_payload
from petkit_local.ha.entities.sensors import LITTER_SENSORS
from petkit_local.ha.learn import learn_generation
from petkit_local.ha.publisher import HAPublisher
from petkit_local.http import appkeys as device_keys
from petkit_local.http.proxy import close_proxy_session
from petkit_local.http.server import create_app
from petkit_local.utils.const import (
    DEVICE_NAMES, HARDWARE_GENERATIONS, MODEL_GENERATIONS, device_display_name,
    generation_choices, generation_from_hardware, valid_generation,
)
from petkit_local.web.api.config_import import import_device
from petkit_local.web.hub import EventHub
from tests._fakes import FakeMqttClient

# --- the table ---------------------------------------------------------------


def test_each_generation_has_its_own_name():
    assert device_display_name("t4", 2) == "Pura Max 2"
    assert device_display_name("t4", 3) == "Pura Max 3"
    assert device_display_name("d4sh", 2) == "YumShare Dual-Hopper 2"
    assert device_display_name("d4h", 2) == "YumShare Solo 2"
    assert device_display_name("T4", 2) == "Pura Max 2"


def test_an_unknown_generation_keeps_todays_name():
    assert device_display_name("t4") == "Pura Max"
    assert device_display_name("t4", None) == "Pura Max"
    assert device_display_name("d4sh", 3) == "YumShare Dual-Hopper"
    assert device_display_name("t5", 2) == "Purobot Max Pro 2"
    assert device_display_name("zz9", 2) == "Unknown"
    assert device_display_name("", 2) == ""


def test_every_row_is_graded_and_generation_one_is_the_codenames_own_name():
    for code, rows in MODEL_GENERATIONS.items():
        assert rows[1].name == DEVICE_NAMES[code], code
        for gen, row in rows.items():
            assert row.grade in codes.GRADES, (code, gen)
            assert row.note, (code, gen)
    for code, rows in HARDWARE_GENERATIONS.items():
        for hardware, (gen, grade, note) in rows.items():
            assert grade in codes.GRADES and note, (code, hardware)
            assert gen in MODEL_GENERATIONS[code], (code, hardware)


def test_the_observed_mappings_are_confirmed_and_the_rest_are_not():
    assert MODEL_GENERATIONS["t4"][2].grade == codes.CONFIRMED
    assert MODEL_GENERATIONS["d4sh"][2].grade == codes.CONFIRMED
    assert MODEL_GENERATIONS["t4"][3].grade == codes.INFERRED
    assert MODEL_GENERATIONS["d4h"][2].grade == codes.INFERRED


def test_only_a_listed_generation_is_valid():
    assert valid_generation("t4", 2) == 2
    assert valid_generation("t4", "3") == 3
    assert valid_generation("d4sh", 3) is None
    assert valid_generation("t4", True) is None
    assert valid_generation("t4", 2.0) is None
    assert valid_generation("t4", "two") is None
    assert valid_generation("t4", None) is None
    assert valid_generation("t5", 1) is None
    assert generation_choices("t5") == {}
    assert generation_choices("d4sh") == {1: "YumShare Dual-Hopper", 2: "YumShare Dual-Hopper 2"}


def test_hardware_names_a_generation_only_where_that_is_known():
    assert generation_from_hardware("d4sh", 2) == 2
    # Graded unverified: seen, never acted on.
    assert generation_from_hardware("d4sh", 1) is None
    assert generation_from_hardware("d4h", 2) is None
    # The Pura Max 2 reports hardware 1: on a T4 the field is not the generation.
    assert generation_from_hardware("t4", 1) is None
    assert generation_from_hardware("d4sh", "2") is None
    assert generation_from_hardware("d4sh", True) is None


# --- the device --------------------------------------------------------------


def test_the_owner_outranks_typecode_which_outranks_hardware():
    d = Device(device_type="d4sh", petkit_id=1)
    assert d.generation_and_source == (None, "")
    assert d.model_name == "YumShare Dual-Hopper"
    assert d.record_generation("hardware", 2)
    assert d.generation_and_source == (2, "hardware")
    assert d.record_generation("typeCode", 1)
    assert d.generation_and_source == (1, "typeCode")
    d.config[GENERATION_KEY] = 2
    assert d.generation_and_source == (2, "owner")
    assert d.model_name == "YumShare Dual-Hopper 2"
    # Learning never touches the owner's choice.
    assert d.record_generation("typeCode", 2)
    assert d.config[GENERATION_KEY] == 2


def test_a_stored_value_the_table_does_not_list_is_ignored():
    d = Device(device_type="t4", petkit_id=1)
    d.config[GENERATION_KEY] = 9
    d.config[GENERATION_LEARNED_KEY] = {"typeCode": "x", "hardware": [2]}
    assert d.generation is None and d.model_name == "Pura Max"
    d.config[GENERATION_LEARNED_KEY] = "garbage"
    assert d.generation is None


def test_learning_the_same_value_or_a_bad_one_changes_nothing():
    d = Device(device_type="t4", petkit_id=1)
    assert d.record_generation("typeCode", 2)
    assert not d.record_generation("typeCode", 2)
    assert not d.record_generation("typeCode", 7)
    assert not d.record_generation("typeCode", None)
    assert not d.record_generation("somewhere", 3)
    assert d.config[GENERATION_LEARNED_KEY] == {"typeCode": 2}


def test_the_generation_survives_a_restart(tmp_path: Path):
    path = tmp_path / "devices.json"
    reg = DeviceRegistry(path)
    d = reg.get_or_create(petkit_id=1, device_type="t4", serial_number="SN1")
    d.record_generation("typeCode", 2)
    d.config[GENERATION_KEY] = 3
    reg.save()
    again = DeviceRegistry(path).get(1)
    assert again.generation_and_source == (3, "owner")
    assert again.config[GENERATION_LEARNED_KEY] == {"typeCode": 2}


def test_a_gen_two_yumshare_supports_recognition_up_front():
    for code in ("d4sh", "d4h"):
        d = Device(device_type=code, petkit_id=1)
        assert d.supports_ai is False
        d.config[GENERATION_KEY] = 2
        assert d.supports_ai is True, code
        d.config[GENERATION_KEY] = 1
        assert d.supports_ai is False, code
    # A generation says nothing about a model whose codename already does.
    t4 = Device(device_type="t4", petkit_id=1)
    t4.config[GENERATION_KEY] = 2
    assert t4.supports_ai is False


# --- the device's own hardware -------------------------------------------------


def test_a_d4sh_state_snapshot_teaches_its_generation():
    d = Device(device_type="d4sh", petkit_id=1)
    assert apply_state_snapshot(d, {"hardware": 2, "firmware": "867"})
    assert d.generation_and_source == (2, "hardware")
    assert d.model_name == "YumShare Dual-Hopper 2"


def test_a_t4_hardware_one_teaches_nothing():
    d = Device(device_type="t4", petkit_id=1)
    apply_state_snapshot(d, {"hardware": 1})
    assert d.generation is None and GENERATION_LEARNED_KEY not in d.config


async def test_an_http_state_report_teaches_a_d4sh_its_generation():
    reg = DeviceRegistry()
    app = create_app(reg, {"api_url": "http://server/6/", "mqtt_port": 1883,
                           "proxy_mode": False, "proxy_upstream": ""})
    client = TestClient(TestServer(app))
    await client.start_server()
    hdr = {"X-Device": "id=100&sn=SN100"}
    try:
        await client.post("/6/d4sh/dev_signup", headers=hdr)
        r = await client.post("/6/d4sh/dev_state_report", headers=hdr,
                              data=json.dumps({"hardware": 2, "food1": 2}))
        assert r.status == 200
        assert reg.get(100).generation_and_source == (2, "hardware")
    finally:
        await client.close()


# --- proxy mode: the cloud's typeCode ------------------------------------------

T4_HDR = {"X-Device": "id=100&sn=SN100&type=T4"}


class _FakePublisher:
    def __init__(self):
        self.states = []

    async def publish_state(self, device):
        self.states.append(device.petkit_id)


async def _proxied(result, *, proxy=True):
    async def cloud(request):
        return web.json_response({"result": result})

    capp = web.Application()
    capp.router.add_route("*", "/{path:.*}", cloud)
    up = TestClient(TestServer(capp))
    await up.start_server()

    config = {
        "api_url": "http://192.0.2.199:8080/6/", "mqtt_port": 1883,
        "bucket_endpoint": "https://192.0.2.199:9000", "data_dir": "/tmp",
        "capture": False, "capture_dir": "/tmp/capture", "proxy_mode": False,
        "proxy_upstream": str(up.make_url("")).rstrip("/"), "proxy_block_run_cmd": True,
        "proxy_block_ota": True, "proxy_media_real_oss": False,
    }
    registry = DeviceRegistry()
    app = create_app(registry, config)
    app[device_keys.EVENT_HUB] = EventHub()
    publisher = _FakePublisher()
    app[device_keys.HA_PUBLISHER] = publisher
    client = TestClient(TestServer(app))
    await client.start_server()
    assert (await client.post("/6/t4/dev_signup", headers=T4_HDR)).status == 200
    client.app[device_keys.CONFIG]["proxy_mode"] = proxy
    return up, client, registry, publisher


async def _close(up, client):
    await close_proxy_session(client.app)
    await client.close()
    await up.close()


async def test_a_proxied_device_info_teaches_the_generation_and_relays_it_unchanged():
    up, client, registry, publisher = await _proxied({"id": 100, "typeCode": 2, "settings": {}})
    try:
        r = await client.post("/6/t4/dev_device_info", headers=T4_HDR)
        assert (await r.json())["result"]["typeCode"] == 2
        device = registry.get(100)
        assert device.generation_and_source == (2, "typeCode")
        assert device.model_name == "Pura Max 2"
        # Published, so Home Assistant's device is renamed (`publish_state`).
        assert publisher.states == [100]
        # A repeat changes nothing and publishes nothing.
        await client.post("/6/t4/dev_device_info", headers=T4_HDR)
        assert publisher.states == [100]
    finally:
        await _close(up, client)


async def test_nothing_is_learned_with_proxy_mode_off():
    up, client, registry, publisher = await _proxied({"id": 100, "typeCode": 2}, proxy=False)
    try:
        await client.post("/6/t4/dev_device_info", headers=T4_HDR)
        assert GENERATION_LEARNED_KEY not in registry.get(100).config
        assert registry.get(100).generation is None
        assert publisher.states == []
    finally:
        await _close(up, client)


async def test_the_owners_choice_wins_over_the_cloud():
    up, client, registry, _ = await _proxied({"id": 100, "typeCode": 2})
    try:
        registry.get(100).config[GENERATION_KEY] = 3
        await client.post("/6/t4/dev_device_info", headers=T4_HDR)
        device = registry.get(100)
        assert device.config[GENERATION_KEY] == 3
        assert device.config[GENERATION_LEARNED_KEY] == {"typeCode": 2}
        assert device.model_name == "Pura Max 3"
    finally:
        await _close(up, client)


async def test_a_typecode_the_table_does_not_name_is_not_stored():
    up, client, registry, publisher = await _proxied({"id": 100, "typeCode": 7})
    try:
        r = await client.post("/6/t4/dev_device_info", headers=T4_HDR)
        assert (await r.json())["result"]["typeCode"] == 7
        assert GENERATION_LEARNED_KEY not in registry.get(100).config
        assert publisher.states == []
    finally:
        await _close(up, client)


def test_learn_generation_ignores_anything_but_a_result_object():
    d = Device(device_type="t4", petkit_id=1)
    assert not learn_generation(d, None)
    assert not learn_generation(d, [1, 2])
    assert not learn_generation(d, {"settings": {}})
    assert learn_generation(d, {"typeCode": 2})


def test_an_imported_device_info_teaches_the_generation():
    reg = DeviceRegistry()
    device, summary = import_device(reg, BLERegistry(), {
        "type": "t4", "dev_device_info": {"result": {"id": 300000001, "typeCode": 2}}})
    assert device.generation_and_source == (2, "typeCode")
    assert "model (Pura Max 2)" in summary["imported"]


# --- Home Assistant ------------------------------------------------------------


def test_the_ha_device_model_is_the_product_name():
    payload = build_discovery_payload(LITTER_SENSORS[0], 1, "t4", "PetKit T4 SN", "SN",
                                      "petkit-local/1/state", model="Pura Max 2")
    assert payload["device"]["model"] == "Pura Max 2"
    # Without one, what every device showed before.
    payload = build_discovery_payload(LITTER_SENSORS[0], 1, "t4", "PetKit T4 SN", "SN",
                                      "petkit-local/1/state")
    assert payload["device"]["model"] == "T4"


def _models(client: FakeMqttClient) -> set[str]:
    return {json.loads(p)["device"]["model"] for t, p, _ in client.published
            if t.endswith("/config") and p}


async def test_a_learned_generation_renames_the_ha_device_on_the_next_state_publish():
    reg = DeviceRegistry()
    dev = reg.get_or_create(petkit_id=1, device_type="t4", serial_number="SN")
    pub = HAPublisher(reg, {})
    pub._client = FakeMqttClient()
    pub._connected = True
    await pub.publish_discovery(dev)
    assert _models(pub._client) == {"Pura Max"}

    pub._client.published.clear()
    await pub.publish_state(dev)
    assert _models(pub._client) == set(), "no re-announce while nothing changed"

    dev.record_generation("typeCode", 2)
    await pub.publish_state(dev)
    assert _models(pub._client) == {"Pura Max 2"}

    pub._client.published.clear()
    await pub.publish_state(dev)
    assert _models(pub._client) == set()


async def test_a_codename_with_no_product_name_keeps_its_codename_as_ha_model():
    reg = DeviceRegistry()
    dev = reg.get_or_create(petkit_id=1, device_type="zz9", serial_number="SN")
    pub = HAPublisher(reg, {})
    pub._client = FakeMqttClient()
    pub._connected = True
    await pub.publish_discovery(dev)
    assert _models(pub._client) <= {"ZZ9"}


# --- the app API -----------------------------------------------------------------


def test_the_app_api_names_the_generation_and_keeps_model():
    d = Device(device_type="t4", petkit_id=1)
    s = serializers.device_summary(d)
    assert (s["model"], s["model_name"], s["generation"]) == ("Pura Max", "Pura Max", None)
    d.record_generation("typeCode", 2)
    s = serializers.device_summary(d)
    assert (s["model"], s["model_name"], s["generation"]) == ("Pura Max", "Pura Max 2", 2)
