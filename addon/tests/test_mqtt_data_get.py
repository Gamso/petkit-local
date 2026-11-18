"""The answer to an MQTT `data_get`, held to the real cloud's own.

A device on MQTT asks for its configuration with `thing/event/data_get/post`
and is answered on `/{pk}/{dn}/user/get`. We used to publish the HTTP body
there — `{"result": ...}` — where the real cloud, captured answering a T4
(1.652) on 2026-11-17, sends `{"msgType": 0, "type": "t4_data_get",
"timestamp": ..., "payload": {"dataType": ..., ...}}`. The capture wins on
shape, so every test here is built from those frames
(`fixtures/t4_ble_relay_and_data_get.json`): the T4's own requests are replayed
into the bridge, and what it publishes is compared with what PetKit sent back.
"""
import json
from pathlib import Path

import pytest

from petkit_local.devices import payloads
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events import codes
from petkit_local.mqtt.bridge import MQTTBridge
from tests._fakes import FakeMqttClient

CAPTURE = json.loads((Path(__file__).parent / "fixtures" / "t4_ble_relay_and_data_get.json")
                     .read_text(encoding="utf-8"))["data_get"]

#: The dataTypes the capture has and this add-on answers over MQTT.
ANSWERED = ("dev_device_info", "dev_schedule_get", "dev_ble_device",
            "dev_multi_config", "dev_serverinfo")


def _cloud(data_type):
    return CAPTURE[data_type]["reply"]


def _t4():
    """The captured T4, configured as the cloud had it, with its CTW3 paired."""
    reg = DeviceRegistry()
    t4 = reg.get_or_create(petkit_id=100000001, device_type="t4",
                           serial_number="20260101T00001")
    # The ranges as the account held them (`multi_config_ranges`' store).
    t4.config["multi_config"] = {
        key: json.loads(value)[key]
        for key, value in _cloud("dev_multi_config")["payload"].items() if key != "dataType"}
    t4.config["schedule"] = _cloud("dev_schedule_get")["payload"]["schedule"]
    ble = BLERegistry()
    entry = _cloud("dev_ble_device")["payload"]["list"][0]
    ble.register(ble_type="ctw3", petkit_id=entry["id"], mac=entry["mac"],
                 secret=entry["secret"], interval=entry["interval"],
                 link_with=t4.petkit_id)
    bridge = MQTTBridge(reg, None, ble, api_url="http://192.168.1.10/6/")
    bridge._client = FakeMqttClient()
    return bridge, t4


async def _ours(data_type):
    bridge, t4 = _t4()
    await bridge._handle_event(t4, "data_get", CAPTURE[data_type]["request"])
    replies = [(t, p) for t, p, _k in bridge._client.published if t.endswith("/user/get")]
    assert len(replies) == 1, f"expected one user/get answer to {data_type}"
    topic, body = replies[0]
    assert topic == f"/{t4.mqtt_product_key}/{t4.mqtt_device_name}/user/get"
    return json.loads(body)


@pytest.mark.parametrize("data_type", ANSWERED)
async def test_the_envelope_is_the_clouds(data_type):
    ours, cloud = await _ours(data_type), _cloud(data_type)
    assert set(ours) == set(cloud) == {"msgType", "payload", "type", "timestamp"}
    assert ours["msgType"] == cloud["msgType"] == 0
    assert ours["type"] == cloud["type"] == "t4_data_get"
    assert isinstance(ours["timestamp"], int)
    assert ours["payload"]["dataType"] == cloud["payload"]["dataType"] == data_type


@pytest.mark.parametrize("data_type", ANSWERED)
async def test_the_resource_sits_where_the_cloud_puts_it(data_type):
    """Nested under one key for some endpoints, spread beside `dataType` for
    the others — and which is which is the capture's, per endpoint."""
    ours, cloud = (await _ours(data_type))["payload"], _cloud(data_type)["payload"]
    key, grade = payloads.DATA_GET_REPLY_SHAPES[data_type]
    assert grade == codes.CONFIRMED
    if key:
        assert set(ours) == set(cloud) == {"dataType", key}
        assert type(ours[key]) is type(cloud[key])
    else:
        # Every field the cloud sends, we send; we may carry what our HTTP
        # answer carries besides (`dns`, `pimServers` on a serverinfo).
        assert set(cloud) <= set(ours), set(cloud) - set(ours)


async def test_the_relay_list_is_the_clouds_entry_for_entry():
    ours, cloud = (await _ours("dev_ble_device"))["payload"], _cloud("dev_ble_device")["payload"]
    assert ours == cloud


async def test_the_schedule_and_the_ranges_are_the_clouds_value_for_value():
    for data_type in ("dev_schedule_get", "dev_multi_config"):
        assert (await _ours(data_type))["payload"] == _cloud(data_type)["payload"], data_type


async def test_device_info_carries_the_device_under_device():
    ours = (await _ours("dev_device_info"))["payload"]["device"]
    cloud = _cloud("dev_device_info")["payload"]["device"]
    assert ours["id"] == cloud["id"] and ours["sn"] == cloud["sn"]
    assert isinstance(ours["settings"], dict)


def test_every_reply_shape_is_graded_and_matches_the_capture():
    for data_type, (key, grade) in payloads.DATA_GET_REPLY_SHAPES.items():
        assert grade in codes.GRADES, data_type
        if data_type in CAPTURE:
            captured = CAPTURE[data_type]["reply"]["payload"]
            if key:
                assert set(captured) == {"dataType", key}, data_type
            else:
                assert len(set(captured) - {"dataType"}) >= 2, data_type
        else:
            # Not captured: it cannot claim to be.
            assert grade != codes.CONFIRMED, data_type


async def test_the_answer_is_recorded_in_the_live_log():
    from petkit_local.web.hub import EventHub

    bridge, t4 = _t4()
    bridge._hub = EventHub()
    await bridge._handle_event(t4, "data_get", CAPTURE["dev_multi_config"]["request"])
    outbound = [e for e in bridge._hub.recent(kinds={"mqtt"})
                if "user/get" in json.dumps(e)]
    assert outbound, "the reply never reached the panel's log"
