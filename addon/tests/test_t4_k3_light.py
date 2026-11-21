"""A T4's switch for the light of its linked K3 (issue #133).

What is held here:

* `k3_light` is published only on a T4 that holds a K3 -- by its own `k3Id`,
  or, before it has reported, by the `k3Config` a linked K3 left in settings;
* its value is the T4's own `k3LightSwitch`, from either transport;
* ON is `thing.service.start {"start_action":7}` and OFF
  `thing.service.end {"end_action":7}`, in the envelope every other litter
  start/end is sent in, and neither writes a setting;
* Home Assistant is re-announced when the K3 comes or goes, and the switch is
  cleared when it goes;
* the phone app sees it in the device detail and switches it through the
  generic entity route.
"""
from __future__ import annotations

import json

from petkit_local.devices.base import Device, Refused
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.devices.state_parsers import normalize_property_params, parse_state_report
from petkit_local.events import codes
from petkit_local.ha.categories import get_entities_for_device, get_setting_fields, has_linked_k3
from petkit_local.ha.commands import handle_ha_command
from petkit_local.ha.discovery import build_discovery_payload
from petkit_local.ha.publisher import HAPublisher
from petkit_local.ha.state_doc import entity_state_doc
from tests._fakes import FakeMqttClient
from tests.test_app_api_writes import WriteEnv, w  # noqa: F401 - `w` is a fixture

import pytest


def _t4(**state) -> Device:
    d = Device(petkit_id=10, device_type="t4", serial_number="SN10")
    d.state.update(state)
    return d


def _k3_light(device: Device):
    return next((e for e in get_entities_for_device(device) if e.key == "k3_light"), None)


# --- which devices publish it ----------------------------------------------------

def test_a_t4_with_a_linked_k3_publishes_the_switch_last():
    entities = get_entities_for_device(_t4(k3Id=555))
    assert entities[-1].key == "k3_light"
    assert entities[-1].component == "switch"
    assert entities[-1].value_path == "state.k3LightSwitch"


@pytest.mark.parametrize("state", [{}, {"k3Id": 0}, {"k3Id": "junk"}, {"k3Id": None}])
def test_a_t4_without_a_k3_does_not(state):
    assert _k3_light(_t4(**state)) is None


@pytest.mark.parametrize("device_type", ["t3", "t5", "t6", "d4sh", "w7h"])
def test_no_other_model_publishes_it_even_reporting_a_k3(device_type):
    d = Device(petkit_id=11, device_type=device_type, serial_number="SN11")
    d.state["k3Id"] = 555
    assert _k3_light(d) is None


def test_before_its_first_report_a_t4_is_judged_by_the_k3_config_a_link_left():
    d = _t4()
    d.config.setdefault("settings", {})["k3Config"] = {"config": {"singleLightTime": 120}}
    assert has_linked_k3(d) and _k3_light(d) is not None
    # Once the device has spoken it decides, even against a stale config.
    d.state["k3Id"] = 0
    assert not has_linked_k3(d) and _k3_light(d) is None


def test_the_rest_of_a_t4s_list_does_not_move_when_the_k3_comes():
    without = [e.key for e in get_entities_for_device(_t4(k3Id=0))]
    with_k3 = [e.key for e in get_entities_for_device(_t4(k3Id=555))]
    assert with_k3 == [*without, "k3_light"]


def test_its_field_is_never_routed_into_settings():
    assert "k3LightSwitch" not in get_setting_fields(_t4(k3Id=555))


# --- its state ---------------------------------------------------------------------

# The shape of the T4's property post in the 2026-11-21 capture, after ON.
T4_PROPERTY_POST = {
    "litter": {"weight": 3119, "usedTimes": 0, "percent": 100, "sandType": 1},
    "device": {"sw": 1, "pet_in_time": 0, "k3LightSwitch": 1},
    "k3Id": 555, "battery": 90, "liquid": 50,
}


def test_the_mqtt_property_post_flattens_the_light_and_the_k3_id():
    flat = normalize_property_params("t4", T4_PROPERTY_POST)
    assert flat["k3LightSwitch"] == 1
    assert flat["k3Id"] == 555


def test_the_http_state_report_does_too():
    flat = parse_state_report("t4", T4_PROPERTY_POST)
    assert flat["k3LightSwitch"] == 1
    assert flat["k3Id"] == 555


@pytest.mark.parametrize("raw, expected", [(0, 0), (1, 1), ("1", 1), (True, 1)])
def test_the_light_is_coerced_to_0_or_1(raw, expected):
    flat = normalize_property_params("t4", {"device": {"k3LightSwitch": raw}})
    assert flat["k3LightSwitch"] == expected


@pytest.mark.parametrize("raw", ["on?", None, [1], {"x": 1}, 7])
def test_an_unreadable_light_is_skipped_not_raised_on(raw):
    flat = normalize_property_params("t4", {"device": {"k3LightSwitch": raw},
                                            "k3Id": {"bad": 1}})
    assert "k3LightSwitch" not in flat and "k3Id" not in flat


def test_the_switch_reads_the_reported_light():
    d = _t4(k3Id=555)
    d.state.update(normalize_property_params("t4", T4_PROPERTY_POST))
    entity = _k3_light(d)
    doc = entity_state_doc(d)
    assert doc["state"]["k3LightSwitch"] == 1
    payload = build_discovery_payload(entity=entity, device_id=10, device_type="t4",
                                      device_name="PetKit T4 SN10", serial_number="SN10",
                                      state_topic="petkit-local/10/state")
    assert "value_json.state.k3LightSwitch" in payload["value_template"]


# --- its commands ------------------------------------------------------------------

def test_on_is_start_action_7():
    d = _t4(k3Id=555, k3LightSwitch=0)
    suffix, envelope = handle_ha_command(d, _k3_light(d), "ON")
    assert suffix == "start"
    assert envelope["method"] == "thing.service.start"
    assert envelope["params"] == {"start_action": 7}
    assert envelope["version"] == "1.0.0" and envelope["id"].isdigit()
    assert set(envelope) == {"method", "id", "params", "version"}
    assert d.state["k3LightSwitch"] == 1, "optimistic, as a settings write is"


def test_off_is_end_action_7():
    d = _t4(k3Id=555, k3LightSwitch=1)
    suffix, envelope = handle_ha_command(d, _k3_light(d), "OFF")
    assert suffix == "end"
    assert envelope["method"] == "thing.service.end"
    assert envelope["params"] == {"end_action": 7}
    assert set(envelope) == {"method", "id", "params", "version"}
    assert d.state["k3LightSwitch"] == 0


def test_neither_writes_a_setting():
    d = _t4(k3Id=555)
    before = json.dumps(d.config, sort_keys=True)
    handle_ha_command(d, _k3_light(d), "ON")
    handle_ha_command(d, _k3_light(d), "OFF")
    assert json.dumps(d.config, sort_keys=True) == before


def test_it_is_the_graded_code():
    assert codes.T4_K3_LIGHT_ACTION == 7
    assert codes.T4_K3_LIGHT_COMMANDS["start_action"][1] == codes.CONFIRMED
    # The OFF echo was never captured.
    assert codes.T4_K3_LIGHT_COMMANDS["end_action"][1] == codes.INFERRED


def test_only_a_t4_is_sent_it():
    entity = _k3_light(_t4(k3Id=555))
    d = Device(petkit_id=12, device_type="t5", serial_number="SN12")
    with pytest.raises(Refused):
        handle_ha_command(d, entity, "ON")


# --- Home Assistant follows the link -----------------------------------------------

def _configs(client) -> dict[str, str]:
    return {topic: payload for topic, payload, _ in client.published
            if topic.endswith("/config") and "k3_light" in topic}


async def test_ha_is_reannounced_when_a_k3_comes_and_cleared_when_it_goes():
    reg = DeviceRegistry()
    dev = reg.get_or_create(petkit_id=10, device_type="t4", serial_number="SN10")
    pub = HAPublisher(reg, {})
    pub._client = FakeMqttClient()
    pub._connected = True
    await pub.publish_discovery(dev)
    assert _configs(pub._client) == {}

    dev.state["k3Id"] = 555
    await pub.publish_state(dev)
    announced = _configs(pub._client)
    assert len(announced) == 1 and json.loads(next(iter(announced.values())))["name"]

    pub._client.published.clear()
    await pub.publish_state(dev)
    assert _configs(pub._client) == {}, "no re-announce while nothing changed"

    dev.state["k3Id"] = 0
    await pub.publish_state(dev)
    assert list(_configs(pub._client).values()) == [""], "cleared, retained-empty"


# --- the phone app ------------------------------------------------------------------

def _app_t4(w: WriteEnv, **state) -> Device:  # noqa: F811
    d = w.registry.get_or_create(petkit_id=300, device_type="t4", serial_number="SN300")
    d.state.update(state)
    return d


async def test_the_app_lists_the_switch_with_its_value(w: WriteEnv):  # noqa: F811
    _app_t4(w, k3Id=555, k3LightSwitch=1)
    detail = (await (await w.call("GET", "/devices/300")).json())["device"]
    entity = next(e for e in detail["entities"] if e["key"] == "k3_light")
    assert entity["component"] == "switch"
    assert entity["value"] is True and entity["settable"] is True
    # A switch, not an action: the actions list stays buttons.
    assert "k3_light" not in {a["key"] for a in detail["actions"]}


async def test_the_app_does_not_list_it_without_a_k3(w: WriteEnv):  # noqa: F811
    _app_t4(w, k3Id=0)
    detail = (await (await w.call("GET", "/devices/300")).json())["device"]
    assert "k3_light" not in {e["key"] for e in detail["entities"]}
    r = await w.call("PUT", "/devices/300/entities/k3_light", {"value": True})
    assert r.status == 404


async def test_the_app_switches_it_through_the_entity_route(w: WriteEnv):  # noqa: F811
    d = _app_t4(w, k3Id=555, k3LightSwitch=0)
    r = await w.call("PUT", "/devices/300/entities/k3_light", {"value": True})
    body = await r.json()
    assert r.status == 200, body
    assert body["delivered"] == "queued" and body["entity"]["value"] is True
    on = d.command_queue[-1]
    assert on["method"] == "thing.service.start" and on["params"] == {"start_action": 7}
    assert on["_service_suffix"] == "start"

    d.mqtt_connected = True
    r = await w.call("PUT", "/devices/300/entities/k3_light", {"value": False})
    body = await r.json()
    assert body["delivered"] == "mqtt" and body["entity"]["value"] is False
    device_id, suffix, off = w.sink.published[-1]
    assert (device_id, suffix) == (300, "end")
    assert off["method"] == "thing.service.end" and off["params"] == {"end_action": 7}

    r = await w.call("PUT", "/devices/300/entities/k3_light", {"value": 1})
    assert r.status == 422, "a switch takes a boolean"
