"""MQTTBridge <-> EventStore/PetRegistry wiring — the MQTT-transport half of
events/ingest.py (the HTTP half is covered by tests/test_ingest_endpoints.py)."""
import json

import pytest

from petkit_local.ai.pets import PetRegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.mqtt.bridge import MQTTBridge
from petkit_local.web.hub import EventHub


class FakePublisher:
    def __init__(self):
        self.events = []
        self.states = []
        self.pet_discoveries = []
        self.pet_states = []

    async def publish_event(self, device, suffix, event_type, attrs=None):
        self.events.append((device.petkit_id, suffix, event_type, attrs))

    async def publish_state(self, device):
        self.states.append(device.petkit_id)

    async def publish_availability(self, device):
        pass

    async def publish_pet_discovery(self, pet):
        self.pet_discoveries.append(pet["id"])

    async def publish_pet_state(self, pet, store):
        self.pet_states.append(pet["id"])


@pytest.fixture
def setup(event_store, tmp_path):
    """A bridge wired to a real store, whose teardown (`conftest.event_store`)
    closes it: built per test by hand, the stores were never closed, and their
    worker threads failed later against a closed loop."""
    reg = DeviceRegistry()
    dev = reg.get_or_create(petkit_id=10, device_type="t5", serial_number="SN")
    pub = FakePublisher()
    hub = EventHub()
    pet_registry = PetRegistry(event_store, str(tmp_path / "faces"))
    bridge = MQTTBridge(reg, pub, event_store=event_store, hub=hub, pet_registry=pet_registry)
    return reg, dev, pub, event_store, hub, pet_registry, bridge


async def test_pet_out_event_is_persisted_to_store(setup):
    reg, dev, pub, store, hub, pet_registry, bridge = setup
    # `params.event_id` is the episode key over MQTT exactly as over HTTP
    # (live T5 capture); `content.related_event` is the cross-episode link.
    await bridge._handle_event(dev, "pet_out", {
        "params": {
            "event_id": "10000001_1785276736",
            "content": json.dumps({"related_event": "r1", "pet_weight": 4200}),
        }
    })
    rows = await store.query_timeline(device_id=10)
    assert len(rows) == 1
    assert rows[0]["event_type"] == "pet_out"
    assert rows[0]["related_event"] == "10000001_1785276736"
    assert rows[0]["parent_event"] == "r1"


async def test_a_retried_mqtt_event_counts_and_fires_once(setup):
    """The same `event_id` + topic twice is one event retried: the store
    updates its row in place, and neither the feeder's daily totals nor the
    HA event entity may see it a second time."""
    reg, dev, pub, store, hub, pet_registry, bridge = setup
    feeder = reg.get_or_create(petkit_id=11, device_type="d4sh", serial_number="SN11")
    msg = {"params": {"event_id": "11_1785276736",
                      "content": json.dumps({"day": 20261023, "real_amount1": 10,
                                             "real_amount2": 5})}}
    for _ in range(2):
        await bridge._handle_event(feeder, "feed_over", msg)
    assert len(await store.query_timeline(device_id=11)) == 1
    assert feeder.state["feedState"]["times"] == 1
    assert feeder.state["feedState"]["realAmountTotal"] == 15
    assert [e[2] for e in pub.events] == ["feed_over"]


async def test_a_feed_echo_keeps_the_stamp_and_the_one_off_feeds(setup):
    """A feeder posting its schedule back must not replace ours wholesale:
    the deferred feeds live only here, and without `v: 2` a schedule whose
    meals all fall before 00:24 was read as a 2.0.x minute save on the next
    `dev_feed_get` and had every time multiplied by 60."""
    from petkit_local.http.handlers.feed import migrate_minute_schedule

    reg, dev, pub, store, hub, pet_registry, bridge = setup
    feeder = reg.get_or_create(petkit_id=11, device_type="d4sh", serial_number="SN11")
    deferred = [{"id": "d_20261024_28800", "t": 28800, "a1": 1, "a2": 0}]
    feeder.config["feed_schedule"] = {"schedule": [], "v": 2, "deferred": deferred}
    echo = {"schedule": [{"re": "1,2,3,4,5,6,7",
                          "it": [{"id": "n_600", "t": 600, "a1": 1, "a2": 1}]}]}
    await bridge._handle_event(feeder, "property", {"params": {"feed": echo}})
    stored = feeder.config["feed_schedule"]
    assert stored["schedule"] == echo["schedule"]
    assert stored["deferred"] == deferred
    assert migrate_minute_schedule(stored) is False
    assert stored["schedule"][0]["it"][0]["t"] == 600

    # Not a schedule at all: what is stored stays.
    await bridge._handle_event(feeder, "property", {"params": {"feed": {"x": 1}}})
    assert feeder.config["feed_schedule"] is stored


async def test_a_device_cannot_plant_one_off_feeds_through_its_feed_echo(setup):
    """`deferred` is ours: the panel writes it and renders it. The broker
    accepts any CONNECT by default, so a `feed` post is LAN input — and one
    carrying its own `deferred` used to be stored whole, then listed by the
    panel's deferred-feed endpoint as if the user had scheduled it."""
    from aiohttp.test_utils import TestClient, TestServer

    from petkit_local.devices.ble import BLERegistry
    from petkit_local.web.panel import create_panel_app

    reg, dev, pub, store, hub, pet_registry, bridge = setup
    feeder = reg.get_or_create(petkit_id=11, device_type="d4sh", serial_number="SN11")
    markup = "<img src=x onerror=alert(1)>"
    hostile = {"schedule": [{"re": "1", "it": [{"id": "n_600", "t": 600, "a1": 1, "a2": 1}]}],
               "deferred": [{"id": "d_1", "fire_at": 1, "t": 1, "a1": markup, "a2": markup}],
               "anything": markup}
    await bridge._handle_event(feeder, "property", {"params": {"feed": hostile}})
    assert feeder.config["feed_schedule"] == {"schedule": hostile["schedule"], "v": 2}

    c = TestClient(TestServer(create_panel_app(reg, BLERegistry(), hub, {})))
    await c.start_server()
    try:
        listed = await (await c.get("/api/devices/11/deferred-feed")).json()
        assert listed == {"deferred": []}
    finally:
        await c.close()


@pytest.mark.parametrize("schedule", [
    [1, 2],                                  # groups that are not objects
    ["<img src=x onerror=alert(1)>"],
    [{"re": "1", "it": "not a list"}],
    [{"re": "1", "it": [600]}],              # meals that are not objects
])
async def test_a_feed_echo_of_the_wrong_shape_is_not_stored(setup, schedule):
    """`dev_feed_get` and the panel's editor index straight into the groups and
    their meals; a list of anything else must never get that far."""
    reg, dev, pub, store, hub, pet_registry, bridge = setup
    feeder = reg.get_or_create(petkit_id=11, device_type="d4sh", serial_number="SN11")
    await bridge._handle_event(feeder, "property", {"params": {"feed": {"schedule": schedule}}})
    assert "feed_schedule" not in feeder.config


async def test_property_event_is_not_persisted(setup):
    reg, dev, pub, store, hub, pet_registry, bridge = setup
    await bridge._handle_event(dev, "property", {"params": {"sandPercent": 40}})
    assert await store.query_timeline(device_id=10) == []


async def test_event_with_pet_id_triggers_pet_publish(setup):
    reg, dev, pub, store, hub, pet_registry, bridge = setup
    pet = await pet_registry.create("Mruczek")
    await bridge._handle_event(dev, "pet_out", {
        "params": {"content": json.dumps({"petId": pet["id"], "pet_weight": 4200})}
    })
    assert pub.pet_discoveries == [pet["id"]]
    assert pub.pet_states == [pet["id"]]


async def test_event_without_pet_id_does_not_touch_pets(setup):
    reg, dev, pub, store, hub, pet_registry, bridge = setup
    await bridge._handle_event(dev, "clean_over", {"params": {}})
    assert pub.pet_discoveries == []
    assert pub.pet_states == []


async def test_event_persistence_emits_hub_event(setup):
    reg, dev, pub, store, hub, pet_registry, bridge = setup
    await bridge._handle_event(dev, "error_start", {"params": {}})
    kinds = [e["kind"] for e in hub.recent(20)]
    assert "event" in kinds


async def test_the_request_credential_never_lands_in_device_state(setup):
    """Every MQTT `params` carries the transport envelope beside the telemetry —
    on a live T5, all 186 `property` posts included `XDevice`, which is the
    signed request credential (`id=...&nonce=...&sign=...`).

    `device.state` is merged into and never pruned, and the panel renders it
    verbatim under "Raw parsed state JSON", so merging `params` wholesale both
    displayed the credential and kept it forever.
    """
    _, device, _, _, _, _, bridge = setup
    await bridge._handle_event(device, "property", {"params": {
        "XDevice": "id=1&nonce=abc&timestamp=2&type=T5&sign=deadbeef",
        "event_id": "4_1_2", "timestamp": "2",
        "litter": {"percent": 40, "weight": 1200},
        "runtime": 900,
    }})

    assert "XDevice" not in device.state
    assert not [k for k in device.state if "sign=" in str(device.state[k])]
    # The telemetry beside it still applies, or this would be a silent regression.
    assert device.state["sandPercent"] == 40
    assert device.state["totalTime"] == 900


async def test_ha_event_fires_with_content_not_transport_envelope(setup):
    """Everything in the event payload besides `event_type` becomes an HA
    event attribute, so the bridge hands publish_event the decoded CONTENT.
    Passing raw `params` shipped the transport envelope — `XDevice`, the
    signed request credential, plus the full state snapshot — into HA's
    logbook and recorder."""
    reg, dev, pub, store, hub, pet_registry, bridge = setup
    await bridge._handle_event(dev, "clean_over", {
        "params": {
            "XDevice": "id=1&nonce=x&sign=y",
            "event_id": "10000001_1785276736",
            "timestamp": 1785276736,
            "state": json.dumps({"sandPercent": 40}),
            "content": json.dumps({"start_reason": 0, "result": 0}),
        }
    })
    assert pub.events == [
        (10, "cleaning_event", "clean_over", {"start_reason": 0, "result": 0}),
    ]
