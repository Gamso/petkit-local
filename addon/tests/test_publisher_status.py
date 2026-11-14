"""The add-on's own availability in Home Assistant (`discovery.STATUS_TOPIC`).

Every `petkit-local/{id}/availability` is retained, and only this process ever
updates it. So when the add-on stopped or crashed they all stayed `online` and
HA went on showing live-looking entities over state nobody was refreshing.
"""
import asyncio
import json
import pathlib
import sys
import types

from petkit_local.devices.ble import BLEDevice, BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.ha.discovery import STATUS_TOPIC
from petkit_local.ha.publisher import HAPublisher
from tests._fakes import FakeMqttClient

LIFECYCLE_PY = (pathlib.Path(__file__).resolve().parent.parent
                / "petkit_local" / "main" / "lifecycle.py")


def _connected_publisher(ble=None):
    reg = DeviceRegistry()
    dev = reg.get_or_create(petkit_id=1, device_type="t5", serial_number="SN")
    pub = HAPublisher(reg, {}, ble_registry=ble)
    pub._client = FakeMqttClient()
    pub._connected = True
    return reg, dev, pub


def _configs(pub):
    return [json.loads(p) for t, p, _ in pub._client.published if t.endswith("/config") and p]


def _assert_gated_on_status(cfg, own_topic):
    entry = {"payload_available": "online", "payload_not_available": "offline"}
    assert cfg["availability"] == [{"topic": own_topic, **entry},
                                   {"topic": STATUS_TOPIC, **entry}]
    # HA's default, `latest`, lets the device topic outvote a dead add-on.
    assert cfg["availability_mode"] == "all"
    # `availability` and `availability_topic` are mutually exclusive in HA's
    # schema; sending both gets the whole config rejected without a word.
    assert "availability_topic" not in cfg


async def test_every_device_entity_needs_the_addon_and_the_device_online():
    _, dev, pub = _connected_publisher()
    await pub.publish_discovery(dev)
    configs = _configs(pub)
    assert configs
    for cfg in configs:
        _assert_gated_on_status(cfg, "petkit-local/1/availability")


async def test_pet_and_ble_entities_are_gated_the_same_way():
    _, _, pub = _connected_publisher(BLERegistry())
    await pub.publish_pet_discovery({"id": 3, "name": "Mruczek"})
    assert _configs(pub)
    for cfg in _configs(pub):
        _assert_gated_on_status(cfg, "petkit-local/pet/3/availability")

    pub._client.published.clear()
    await pub.publish_ble_discovery(BLEDevice(ble_type="w5", petkit_id=900))
    assert _configs(pub)
    for cfg in _configs(pub):
        _assert_gated_on_status(cfg, "petkit-local/900/availability")


async def test_per_device_availability_is_published_exactly_as_before():
    _, dev, pub = _connected_publisher()
    dev.online = True
    await pub.publish_availability(dev)
    dev.online = False
    await pub.publish_availability(dev)
    assert pub._client.published == [
        ("petkit-local/1/availability", "online", {"retain": True}),
        ("petkit-local/1/availability", "offline", {"retain": True}),
    ]


async def test_stop_says_offline_retained():
    _, _, pub = _connected_publisher()
    await pub.stop()
    assert pub._client.published == [(STATUS_TOPIC, "offline", {"qos": 1, "retain": True})]


async def test_stop_without_a_connection_does_nothing_and_a_dead_broker_cannot_raise():
    _, _, pub = _connected_publisher()
    pub._connected = False
    await pub.stop()
    assert pub._client.published == []

    class Dead:
        async def publish(self, *a, **kw):
            raise RuntimeError("broker gone")

    pub._client, pub._connected = Dead(), True
    await pub.stop()


async def test_stop_does_not_hold_shutdown_open_on_a_silent_broker(monkeypatch):
    from petkit_local.ha import publisher as publisher_module

    monkeypatch.setattr(publisher_module, "STOP_PUBLISH_TIMEOUT_SECONDS", 0.05)
    _, _, pub = _connected_publisher()

    class Silent:
        async def publish(self, *a, **kw):
            await asyncio.Event().wait()

    pub._client = Silent()
    await asyncio.wait_for(pub.stop(), timeout=2)


async def test_start_registers_a_will_and_says_online_before_discovery():
    reg, dev, pub = _connected_publisher()
    pub._client, pub._connected = None, False
    pub._host = "ha-broker"
    connects, published = [], []
    ready = asyncio.Event()

    class FakeClient:
        def __init__(self, **kwargs):
            connects.append(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def subscribe(self, topic):
            pass

        async def publish(self, topic, payload, **kw):
            published.append((topic, payload, kw))

        @property
        def messages(self):
            return self._messages()

        async def _messages(self):
            ready.set()
            await asyncio.Event().wait()
            yield  # pragma: no cover

    fake = types.ModuleType("aiomqtt")
    fake.Client = FakeClient
    fake.MqttError = type("MqttError", (Exception,), {})
    fake.Will = lambda topic, payload, qos=0, retain=False: {
        "topic": topic, "payload": payload, "qos": qos, "retain": retain}

    real = sys.modules.get("aiomqtt")
    sys.modules["aiomqtt"] = fake
    try:
        task = asyncio.create_task(pub.start())
        await asyncio.wait_for(ready.wait(), timeout=5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    finally:
        if real is None:
            sys.modules.pop("aiomqtt", None)
        else:
            sys.modules["aiomqtt"] = real

    assert connects[0]["will"] == {"topic": STATUS_TOPIC, "payload": "offline",
                                   "qos": 1, "retain": True}
    assert published[0] == (STATUS_TOPIC, "online", {"retain": True})
    assert any(t.endswith("/config") for t, _, _ in published[1:])


def test_the_real_aiomqtt_will_takes_these_arguments():
    """The fakes above would accept anything; the library has to as well."""
    import aiomqtt

    will = aiomqtt.Will(STATUS_TOPIC, "offline", qos=1, retain=True)
    assert (will.topic, will.payload, will.qos, will.retain) == (STATUS_TOPIC, "offline", 1, True)


def test_shutdown_says_offline_before_the_publisher_task_is_cancelled():
    """Cancelling the task disconnects cleanly, which publishes no will — so
    the explicit `offline` has to go out while the connection still exists."""
    body = LIFECYCLE_PY.read_text().split("async def cleanup_background(", 1)[1]
    body = body.split('"""')[2]
    assert body.index("ha_publisher.stop()") < body.index("await _stop_tasks(")
