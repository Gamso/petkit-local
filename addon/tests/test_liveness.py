"""The offline watchdog (`devices/liveness.py`), which runs with or without HA.

Held here: MQTT contact counts as contact (a device on the broker stops polling
the HTTP heartbeat); a stale device loses `online` AND `mqtt_connected`, since
that flag picks the transport; the hub gets the `online: False` entry the phone
notifier keys on; Home Assistant is only a listener, and one that fails does
not stop the sweep.
"""
from __future__ import annotations

import asyncio
import time

import pytest

from petkit_local.devices.liveness import default_interval, device_is_stale, offline_watchdog
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.web.hub import EventHub


def test_mqtt_contact_counts_and_a_never_seen_device_is_not_stale():
    registry = DeviceRegistry()
    device = registry.get_or_create(petkit_id=1, device_type="t5")
    now = 10_000.0
    assert not device_is_stale(device, now, 180)
    device.last_seen = now - 1000  # HTTP long silent...
    assert device_is_stale(device, now, 180)
    device.last_mqtt = now - 10  # ...but it is talking on the broker
    assert not device_is_stale(device, now, 180)
    assert default_interval(180) == 30.0 and default_interval(10) == 15.0


async def _sweep(registry, **kw) -> None:
    task = asyncio.create_task(offline_watchdog(registry, 1, interval=0.01, **kw))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def _devices() -> tuple[DeviceRegistry, object, object]:
    registry = DeviceRegistry()
    silent = registry.get_or_create(petkit_id=1, device_type="t5")
    live = registry.get_or_create(petkit_id=2, device_type="t5")
    for device in (silent, live):
        device.online = True
        device.mqtt_connected = True
    silent.last_seen = 1.0
    live.last_mqtt = time.time() + 3600  # fresh for the whole test
    return registry, silent, live


async def test_without_ha_a_silent_device_goes_offline_and_the_hub_is_told():
    registry, silent, live = _devices()
    hub = EventHub()
    await _sweep(registry, hub=hub)
    assert silent.online is False and silent.mqtt_connected is False
    assert live.online is True and live.mqtt_connected is True
    lines = [e for e in hub.recent() if e["kind"] == "connect"]
    assert [(e["device_id"], e["detail"]) for e in lines] == [
        (1, {"online": False, "timeout": 1})]


async def test_ha_is_a_listener_and_a_failing_one_does_not_stop_the_sweep():
    registry, silent, _ = _devices()
    other = registry.get_or_create(petkit_id=3, device_type="d4h")
    other.online = True
    other.last_seen = 1.0
    hub = EventHub()
    told: list[int] = []

    async def publish_availability(device):
        told.append(device.petkit_id)
        raise ConnectionError("HA broker went away")

    await _sweep(registry, hub=hub, on_offline=publish_availability)
    assert sorted(told) == [1, 3]
    assert not silent.online and not other.online
    assert sorted(e["device_id"] for e in hub.recent() if e["kind"] == "connect") == [1, 3]
