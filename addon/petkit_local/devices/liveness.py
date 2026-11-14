"""Whether a device is still there: the stale check and the offline watchdog.

The decision lives here, not in Home Assistant's publisher, because it is not
an HA question. Without it `device.online` is only ever set True (on contact),
`mqtt_connected` stays up for a session that silently died — and that flag
picks the transport, so a stale True swallows every command — and the phone's
"Device offline" notification has nothing to fire on. All of that holds with
`--no-ha` as much as with it. HA is one listener among others: it is handed
the decision through `on_offline` to publish availability, and the event hub
gets a `connect` entry with `online: False` that the ntfy notifier reads.

`mqtt_connected` comes down three ways (AGENTS.md): the broker's disconnect
hook, the heartbeat's `?iotStatus=0`, and this watchdog, the last resort.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:  # pragma: no cover - typing only
    from petkit_local.devices.base import Device
    from petkit_local.devices.registry import DeviceRegistry

log = logging.getLogger(__name__)


class _Hub(Protocol):
    def publish(self, kind: str, device_id: Any = None, summary: str = "",
                detail: Any = None) -> dict: ...


def device_is_stale(device: Device, now: float, timeout: int) -> bool:
    """True if the device hasn't been heard from within `timeout` seconds.

    A freshly-created device that has never reported (last_seen == 0) is not
    considered stale until it has been seen at least once.

    `last_mqtt` counts as contact, and has to: a device that gets onto the
    broker stops polling the HTTP heartbeat altogether, so judging it on HTTP
    alone marks the healthiest devices offline and — worse — clears
    `mqtt_connected` in `offline_watchdog`, sending their commands to a queue
    that is never drained again.
    """
    last_seen = max(device.last_heartbeat, device.last_state_report,
                    device.last_seen, device.last_mqtt)
    if last_seen <= 0:
        return False
    return (now - last_seen) > timeout


def default_interval(timeout: int) -> float:
    """Half the timeout, clamped to 15-30s, so a long `offline_timeout` does
    not make the watchdog itself the source of the reporting lag."""
    return max(15.0, min(30.0, timeout / 2))


async def offline_watchdog(registry: DeviceRegistry, timeout: int, *,
                           hub: _Hub | None = None,
                           on_offline: Callable[[Device], Awaitable[None]] | None = None,
                           interval: float | None = None) -> None:
    """Periodically mark stale devices offline. Runs forever, with or without HA.

    For each device that is `online` and stale: `online` and `mqtt_connected`
    come down, `on_offline(device)` runs (HA's availability publish, when HA
    publishing is on), and the hub gets `connect` with
    `detail={"online": False, "timeout": timeout}` — the one entry that means
    "the watchdog decided", which is what a listener keys on.

    A failing `on_offline` is logged and does not stop the sweep: the decision
    has already been made, and the hub entry and the next devices still need it.
    """
    if interval is None:
        interval = default_interval(timeout)
    while True:
        await asyncio.sleep(interval)
        now = time.time()
        for device in registry.all():
            if not (device.online and device_is_stale(device, now, timeout)):
                continue
            device.online = False
            device.mqtt_connected = False
            log.info("Device %d (%s) marked offline (no contact for >%ds)",
                     device.petkit_id, device.device_type, timeout)
            if on_offline is not None:
                try:
                    await on_offline(device)
                except Exception:  # noqa: BLE001 - one listener must not stop the sweep
                    log.warning("Publishing device %d offline failed", device.petkit_id,
                                exc_info=True)
            if hub is not None:
                hub.publish("connect", device.petkit_id,
                            f"offline (no contact for >{timeout}s)",
                            detail={"online": False, "timeout": timeout})
