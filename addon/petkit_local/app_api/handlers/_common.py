"""Request parsing every app API handler starts with.

Each helper either returns a clean value or raises `ApiError`, which the
middleware renders as the API's JSON refusal. Nothing request-supplied reaches
a handler unparsed.
"""
from __future__ import annotations

import logging
from typing import Any

from aiohttp import web

from petkit_local.app_api import inventory
from petkit_local.app_api.appkeys import (
    BLE_REGISTRY, COMMAND_SINK, HA_PUBLISHER, HUB, REGISTRY, SESSION,
)
from petkit_local.app_api.middleware import ApiError
from petkit_local.devices.base import Device, Refused
from petkit_local.devices.ble import BLEDevice, relay_blocker
from petkit_local.ha.command_router import (
    DELIVERED_MQTT, ble_command_value, build_ble_command, deliver_command, reflect_ble_write,
)
from petkit_local.ha.commands import Command
from petkit_local.ha.discovery import EntityDef
from petkit_local.utils.coerce import to_int

log = logging.getLogger(__name__)

#: `delivered` when a write changed only petkit-local's own state.
DELIVERED_LOCAL = "local"


async def json_body(request: web.Request) -> dict[str, Any]:
    """The request body as a JSON object.

    Raises:
        ApiError: 415 when not declared as JSON, 400 when not a JSON object.
    """
    if request.content_type != "application/json":
        raise ApiError(415, "unsupported_media_type",
                       "send JSON with Content-Type: application/json")
    try:
        body = await request.json()
    except Exception:
        raise ApiError(400, "bad_json", "the body is not valid JSON") from None
    if not isinstance(body, dict):
        raise ApiError(400, "bad_json", "the body must be a JSON object")
    return body


async def optional_json_body(request: web.Request) -> dict[str, Any]:
    """`json_body` for a route whose body may be left out entirely: `{}` then."""
    if not request.body_exists:
        return {}
    return await json_body(request)


def device_or_404(request: web.Request) -> Device:
    """The device the `{id}` segment names.

    Raises:
        ApiError: 404 `device_not_found`.
    """
    device = request.app[REGISTRY].get(path_int(request, "id", "device"))
    if device is None:
        raise ApiError(404, "device_not_found", "no such device")
    return device


def device_or_accessory_or_404(request: web.Request) -> Device | BLEDevice:
    """The device or Bluetooth accessory the `{id}` segment names.

    Only for the routes an accessory has too (`app_api/inventory.py`).

    Raises:
        ApiError: 404 `device_not_found`.
    """
    registry = request.app[REGISTRY]
    node_id = path_int(request, "id", "device")
    found = registry.get(node_id) or inventory.accessory(
        registry, request.app.get(BLE_REGISTRY), node_id)
    if found is None:
        raise ApiError(404, "device_not_found", "no such device")
    return found


async def send_command(request: web.Request, device: Device, command: Command | None,
                       summary: str) -> str:
    """Deliver what a write produced, persist it, and tell everyone who shows it.

    The command (if any) goes the way Home Assistant's do
    (`ha/command_router.py::deliver_command`): published when the device holds
    an MQTT session, queued for its heartbeat otherwise. Then the registry is
    saved, the panel's log is told (as `app: ...`, so a press from the phone
    can be told apart), and Home Assistant gets the optimistic state at once,
    as it does after its own writes.

    Returns:
        `mqtt`, `queued`, or `local` when there was nothing to send.
    """
    if command is None:
        delivered = DELIVERED_LOCAL
    else:
        delivered = await deliver_command(device, *command, request.app.get(COMMAND_SINK))
    request.app[REGISTRY].save()
    session = request.get(SESSION)
    log.info("app API: %s on device %d from %r -> %s", summary, device.petkit_id,
             session.name if session else "?", delivered)
    hub = request.app.get(HUB)
    if hub is not None:
        hub.record_command(device.petkit_id, delivered, f"app: {summary}")
    publisher = request.app.get(HA_PUBLISHER)
    if publisher is not None:
        await publisher.publish_state(device)
    return delivered


async def send_accessory_command(request: web.Request, accessory: BLEDevice,
                                 entity: EntityDef, payload: str, summary: str) -> str:
    """Write one entity of a Bluetooth accessory, through the parent relaying it.

    `payload` is what Home Assistant would have published (`payload_for`), so
    the value becomes the byte a frame carries exactly as HA's does
    (`ha/command_router.py`). Unlike a device's, an accessory's write cannot
    wait in a queue: it is a `thing/service/ble` publish to its parent, which
    exists only while the parent holds an MQTT session. So it is sent now or
    refused, and the answer is always `mqtt`.

    Raises:
        ApiError: 422 `refused` when the frame cannot be built (a block the
            accessory has never reported in full, a field it cannot write),
            422 `on_battery` for a CTW3 mode write while it is on battery;
            409 `accessory_unreachable` when no parent on MQTT can relay it.
    """
    value = ble_command_value(entity, payload)
    if value is None:
        raise ApiError(422, "refused", f"unusable value for {entity.name}")
    parent = inventory.parent_of(request.app[REGISTRY], accessory)
    sink = request.app.get(COMMAND_SINK)
    # The panel's words, from the one place that decides (issue #99).
    blocker = relay_blocker(accessory, parent)
    if blocker:
        raise ApiError(409, "accessory_unreachable", blocker)
    if parent is None:
        raise ApiError(409, "accessory_unreachable",
                       "no registered device relays this accessory")
    if sink is None:
        raise ApiError(409, "accessory_unreachable", "the MQTT bridge is not running")
    try:
        cmd, frame = build_ble_command(accessory, entity, value)
    except Refused as exc:
        # `on_battery` for a CTW3 mode write on battery, `refused` otherwise.
        raise ApiError(422, exc.code, str(exc)) from None
    if not await sink.publish_ble_command(parent, accessory, cmd, frame):
        raise ApiError(409, "accessory_unreachable", "nothing could be sent to it")

    reflect_ble_write(accessory, entity, value)
    ble = request.app.get(BLE_REGISTRY)
    if ble is not None:
        ble.mark_dirty()
    session = request.get(SESSION)
    log.info("app API: %s on accessory %d via %d from %r", summary, accessory.petkit_id,
             parent.petkit_id, session.name if session else "?")
    hub = request.app.get(HUB)
    if hub is not None:
        hub.record_command(accessory.petkit_id, DELIVERED_MQTT, f"app: {summary}")
    publisher = request.app.get(HA_PUBLISHER)
    if publisher is not None:
        await publisher.publish_ble_state(accessory)
    return DELIVERED_MQTT


def path_int(request: web.Request, name: str, what: str) -> int:
    """A numeric path segment. A non-number is a 404: no such `what` can exist."""
    value = to_int(request.match_info.get(name), None)
    if value is None or str(value) != request.match_info.get(name, "").strip():
        raise ApiError(404, f"{what}_not_found", f"no such {what}")
    return value


def query_int(request: web.Request, name: str, *, default: int | None = None,
              minimum: int | None = None, maximum: int | None = None) -> int | None:
    """An integer query parameter, clamped; absent gives `default`.

    Raises:
        ApiError: 400 when present but not an integer.
    """
    raw = request.query.get(name)
    if raw is None or raw == "":
        return default
    value = to_int(raw, None)
    if value is None or isinstance(value, bool):
        raise ApiError(400, "bad_parameter", f"{name} must be an integer")
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value
