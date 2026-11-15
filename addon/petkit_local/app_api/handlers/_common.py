"""Request parsing every app API handler starts with.

Each helper either returns a clean value or raises `ApiError`, which the
middleware renders as the API's JSON refusal. Nothing request-supplied reaches
a handler unparsed.
"""
from __future__ import annotations

import logging
from typing import Any

from aiohttp import web

from petkit_local.app_api.appkeys import COMMAND_SINK, HA_PUBLISHER, HUB, REGISTRY, SESSION
from petkit_local.app_api.middleware import ApiError
from petkit_local.devices.base import Device
from petkit_local.ha.command_router import deliver_command
from petkit_local.ha.commands import Command
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
