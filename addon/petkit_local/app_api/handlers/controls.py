"""`/app/v1/devices/{id}/actions/...` and `/entities/...`: the writes Home Assistant can make.

The app may do to a device exactly what Home Assistant may, through the same
code: an action is a button HA shows for this model
(`ha/commands.py::device_actions`), a setting is an entity HA can write, and
both go through `handle_ha_command` — so a value's coercion, its bounds, the
optimistic update and a button's side effects (a consumable reset records its
date there) are HA's own. What differs is the input: the app sends typed JSON,
which `payload_for` turns into the string HA would have published, refusing a
value of the wrong type instead of letting it fall through as "nothing to send".

Actions the panel confirms before sending (`DESTRUCTIVE_ACTIONS`) are refused
here unless the body says `{"confirm": true}`: the app shows the same
confirmation, and a client that forgot to cannot dump somebody's litter.

A Bluetooth accessory takes the same two routes under its own id, and the
same entities HA writes (`ha/entities/ble.py`). Its write is a frame relayed by
its parent (`_common.send_accessory_command`), not a device command.
"""
from __future__ import annotations

from aiohttp import web

from petkit_local.app_api import serializers
from petkit_local.app_api.handlers._common import (
    device_or_accessory_or_404, json_body, optional_json_body, send_accessory_command,
    send_command,
)
from petkit_local.app_api.middleware import ApiError
from petkit_local.devices.base import Device, Refused
from petkit_local.ha.categories import get_entities_for_device
from petkit_local.ha.commands import (
    DESTRUCTIVE_ACTIONS, device_actions, handle_ha_command, payload_for,
)
from petkit_local.ha.entities.ble import get_ble_entities
from petkit_local.ha.schedules import check_raw_schedule_text
from petkit_local.ha.state_doc import entity_state_doc


async def run_action(request: web.Request) -> web.Response:
    """`POST /app/v1/devices/{id}/actions/{action}` -> `ActionResult`.

    Raises:
        ApiError: 404 for an action this device does not offer, 422
            `confirmation_required` for a destructive one sent without
            `{"confirm": true}`, 422 `refused` when the command refuses, 409
            `accessory_unreachable` for an accessory no parent can relay to now.
    """
    device = device_or_accessory_or_404(request)
    key = request.match_info["action"]
    if isinstance(device, Device):
        actions = device_actions(get_entities_for_device(device))
    else:
        actions = serializers.accessory_actions(get_ble_entities(device.ble_type))
    entity = next((e for e in actions if e.key == key), None)
    if entity is None:
        raise ApiError(404, "action_not_found", "this device offers no such action")
    body = await optional_json_body(request)
    if key in DESTRUCTIVE_ACTIONS and body.get("confirm") is not True:
        raise ApiError(422, "confirmation_required",
                       f"{entity.name} cannot be undone; send {{\"confirm\": true}}")
    if not isinstance(device, Device):
        delivered = await send_accessory_command(request, device, entity, "", key)
        return web.json_response({"ok": True, "action": key, "delivered": delivered})
    try:
        command = handle_ha_command(device, entity, "")
    except Refused as exc:
        raise ApiError(422, "refused", str(exc)) from None
    delivered = await send_command(request, device, command, key)
    return web.json_response({"ok": True, "action": key, "delivered": delivered})


async def set_entity(request: web.Request) -> web.Response:
    """`PUT /app/v1/devices/{id}/entities/{key} {"value": ...}` -> `EntityWriteResult`.

    Only entities whose `settable` is true. The answer carries the entity with
    the value now stored — optimistic, as in Home Assistant: the device's next
    report confirms or overwrites it.

    Raises:
        ApiError: 404 for no such entity, 422 `not_settable` for a read-only
            one, 422 `refused` for a value of the wrong type or out of bounds,
            409 `accessory_unreachable` as for an action.
    """
    device = device_or_accessory_or_404(request)
    key = request.match_info["key"]
    entities = (get_entities_for_device(device) if isinstance(device, Device)
                else get_ble_entities(device.ble_type))
    entity = next((e for e in entities
                   if e.key == key and e.component in serializers.VALUE_COMPONENTS), None)
    if entity is None:
        raise ApiError(404, "entity_not_found", "this device has no such entity")
    if not entity.is_settable:
        raise ApiError(422, "not_settable", f"{entity.name} is read-only")
    body = await json_body(request)
    if "value" not in body:
        raise ApiError(400, "bad_request", "send {\"value\": ...}")
    if not isinstance(device, Device):
        try:
            payload = payload_for(entity, body["value"])
        except Refused as exc:
            raise ApiError(422, "refused", str(exc)) from None
        delivered = await send_accessory_command(request, device, entity, payload,
                                                 f"{key}={payload[:60]}")
        return web.json_response({
            "ok": True, "delivered": delivered,
            "entity": serializers.entity_value(entity, device.state),
        })
    try:
        payload = payload_for(entity, body["value"])
        if entity.component == "text":
            # The raw-JSON schedules: Home Assistant stores whatever it is
            # sent; the app gets the schedule editor's validation as well.
            check_raw_schedule_text(device, entity.value_path, payload)
        command = handle_ha_command(device, entity, payload)
    except Refused as exc:
        raise ApiError(422, "refused", str(exc)) from None
    delivered = await send_command(request, device, command, f"{key}={payload[:60]}")
    return web.json_response({
        "ok": True, "delivered": delivered,
        "entity": serializers.entity_value(entity, entity_state_doc(device)),
    })
