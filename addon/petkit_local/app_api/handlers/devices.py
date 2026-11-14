"""`/app/v1/devices`: the device list and one device's values.

Values come from the same two functions the panel's device view and Home
Assistant use — `ha/categories.py::get_entities_for_device` for which entities
a device has, `ha/state_doc.py::entity_state_doc` for the document their
`value_path`s are read from — so the app, the panel and HA show one value.

Phase 2 adds `POST /devices/{id}/actions/{action}` and the schedule routes here.
"""
from __future__ import annotations

from aiohttp import web

from petkit_local.app_api import serializers
from petkit_local.app_api.appkeys import REGISTRY
from petkit_local.app_api.handlers._common import path_int
from petkit_local.app_api.middleware import ApiError
from petkit_local.ha.categories import get_entities_for_device
from petkit_local.ha.state_doc import entity_state_doc


async def list_devices(request: web.Request) -> web.Response:
    """`GET /app/v1/devices` -> `{"devices": [DeviceSummary]}`, ordered by id."""
    devices = sorted(request.app[REGISTRY].all(), key=lambda d: d.petkit_id)
    return web.json_response({"devices": [serializers.device_summary(d) for d in devices]})


async def device_detail(request: web.Request) -> web.Response:
    """`GET /app/v1/devices/{id}` -> `{"device": DeviceDetail}`."""
    device = request.app[REGISTRY].get(path_int(request, "id", "device"))
    if device is None:
        raise ApiError(404, "device_not_found", "no such device")
    detail = serializers.device_detail(device, get_entities_for_device(device),
                                       entity_state_doc(device))
    return web.json_response({"device": detail})
