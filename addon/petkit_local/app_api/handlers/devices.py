"""`/app/v1/devices`: the device list and one device's values.

Values come from the same two functions the panel's device view and Home
Assistant use — `ha/categories.py::get_entities_for_device` for which entities
a device has, `ha/state_doc.py::entity_state_doc` for the document their
`value_path`s are read from — so the app, the panel and HA show one value.

The Bluetooth accessories are listed here too, each under its own id with the
entities Home Assistant gives it (`app_api/inventory.py`).

The writes to a device are `handlers/controls.py` (actions, settings) and
`handlers/feeding.py` (feeding plan, one-off feeds).
"""
from __future__ import annotations

from aiohttp import web

from petkit_local.app_api import inventory
from petkit_local.app_api.appkeys import BLE_REGISTRY, REGISTRY
from petkit_local.app_api.handlers._common import path_int
from petkit_local.app_api.middleware import ApiError


async def list_devices(request: web.Request) -> web.Response:
    """`GET /app/v1/devices` -> `{"devices": [DeviceSummary]}`, ordered by id."""
    return web.json_response({"devices": inventory.all_summaries(
        request.app[REGISTRY], request.app.get(BLE_REGISTRY))})


async def device_detail(request: web.Request) -> web.Response:
    """`GET /app/v1/devices/{id}` -> `{"device": DeviceDetail}`."""
    detail = inventory.detail(request.app[REGISTRY], request.app.get(BLE_REGISTRY),
                              path_int(request, "id", "device"))
    if detail is None:
        raise ApiError(404, "device_not_found", "no such device")
    return web.json_response({"device": detail})
