"""The schedule editors' one write endpoint, and the one-off feeds beside it.

What a valid schedule is, how it is stored and which command pushes it live in
`ha/schedules.py`, shared with the phone app API; these handlers only parse the
request, deliver the command (`_deliver`) and answer in the panel's shape.
"""
from __future__ import annotations

from aiohttp import web

from petkit_local.devices.base import Refused
from petkit_local.ha import schedules
from petkit_local.web.api._common import _deliver, _device_or_404, _json_body
from petkit_local.web.appkeys import BRIDGE, HUB, REGISTRY


async def api_save_schedule(request: web.Request) -> web.Response:
    """Store one schedule and push it to the device.

    Body: `{"target": <field>, "value": <parsed JSON>}`, where `target` is one
    of the names `defaults.schedule_targets()` gave out. Anything else is refused
    rather than stored, so the panel cannot invent a field.

    Validation, storage and the push are `ha/schedules.py::save_schedule`;
    the command goes out by whichever transport is live (`_deliver`).
    """
    reg = request.app[REGISTRY]
    hub = request.app[HUB]
    bridge = request.app[BRIDGE]
    d = _device_or_404(request)

    body = await _json_body(request)
    try:
        suffix, envelope = schedules.save_schedule(d, body.get("target"), body.get("value"))
    except Refused as exc:
        return web.json_response({"error": str(exc)}, status=400)
    reg.save()
    return await _deliver(hub, bridge, d, suffix, envelope)


async def api_deferred_feed(request: web.Request) -> web.Response:
    """Add or list deferred (one-off) feeds for a feeder.

    POST ``{"date": "2026-08-13", "time": "17:05", "a1": 0, "a2": 1}``
    (Dual-Hopper) or ``{..., "a": 20}`` (single-hopper D4H/D4H2) adds a deferred
    feed. GET lists pending ones. DELETE with ``{sound_id}`` in path removes one.
    The amount is stored in the model's own key so the feed actually dispenses
    (`ha/schedules.py::clean_meal_amounts`); a legacy `a1` is read as `a` on a single hopper.

    The device picks this up on its next ``dev_feed_get`` poll, which the
    heartbeat ``feed_get:1`` command triggers immediately.
    """
    reg = request.app[REGISTRY]
    d = _device_or_404(request)

    if request.method == "GET":
        return web.json_response({"deferred": schedules.deferred_feeds(d)})

    if request.method == "DELETE":
        try:
            feed_id = request.match_info["feed_id"]
        except KeyError:
            return web.json_response({"error": "missing feed_id"}, status=400)
        schedules.remove_deferred_feed(d, feed_id)
        reg.save()
        schedules.request_feed_get(d)
        return web.json_response({"ok": True})

    body = await _json_body(request)
    try:
        amounts = schedules.deferred_amounts(d, body)
        entry = schedules.add_deferred_feed(d, body.get("date", ""), body.get("time", ""),
                                            amounts)
    except Refused as exc:
        return web.json_response({"error": str(exc)}, status=400)
    reg.save()
    schedules.request_feed_get(d)
    return web.json_response({"ok": True, "feed": entry})
