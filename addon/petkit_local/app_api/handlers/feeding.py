"""`/app/v1/devices/{id}/schedule` and `/deferred-feeds`: a feeder's plan and one-off feeds.

Storage, validation and the push are `ha/schedules.py`, shared with the panel's
schedule editor, so a plan one accepts the other cannot refuse. This module
translates between the API's shape (`FeedingPlan`: days as numbers, meals with
named amounts) and the device's (`re`, `it`, `t`, `a`/`a1`/`a2`), and checks
TYPES on the way in — `ha/schedules.py` is lenient about a `"7"` where a 7
belongs, because the panel's editor sends what the official app did; a typed
API has no reason to be.

The pause is the plan's `active` flag: off, the meals stay stored and the
feeder is served a plan without them (`Device.feed_schedule_active`). A
one-off feed is not withheld by a pause.
"""
from __future__ import annotations

from typing import Any

from aiohttp import web

from petkit_local.app_api import serializers
from petkit_local.app_api.handlers._common import device_or_404, json_body, send_command
from petkit_local.app_api.middleware import ApiError
from petkit_local.devices.base import Device, Refused
from petkit_local.ha import schedules
from petkit_local.ha.commands import feed_schedule_command

#: Groups and meals one plan may carry. Far above what a feeder runs (the app
#: offers 10 meals a day); the point is that the number exists.
MAX_GROUPS = 7
MAX_MEALS = 24
#: A deferred feed's id is ours (`d_<date>_<seconds>`), so it is short.
MAX_FEED_ID = 64


def _feeder_or_404(request: web.Request) -> Device:
    device = device_or_404(request)
    if not device.is_feeder:
        raise ApiError(404, "not_a_feeder", "this device has no feeding plan")
    return device


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _invalid(message: str) -> ApiError:
    return ApiError(422, "invalid_schedule", message)


def _meal_to_device(meal: Any, dual: bool) -> dict[str, Any]:
    """One API meal as the device's `{id?, t, a | a1, a2}`; types checked."""
    if not isinstance(meal, dict):
        raise _invalid("each meal must be an object")
    if not _is_int(meal.get("time")):
        raise _invalid("a meal's time is seconds since local midnight, an integer")
    out: dict[str, Any] = {"t": meal["time"]}
    if isinstance(meal.get("id"), str) and meal["id"]:
        out["id"] = meal["id"][:MAX_FEED_ID]
    names = {"a1": "amount1", "a2": "amount2"} if dual else {"a": "amount"}
    for device_key, api_key in names.items():
        value = meal.get(api_key, 0 if api_key == "amount2" else None)
        if not _is_int(value):
            raise _invalid(f"a meal on this feeder needs an integer {api_key}"
                           if value is None else f"{api_key} must be an integer")
        out[device_key] = value
    return out


def _groups_to_device(groups: Any, dual: bool) -> list[dict[str, Any]]:
    """The API's groups as the device's `[{re, it}]`; types and counts checked."""
    if not isinstance(groups, list) or len(groups) > MAX_GROUPS:
        raise _invalid(f"groups must be a list of at most {MAX_GROUPS}")
    out = []
    for group in groups:
        if not isinstance(group, dict):
            raise _invalid("each group must be an object")
        days, meals = group.get("days"), group.get("meals", [])
        if (not isinstance(days, list) or not days
                or not all(_is_int(day) for day in days)):
            raise _invalid("a group's days must be a non-empty list of integers 1-7")
        if not isinstance(meals, list) or len(meals) > MAX_MEALS:
            raise _invalid(f"a group's meals must be a list of at most {MAX_MEALS}")
        out.append({"re": ",".join(str(day) for day in days),
                    "it": [_meal_to_device(meal, dual) for meal in meals]})
    return out


async def get_schedule(request: web.Request) -> web.Response:
    """`GET /app/v1/devices/{id}/schedule` -> `{"schedule": FeedingPlan}`."""
    device = _feeder_or_404(request)
    return web.json_response({"schedule": serializers.feeding_plan(device)})


async def put_schedule(request: web.Request) -> web.Response:
    """`PUT /app/v1/devices/{id}/schedule {groups?, active?}` -> `ScheduleWriteResult`.

    `groups` replaces every recurring meal; `active` pauses or resumes them.
    Either may be left out, not both. Pending one-off feeds are kept. The
    plan is pushed once, after both are applied.

    Raises:
        ApiError: 422 `invalid_schedule` for a plan the panel would refuse too.
    """
    device = _feeder_or_404(request)
    body = await json_body(request)
    if "groups" not in body and "active" not in body:
        raise ApiError(400, "bad_request", "send groups, active, or both")
    if "active" in body and not isinstance(body["active"], bool):
        raise _invalid("active must be true or false")

    plan = None
    if "groups" in body:
        dual = schedules.is_dual_hopper(device)
        stored = device.config.get("feed_schedule")
        plan = {**(stored if isinstance(stored, dict) else {}),
                "schedule": _groups_to_device(body["groups"], dual)}
        # Checked before anything is stored, so a refused plan leaves the
        # pause exactly as it was too.
        if schedules.clean_feed_schedule(plan, dual) is None:
            raise _invalid("not a valid feeding schedule")
    if "active" in body:
        schedules.set_feed_plan_active(device, body["active"])
    try:
        command = (schedules.save_schedule(device, "feed_schedule", plan) if plan is not None
                   else feed_schedule_command(device))
    except Refused as exc:
        raise _invalid(str(exc)) from None

    summary = ("feeding plan" if "groups" in body
               else "feeding plan " + ("resumed" if body["active"] else "paused"))
    delivered = await send_command(request, device, command, summary)
    return web.json_response({"ok": True, "delivered": delivered,
                              "schedule": serializers.feeding_plan(device)})


def _pending(device: Device) -> list[dict[str, Any]]:
    """The stored one-off feeds, read without creating a plan for the asking."""
    stored = device.config.get("feed_schedule")
    deferred = stored.get("deferred") if isinstance(stored, dict) else None
    return [e for e in deferred or [] if isinstance(e, dict)]


async def list_deferred_feeds(request: web.Request) -> web.Response:
    """`GET /app/v1/devices/{id}/deferred-feeds` -> `{"deferred_feeds": [DeferredFeed]}`.

    Soonest first. A feed whose time has passed is pruned by the next
    `dev_feed_get` (`http/handlers/feed.py`), not by this listing.
    """
    device = _feeder_or_404(request)
    feeds = sorted((serializers.deferred_feed(device, e) for e in _pending(device)),
                   key=lambda f: f["fire_at"] or 0)
    return web.json_response({"deferred_feeds": feeds})


async def add_deferred_feed(request: web.Request) -> web.Response:
    """`POST /app/v1/devices/{id}/deferred-feeds {date, time, amount | amount1, amount2}`.

    `date` is `YYYY-MM-DD` and `time` `HH:MM`, both in the feeder's own
    timezone. Answers 201 with `DeferredFeedResult`.

    Raises:
        ApiError: 422 `invalid_feed` for a bad or past time or a bad amount.
    """
    device = _feeder_or_404(request)
    body = await json_body(request)
    if not isinstance(body.get("date"), str) or not isinstance(body.get("time"), str):
        raise ApiError(422, "invalid_feed", "date (YYYY-MM-DD) and time (HH:MM) are required")
    dual = schedules.is_dual_hopper(device)
    names = {"a1": "amount1", "a2": "amount2"} if dual else {"a": "amount"}
    if not any(api in body for api in names.values()):
        raise ApiError(422, "invalid_feed",
                       "this feeder needs amount1, amount2 or both" if dual
                       else "this feeder needs an amount")
    if not all(_is_int(body.get(api, 0)) for api in names.values()):
        raise ApiError(422, "invalid_feed", "amounts must be integers")
    try:
        amounts = schedules.deferred_amounts(
            device, {k: body.get(api, 0) for k, api in names.items()})
        entry = schedules.add_deferred_feed(device, body["date"], body["time"], amounts)
    except Refused as exc:
        raise ApiError(422, "invalid_feed", str(exc)) from None
    delivered = await send_command(request, device, feed_schedule_command(device),
                                   f"one-off feed {entry['id']}")
    return web.json_response({"ok": True, "delivered": delivered,
                              "feed": serializers.deferred_feed(device, entry)}, status=201)


async def delete_deferred_feed(request: web.Request) -> web.Response:
    """`DELETE /app/v1/devices/{id}/deferred-feeds/{feed_id}` -> `WriteResult`."""
    device = _feeder_or_404(request)
    feed_id = request.match_info["feed_id"]
    if not any(e.get("id") == feed_id for e in _pending(device)):
        raise ApiError(404, "feed_not_found", "no such one-off feed")
    schedules.remove_deferred_feed(device, feed_id)
    delivered = await send_command(request, device, feed_schedule_command(device),
                                   f"one-off feed {feed_id} removed")
    return web.json_response({"ok": True, "delivered": delivered})
