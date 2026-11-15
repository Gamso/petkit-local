"""What a valid schedule is, how one is stored, and the one-off feeds beside it.

The panel's schedule editor (`web/api/schedules.py`) and the phone app API
(`app_api/handlers/feeding.py`) write schedules through here, so a plan one of
them accepts the other cannot refuse, and both reach the device the same way.
Each function stores what it was given and RETURNS the command that pushes it;
delivering that command is the caller's, by whichever transport is live.

Four shapes, one per `defaults.schedule_targets()` kind: a range list, a weekly
list, the litter box's timed jobs, and the feeder's meals. Each cleaner is
deliberately permissive about the CONTENT and strict about the SHAPE, because
the odd-looking values are the ones that came out of the real app.
"""
from __future__ import annotations

import datetime
import json
import time
from typing import Any

from petkit_local.devices import defaults
from petkit_local.devices.base import Device, Refused, encode_multi_range
from petkit_local.devices.defaults import DAY_MINUTES, clean_range_list, clean_weekly_list
from petkit_local.ha.commands import (
    PROPERTY_SET_SUFFIX, Command, feed_schedule_command, make_mqtt_property_set,
    store_multi_range,
)
from petkit_local.utils.coerce import to_int
from petkit_local.utils.const import DEVICE_TYPES_FEEDER_DUAL

#: Seconds in a day — the unit of a feeder meal's `t`, alone among the
#: schedule shapes (see `clean_feed_schedule`).
DAY_SECONDS = 24 * 3600

#: A portion is one byte on the device.
MAX_AMOUNT = 255

#: The config keys a raw-JSON schedule `text` entity writes (`ha/entities/text.py`),
#: and the schedule target each one is.
RAW_SCHEDULE_TARGETS = {"schedule": "schedule", "feed_schedule": "feed_schedule"}


def is_dual_hopper(device: Device) -> bool:
    """Whether a feeder's meals carry two portions (`a1`/`a2`) or one (`a`)."""
    return device.device_type.lower() in DEVICE_TYPES_FEEDER_DUAL


def clean_point_list(value: Any) -> list[dict[str, Any]] | None:
    """`[{id, repeats, time, type}]` — the litter box's timed jobs.

    ONE array carries both of them: `type` 0 is a cleaning and 1 a deodorizing,
    confirmed on a T5 whose array gained a `type: 1` entry when a periodic
    deodorizing time was added. An unrecognised `type` is kept as it is rather
    than rejected — the editor groups by it, and a value nobody has seen is not
    a reason to delete somebody's entry.
    """
    if not isinstance(value, list):
        return None
    cleaned: list[dict[str, Any]] = []
    for entry in value:
        if not isinstance(entry, dict):
            return None
        minute = to_int(entry.get("time"), None)
        kind = to_int(entry.get("type"), None)
        entry_id = to_int(entry.get("id"), None)
        if minute is None or kind is None or entry_id is None:
            return None
        if not 0 <= minute < DAY_MINUTES:
            return None
        days = []
        for part in str(entry.get("repeats", "")).split(","):
            day = to_int(part, None)
            if day is None or not (1 <= day <= 7):
                return None
            days.append(day)
        if not days:
            return None
        cleaned.append({
            "id": entry_id,
            "repeats": ",".join(str(d) for d in sorted(set(days))),
            "time": minute,
            "type": kind,
        })
    return cleaned


def clean_meal_amounts(meal: dict, dual: bool) -> dict[str, Any] | None:
    """A meal's dispensed amount, validated, in the model's own shape.

    A Dual-Hopper carries `a1`/`a2`, one per hopper (`a2` defaults 0); a
    single-hopper D4H/D4H2 carries a lone `a`, which is the field its
    `pk_schmg_parse_schedule` reads — sent `a1`/`a2` it dispenses nothing
    (`err_code: 8`, the mirror of the Dual-Hopper's issue #2). Which key a
    feeder reads is per model, exactly as `feed_realtime` is (`ha/commands.py::
    _feed`), and the two amounts never convert: `a` is the device's own unit,
    not the portions a hopper counts.

    For a single hopper a legacy `a1` is accepted as `a` — a schedule the
    dual-only editor saved before this split is read, not silently emptied —
    but never the other way. All are a single byte on the device, so 0..255.
    """
    if dual:
        first = to_int(meal.get("a1"), None)
        second = to_int(meal.get("a2"), 0)
        if first is None or second is None:
            return None
        if not (0 <= first <= MAX_AMOUNT and 0 <= second <= MAX_AMOUNT):
            return None
        return {"a1": first, "a2": second}
    amount = to_int(meal.get("a"), None)
    if amount is None:
        amount = to_int(meal.get("a1"), None)  # legacy dual-only editor save
    if amount is None or not 0 <= amount <= MAX_AMOUNT:
        return None
    return {"a": amount}


def clean_feed_schedule(value: Any, dual: bool) -> dict[str, Any] | None:
    """`{schedule: [{re, it, itemJsonString}], nextTick, latest}` — the feeder's.

    The shape comes from a D4SH 867 `ctrl` (`pk_schmg_parse_schedule`), which
    reads `re` and `it` per group and `id`/`t` per meal, then the amount in the
    model's own key: `a1`/`a2` on a Dual-Hopper, a lone `a` on the single-hopper
    D4H/D4H2 (`dual` selects which; see `events/codes.py::FEED_SCHEDULE_ITEM_KEYS`
    and `clean_meal_amounts`). Unlike every other schedule here, a meal's `t`
    counts SECONDS since local midnight — the cloud's `n_46560` fires at 12:56:00
    (D4SH capture, 2026-08-12) — and the id is the cloud's `n_<seconds>` scheme,
    regenerated whenever a client sends an int.

    `itemJsonString` is rebuilt from `it` rather than trusted: the real cloud
    sends both, they are the same list twice, and two copies of one value in one
    payload is exactly the pair that drifts. Key order in it is the cloud's —
    alphabetical. `nextTick` and `latest` are recomputed at serve time, and the
    `v: 2` stamp marks the schedule as seconds-based so the one-time minute
    migration (`feed.migrate_minute_schedule`) never touches a current save.
    """
    if not isinstance(value, dict):
        return None
    groups_in = value.get("schedule")
    if not isinstance(groups_in, list):
        return None

    groups: list[dict[str, Any]] = []
    for group in groups_in:
        if not isinstance(group, dict):
            return None
        days = []
        for part in str(group.get("re", "")).split(","):
            day = to_int(part, None)
            if day is None or not (1 <= day <= 7):
                return None
            days.append(day)
        if not days:
            return None

        meals: list[dict[str, Any]] = []
        for meal in group.get("it") or []:
            if not isinstance(meal, dict):
                return None
            second_of_day = to_int(meal.get("t"), None)
            if second_of_day is None or not 0 <= second_of_day < DAY_SECONDS:
                return None
            amounts = clean_meal_amounts(meal, dual)
            if amounts is None:
                return None
            raw_id = meal.get("id")
            if not isinstance(raw_id, str) or not raw_id:
                raw_id = f"n_{second_of_day}"
            meals.append({"id": raw_id, "t": second_of_day, **amounts})

        groups.append({
            "re": ",".join(str(d) for d in sorted(set(days))),
            "it": meals,
            "itemJsonString": json.dumps(
                meals, separators=(",", ":"), sort_keys=True),
        })

    cleaned = dict(value)
    cleaned["schedule"] = groups
    cleaned["v"] = 2
    return cleaned


def save_schedule(device: Device, target: Any, raw: Any) -> Command:
    """Validate and store one schedule; returns the command that pushes it.

    `target` is one of the names `defaults.schedule_targets()` gives out —
    anything else is refused rather than stored, so no client can invent a
    field. The write goes to the device as `property.set`, which is how the
    real cloud does it — captured going through this add-on to a T5 on
    2026-08-09. The two shapes on the wire are NOT the same and that is the
    trap:

      * a range field carries a JSON string that wraps its own key again
        (`encode_multi_range`),
      * `schedule` carries a plain JSON string of the array, no wrapper.

    Storing happens either way, because `dev_multi_config` and
    `dev_schedule_get` are what the device actually reads on its own clock; the
    push only saves it the wait. A feeding schedule is pushed twice over by
    `feed_schedule_command`: a `feed_get: 1` on the heartbeat queue, and a
    `property.set{feed}` built by `feed_push` — which, while the plan is paused,
    tells the device it has no meals although they are stored.

    Raises:
        Refused: an unknown target, or a value that is not that schedule's shape.
    """
    known = {t["target"]: t["kind"] for t in defaults.schedule_targets(device)}
    if target not in known:
        raise Refused(f"unknown schedule {target}")
    kind = known[target]

    if kind == "feed":
        feed = clean_feed_schedule(raw, is_dual_hopper(device))
        if feed is None:
            raise Refused("not a valid feeding schedule")
        device.config["feed_schedule"] = feed
        return feed_schedule_command(device)

    cleaner = {"ranges": clean_range_list, "weekly": clean_weekly_list,
               "points": clean_point_list}[kind]
    value = cleaner(raw)
    if value is None:
        raise Refused(f"not a valid {kind} schedule")

    if kind == "points":
        device.config["schedule"] = value
        params = {"schedule": json.dumps(value, separators=(",", ":"))}
    else:
        store_multi_range(device, target, value)
        params = {target: encode_multi_range(target, value)}
    return (PROPERTY_SET_SUFFIX, make_mqtt_property_set(params))


def check_raw_schedule_text(device: Device, config_key: str, text: str) -> None:
    """Refuse a raw-JSON schedule `text` write that `save_schedule` would refuse.

    Home Assistant's `Cleaning/Feeding Schedule (JSON)` entities store what
    they are sent; a caller that wants the editor's validation as well asks
    this first. A key that is not a raw schedule passes.

    Raises:
        Refused: not JSON, or not that schedule's shape.
    """
    target = RAW_SCHEDULE_TARGETS.get(config_key)
    if target is None:
        return
    try:
        parsed = json.loads(text)
    except ValueError:
        raise Refused("the schedule must be JSON") from None
    known = {t["target"]: t["kind"] for t in defaults.schedule_targets(device)}
    kind = known.get(target)
    ok = (clean_feed_schedule(parsed, is_dual_hopper(device)) if kind == "feed"
          else clean_point_list(parsed) if kind == "points" else None)
    if ok is None:
        raise Refused(f"not a valid {'feeding' if kind == 'feed' else 'cleaning'} schedule")


def set_feed_plan_active(device: Device, active: bool) -> None:
    """Pause (False) or resume (True) a feeder's recurring meals, keeping them.

    The same stored flag Home Assistant's `Feeding Schedule Active` switch
    writes (`ha/commands.py::FEED_SCHEDULE_SWITCH`) and
    `Device.feed_schedule_active` reads. The caller pushes the plan.
    """
    device.config.setdefault("local", {})["feedScheduleActive"] = bool(active)


# --- one-off (deferred) feeds ---------------------------------------------------

def feed_plan(device: Device) -> dict[str, Any]:
    """The stored feeding plan, created empty (every day, no meals) if absent."""
    return device.config.setdefault("feed_schedule", {
        "schedule": [{"re": "1,2,3,4,5,6,7", "it": [], "itemJsonString": "[]"}],
    })


def deferred_feeds(device: Device) -> list[dict[str, Any]]:
    """The pending one-off feeds, `[{id, a | a1+a2, fire_at}]`, as stored."""
    return feed_plan(device).setdefault("deferred", [])


def deferred_amounts(device: Device, body: dict[str, Any]) -> dict[str, int]:
    """A deferred feed's portion(s) from a request body, in the model's own key.

    A missing amount is 0, as the panel has always read it; a legacy `a1` is
    read as `a` on a single hopper (`clean_meal_amounts`).

    Raises:
        Refused: an amount that is not a whole number from 0 to 255.
    """
    if is_dual_hopper(device):
        fields = {"a1": body.get("a1", 0), "a2": body.get("a2", 0)}
    else:
        fields = {"a": body.get("a", body.get("a1", 0))}
    amounts: dict[str, int] = {}
    for key, raw in fields.items():
        value = None if isinstance(raw, bool) else to_int(raw, None)
        if value is None or not 0 <= value <= MAX_AMOUNT:
            raise Refused(f"{key} must be a whole number from 0 to {MAX_AMOUNT}")
        amounts[key] = value
    return amounts


def add_deferred_feed(device: Device, date_str: Any, time_str: Any,
                      amounts: dict[str, int], now: float | None = None) -> dict[str, Any]:
    """Store a one-off feed at `date_str time_str` (device-local); returns it.

    The device picks it up on its next `dev_feed_get` (or the `latest` of the
    next `property.set{feed}`); the caller delivers `feed_schedule_command`.
    A paused plan does not withhold it (`feed_push`).

    Raises:
        Refused: a date/time that does not parse, or one already past.
    """
    try:
        dt = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        dt = dt.replace(tzinfo=datetime.timezone(
            datetime.timedelta(hours=device.timezone_offset)))
    except (ValueError, TypeError):
        raise Refused("bad date/time") from None

    fire_at = dt.timestamp()
    if fire_at <= (time.time() if now is None else now):
        raise Refused("time is in the past")

    secs_since_midnight = dt.hour * 3600 + dt.minute * 60 + dt.second
    entry = {"id": f"d_{dt.strftime('%Y%m%d')}_{secs_since_midnight}", **amounts,
             "fire_at": fire_at}
    deferred_feeds(device).append(entry)
    return entry


def remove_deferred_feed(device: Device, feed_id: str) -> bool:
    """Drop one pending one-off feed by id; False when there was none."""
    plan = feed_plan(device)
    before = deferred_feeds(device)
    plan["deferred"] = [entry for entry in before if entry.get("id") != feed_id]
    return len(plan["deferred"]) != len(before)


def request_feed_get(device: Device) -> None:
    """Queue a `feed_get: 1` heartbeat command, so a device on HTTP re-fetches
    its plan. The `property.set{feed}` that reaches one on MQTT is separate."""
    device.command_queue.append({"msgType": 1, "payload": {"feed_get": "1"},
                                 "timestamp": int(time.time())})
