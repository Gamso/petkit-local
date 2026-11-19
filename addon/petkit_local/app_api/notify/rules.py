"""What is worth a notification, and what it says. Pure functions, no I/O.

Two sources, because the things an owner wants to hear about arrive two ways:

* **Events** — a stored `events` row, as the event hub narrates it. Classified
  through `events/codes.py`, never by name: HTTP `event_type` codes are per
  device category (`2` is a cleared fault on a litter box and a finished meal
  on a feeder), so every lookup passes the row's `device_type`. A toilet visit
  is the visit SUMMARY only (`ROLE_VISIT_SUMMARY`) — the mid-visit weight check
  shares the visit's kind and would notify twice — and a feed is a finished
  dispense (`codes.is_feed_done`), not a cat eating from the bowl.
* **State** — a condition read off `device.state`: food low, a consumable spent,
  a fault flag. These have no event of their own; `CONDITIONS` says which keys,
  and the notifier turns each into an edge (it fires when the condition starts,
  not while it lasts). Every condition is gated on its key being present: a
  model that never sends `food` says nothing about food, it does not say "full".

Payloads stay minimal on purpose: a device's product name, a pet's name, what
happened. No ids, no URLs, no token — a notification is shown on a lock screen.
That wording (`title`, `message`, `tags`) does not leave the server today: the
phones are sent only the stable keys below and write their own text, so here it
is what a notification reads as, and what the tests pin a classification by.

Each `Notification` also carries the stable keys the phone app's own push
message is built from (`notify/push.py`): `code` — the event's `event_type` for
an event, the `state` key for a condition, None for `offline` — `pet_id` and
`ts`. Never the title or the message: the app writes those itself, in the
phone's language.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from petkit_local.app_api.notify.settings import (
    KIND_CONSUMABLE, KIND_ERROR, KIND_FEED_FAILED, KIND_FEED_SERVED, KIND_FOOD_LOW,
    KIND_OFFLINE, KIND_TOILET,
)
from petkit_local.events import codes, decode
from petkit_local.utils.coerce import to_float, to_int
from petkit_local.utils.const import device_display_name

#: Priorities: 1 min, 2 low, 3 default, 4 high, 5 urgent. `PRIORITY_HIGH` and
#: above is pushed with `Urgency: high` (`push.py`).
PRIORITY_LOW = 2
PRIORITY_DEFAULT = 3
PRIORITY_HIGH = 4

#: `FEED_RESULT[10]`: the feeder held a meal back because the bowl was not
#: empty (surplus control). A meal skipped on purpose, not a failed one.
FEED_SKIPPED_BOWL_NOT_EMPTY = 10

#: Ours, not the device's: no model reports a "litter low" flag (`sandLack` was
#: searched for and found nowhere), only `sandPercent`. Below this the box is
#: worth topping up.
LITTER_LOW_PERCENT = 20

MAX_MESSAGE = 500


@dataclass(frozen=True)
class Notification:
    """One message to publish. `dedupe_key` is set for an event, None otherwise."""

    kind: str
    device_id: Any
    title: str
    message: str
    priority: int = PRIORITY_DEFAULT
    tags: tuple[str, ...] = ()
    dedupe_key: str | None = None
    #: The stable key the app translates: an event's `event_type`, a state
    #: condition's `state` key (`errorMsg` for the device's own fault text,
    #: which is never sent), None when the kind says it all.
    code: str | None = None
    pet_id: int | None = None
    #: When it happened (epoch seconds), when the row says; else when announced.
    ts: float | None = None


def device_name(device_type: str | None) -> str:
    """The product name a notification calls a device by."""
    name = device_display_name(device_type or "")
    if name and name != "Unknown":
        return name
    return f"PetKit {device_type.upper()}" if device_type else "PetKit device"


def _content(row: Mapping[str, Any]) -> dict[str, Any]:
    """The row's decoded `content`, `{}` for anything that is not an object."""
    raw = row.get("content_json")
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        value = json.loads(raw)
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def _clip(text: str) -> str:
    return text if len(text) <= MAX_MESSAGE else text[:MAX_MESSAGE - 1] + "…"


def from_event(row: Mapping[str, Any], pet_name: str | None = None) -> Notification | None:
    """The notification a stored event row deserves, or None.

    `row` is the dict `events/normalize.py` builds (`event_type`,
    `device_type`, `device_id`, `event_uid`, `content_json`). `pet_name` is the
    name of the pet the row was attributed to, when it was.
    """
    event_type = str(row.get("event_type") or "")
    device_type = row.get("device_type") or None
    code = codes.lookup(event_type, device_type)
    if code is None:
        return None
    content = _content(row)
    device = device_name(device_type)
    common = {"device_id": row.get("device_id"), "dedupe_key": row.get("event_uid") or None,
              "code": event_type, "pet_id": to_int(row.get("pet_id"), None),
              "ts": to_float(row.get("ts"), None)}

    if code.kind == codes.KIND_TOILET and code.role == codes.ROLE_VISIT_SUMMARY:
        bits = decode.summary_bits(event_type, content)
        extra = f" ({', '.join(bits)})" if bits else ""
        who = f"{pet_name} used the {device}" if pet_name else f"Visit to the {device}"
        return Notification(kind=KIND_TOILET, title="Toilet visit",
                            message=_clip(f"{who}{extra}."), priority=PRIORITY_LOW,
                            tags=("cat", KIND_TOILET), **common)

    if codes.is_feed_done(code):
        label = decode.event_label(event_type, content, device_type)
        result = to_int(content.get("result"), None)
        outcome = (codes.feed_result(result, to_int(content.get("err_code"), None))
                   if result is not None else 0)
        if outcome == 0:
            return Notification(kind=KIND_FEED_SERVED, title="Meal served",
                                message=_clip(f"{device}: {label}."), priority=PRIORITY_LOW,
                                tags=("fork_and_knife", KIND_FEED_SERVED), **common)
        if outcome == FEED_SKIPPED_BOWL_NOT_EMPTY:
            return Notification(kind=KIND_FEED_SERVED, title="Meal skipped",
                                message=_clip(f"{device}: {label}."), priority=PRIORITY_LOW,
                                tags=("fork_and_knife", KIND_FEED_SERVED), **common)
        return Notification(kind=KIND_FEED_FAILED, title="Meal failed",
                            message=_clip(f"{device}: {label}."), priority=PRIORITY_HIGH,
                            tags=("x", KIND_FEED_FAILED), **common)

    if code.kind == codes.KIND_ERROR and code.role != codes.ROLE_ERROR_OVER:
        label = decode.event_label(event_type, content, device_type)
        return Notification(kind=KIND_ERROR, title="Device error",
                            message=_clip(f"{device}: {label}."), priority=PRIORITY_HIGH,
                            tags=("rotating_light", KIND_ERROR), **common)
    return None


def offline(device_id: Any, device_type: str | None) -> Notification:
    """The watchdog decided a device is gone."""
    return Notification(kind=KIND_OFFLINE, device_id=device_id, title="Device offline",
                        message=f"The {device_name(device_type)} stopped responding.",
                        priority=PRIORITY_HIGH, tags=("electric_plug", KIND_OFFLINE))


# --- conditions read off device.state -----------------------------------------

@dataclass(frozen=True)
class Condition:
    """One state condition worth a notification when it starts.

    `test` gets the raw value of `key` (present, never None) and answers
    whether the condition holds. `hold` is how long it must keep holding, seen
    again in a later report, before it is announced: `food` reads 0 for a
    moment while a meal moves through the hopper, and a full-bin switch can
    bounce.
    """

    key: str
    kind: str
    title: str
    message: str
    test: Callable[[Any], bool]
    hold: float = 60.0
    priority: int = PRIORITY_DEFAULT
    tag: str = "warning"


def _flag(value: Any) -> bool:
    return bool(to_int(value, 0))


def _zero(value: Any) -> bool:
    return to_float(value, None) == 0


def _spent(value: Any) -> bool:
    days = to_float(value, None)
    return days is not None and days <= 0


def _litter_low(value: Any) -> bool:
    percent = to_float(value, None)
    return percent is not None and percent < LITTER_LOW_PERCENT


#: Ten minutes: long enough that a meal moving through the hopper (which reads
#: as empty for a moment) has been followed by a report saying it is not.
FOOD_HOLD = 600.0

CONDITIONS: tuple[Condition, ...] = (
    # `foodLow` is derived (`state_parsers._derive_food_low`) from a single
    # hopper's `food`; a D4SH reports `food1`/`food2` instead, 0 = empty.
    Condition("foodLow", KIND_FOOD_LOW, "Food low", "The {device} hopper is empty.",
              _flag, hold=FOOD_HOLD, tag="bowl_with_spoon"),
    Condition("food1", KIND_FOOD_LOW, "Food low", "Hopper 1 of the {device} is empty.",
              _zero, hold=FOOD_HOLD, tag="bowl_with_spoon"),
    Condition("food2", KIND_FOOD_LOW, "Food low", "Hopper 2 of the {device} is empty.",
              _zero, hold=FOOD_HOLD, tag="bowl_with_spoon"),
    Condition("boxFull", KIND_CONSUMABLE, "Waste bin full",
              "Empty the waste bin of the {device}.", _flag, tag="wastebasket"),
    Condition("sandPercent", KIND_CONSUMABLE, "Litter low",
              "The {device} is low on litter.", _litter_low, hold=600.0),
    Condition("deodorantLeftDays", KIND_CONSUMABLE, "N50 deodorizer to replace",
              "The N50 deodorizer of the {device} is spent.", _spent, hold=0.0),
    Condition("sprayLeftDays", KIND_CONSUMABLE, "N60 spray to replace",
              "The N60 spray of the {device} is spent.", _spent, hold=0.0),
    Condition("desiccantLeftDays", KIND_CONSUMABLE, "Desiccant to replace",
              "The desiccant pack of the {device} is spent.", _spent, hold=0.0),
    Condition("filterWarning", KIND_CONSUMABLE, "Filter to replace",
              "Replace the filter of the {device}.", _flag),
    Condition("lackWarning", KIND_CONSUMABLE, "Water low", "Refill the {device}.", _flag),
    Condition("stgFullState", KIND_CONSUMABLE, "Tray full",
              "Empty the tray of the {device}.", _flag),
    Condition("liquidLack", KIND_CONSUMABLE, "Deodorizer liquid low",
              "Refill the deodorizer liquid of the {device}.", _flag),
)

#: The device's own fault summary (`state_parsers._extract_error_flags`). A
#: changed message is a new fault, so the condition key carries the text.
ERROR_KEY = "errorMsg"
ERROR_HOLD = 60.0


@dataclass(frozen=True)
class ActiveCondition:
    """A condition holding right now, ready to become a notification."""

    hold: float
    notification: Notification


def active_conditions(device_id: Any, device_type: str | None,
                      state: Mapping[str, Any]) -> dict[str, ActiveCondition]:
    """Every condition holding in `state`, keyed by a stable condition id.

    Keys absent from `state` are skipped, never read as "fine" or "not fine".
    """
    name = device_name(device_type)
    out: dict[str, ActiveCondition] = {}
    for cond in CONDITIONS:
        value = state.get(cond.key)
        if value is None or not cond.test(value):
            continue
        out[cond.key] = ActiveCondition(cond.hold, Notification(
            kind=cond.kind, device_id=device_id, title=cond.title,
            message=cond.message.format(device=name), priority=cond.priority,
            tags=(cond.tag, cond.kind), code=cond.key))
    error = state.get(ERROR_KEY)
    if isinstance(error, str) and error.strip():
        text = error.strip()
        out[f"{ERROR_KEY}:{text}"] = ActiveCondition(ERROR_HOLD, Notification(
            kind=KIND_ERROR, device_id=device_id, title="Device error",
            message=_clip(f"{name}: {text}."), priority=PRIORITY_HIGH,
            tags=("rotating_light", KIND_ERROR), code=ERROR_KEY))
    return out
