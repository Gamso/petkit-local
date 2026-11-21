"""Every JSON object the app API returns, built field by field.

The panel's handlers return internal dicts whose shape the panel's own
JavaScript tracks; they change whenever the panel does. The app is a separate
project on a separate release cycle, built from `openapi.yaml`, so nothing here
passes such a dict through. Each function names every key it emits, and the
OpenAPI schema with the same name describes exactly those keys.

Adding a key is a compatible change. Renaming or removing one is not, and
needs `/app/v2`.

Conventions, so the app can rely on them everywhere:

* times are Unix epoch seconds (float), `null` when unknown — never `0`;
* weights are grams, durations seconds;
* a media URL is an absolute PATH on this API (`/app/v1/media/...`), to be
  joined to whatever base URL the app reached the server at — so it survives
  any reverse proxy in front;
* identifiers are the ones the app sends back: a device's `id`, a pet's `id`,
  a timeline item's `id`.
"""
from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

from petkit_local.ai.pets import (
    face_acceptance, learned_weight_in_use, pet_weight_grams, reference_weight_grams,
)
from petkit_local.devices.base import Device
from petkit_local.devices.ble import BLEDevice
from petkit_local.events import decode
from petkit_local.events.sessions import content_of_row, served_amounts, session_filters
from petkit_local.ha.commands import DESTRUCTIVE_ACTIONS, device_actions
from petkit_local.ha.discovery import EntityDef
from petkit_local.ha.schedules import is_dual_hopper
from petkit_local.media import talk
from petkit_local.media.serving import has_media, session_media_slots
from petkit_local.utils.coerce import to_float, to_int
from petkit_local.utils.const import (
    DEVICE_TYPES_PURIFIER, DEVICE_TYPES_WATER_FOUNTAIN, device_display_name,
)
from petkit_local.utils.dicts import dig_path

API_PREFIX = "/app/v1"

#: Entity components that carry a value. Buttons are commands (a device's
#: `actions`), and `event`/`image` entities carry no state worth a value here.
VALUE_COMPONENTS = ("sensor", "binary_sensor", "switch", "number", "select", "text", "time")


def _int_list(raw: Any) -> list[int]:
    """A stored JSON array of ids (or an actual list) as ints; junk is skipped."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return []
    if not isinstance(raw, list):
        return []
    return [n for n in (to_int(x, None) for x in raw if not isinstance(x, bool)) if n is not None]


def _ts(value: Any) -> float | None:
    """A stored epoch as the API's time: None for missing or non-positive."""
    number = to_float(value, None)
    return number if number is not None and number > 0 else None


def media_url(rel: str | None) -> str | None:
    """The API path serving a media-root-relative file, each segment quoted."""
    return f"{API_PREFIX}/media/{quote(rel)}" if rel else None


def thumbnail_url(rel: str | None) -> str | None:
    """The API path serving that file's thumbnail."""
    return f"{API_PREFIX}/media/thumb/{quote(rel)}" if rel else None


# --- devices -------------------------------------------------------------------

def device_kind(d: Device) -> str:
    """What sort of thing this is, for the app's icon and grouping."""
    if d.is_litter:
        return "litter_box"
    if d.is_feeder:
        return "feeder"
    if d.is_water_fountain:
        return "fountain"
    if d.is_purifier:
        return "purifier"
    return "other"


def accessory_kind(a: BLEDevice) -> str:
    """`device_kind` for a Bluetooth accessory, which has a codename and no `Device`."""
    if a.ble_type in DEVICE_TYPES_WATER_FOUNTAIN:
        return "fountain"
    if a.ble_type in DEVICE_TYPES_PURIFIER:
        return "purifier"
    return "other"


def accessory_ref(a: BLEDevice) -> dict[str, Any]:
    """An accessory as its parent lists it: enough to know what it is.

    A litter box's page decides from `kind` alone whether it has a Pura Air
    to show, and fetches `/devices/{id}` for its readings.
    """
    return {"id": a.petkit_id, "type": a.ble_type, "kind": accessory_kind(a)}


def device_summary(d: Device, accessories: list[BLEDevice] | tuple = ()) -> dict[str, Any]:
    """One device in the list. `last_seen` is the latest contact on either transport.

    A device on MQTT stops polling HTTP altogether (AGENTS.md), so the HTTP
    timestamps alone would make the healthiest device look abandoned.

    `model` stays the codename's own name whatever the generation, as released
    apps read it; `model_name` is the full product name ("Pura Max 2") and
    `generation` the number behind it, null while unknown (`Device.generation`).

    `accessories` are the Bluetooth accessories it relays, in the order the
    registry holds them — which is also the order `BLERegistry.get_linked_k3`
    picks the one K3 the firmware has a slot for.

    `talk` is whether `/devices/{id}/talk` can start now
    (`media/talk.py::availability`): computed from what is recorded, never by
    reaching the device, so it is cheap enough for every summary — and in the
    summary rather than only the detail so that `/ws` announces the change
    when the patcher is applied or the address arrives.

    `supports_ai` is whether the device's own NPU recognises pets
    (`Device.supports_ai`): the devices a pet's face photos are worth assigning
    to. In the summary for the same reason as `talk` -- it can turn on once the
    device first asks for its recognition config, and `/ws` should say so.
    """
    last = max(d.last_seen or 0, d.last_mqtt or 0, d.last_heartbeat or 0,
               d.last_state_report or 0)
    return {
        "id": d.petkit_id,
        "type": d.device_type,
        "model": device_display_name(d.device_type),
        "model_name": d.model_name,
        "generation": d.generation,
        "kind": device_kind(d),
        "online": bool(d.online),
        "transport": "mqtt" if d.mqtt_connected else "http",
        "last_seen": _ts(last),
        "firmware": str(d.firmware) if d.firmware else None,
        "is_camera": bool(d.is_camera),
        "talk": talk.availability(d),
        "supports_ai": bool(d.supports_ai),
        "connection": "wifi",
        "parent_id": None,
        "accessories": [accessory_ref(a) for a in accessories],
    }


def accessory_summary(a: BLEDevice, parent: Device | None) -> dict[str, Any]:
    """A Bluetooth accessory in the device list, shaped like a device.

    It has no network identity of its own (`devices/ble/registry.py`): it is
    reachable while its parent is, over whichever transport the parent uses,
    and has no firmware version we know. `last_seen` is its own — when a frame
    from it last decoded, or for a K3, whose readings ride on its parent's
    reports rather than arriving as frames, when that report last carried them.

    `model`, `model_name` and `generation` mean what they do for a device: a
    CTW3 the owner named an EverSweet Max 2 in the panel has `model_name`
    "EverSweet Max 2" and `generation` 2 (`BLEDevice.generation`).
    """
    return {
        "id": a.petkit_id,
        "type": a.ble_type,
        "model": device_display_name(a.ble_type),
        "model_name": a.model_name,
        "generation": a.generation,
        "kind": accessory_kind(a),
        "online": bool(parent and parent.online),
        "transport": "mqtt" if parent and parent.mqtt_connected else "http",
        "last_seen": _ts(a.last_seen),
        "firmware": None,
        "is_camera": False,
        "talk": talk.UNSUPPORTED,
        "supports_ai": False,
        "connection": "ble",
        "parent_id": parent.petkit_id if parent else None,
        "accessories": [],
    }


def _display(entity: EntityDef, value: Any) -> str | None:
    """The option label an enum value stands for, as HA and the panel render it.

    `option_values` when the entity has them, the option's index otherwise —
    the same fallback `ha/discovery.py` and the panel's `entities.js` use.

    A value the select only shows (`readonly_options`, a CTW3's `battery`) has
    a label too, and it is NOT among `options`: that absence is how the app
    tells a state it can show from a choice it can offer.
    """
    if not entity.options or value is None:
        return None
    values = entity.option_values or list(range(len(entity.options)))
    for i, candidate in enumerate(values):
        if i < len(entity.options) and (candidate == value or str(candidate) == str(value)):
            return entity.options[i]
    for candidate, label in entity.readonly_options.items():
        if candidate == value or str(candidate) == str(value):
            return label
    return None


def entity_value(entity: EntityDef, doc: dict[str, Any]) -> dict[str, Any]:
    """One entity with its value resolved from the HA state document.

    The value is read with the entity's own `value_path`, so it is the value
    Home Assistant shows. A binary sensor or switch is a JSON boolean (HA
    tests the value for truth), `null` until the device has reported it.
    """
    raw = dig_path(doc, entity.value_path)
    value = bool(raw) if entity.component in ("binary_sensor", "switch") and raw is not None else raw
    return {
        "key": entity.key,
        "name": entity.name,
        "component": entity.component,
        "value": value,
        "display": _display(entity, raw),
        "unit": entity.unit or None,
        "device_class": entity.device_class or None,
        "category": entity.entity_category or None,
        "options": list(entity.options) if entity.component == "select" else None,
        "settable": entity.is_settable,
        "min": entity.min_value if entity.component == "number" else None,
        "max": entity.max_value if entity.component == "number" else None,
        "step": entity.step if entity.component == "number" else None,
    }


def entity_change(value: dict[str, Any]) -> dict[str, Any]:
    """The part of an `entity_value` that changes: what a live update carries."""
    return {"key": value["key"], "value": value["value"], "display": value["display"]}


def device_action(entity: EntityDef) -> dict[str, Any]:
    """One button the app may press. `destructive` ones need `{"confirm": true}`."""
    return {"key": entity.key, "name": entity.name,
            "destructive": entity.key in DESTRUCTIVE_ACTIONS}


def device_entities(entities: list[EntityDef], doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Every value-bearing entity of a device, with its value."""
    return [entity_value(e, doc) for e in entities if e.component in VALUE_COMPONENTS]


def _detail(summary: dict[str, Any], serial: str, entities: list[EntityDef],
            actions: list[EntityDef], doc: dict[str, Any]) -> dict[str, Any]:
    return {
        **summary,
        "serial": serial or None,
        "entities": device_entities(entities, doc),
        "actions": sorted((device_action(e) for e in actions),
                          key=lambda a: a["destructive"]),
    }


def device_detail(d: Device, entities: list[EntityDef], doc: dict[str, Any],
                  accessories: list[BLEDevice] | tuple = ()) -> dict[str, Any]:
    """A device's summary, every value-bearing entity, and the actions it offers.

    Actions are in the panel's order: destructive last, so a mis-tap lands on
    a safe button.
    """
    return _detail(device_summary(d, accessories), d.serial_number, entities,
                   device_actions(entities), doc)


def accessory_actions(entities: list[EntityDef]) -> list[EntityDef]:
    """An accessory's buttons. Each one is a frame (`devices/ble/ble_command_for`),
    so there is no `ALL_ACTIONS` entry to check them against."""
    return [e for e in entities if e.component == "button"]


def accessory_detail(a: BLEDevice, parent: Device | None,
                     entities: list[EntityDef]) -> dict[str, Any]:
    """`device_detail` for an accessory: its entities read from its own state.

    No adapter is needed for the values: an accessory's `value_path`s are
    `states.x`/`consumables.x`, and `BLEDevice.state` has exactly those
    sections — the document Home Assistant's templates read too.
    """
    return _detail(accessory_summary(a, parent), a.serial_number, entities,
                   accessory_actions(entities), a.state)


# --- feeding -------------------------------------------------------------------

def _meal_amounts(source: dict[str, Any], dual: bool) -> dict[str, Any]:
    """`amount` on a single hopper, `amount1`/`amount2` on a Dual-Hopper; the
    other side null, so the shape is the same for every feeder."""
    if dual:
        return {"amount": None, "amount1": to_int(source.get("a1"), None),
                "amount2": to_int(source.get("a2"), 0)}
    amount = to_int(source.get("a"), None)
    if amount is None:
        amount = to_int(source.get("a1"), None)  # a legacy single-hopper save
    return {"amount": amount, "amount1": None, "amount2": None}


def feeding_plan(d: Device) -> dict[str, Any]:
    """A feeder's recurring meals, and whether they are being served.

    `active` false is the pause: the meals are kept and the feeder is told it
    has none. Days are the device's own numbers, 1-7, which the panel renders
    as Sunday..Saturday; a meal's `time` is seconds since local midnight.
    """
    dual = is_dual_hopper(d)
    stored = d.config.get("feed_schedule")
    groups = stored.get("schedule") if isinstance(stored, dict) else None
    return {
        "device_id": d.petkit_id,
        "active": d.feed_schedule_active,
        "dual_hopper": dual,
        "groups": [{
            "days": sorted({n for n in (to_int(p, None) for p in str(g.get("re", "")).split(","))
                            if n is not None}),
            "meals": [{
                "id": str(m.get("id") or ""),
                "time": to_int(m.get("t"), 0),
                **_meal_amounts(m, dual),
            } for m in g.get("it") or [] if isinstance(m, dict)],
        } for g in groups or [] if isinstance(g, dict)],
    }


def deferred_feed(d: Device, entry: dict[str, Any]) -> dict[str, Any]:
    """One pending one-off feed."""
    return {"id": str(entry.get("id") or ""), "fire_at": _ts(entry.get("fire_at")),
            **_meal_amounts(entry, is_dual_hopper(d))}


# --- timeline ------------------------------------------------------------------

def media_slots(rows: list[dict[str, Any]], media_root: str) -> dict[str, Any] | None:
    """An episode's media as API URLs, or None when there is nothing to show.

    Built on `media/serving.py::session_media_slots`, the function the panel's
    Timeline uses, so the two agree about what is ready to play.
    """
    slots = session_media_slots(rows or [], media_root)
    if not has_media(slots):
        return None
    thumb_source = (slots["snapshot_url"] or slots["highlight_url"]
                    or slots["playback_url"] or slots["preview_url"])
    return {
        "thumbnail": thumbnail_url(thumb_source),
        "snapshot": media_url(slots["snapshot_url"]),
        "highlight": media_url(slots["highlight_url"]),
        "playback": media_url(slots["playback_url"]),
        "timelapse": media_url(slots["preview_url"]),
        "waste": [media_url(r) for r in slots["waste"]],
        "health": [media_url(r) for r in slots["health"]],
        "video_pending": bool(slots["video_pending"]),
    }


def pet_ref(pets: dict[int, dict[str, Any]], pet_id: Any) -> dict[str, Any] | None:
    """`{id, name}` for an attributed pet, None when the row is unattributed.

    A `pet_id` with no pet row is possible — a pet can be deleted while its
    events remain — and then the id is still given, the name is not.
    """
    if pet_id is None:
        return None
    pet = pets.get(pet_id)
    return {"id": pet_id, "name": pet.get("name") if pet else None}


def timeline_item(session: dict[str, Any], device_type: str | None,
                  pets: dict[int, dict[str, Any]], media_root: str) -> dict[str, Any]:
    """One card of the timeline: a grouped session (`events/sessions.py`).

    `label` is None for a visit, whose card the app builds from `pet`,
    `duration_sec` and `weight_g`, exactly as the panel does. `served` is a
    finished feed's output, so the app need not read each meal's event for it.
    """
    content = session.get("content") or {}
    weight = to_float(session.get("weight"), None)
    duration = to_float(session.get("duration_sec"), None)
    return {
        "id": session["id"],
        "kind": session["kind"],
        "filters": sorted(session_filters(session)),
        "device_id": session.get("device_id"),
        "device_type": device_type,
        "ts": _ts(session.get("ts")),
        "display_ts": _ts(session.get("display_ts") or session.get("ts")),
        "event_type": session.get("event_type"),
        "event_kind": session.get("event_kind"),
        "label": None if session["kind"] == "visit"
                 else decode.event_label(session.get("event_type"), content,
                                         device_type, session.get("state")),
        "pet": pet_ref(pets, session.get("pet_id")),
        "duration_sec": round(duration) if duration is not None else None,
        "weight_g": round(weight) if weight is not None else None,
        "served": served_amounts(session, device_type),
        "facts": decode.summary_bits(session.get("event_type"), content),
        "media": media_slots(session.get("media") or [], media_root),
        "steps": [{
            "id": e.get("id"),
            "ts": _ts(e.get("ts")),
            "label": decode.event_label(e.get("event_type"), e.get("content") or {},
                                        device_type, e.get("state")),
            "detail": bool(e.get("detail", False)),
        } for e in session.get("sub_events", [])],
    }


def event_detail(row: dict[str, Any], pets: dict[int, dict[str, Any]],
                 media_rows: list[dict[str, Any]], media_root: str) -> dict[str, Any]:
    """One stored event: what it was, its decoded fields, and its media.

    The raw device `content` and `state` stay out — they are the panel's
    debugging view, average over a kilobyte per event, and are not a stable
    shape.
    """
    event_type = row.get("event_type")
    device_type = row.get("device_type")
    content = content_of_row(row)
    return {
        "id": row.get("id"),
        "device_id": row.get("device_id"),
        "device_type": device_type,
        "ts": _ts(row.get("ts")),
        "event_type": event_type,
        "event_kind": row.get("event_kind"),
        "label": decode.event_label(event_type, content, device_type),
        "pet": pet_ref(pets, row.get("pet_id")),
        "facts": decode.summary_bits(event_type, content),
        "fields": [{"key": f.key, "label": f.label, "text": f.text}
                   for f in decode.decode_content(event_type, content, device_type)],
        "media": media_slots(media_rows, media_root),
    }


# --- pets ----------------------------------------------------------------------

def pet_face(pet: dict[str, Any], face: dict[str, Any], devices: Any) -> dict[str, Any]:
    """One reference photo of a pet. Its stored path stays private.

    `accepted_by` / `waiting_for` are the pet's devices that hold the photo and
    that have yet to (`ai/pets.py::face_acceptance`, from each device's
    `discernPic`); `devices` is the `DeviceRegistry`.
    """
    return {"id": face["id"],
            "photo": f"{API_PREFIX}/pets/{pet['id']}/faces/{face['id']}/photo",
            **face_acceptance(face["id"], pet, devices)}


def pet_summary(pet: dict[str, Any], faces: list[dict[str, Any]],
                devices: Any) -> dict[str, Any]:
    """One pet. `weight_g` is what a visit is compared with (`ai/pets.py`).

    `faces` are its reference photos (`PetRegistry.faces`), oldest first;
    `photo` is the first of them.
    """
    return {
        "id": pet["id"],
        "name": pet.get("name") or "",
        "device_ids": _int_list(pet.get("device_ids_json")),
        "entered_weight_g": pet_weight_grams(pet.get("weight")),
        "weight_g": reference_weight_grams(pet),
        "weight_learned": learned_weight_in_use(pet),
        "photo": f"{API_PREFIX}/pets/{pet['id']}/photo" if faces else None,
        "faces": [pet_face(pet, f, devices) for f in faces],
    }


def pet_stats(stats: dict[str, Any]) -> dict[str, Any]:
    """`EventStore.pet_visit_stats`, renamed into the API's units."""
    return {
        "median_weight_g": stats.get("weight"),
        "visits_today": int(stats.get("visits_today") or 0),
        "last_visit_ts": _ts(stats.get("last_visit_ts")),
        "last_visit_weight_g": stats.get("last_visit_weight"),
        "last_visit_duration_sec": stats.get("last_visit_duration"),
        "last_device_id": stats.get("last_device_id"),
    }
