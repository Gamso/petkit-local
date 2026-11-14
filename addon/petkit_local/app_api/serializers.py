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

from petkit_local.ai.pets import learned_weight_in_use, pet_weight_grams, reference_weight_grams
from petkit_local.devices.base import Device
from petkit_local.events import decode
from petkit_local.events.sessions import content_of_row, session_filters
from petkit_local.ha.discovery import EntityDef
from petkit_local.media.serving import has_media, session_media_slots
from petkit_local.utils.coerce import to_float, to_int
from petkit_local.utils.const import device_display_name
from petkit_local.utils.dicts import dig_path

API_PREFIX = "/app/v1"

#: Entity components the read-only API shows. Buttons are commands (phase 2's
#: actions), and `event`/`image` entities carry no state worth a value here.
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


def device_summary(d: Device) -> dict[str, Any]:
    """One device in the list. `last_seen` is the latest contact on either transport.

    A device on MQTT stops polling HTTP altogether (AGENTS.md), so the HTTP
    timestamps alone would make the healthiest device look abandoned.
    """
    last = max(d.last_seen or 0, d.last_mqtt or 0, d.last_heartbeat or 0,
               d.last_state_report or 0)
    return {
        "id": d.petkit_id,
        "type": d.device_type,
        "model": device_display_name(d.device_type),
        "kind": device_kind(d),
        "online": bool(d.online),
        "transport": "mqtt" if d.mqtt_connected else "http",
        "last_seen": _ts(last),
        "firmware": str(d.firmware) if d.firmware else None,
        "is_camera": bool(d.is_camera),
    }


def _display(entity: EntityDef, value: Any) -> str | None:
    """The option label an enum value stands for, as HA and the panel render it.

    `option_values` when the entity has them, the option's index otherwise —
    the same fallback `ha/discovery.py` and the panel's `entities.js` use.
    """
    if not entity.options or value is None:
        return None
    values = entity.option_values or list(range(len(entity.options)))
    for i, candidate in enumerate(values):
        if i < len(entity.options) and (candidate == value or str(candidate) == str(value)):
            return entity.options[i]
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
    }


def device_detail(d: Device, entities: list[EntityDef], doc: dict[str, Any]) -> dict[str, Any]:
    """A device's summary plus every value-bearing entity."""
    return {
        **device_summary(d),
        "serial": d.serial_number or None,
        "entities": [entity_value(e, doc) for e in entities
                     if e.component in VALUE_COMPONENTS],
    }


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
    `duration_sec` and `weight_g`, exactly as the panel does.
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

def pet_summary(pet: dict[str, Any], has_photo: bool) -> dict[str, Any]:
    """One pet. `weight_g` is what a visit is compared with (`ai/pets.py`)."""
    return {
        "id": pet["id"],
        "name": pet.get("name") or "",
        "device_ids": _int_list(pet.get("device_ids_json")),
        "entered_weight_g": pet_weight_grams(pet.get("weight")),
        "weight_g": reference_weight_grams(pet),
        "weight_learned": learned_weight_in_use(pet),
        "photo": f"{API_PREFIX}/pets/{pet['id']}/photo" if has_photo else None,
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
