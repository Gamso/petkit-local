"""`/app/v1/timeline`: the grouped history, paged by cursor, and one event.

The rows come from `EventStore.query_timeline` and are grouped by
`events/sessions.py::group_sessions` — the panel's Timeline makes the same two
calls. What differs is the paging. The panel asks for one local day; a phone
scrolls back through all of history, so this pages by an opaque cursor instead.

Why that is not a plain `WHERE ts < cursor LIMIT n`
---------------------------------------------------
A card is a SESSION, built from several rows, and grouping is a heuristic over
time windows: a cleaning reported up to `SUB_EVENT_WINDOW_SEC` after a visit
joins that visit's card, and a session's rows can sit on both sides of any
cut. Cut the rows at the cursor and the older half of a card already shown
comes back on the next page as a card of its own.

So the cursor is the sort key `(ts, id)` of the last CARD returned, and each
page groups a window of rows that reaches `SPAN_MARGIN` past it on both
sides: above, so the cards already shown are rebuilt whole and filtered out
by key rather than split; below, so a card is only returned once the rows
under it are all in the window. If the row budget ran out first, the cards
within `SPAN_MARGIN` of the oldest row fetched are held back for the next
page, whose window will contain them whole.

The trade-off is that a card whose rows span more than `SPAN_MARGIN` can be
split. Grouping itself never joins rows further apart than its own windows,
of which `SPAN_MARGIN` is several times the largest.
"""
from __future__ import annotations

import base64
import binascii
import json
import time
from typing import Any

from aiohttp import web

from petkit_local.app_api import serializers
from petkit_local.app_api.appkeys import EVENT_STORE, MEDIA_ROOT, PET_REGISTRY, REGISTRY
from petkit_local.app_api.handlers._common import path_int, query_int
from petkit_local.app_api.middleware import ApiError
from petkit_local.events import ingest
from petkit_local.events.sessions import FILTERS, SUB_EVENT_WINDOW_SEC
from petkit_local.utils.timeutil import local_day_bounds, parse_date

DEFAULT_LIMIT = 30
MAX_LIMIT = 100
#: Rows grouped per page at most — the panel's own per-day ceiling.
ROW_BUDGET = 2000
#: How far a session's rows may lie from its sort key and still be grouped
#: whole. Six times the widest grouping window (`SUB_EVENT_WINDOW_SEC`).
SPAN_MARGIN = 6 * SUB_EVENT_WINDOW_SEC


def _store(request: web.Request) -> Any:
    store = request.app.get(EVENT_STORE)
    if store is None:
        raise ApiError(503, "unavailable", "the event store is not available")
    return store


def encode_cursor(ts: float, item_id: int) -> str:
    """An opaque continuation token: the sort key of the last card returned."""
    raw = json.dumps([ts, item_id], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(token: str) -> tuple[float, int]:
    """The `(ts, id)` a cursor carries.

    Raises:
        ApiError: 400 for anything this server did not hand out.
    """
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        ts, item_id = json.loads(raw)
        if isinstance(ts, bool) or isinstance(item_id, bool):
            raise ValueError
        return float(ts), int(item_id)
    except (binascii.Error, ValueError, TypeError, UnicodeDecodeError):
        raise ApiError(400, "bad_cursor", "cursor is not one this server issued") from None


def _key(session: dict[str, Any]) -> tuple[float, int]:
    return float(session.get("ts") or 0), int(session.get("id") or 0)


async def pets_by_id(request: web.Request) -> dict[int, dict[str, Any]]:
    """Every pet row keyed by id; `{}` without a pet registry."""
    registry = request.app.get(PET_REGISTRY)
    if registry is None:
        return {}
    return {p["id"]: p for p in await registry.all()}


def serialize_sessions(request: web.Request, sessions: list[dict[str, Any]],
                       pets: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    """Cards as the API's `TimelineItem`, with each one's device codename."""
    registry = request.app[REGISTRY]
    media_root = request.app[MEDIA_ROOT]
    items = []
    for s in sessions:
        device = registry.get(s.get("device_id"))
        # The stored row's codename first, as the panel does: a numeric code
        # means different things per category, and the device may be gone.
        device_type = s.get("device_type") or (device.device_type if device else None)
        items.append(serializers.timeline_item(s, device_type, pets, media_root))
    return items


async def timeline(request: web.Request) -> web.Response:
    """`GET /app/v1/timeline?device=&day=&kind=&pet=&limit=&cursor=`.

    Newest first. `day` (`YYYY-MM-DD`) restricts to one LOCAL day, cut at
    local midnight like everything else here. `kind` is one of the panel's
    filter chips. `next_cursor` is null on the last page.
    """
    store = _store(request)
    device_id = query_int(request, "device")
    pet_id = query_int(request, "pet")
    limit = query_int(request, "limit", default=DEFAULT_LIMIT, minimum=1, maximum=MAX_LIMIT)
    kind = request.query.get("kind", "all") or "all"
    if kind != "all" and kind not in FILTERS:
        raise ApiError(400, "bad_parameter", f"kind must be one of: all, {', '.join(FILTERS)}")

    lower: float | None = None
    upper: float | None = None
    day = request.query.get("day")
    if day:
        if parse_date(day) is None:
            raise ApiError(400, "bad_parameter", "day must be YYYY-MM-DD")
        lower, upper, _label = local_day_bounds(day)

    cursor = decode_cursor(request.query["cursor"]) if request.query.get("cursor") else None
    if cursor is not None:
        reach = cursor[0] + SPAN_MARGIN
        upper = reach if upper is None else min(upper, reach)

    rows = await store.query_timeline(device_id=device_id, start_ts=lower, end_ts=upper,
                                      limit=ROW_BUDGET)
    truncated = len(rows) >= ROW_BUDGET
    sessions = ingest.group_sessions(rows)
    sessions.sort(key=_key, reverse=True)
    if pet_id is not None:
        sessions = [s for s in sessions if s.get("pet_id") == pet_id]
    sessions = [s for s in sessions if ingest.matches_filter(s, kind)]
    if cursor is not None:
        sessions = [s for s in sessions if _key(s) < cursor]

    if truncated and rows:
        floor = min(float(r.get("ts") or 0) for r in rows)
        complete = [s for s in sessions if _key(s)[0] >= floor + SPAN_MARGIN]
        # A window so dense that no card clears the margin: serve what there
        # is rather than an empty page with a cursor that never moves.
        if complete:
            sessions = complete

    page = sessions[:limit]
    more = len(sessions) > limit or truncated
    next_cursor = encode_cursor(*_key(page[-1])) if page and more else None
    pets = await pets_by_id(request)
    return web.json_response({
        "items": serialize_sessions(request, page, pets),
        "next_cursor": next_cursor,
    })


async def timeline_event(request: web.Request) -> web.Response:
    """`GET /app/v1/timeline/{event_id}` -> `{"event": EventDetail}`.

    `event_id` is a timeline item's `id` (or a step's): the stored row
    anchoring it.
    """
    store = _store(request)
    row = await store.get_event(path_int(request, "event_id", "event"))
    if row is None:
        raise ApiError(404, "event_not_found", "no such event")
    related = row.get("related_event")
    media_rows = await store.media_for_related_event(related) if related else []
    detail = serializers.event_detail(row, await pets_by_id(request), media_rows,
                                      request.app[MEDIA_ROOT])
    return web.json_response({"event": detail})


async def recent_visits(request: web.Request, pet_id: int, *, days: int = 7,
                        limit: int = 20) -> list[dict[str, Any]]:
    """A pet's newest visits as `TimelineItem`s — the timeline's own grouping."""
    store = request.app.get(EVENT_STORE)
    if store is None:
        return []
    rows = await store.query_timeline(start_ts=time.time() - days * 86400, limit=ROW_BUDGET)
    visits = [s for s in ingest.group_sessions(rows)
              if s.get("kind") == "visit" and s.get("pet_id") == pet_id]
    visits.sort(key=_key, reverse=True)
    return serialize_sessions(request, visits[:limit], await pets_by_id(request))
