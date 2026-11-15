"""`/app/v1/pets`: the pets, one pet's weight and visits, and its photo.

Pet rows come from `PetRegistry` and the visit statistics from
`EventStore.pet_visit_stats` — the same call behind each pet's Home Assistant
sensors, so the app and HA report one weight. Recent visits are the
timeline's own cards (`handlers/timeline.py::recent_visits`).
"""
from __future__ import annotations

import os
from typing import Any

from aiohttp import web

from petkit_local.app_api import serializers
from petkit_local.app_api.appkeys import DATA_DIR, EVENT_STORE, PET_REGISTRY
from petkit_local.app_api.handlers._common import path_int
from petkit_local.app_api.handlers.timeline import recent_visits
from petkit_local.app_api.middleware import ApiError
from petkit_local.utils.paths import UnsafePathError, safe_join


def _registry(request: web.Request) -> Any:
    registry = request.app.get(PET_REGISTRY)
    if registry is None:
        raise ApiError(503, "unavailable", "pets are not available")
    return registry


async def _pet(request: web.Request) -> tuple[Any, dict[str, Any]]:
    registry = _registry(request)
    pet = await registry.get(path_int(request, "id", "pet"))
    if pet is None:
        raise ApiError(404, "pet_not_found", "no such pet")
    return registry, pet


async def list_pets(request: web.Request) -> web.Response:
    """`GET /app/v1/pets` -> `{"pets": [Pet]}`, ordered by id."""
    registry = _registry(request)
    pets = [serializers.pet_summary(p, bool(await registry.faces(p["id"])))
            for p in await registry.all()]
    return web.json_response({"pets": pets})


async def pet_detail(request: web.Request) -> web.Response:
    """`GET /app/v1/pets/{id}` -> `{"pet": PetDetail}`: stats and recent visits."""
    registry, pet = await _pet(request)
    store = request.app.get(EVENT_STORE)
    stats = await store.pet_visit_stats(pet["id"]) if store is not None else {}
    return web.json_response({"pet": {
        **serializers.pet_summary(pet, bool(await registry.faces(pet["id"]))),
        "stats": serializers.pet_stats(stats),
        "recent_visits": await recent_visits(request, pet["id"]),
    }})


async def pet_photo(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/pets/{id}/photo` — the pet's first reference photo (a JPEG).

    The stored path is re-resolved under the faces directory rather than
    trusted, for the panel's reason (`web/api/pets.py::api_pet_face_photo`):
    `photo_path` is a writable column.
    """
    registry, pet = await _pet(request)
    faces = await registry.faces(pet["id"])
    stored = faces[0].get("photo_path") if faces else None
    if not stored:
        raise ApiError(404, "photo_not_found", "this pet has no photo")
    try:
        path = safe_join(os.path.join(request.app[DATA_DIR], "faces"), os.path.basename(stored))
    except UnsafePathError:
        raise ApiError(404, "photo_not_found", "this pet has no photo") from None
    if not os.path.isfile(path):
        raise ApiError(404, "photo_not_found", "this pet has no photo")
    return web.FileResponse(path)
