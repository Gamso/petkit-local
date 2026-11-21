"""`/app/v1/pets`: the pets, their reference face photos, one pet's weight and visits.

Pet rows come from `PetRegistry` and the visit statistics from
`EventStore.pet_visit_stats` — the same call behind each pet's Home Assistant
sensors, so the app and HA report one weight. Recent visits are the
timeline's own cards (`handlers/timeline.py::recent_visits`).

The writes are the panel's (`web/api/pets.py`), through the same registry
methods: `PetRegistry.edit` resets the learned weight when the typed one
changes, `face_refusal` turns an upload down for the panel's reasons,
`owned_face` checks that a face belongs to the pet in the path, and `add_face`
normalises the photo to the 224x224 crop the device's NPU is fed. A new or
edited pet is published to Home Assistant as the panel's is. Where the app
differs it is stricter, never looser: a field of the wrong type is refused
rather than read as "no value", and weights are grams, as everywhere in this
API.

Every write tells the live-updates feed (`UpdateFeed.pets_changed`), which
sends the changed pet to every open `/ws` socket.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from aiohttp import web

from petkit_local.ai.pets import (
    FACE_EMPTY, FACE_LIMIT, FACE_NOT_JPEG, entered_weight,
)
from petkit_local.app_api import serializers
from petkit_local.app_api.appkeys import (
    DATA_DIR, EVENT_STORE, HA_PUBLISHER, PET_REGISTRY, SESSION, UPDATE_FEED,
)
from petkit_local.app_api.handlers._common import json_body, path_int
from petkit_local.app_api.handlers.timeline import recent_visits
from petkit_local.app_api.middleware import ApiError
from petkit_local.events.store import MAX_FACES_PER_PET
from petkit_local.utils.paths import UnsafePathError, safe_join

log = logging.getLogger(__name__)

#: The largest face photo accepted, in bytes: the panel's own body cap. A
#: phone camera's JPEG is 2-8 MB, and it is cropped to 224x224 on arrival.
MAX_FACE_BYTES = 16 * 1024 * 1024

#: The heaviest weight that can be typed, in grams. Stored as kilograms, and a
#: stored value above 100 is read back as grams (`ai/pets.py::pet_weight_grams`).
MAX_WEIGHT_G = 100_000


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


async def _face(request: web.Request) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """The pet and the face in the path. A face of ANOTHER pet is not found."""
    registry, pet = await _pet(request)
    face = await registry.owned_face(pet["id"], path_int(request, "face_id", "face"))
    if face is None:
        raise ApiError(404, "face_not_found", "this pet has no such photo")
    return registry, pet, face


async def _summary(registry: Any, pet: dict[str, Any]) -> dict[str, Any]:
    return serializers.pet_summary(pet, await registry.faces(pet["id"]))


def _photo_file(request: web.Request, face: dict[str, Any] | None) -> str:
    """A face's file, re-resolved under the faces directory rather than trusted.

    For the panel's reason (`web/api/pets.py::api_pet_face_photo`):
    `photo_path` is a writable column.

    Raises:
        ApiError: 404 `photo_not_found`.
    """
    stored = face.get("photo_path") if face else None
    if not stored:
        raise ApiError(404, "photo_not_found", "this pet has no photo")
    try:
        path = safe_join(os.path.join(request.app[DATA_DIR], "faces"), os.path.basename(stored))
    except UnsafePathError:
        raise ApiError(404, "photo_not_found", "this pet has no photo") from None
    if not os.path.isfile(path):
        raise ApiError(404, "photo_not_found", "this pet has no photo")
    return path


async def _changed(request: web.Request, summary: str,
                   pet: dict[str, Any] | None = None) -> None:
    """After a write: log it, tell the phones, and Home Assistant if `pet` is given.

    Home Assistant is told of a new or edited pet only, as the panel tells it.
    """
    session = request.get(SESSION)
    log.info("app API: %s from %r", summary, session.name if session else "?")
    request.app[UPDATE_FEED].pets_changed()
    publisher = request.app.get(HA_PUBLISHER)
    if pet is not None and publisher is not None:
        await publisher.publish_pet_discovery(pet)


def _invalid(message: str) -> ApiError:
    return ApiError(400, "invalid_pet", message)


def _pet_fields(body: dict[str, Any]) -> dict[str, Any]:
    """The `PetRegistry` columns a create or edit body sets; absent keys stay out.

    Raises:
        ApiError: 400 `invalid_pet` for a field of the wrong type or range.
    """
    fields: dict[str, Any] = {}
    if "name" in body:
        name = body["name"]
        if not isinstance(name, str) or not name.strip():
            raise _invalid("name must be a non-empty string")
        fields["name"] = name.strip()
    if "weight_g" in body:
        grams = body["weight_g"]
        if grams is not None and (isinstance(grams, bool) or not isinstance(grams, int | float)
                                  or not 0 < grams <= MAX_WEIGHT_G):
            raise _invalid(f"weight_g must be null or a number of grams, 0 < weight_g <= "
                           f"{MAX_WEIGHT_G}")
        fields["weight"] = entered_weight(grams / 1000) if grams is not None else None
    if "device_ids" in body:
        ids = body["device_ids"]
        if not isinstance(ids, list) or not all(
                isinstance(i, int) and not isinstance(i, bool) for i in ids):
            raise _invalid("device_ids must be a list of device ids")
        fields["device_ids_json"] = list(dict.fromkeys(ids))
    return fields


async def list_pets(request: web.Request) -> web.Response:
    """`GET /app/v1/pets` -> `{"pets": [Pet]}`, ordered by id."""
    registry = _registry(request)
    return web.json_response({"pets": [await _summary(registry, p) for p in await registry.all()]})


async def create_pet(request: web.Request) -> web.Response:
    """`POST /app/v1/pets {name, weight_g?, device_ids?}` -> 201 `{"pet": Pet}`.

    Raises:
        ApiError: 400 `invalid_pet` without a name or for a bad field.
    """
    registry = _registry(request)
    fields = _pet_fields(await json_body(request))
    if "name" not in fields:
        raise _invalid("name is required")
    pet = await registry.create(fields["name"], device_ids=fields.get("device_ids_json"),
                                weight=fields.get("weight"))
    await _changed(request, f"pet {pet['id']} created", pet)
    return web.json_response({"pet": await _summary(registry, pet)}, status=201)


async def pet_detail(request: web.Request) -> web.Response:
    """`GET /app/v1/pets/{id}` -> `{"pet": PetDetail}`: stats and recent visits."""
    registry, pet = await _pet(request)
    store = request.app.get(EVENT_STORE)
    stats = await store.pet_visit_stats(pet["id"]) if store is not None else {}
    return web.json_response({"pet": {
        **await _summary(registry, pet),
        "stats": serializers.pet_stats(stats),
        "recent_visits": await recent_visits(request, pet["id"]),
    }})


async def update_pet(request: web.Request) -> web.Response:
    """`PATCH /app/v1/pets/{id} {name?, weight_g?, device_ids?, reset_learned_weight?}`.

    Only the fields sent change. A `weight_g` that changes the weight, or
    `reset_learned_weight: true`, also forgets the learned weight
    (`PetRegistry.edit`). Answers `{"pet": Pet}`.

    Raises:
        ApiError: 400 `invalid_pet` for a bad field, 400 `bad_request` for
            a body with none of them.
    """
    registry, pet = await _pet(request)
    body = await json_body(request)
    fields = _pet_fields(body)
    reset = body.get("reset_learned_weight", False)
    if not isinstance(reset, bool):
        raise _invalid("reset_learned_weight must be a boolean")
    if not fields and not reset:
        raise ApiError(400, "bad_request",
                       "send name, weight_g, device_ids or reset_learned_weight")
    pet = await registry.edit(pet["id"], reset_learned=reset, **fields)
    if pet is None:  # deleted in between
        raise ApiError(404, "pet_not_found", "no such pet")
    await _changed(request, f"pet {pet['id']} edited", pet)
    return web.json_response({"pet": await _summary(registry, pet)})


async def delete_pet(request: web.Request) -> web.Response:
    """`DELETE /app/v1/pets/{id}` -> `{"ok": true}`, with every face photo it had.

    Its visits stay in the history, attributed by id with no name, as after a
    delete in the panel.
    """
    registry, pet = await _pet(request)
    if not await registry.delete(pet["id"]):
        raise ApiError(404, "pet_not_found", "no such pet")
    await _changed(request, f"pet {pet['id']} deleted")
    return web.json_response({"ok": True})


async def pet_photo(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/pets/{id}/photo` — the pet's first reference photo (a JPEG)."""
    registry, pet = await _pet(request)
    faces = await registry.faces(pet["id"])
    return web.FileResponse(_photo_file(request, faces[0] if faces else None))


async def list_faces(request: web.Request) -> web.Response:
    """`GET /app/v1/pets/{id}/faces` -> `{"faces": [PetFace], "max_faces": int}`."""
    registry, pet = await _pet(request)
    return web.json_response({
        "faces": [serializers.pet_face(pet["id"], f) for f in await registry.faces(pet["id"])],
        "max_faces": MAX_FACES_PER_PET,
    })


async def _read_photo(request: web.Request) -> bytes:
    """The request body, refused once it passes `MAX_FACE_BYTES`.

    Read from the stream in chunks rather than with `read()`: that is bound by
    the application's `client_max_size`, which is sized for JSON, and a
    `Content-Length` is not a promise, so the cap holds while reading.

    Raises:
        ApiError: 413 `photo_too_large`.
    """
    too_large = ApiError(413, "photo_too_large",
                         f"a photo may be at most {MAX_FACE_BYTES // (1024 * 1024)} MiB")
    if (request.content_length or 0) > MAX_FACE_BYTES:
        raise too_large
    data = bytearray()
    async for chunk in request.content.iter_chunked(64 * 1024):
        data += chunk
        if len(data) > MAX_FACE_BYTES:
            raise too_large
    return bytes(data)


async def add_face(request: web.Request) -> web.Response:
    """`POST /app/v1/pets/{id}/faces`, the body a raw JPEG -> 201 `{"face": PetFace}`.

    Not a multipart form, as in the panel. The photo is cropped to a centred
    square and scaled to 224x224 before it is stored (`PetRegistry.add_face`).

    Raises:
        ApiError: 400 `bad_request` for an empty body, 413 `photo_too_large`,
            415 `not_jpeg`, 409 `too_many_faces` once the pet holds
            `MAX_FACES_PER_PET`, 500 `photo_not_stored` when it could not be
            written.
    """
    registry, pet = await _pet(request)
    data = await _read_photo(request)
    refusal = await registry.face_refusal(pet["id"], data)
    if refusal == FACE_EMPTY:
        raise ApiError(400, "bad_request", "send the photo as the request body (image/jpeg)")
    if refusal == FACE_NOT_JPEG:
        raise ApiError(415, "not_jpeg",
                       "the body must be the bytes of a JPEG photo (image/jpeg), not a form")
    if refusal == FACE_LIMIT:
        raise ApiError(409, "too_many_faces", f"a pet has at most {MAX_FACES_PER_PET} photos")
    face = await registry.add_face(pet["id"], data)
    if face is None:
        # Another upload may have taken the last place while this one was read.
        if await registry.face_refusal(pet["id"], data) == FACE_LIMIT:
            raise ApiError(409, "too_many_faces", f"a pet has at most {MAX_FACES_PER_PET} photos")
        raise ApiError(500, "photo_not_stored", "the photo could not be stored")
    await _changed(request, f"photo {face['id']} added to pet {pet['id']}")
    return web.json_response({"face": serializers.pet_face(pet["id"], face)}, status=201)


async def delete_face(request: web.Request) -> web.Response:
    """`DELETE /app/v1/pets/{id}/faces/{face_id}` -> `{"ok": true}`.

    The device keeps what it learned from the photo until its next
    `dev_discern_pic` poll, so recognition does not change at once.
    """
    registry, pet, face = await _face(request)
    if not await registry.delete_face(face["id"]):
        raise ApiError(404, "face_not_found", "this pet has no such photo")
    await _changed(request, f"photo {face['id']} removed from pet {pet['id']}")
    return web.json_response({"ok": True})


async def face_photo(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/pets/{id}/faces/{face_id}/photo` — one reference photo (a JPEG)."""
    *_, face = await _face(request)
    return web.FileResponse(_photo_file(request, face))
