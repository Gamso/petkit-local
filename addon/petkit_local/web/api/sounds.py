"""Custom sounds: upload, list, delete, play and select.

A camera feeder can play custom audio through its speaker. The flow:

1. Upload an audio file through this API; it is converted to the one format
   the device plays (``media/sounds.py``)
2. The sound appears in ``dev_sound_get`` for the device to download
3. ``property.set{selectedSound: N}`` selects the active sound
4. ``thing/service/play_sound{soundId: N}`` plays it

Sound files live in ``{data_dir}/sounds/{device_id}/`` with a metadata
sidecar ``sounds.json``. The device downloads them over plain HTTP from the
device API port, at ``/sounds/{device_id}/{filename}``
(``http/handlers/stubs.py::handle_sound_download``); the bucket serves the
same path, but over TLS the feeder's downloader cannot speak.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any
from urllib.parse import urlparse

from aiohttp import web

from petkit_local.ha.commands import make_mqtt_property_set, _envelope
from petkit_local.media.sounds import (
    META_LOCK, TranscodeError, describe, transcode_to_adts,
)
from petkit_local.utils.coerce import to_int
from petkit_local.utils.jsonio import atomic_write_bytes, atomic_write_json, read_json
from petkit_local.utils.paths import UnsafePathError, safe_join, sanitize_filename
from petkit_local.web.api._common import _device_or_404, _deliver, _refuse
from petkit_local.web.appkeys import BRIDGE, CFG, HUB

log = logging.getLogger(__name__)

MAX_SOUND_SIZE = 2 * 1024 * 1024  # 2 MB
MAX_SOUNDS_PER_DEVICE = 10


# The helpers below take `data_dir`, not the request: `sound_list_for_device`
# is also called from the DEVICE app's `dev_sound_get`, whose config lives under
# `app["config"]`. Reading the panel's `app["cfg"]` there raised KeyError on
# every poll, the never-fail middleware answered an empty success, and no
# uploaded sound ever reached a device.
def _data_dir(request: web.Request) -> str:
    """The PANEL app's data directory."""
    return request.app[CFG].get("data_dir", "/data")


def _sounds_dir(data_dir: str, device_id: int) -> str:
    return os.path.join(data_dir, "sounds", str(device_id))


def _sounds_meta_path(data_dir: str, device_id: int) -> str:
    return os.path.join(_sounds_dir(data_dir, device_id), "sounds.json")


def _load_sounds(data_dir: str, device_id: int) -> list[dict[str, Any]]:
    """The device's sound entries; `[]` for a missing or damaged list.

    Entries without an integer `id` and a string `filename` are dropped here:
    the list is read on the device-facing server too, where a KeyError is an
    answer the device never gets.
    """
    sounds = read_json(_sounds_meta_path(data_dir, device_id), [])
    if not isinstance(sounds, list):
        return []
    return [s for s in sounds
            if isinstance(s, dict) and type(s.get("id")) is int
            and isinstance(s.get("filename"), str)]


def _save_sounds(data_dir: str, device_id: int,
                 sounds: list[dict[str, Any]]) -> None:
    atomic_write_json(_sounds_meta_path(data_dir, device_id), sounds)


def _sound_path(data_dir: str, device_id: int, filename: str) -> str | None:
    """Where an entry's file is, or None if its name leaves the device's folder."""
    try:
        return safe_join(_sounds_dir(data_dir, device_id), filename)
    except UnsafePathError:
        return None


def _next_id(sounds: list[dict[str, Any]]) -> int:
    if not sounds:
        return 1
    return max(s["id"] for s in sounds) + 1


def sound_download_base(api_url: Any, request_host: str = "") -> str:
    """`http://<api host>`: where a device fetches its sounds from.

    The API host, not the bucket: the bucket is HTTPS, and the BusyBox wget
    the firmware downloads with cannot complete a TLS handshake with it on
    the Axera YumShare -- see `handlers/stubs.py::handle_sound_get` for what
    the failed download did to the device. Built exactly as the face-photo
    URLs are (`handle_discern_pic`): the host in `api_url` where there is one,
    because the device fetches in a separate connection and needs an address
    it can reach on its own, else the host it dialled us on.

    Returns:
        The base URL without a trailing slash, or "" when neither is known.
    """
    host = urlparse(api_url if isinstance(api_url, str) else "").netloc or request_host
    return f"http://{host}" if host else ""


def sound_list_for_device(data_dir: str, device_id: int,
                          base_url: str) -> list[dict[str, Any]]:
    """Build the ``dev_sound_get`` result list with download URLs under `base_url`.

    Called from both apps, so it is given the values it needs rather than a
    request to dig them out of. An entry whose file is not on disk is left
    out: the device fetches every URL it is handed.
    """
    sounds = _load_sounds(data_dir, device_id)
    if not sounds or not base_url:
        return []
    base = base_url.rstrip("/")
    result = []
    for s in sounds:
        path = _sound_path(data_dir, device_id, s["filename"])
        if path is None or not os.path.isfile(path):
            continue
        result.append({
            "id": s["id"],
            "name": str(s.get("name", "")),
            "duration": to_int(s.get("duration"), 0),
            "url": f"{base}/sounds/{device_id}/{s['filename']}",
            "digest": str(s.get("digest", "")),
            "size": to_int(s.get("size"), 0),
        })
    return result


def _hub_and_bridge(request: web.Request) -> tuple[Any, Any]:
    """The panel's event hub and MQTT bridge.

    Play and Select looked the hub up as "event_hub", which is not a key of
    the panel app (`web/appkeys.py`), so every click answered 400 "no event hub".
    """
    return request.app.get(HUB), request.app.get(BRIDGE)


async def api_sounds_list(request: web.Request) -> web.Response:
    d = _device_or_404(request)
    sounds = _load_sounds(_data_dir(request), d.petkit_id)
    return web.json_response({"sounds": sounds})


async def _read_capped(field: Any, limit: int) -> bytes | None:
    """The upload's bytes, or None once it passes `limit`.

    Read in chunks so the cap holds WHILE reading: `field.read()` takes the
    whole part into memory before anything can measure it, and a multipart
    body is not bound by the application's `client_max_size`.
    """
    buf = bytearray()
    while chunk := await field.read_chunk(64 * 1024):
        buf += chunk
        if len(buf) > limit:
            return None
    return bytes(buf)


async def api_sounds_upload(request: web.Request) -> web.Response:
    d = _device_or_404(request)
    data_dir = _data_dir(request)
    if len(_load_sounds(data_dir, d.petkit_id)) >= MAX_SOUNDS_PER_DEVICE:
        raise _refuse(web.HTTPBadRequest, f"max {MAX_SOUNDS_PER_DEVICE} sounds")

    reader = await request.multipart()
    field = await reader.next()
    if field is None or field.name != "file":
        raise _refuse(web.HTTPBadRequest, "missing file field")

    # A label only: it is shown in the panel and never becomes a path.
    name = sanitize_filename(os.path.splitext(os.path.basename(field.filename or ""))[0],
                             fallback="sound", max_length=64)
    data = await _read_capped(field, MAX_SOUND_SIZE)
    if data is None:
        raise _refuse(web.HTTPBadRequest, f"file too large (max {MAX_SOUND_SIZE // 1024}KB)")
    if not data:
        raise _refuse(web.HTTPBadRequest, "empty file")

    # The device keeps a download as `/opt/user_feed_over_<id>.aac` and plays
    # it through the ADTS parser its own prompts use, whatever the upload
    # was -- an .m4a from a phone went down fine and never made a sound. So
    # every upload becomes ADTS AAC-LC 16 kHz mono here (`media/sounds.py`),
    # and the entry carries the duration the device is told.
    try:
        adts = await transcode_to_adts(data)
    except TranscodeError as e:
        raise _refuse(web.HTTPBadRequest, f"could not convert {name}: {e}") from None

    # Read again under the lock: the conversion was awaited, and a second
    # upload (or the startup conversion) may have written the list since.
    async with META_LOCK:
        sounds = _load_sounds(data_dir, d.petkit_id)
        if len(sounds) >= MAX_SOUNDS_PER_DEVICE:
            raise _refuse(web.HTTPBadRequest, f"max {MAX_SOUNDS_PER_DEVICE} sounds")
        sound_id = _next_id(sounds)
        filename = f"sound_{sound_id}.aac"  # always .aac, whatever came in
        entry: dict[str, Any] = {
            "id": sound_id,
            "name": name,
            "filename": filename,
            **describe(adts),
            "uploaded_at": int(time.time()),
        }
        sounds.append(entry)
        await asyncio.to_thread(
            atomic_write_bytes, os.path.join(_sounds_dir(data_dir, d.petkit_id), filename), adts)
        await asyncio.to_thread(_save_sounds, data_dir, d.petkit_id, sounds)

    # No request-host fallback here: this request came to the PANEL, and its
    # host is not one a device downloads from.
    base_url = sound_download_base(request.app[CFG].get("api_url", ""))
    if base_url:
        sound_list = sound_list_for_device(data_dir, d.petkit_id, base_url)
        hub, bridge = _hub_and_bridge(request)
        if hub and bridge:
            envelope = make_mqtt_property_set({"soundList": sound_list})
            await _deliver(hub, bridge, d, "property/set", envelope)

    log.info("Uploaded sound %d for device %d: %s (%d bytes in, %d bytes ADTS, %d s)",
             sound_id, d.petkit_id, filename, len(data), entry["size"], entry["duration"])
    return web.json_response({"ok": True, "sound": entry})


async def api_sounds_delete(request: web.Request) -> web.Response:
    d = _device_or_404(request)
    try:
        sound_id = int(request.match_info["sound_id"])
    except (ValueError, KeyError):
        raise _refuse(web.HTTPBadRequest, "bad sound_id") from None

    data_dir = _data_dir(request)
    async with META_LOCK:
        sounds = _load_sounds(data_dir, d.petkit_id)
        entry = next((s for s in sounds if s["id"] == sound_id), None)
        if entry is None:
            raise _refuse(web.HTTPNotFound, "sound not found")

        filepath = _sound_path(data_dir, d.petkit_id, entry["filename"])
        if filepath and os.path.isfile(filepath):
            os.remove(filepath)

        sounds = [s for s in sounds if s["id"] != sound_id]
        _save_sounds(data_dir, d.petkit_id, sounds)

    log.info("Deleted sound %d for device %d", sound_id, d.petkit_id)
    return web.json_response({"ok": True})


async def api_sounds_play(request: web.Request) -> web.Response:
    d = _device_or_404(request)
    try:
        sound_id = int(request.match_info["sound_id"])
    except (ValueError, KeyError):
        raise _refuse(web.HTTPBadRequest, "bad sound_id") from None

    hub, bridge = _hub_and_bridge(request)
    if not hub:
        raise _refuse(web.HTTPBadRequest, "no event hub")

    envelope = _envelope("thing.service.play_sound", {"soundId": sound_id})
    return await _deliver(hub, bridge, d, "play_sound", envelope)


async def api_sounds_select(request: web.Request) -> web.Response:
    d = _device_or_404(request)
    try:
        sound_id = int(request.match_info["sound_id"])
    except (ValueError, KeyError):
        raise _refuse(web.HTTPBadRequest, "bad sound_id") from None

    d.config.setdefault("settings", {})["selectedSound"] = sound_id

    hub, bridge = _hub_and_bridge(request)
    if not hub:
        raise _refuse(web.HTTPBadRequest, "no event hub")

    envelope = make_mqtt_property_set({"selectedSound": sound_id})
    return await _deliver(hub, bridge, d, "property/set", envelope)
