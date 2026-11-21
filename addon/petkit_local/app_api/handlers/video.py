"""`/app/v1/devices/{id}/live`: a camera's live view, as fMP4 over a WebSocket or HLS.

The mechanics — caps, the relay, HLS rewriting — are `app_api/video.py`'s; this
module only resolves the device and turns each refusal into the API's error.
Both routes are authenticated like every other one: the bearer header is
checked by the middleware on the upgrade request and on every HLS request.
"""
from __future__ import annotations

import asyncio

import aiohttp
from aiohttp import web

from petkit_local.app_api.appkeys import AUTH, GO2RTC, REGISTRY, SESSION, VIDEO_SESSIONS
from petkit_local.app_api.handlers._common import path_int
from petkit_local.app_api.video import (
    CLIENT_HEARTBEAT_SECONDS, CLIENT_MAX_MESSAGE, HLS_FILES, RELAYED_OPEN_TIMEOUT,
    UPSTREAM_MAX_MESSAGE, UPSTREAM_TIMEOUT, device_stream, master_playlist, valid_hls_id,
    valid_segment_number,
)
from petkit_local.app_api.middleware import ApiError
from petkit_local.patchers.camera import is_relayed


def _stream(request: web.Request) -> tuple[int, str]:
    """`(device id, go2rtc stream name)` for the path's device, or the refusal.

    404 for no such device and for one without a stream to watch; 503 when
    there is a stream but go2rtc, which serves it, is not running.
    """
    device = request.app[REGISTRY].get(path_int(request, "id", "device"))
    if device is None:
        raise ApiError(404, "device_not_found", "no such device")
    go2rtc = request.app[GO2RTC]
    if go2rtc is None:
        raise ApiError(503, "live_unavailable", "live video is not available on this server")
    name = device_stream(device, go2rtc)
    if name is None:
        raise ApiError(404, "stream_not_found",
                       "this device has no live stream (not a camera, the camera patch "
                       "is not applied, or the camera is not reachable)")
    if not go2rtc.running:
        raise ApiError(503, "live_unavailable", "go2rtc is not running; try again shortly")
    return device.petkit_id, name


def _signed_in(request: web.Request):
    auth, session_id = request.app[AUTH], request[SESSION].id
    return lambda: auth.password_set and any(s.id == session_id for s in auth.sessions())


async def video_ws(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/devices/{id}/live` — WebSocket upgrade to the camera's fMP4 feed."""
    device_id, name = _stream(request)
    client = web.WebSocketResponse(heartbeat=CLIENT_HEARTBEAT_SECONDS,
                                   max_msg_size=CLIENT_MAX_MESSAGE, compress=False)
    if not client.can_prepare(request).ok:
        raise ApiError(426, "upgrade_required",
                       "open this route as a WebSocket", {"Upgrade": "websocket"})
    sessions = request.app[VIDEO_SESSIONS]
    slot = sessions.admit(request[SESSION].id, device_id, "ws")
    try:
        async with aiohttp.ClientSession() as http:
            # Dialled BEFORE the phone's upgrade is accepted, so a dead go2rtc
            # is a 503 the app can read rather than a socket that opens and
            # closes at once.
            try:
                upstream = await asyncio.wait_for(http.ws_connect(
                    f"ws://{sessions.go2rtc_api}/api/ws?src={name}",
                    timeout=aiohttp.ClientWSTimeout(ws_close=UPSTREAM_TIMEOUT),
                    max_msg_size=UPSTREAM_MAX_MESSAGE), UPSTREAM_TIMEOUT)
            except (aiohttp.ClientError, ConnectionError, TimeoutError):
                raise ApiError(503, "live_unavailable", "go2rtc did not answer") from None
            await client.prepare(request)
            await sessions.relay(slot, request, client, upstream, _signed_in(request))
    finally:
        sessions.release(slot)
    return client


async def video_hls(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/devices/{id}/live/hls/{file}` — the HLS fallback.

    `playlist.m3u8` without `id` opens a session (a viewer slot) and answers a
    master playlist; every other request names that session's `id`, which is
    honoured only for the phone and the camera it was opened for.
    """
    file = request.match_info.get("file", "")
    if file not in HLS_FILES:
        raise ApiError(404, "not_found", "no such HLS file")
    device_id, name = _stream(request)
    sessions = request.app[VIDEO_SESSIONS]
    session_id = request[SESSION].id
    hls_id = request.query.get("id")
    headers = {"Cache-Control": "no-store"}

    async with aiohttp.ClientSession() as http:
        if file == "playlist.m3u8" and hls_id is None:
            slot = sessions.admit(session_id, device_id, "hls")
            try:
                device = request.app[REGISTRY].get(device_id)
                relayed = device is not None and is_relayed(device)
                timeout = RELAYED_OPEN_TIMEOUT if relayed else UPSTREAM_TIMEOUT
                slot.hls_id, upstream = await sessions.hls_open(http, name, timeout)
            except BaseException:
                sessions.release(slot)
                raise
            return web.Response(text=master_playlist(upstream, slot.hls_id),
                                content_type="application/vnd.apple.mpegurl",
                                headers=headers)

        slot = sessions.hls_slot(session_id, device_id, hls_id or "") \
            if valid_hls_id(hls_id) else None
        if slot is None or hls_id is None:
            raise ApiError(404, "hls_session_not_found",
                           "no HLS session with this id for this device; request "
                           "playlist.m3u8 without an id to start one")
        n = request.query.get("n")
        if file == "segment.m4s" and not valid_segment_number(n):
            raise ApiError(404, "not_found", "no such segment")
        try:
            if file == "playlist.m3u8":
                text = await sessions.hls_playlist(http, hls_id)
                return web.Response(text=text, content_type="application/vnd.apple.mpegurl",
                                    headers=headers)
            return await sessions.hls_file(request, http, file, hls_id,
                                           n if file == "segment.m4s" else None)
        except ApiError as e:
            if e.code == "hls_session_expired":
                sessions.release(slot)  # go2rtc forgot it; so does the cap
            raise
