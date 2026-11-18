"""`/app/v1/devices/{id}/talk`: the phone's microphone, to a camera's speaker.

A WebSocket, authenticated like every route (the bearer header on the upgrade
request, checked by the middleware). The mechanics — ffmpeg, the device's sink,
one talk per device — are `media/talk.py`'s, shared with the panel; this module
is the app's protocol around them:

1. The upgrade is refused as a JSON `Error` only for what every route refuses
   (401, 404 `device_not_found`, 426 not a WebSocket). Everything specific to
   talking is said on the open socket, so the app has one place to read it.
2. The server checks the device, takes its one talk slot, starts ffmpeg and
   connects to the sink, then sends `{"type":"ready", ...}`. Or it sends
   `{"type":"error","code":...,"message":...}` and closes with `CLOSE_ERROR`.
3. The phone sends binary frames of raw PCM — s16le, 16 kHz, mono, any length
   (the frames are one stream) — and ends with `{"type":"stop"}` or by closing.
   Either way the audio already sent is played to its end; after `stop` the
   server answers `{"type":"stopped"}` and closes with 1000.

A failure after `ready` (ffmpeg exiting, the device hanging up or stalling,
no audio for `IDLE_SECONDS`) is an `error` message and `CLOSE_ERROR` too. The
socket also ends with the phone's session (4401, as live video) and at
shutdown (1001).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time

import aiohttp
from aiohttp import web

from petkit_local.app_api.appkeys import AUTH, SESSION, TALKS
from petkit_local.app_api.handlers._common import device_or_404
from petkit_local.app_api.middleware import ApiError
from petkit_local.media.talk import IDLE_TIMEOUT, PCM_INPUT, TalkError, TalkSession

log = logging.getLogger(__name__)

#: What the phone must send, as `ready` restates it.
SAMPLE_RATE = 16000
CHANNELS = 1
SAMPLE_FORMAT = "s16le"

#: The largest frame taken: two seconds of audio. A phone sends tens of
#: milliseconds at a time; a bigger frame closes the socket (1009).
MAX_FRAME = 64 * 1024
#: Ping interval towards the phone, as live video's: a phone that vanished
#: without a close is noticed, and the device freed, within this.
HEARTBEAT_SECONDS = 20.0
#: A talk with no audio for this long ends: it holds the device's speaker.
IDLE_SECONDS = 15.0
#: How often the loop looks up from the socket to check the session, the
#: phone's sign-in and shutdown.
TICK_SECONDS = 0.5

#: Close codes (documented in openapi.yaml).
CLOSE_NORMAL = 1000
CLOSE_SHUTDOWN = 1001
CLOSE_ERROR = 4000          # after an `error` message, which says why
CLOSE_SESSION_ENDED = 4401  # logout, revocation, password change: sign in again


def _signed_in(request: web.Request):
    auth, session_id = request.app[AUTH], request[SESSION].id
    return lambda: auth.password_set and any(s.id == session_id for s in auth.sessions())


async def _end(ws: web.WebSocketResponse, session: TalkSession | None,
               error: TalkError | None = None, *, code: int = CLOSE_NORMAL,
               reason: bytes = b"") -> None:
    """Free the device, then tell the phone and close. Never raises: it may be gone.

    In that order, so a phone that hears the talk is over finds the device
    free: retrying at once after `stopped` or an error is never `busy`.
    `stopped` is sent only for a clean end without an error.
    """
    if session is not None:
        await asyncio.shield(session.end(graceful=False))
    with contextlib.suppress(Exception):
        if error is not None:
            await ws.send_json({"type": "error", "code": error.code, "message": error.message})
            code, reason = CLOSE_ERROR, error.code.encode()
        elif code == CLOSE_NORMAL:
            await ws.send_json({"type": "stopped"})
    with contextlib.suppress(Exception):
        await ws.close(code=code, message=reason)


async def talk_ws(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/devices/{id}/talk` — WebSocket upgrade to the camera's speaker."""
    device = device_or_404(request)
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT_SECONDS, max_msg_size=MAX_FRAME,
                               compress=False)
    if not ws.can_prepare(request).ok:
        raise ApiError(426, "upgrade_required",
                       "open this route as a WebSocket", {"Upgrade": "websocket"})
    signed_in = _signed_in(request)
    who = request[SESSION].name
    await ws.prepare(request)

    session: TalkSession | None = None
    try:
        try:
            session = request.app[TALKS].open(device, PCM_INPUT)
            await session.start()
        except TalkError as exc:
            log.info("app API: talk on device %d from %r refused: %s", device.petkit_id,
                     who, exc.code)
            await _end(ws, session, exc)
            return ws
        log.info("app API: talk on device %d from %r", device.petkit_id, who)
        await ws.send_json({"type": "ready", "format": SAMPLE_FORMAT,
                            "sample_rate": SAMPLE_RATE, "channels": CHANNELS})
        await _run(ws, session, signed_in)
    except (ConnectionError, RuntimeError):
        pass  # the phone went away mid-send; `close()` below is the whole cleanup
    finally:
        if session is not None:
            # A no-op after `_end`; the whole cleanup after anything else.
            await asyncio.shield(session.end(graceful=False))
    return ws


async def _run(ws: web.WebSocketResponse, session: TalkSession, signed_in) -> None:
    """Feed the phone's audio to the session until something ends the talk."""
    while True:
        if session.shutdown:
            await _end(ws, session, code=CLOSE_SHUTDOWN, reason=b"server shutting down")
            return
        if session.error is not None:
            await _end(ws, session, session.error)
            return
        if not signed_in():
            await _end(ws, session, code=CLOSE_SESSION_ENDED, reason=b"session ended")
            return
        if time.monotonic() - session.last_audio > IDLE_SECONDS:
            await _end(ws, session, TalkError(IDLE_TIMEOUT,
                                              f"no audio for {IDLE_SECONDS:.0f} seconds"))
            return
        try:
            msg = await ws.receive(timeout=TICK_SECONDS)
        except TimeoutError:
            continue
        if msg.type == aiohttp.WSMsgType.BINARY:
            try:
                await session.feed(msg.data)
            except TalkError as exc:
                await _end(ws, session, exc)
                return
        elif msg.type == aiohttp.WSMsgType.TEXT:
            if _is_stop(msg.data):
                await asyncio.shield(session.end(graceful=True))
                await _end(ws, session, session.error)
                return
        else:
            # CLOSE, CLOSING, CLOSED, ERROR: the phone hung up (or vanished,
            # which the heartbeat turns into the same). What it already sent
            # still plays to its end.
            await asyncio.shield(session.end(graceful=True))
            return


def _is_stop(text: str) -> bool:
    """Whether a text frame is `{"type":"stop"}`. Anything else is ignored."""
    try:
        msg = json.loads(text)
    except ValueError:
        return False
    return isinstance(msg, dict) and msg.get("type") == "stop"
