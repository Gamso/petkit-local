"""Two-way talk: stream the browser microphone to the device speaker.

The `talk` patcher installs a TCP audio sink on the device (see
`patchers/talk.py`); this WebSocket is the panel's way to it. The transcoder
and the sink connection are `media/talk.py`'s, which also holds one talk per
device. What stays here is the panel's own small message protocol.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging

import aiohttp
from aiohttp import web

from petkit_local.media import talk
from petkit_local.media.talk import PROBED_INPUT, TalkError, TalkSession
from petkit_local.utils.coerce import to_int
from petkit_local.web.appkeys import REGISTRY

log = logging.getLogger(__name__)

#: How often the loop looks up from the socket to see a session that failed
#: while the browser was not sending.
_TICK_SECONDS = 0.5


async def api_talk(request: web.Request) -> web.WebSocketResponse:
    """Two-way talk: stream the browser microphone to the device speaker.

    The frontend's MediaRecorder sends webm/opus over this WebSocket in ~250 ms
    slices: a `talk_start` text frame, then binary, then `talk_stop`. Each
    talk is a `media/talk.py` session — ffmpeg transcoding to the speaker's
    16 kHz mono ADTS-AAC, into a connection to the device's sink — answered
    `talking` once the sink has accepted and `stopped` once the last audio has
    reached it. A binary frame with no talk open starts one, as it always has.

    Anything that stops a talk is an `{"type": "error", "msg", "code"}` frame
    and the end of the socket: no device, no patcher or no address, someone
    else talking through the device (another tab), the sink not
    answering, ffmpeg missing or exiting (with its own last line). The audio
    is never dropped in silence any more.

    Half-duplex by design: the panel mutes listening while talking, because the
    echo cancellation that made full duplex safe lived on the Agora path the
    camera patcher replaced.
    """
    reg = request.app[REGISTRY]
    ws = web.WebSocketResponse(max_msg_size=4 * 1024 * 1024, heartbeat=25)
    await ws.prepare(request)

    did = to_int(request.match_info.get("id"), None)
    device = reg.get(did) if did is not None else None
    if device is None:
        await _fail(ws, TalkError("device_not_found", "no such device"))
        return ws

    session: TalkSession | None = None

    async def start() -> bool:
        """Open the talk if none is open. False (and the socket closed) if it failed."""
        nonlocal session
        if session is not None:
            return True
        try:
            session = talk.TALKS.open(device, PROBED_INPUT)
            await session.start()
        except TalkError as exc:
            await stop(graceful=False)
            await _fail(ws, exc)
            return False
        return True

    async def stop(*, graceful: bool = True) -> TalkError | None:
        """End the open talk; the failure it ended on, if any.

        Graceful lets what was sent reach the device first. Either way the
        device is free when this returns, before the browser is told anything.
        """
        nonlocal session
        if session is None:
            return None
        s, session = session, None
        await asyncio.shield(s.end(graceful=graceful))
        return s.error

    try:
        while not ws.closed:
            if session is not None and session.error is not None:
                failure = session.error
                await stop(graceful=False)
                await _fail(ws, failure)
                break
            try:
                msg = await ws.receive(timeout=_TICK_SECONDS)
            except TimeoutError:
                continue
            if msg.type == aiohttp.WSMsgType.BINARY:
                if not await start():
                    break
                assert session is not None
                try:
                    await session.feed(msg.data)
                except TalkError as exc:
                    await stop(graceful=False)
                    await _fail(ws, exc)
                    break
            elif msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    evt = json.loads(msg.data)
                except ValueError:
                    continue
                kind = evt.get("type") if isinstance(evt, dict) else None
                if kind == "talk_start":
                    if not await start():
                        break
                    await ws.send_json({"type": "talking"})
                elif kind == "talk_stop":
                    error = await stop()
                    if error is not None:
                        await _fail(ws, error)
                        break
                    await ws.send_json({"type": "stopped"})
            else:
                break  # CLOSE, CLOSING, CLOSED, ERROR: the browser is gone
    except (asyncio.CancelledError, ConnectionError, RuntimeError):
        # The browser went away mid-send; the finally is the whole cleanup.
        pass
    finally:
        if session is not None:
            # Whatever was already sent plays to its end, then the device is
            # free — in a task of its own, which a cancellation here cannot cut.
            await asyncio.shield(session.end(graceful=True))
    return ws


async def _fail(ws: web.WebSocketResponse, error: TalkError) -> None:
    """Tell the browser why, then close. Never raises: the browser may be gone.

    `msg` is what `static/js/talk.js` shows; `code` is the machine-readable
    reason (`media/talk.py`).
    """
    log.info("talk: %s: %s", error.code, error.message)
    with contextlib.suppress(Exception):
        await ws.send_json({"type": "error", "msg": error.message, "code": error.code})
    with contextlib.suppress(Exception):
        await ws.close()
