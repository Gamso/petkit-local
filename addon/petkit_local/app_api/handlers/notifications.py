"""`/app/v1/notifications`: a phone's notification kinds, and its notification socket.

* `GET`/`PUT /notifications` — the calling session's `kinds`, which choose what
  it receives on every channel (this socket and its UnifiedPush endpoint, if it
  registered one), without needing a push endpoint to set them;
* `GET /notifications/ws?after=<id>` — the socket itself (`notify/channel.py`):
  a `hello`, then what was missed after `after`, then each notification as it
  is sent. Authenticated like `/ws`: the bearer token on the upgrade request.

Like `/ws` the socket is send-only in spirit: what the app sends is read (or its
close frame is never seen) and ignored, except `{"type": "ping"}`, answered
`{"type": "pong"}`. Keepalive is aiohttp's protocol ping every `HEARTBEAT_SEC`.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from aiohttp import WSCloseCode, WSMsgType, web

from petkit_local.app_api.appkeys import AUTH, NOTIFY_CHANNEL, SESSION
from petkit_local.app_api.handlers._common import json_body
from petkit_local.app_api.handlers.updates import CLOSE_TIMEOUT, MAX_INBOUND, is_ping, write_queue
from petkit_local.app_api.middleware import ApiError
from petkit_local.app_api.notify.channel import HEARTBEAT_SEC
from petkit_local.app_api.push import PushRejected, check_kinds

log = logging.getLogger(__name__)

#: Longest `after` accepted: any id this server can have handed out fits.
MAX_AFTER_DIGITS = 18


def _settings(request: web.Request) -> dict[str, Any]:
    session = request[SESSION]
    channel = request.app[NOTIFY_CHANNEL]
    return {"kinds": list(session.kinds) if session.kinds is not None else None,
            "server_kinds": channel.server_kinds(),
            "connected": channel.connected(session.id),
            "last_id": channel.last_id}


async def get_settings(request: web.Request) -> web.Response:
    """`GET /app/v1/notifications` — this session's kinds and where its socket stands."""
    return web.json_response(_settings(request))


async def put_settings(request: web.Request) -> web.Response:
    """`PUT /app/v1/notifications {kinds}` — choose this session's kinds, for every channel."""
    body = await json_body(request)
    if "kinds" not in body:
        raise ApiError(400, "bad_parameter", "kinds is required: a list of kinds, or null")
    try:
        kinds = check_kinds(body["kinds"])
    except PushRejected as e:
        raise ApiError(400, "bad_parameter", str(e)) from None
    session = request[SESSION]
    if not await request.app[AUTH].set_kinds(session.id, kinds):
        raise ApiError(401, "invalid_token", "the session has ended")
    log.info("app API: %r chose notification kinds %s (session %s)", session.name,
             "all" if kinds is None else ",".join(kinds) or "none", session.id)
    return web.json_response(_settings(request))


def _after(request: web.Request) -> int | None:
    """The `after` query parameter: absent, or a non-negative integer."""
    raw = request.query.get("after")
    if raw is None or raw == "":
        return None
    if not (raw.isascii() and raw.isdigit()) or len(raw) > MAX_AFTER_DIGITS:
        raise ApiError(400, "bad_parameter", "after must be a non-negative integer")
    return int(raw)


async def notifications_ws(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/notifications/ws` — upgrade, then `hello`, catch-up, live."""
    after = _after(request)
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT_SEC, max_msg_size=MAX_INBOUND,
                               timeout=CLOSE_TIMEOUT)
    await ws.prepare(request)
    channel = request.app[NOTIFY_CHANNEL]
    session = request[SESSION]
    try:
        sub = await channel.attach(session, after)
    except Exception:
        # Past `prepare()` an error can no longer be a JSON refusal.
        log.exception("app API: could not open the notification socket")
        await ws.close(code=WSCloseCode.INTERNAL_ERROR, message=b"internal error")
        return ws
    writer = asyncio.create_task(write_queue(ws, sub), name="app-notify-writer")
    log.info("app API: notification socket opened by %r (after=%s)", session.name, after)
    try:
        async for msg in ws:
            if writer.done():
                break
            if msg.type == WSMsgType.TEXT and is_ping(msg.data):
                sub.push('{"type":"pong"}')
    except ConnectionResetError:
        pass
    finally:
        await channel.detach(sub)
        if not writer.done():
            writer.cancel()
        with contextlib.suppress(asyncio.CancelledError, ConnectionResetError, RuntimeError):
            await writer
        if not ws.closed:
            with contextlib.suppress(ConnectionResetError, RuntimeError):
                await ws.close()
        log.info("app API: notification socket closed for %r (%s)", session.name,
                 sub.close_reason or "client left")
    return ws
