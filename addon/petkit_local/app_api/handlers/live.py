"""`GET /app/v1/ws`: live updates over a WebSocket.

Authenticated like every other route — the middleware checks the bearer token
on the upgrade request, so a socket opens only with a session — and then kept
honest by `app_api/live.py`, which closes it within a second or so of that
session ending. The messages and close codes are described there and in
`openapi.yaml`.

The socket is send-only in spirit: anything the app sends is read (a client
must be read from, or its close frame is never seen) and ignored, except a
text `{"type": "ping"}`, answered `{"type": "pong"}` for a client that wants
an application-level round trip. Keepalive is aiohttp's protocol ping every
`HEARTBEAT_SEC`, which is what a proxy like Cloudflare sees as traffic.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging

from aiohttp import WSCloseCode, WSMsgType, web

from petkit_local.app_api.appkeys import LIVE, SESSION
from petkit_local.app_api.live import HEARTBEAT_SEC, Subscriber

log = logging.getLogger(__name__)

#: Nothing the app sends is meaningful beyond a ping.
MAX_INBOUND = 1024
#: How long a close handshake may take, within the shutdown drain.
CLOSE_TIMEOUT = 1.0


async def _write(ws: web.WebSocketResponse, sub: Subscriber) -> None:
    """Send the subscriber's messages until it is ended, then close the socket."""
    while True:
        item = await sub.queue.get()
        if Subscriber.is_close(item):
            await ws.close(code=sub.close_code or 1000,
                           message=sub.close_reason.encode()[:120])
            return
        await ws.send_str(item)


async def live(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/ws` — upgrade, then stream `hello` and the changes after it."""
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT_SEC, max_msg_size=MAX_INBOUND,
                               timeout=CLOSE_TIMEOUT)
    await ws.prepare(request)
    feed = request.app[LIVE]
    session = request[SESSION]
    try:
        sub = await feed.attach(session)
    except Exception:
        # Past `prepare()` an error can no longer be a JSON refusal.
        log.exception("app API: could not open live updates")
        await ws.close(code=WSCloseCode.INTERNAL_ERROR, message=b"internal error")
        return ws
    writer = asyncio.create_task(_write(ws, sub), name="app-ws-writer")
    log.info("app API: live updates opened by %r", session.name)
    try:
        async for msg in ws:
            if writer.done():
                break
            if msg.type == WSMsgType.TEXT and _is_ping(msg.data):
                sub.push('{"type":"pong"}')
    except ConnectionResetError:
        pass
    finally:
        await feed.detach(sub)
        if not writer.done():
            writer.cancel()
        with contextlib.suppress(asyncio.CancelledError, ConnectionResetError, RuntimeError):
            await writer
        if not ws.closed:
            with contextlib.suppress(ConnectionResetError, RuntimeError):
                await ws.close()
        log.info("app API: live updates closed for %r (%s)", session.name,
                 sub.close_reason or "client left")
    return ws


def _is_ping(data: str) -> bool:
    try:
        return json.loads(data) == {"type": "ping"}
    except ValueError:
        return False
