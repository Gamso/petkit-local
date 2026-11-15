"""`PUT`/`DELETE /app/v1/push` — the calling phone's own UnifiedPush endpoint.

One endpoint per session, stored with it (`auth.py`), so whatever ends the
session ends its pushes. The endpoint is a capability URL: it is never echoed
back, never logged — only its host is (`push.py`).

The `kinds` a registration may carry are the session's, shared with the
notification socket (`handlers/notifications.py`): sent, they replace the
session's choice; left out, they leave it as it is.
"""
from __future__ import annotations

import logging

from aiohttp import web

from petkit_local.app_api.appkeys import AUTH, SESSION
from petkit_local.app_api.auth import UNCHANGED
from petkit_local.app_api.handlers._common import json_body
from petkit_local.app_api.middleware import ApiError
from petkit_local.app_api.push import (
    PushRegistration, PushRejected, check_endpoint, check_kinds,
)

log = logging.getLogger(__name__)


async def register_push(request: web.Request) -> web.Response:
    """`PUT /app/v1/push {endpoint, kinds?}` — register or replace this session's endpoint."""
    body = await json_body(request)
    try:
        endpoint = check_endpoint(body.get("endpoint"))
    except PushRejected as e:
        raise ApiError(400, "invalid_push_endpoint", str(e)) from None
    try:
        kinds = check_kinds(body["kinds"]) if "kinds" in body else UNCHANGED
    except PushRejected as e:
        raise ApiError(400, "bad_parameter", str(e)) from None
    session = request[SESSION]
    auth = request.app[AUTH]
    registration = PushRegistration(endpoint, auth.now())
    if not await auth.set_push(session.id, registration, kinds):
        # Revoked between the middleware's check and here.
        raise ApiError(401, "invalid_token", "the session has ended")
    log.info("app API: %r registered for push on %s (session %s)", session.name,
             registration.host, session.id)
    return web.json_response({"push": registration.public(session.kinds)})


async def unregister_push(request: web.Request) -> web.Response:
    """`DELETE /app/v1/push` — stop pushing to this session. Idempotent.

    The session's kinds stay: they also choose what its notification socket gets.
    """
    session = request[SESSION]
    if await request.app[AUTH].clear_push(session.id):
        log.info("app API: %r unregistered from push (session %s)", session.name, session.id)
    return web.json_response({"ok": True})
