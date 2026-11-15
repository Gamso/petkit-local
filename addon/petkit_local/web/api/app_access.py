"""Setup -> Phone app access: the app API's password, its sessions, its proxies.

The panel is where the app API's password is set, and the recovery path when
it is forgotten: it is reachable only through Ingress or the LAN, never through
the reverse proxy the app uses. So setting or resetting the password here never
asks for the old one — and revokes every session, so a reset also signs out a
phone that should not be signed in.

The password travels to the server once, in the POST body, and is never sent
back: `GET` says only whether one is set. A session's push registration is
shown by its host only — the endpoint is a capability URL (`app_api/push.py`).
"""
from __future__ import annotations

import logging
from typing import Any

from aiohttp import web

from petkit_local.app_api.auth import (
    MIN_PASSWORD_LENGTH, SESSION_IDLE_LIFETIME, AppAuth, PasswordRejected,
)
from petkit_local.web.api._common import _json_body, _refuse
from petkit_local.web.appkeys import APP_AUTH, CFG

log = logging.getLogger(__name__)


def _auth(request: web.Request) -> AppAuth:
    auth = request.app.get(APP_AUTH)
    if auth is None:
        raise _refuse(web.HTTPServiceUnavailable, "the app API is not running")
    return auth


def _view(request: web.Request, auth: AppAuth) -> dict[str, Any]:
    return {
        "password_set": auth.password_set,
        "min_password_length": MIN_PASSWORD_LENGTH,
        "idle_timeout_days": SESSION_IDLE_LIFETIME // 86400,
        "app_port": request.app[CFG].get("app_port"),
        "trusted_proxies": auth.trusted_proxies,
        "sessions": [s.public() for s in auth.sessions()],
    }


async def api_app_access(request: web.Request) -> web.Response:
    """`GET /api/app-access` — everything the card shows. Never the password."""
    return web.json_response(_view(request, _auth(request)))


async def api_app_password(request: web.Request) -> web.Response:
    """`POST {password}` sets or resets it; `DELETE` removes it. Both revoke every session.

    Removing it closes the app API: every route answers 401 until one is set.
    """
    auth = _auth(request)
    if request.method == "DELETE":
        revoked = await auth.clear_password()
        log.info("app API: password removed from the panel; %d session(s) revoked", revoked)
    else:
        body = await _json_body(request)
        try:
            revoked = await auth.set_password(body.get("password"))
        except PasswordRejected as e:
            raise _refuse(web.HTTPBadRequest, str(e)) from None
        log.info("app API: password set from the panel; %d session(s) revoked", revoked)
    return web.json_response({"ok": True, "revoked": revoked, **_view(request, auth)})


async def api_app_sessions(request: web.Request) -> web.Response:
    """`DELETE /api/app-access/sessions[/{session_id}]` — sign out one phone, or all."""
    auth = _auth(request)
    session_id = request.match_info.get("session_id")
    if session_id is None:
        revoked = await auth.revoke_all()
    else:
        if not await auth.revoke(session_id):
            raise _refuse(web.HTTPNotFound, "no such session")
        revoked = 1
    log.info("app API: %d session(s) revoked from the panel", revoked)
    return web.json_response({"ok": True, "revoked": revoked, **_view(request, auth)})


async def api_app_trusted_proxies(request: web.Request) -> web.Response:
    """`POST {trusted_proxies: [...]}` — the peers whose forwarded headers are believed.

    Addresses or CIDRs. Empty (the default) believes nobody, which is right
    unless the app API is reached through a reverse proxy: then every request
    comes from the proxy, and all phones share one failed-login allowance.
    """
    auth = _auth(request)
    body = await _json_body(request)
    try:
        await auth.set_trusted_proxies(body.get("trusted_proxies"))
    except ValueError as e:
        raise _refuse(web.HTTPBadRequest, str(e)) from None
    return web.json_response({"ok": True, **_view(request, auth)})
