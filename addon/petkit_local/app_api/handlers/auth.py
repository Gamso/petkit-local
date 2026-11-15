"""Signing a phone in and out, and changing the password from the app.

Login and the in-app password change both check a password, so both go
through the same throttle, charged to the same address (`throttle.py`): a
stolen session token must not become a way to guess the current password at
full speed. The throttle is asked BEFORE the password is hashed, so a locked
address costs nothing.

Nothing here logs a password or a token. A failed login is logged with the
address it was charged to, because that is what an owner reading the log
needs in order to recognise an attack.
"""
from __future__ import annotations

import logging
import math

from aiohttp import web

from petkit_local.app_api.appkeys import AUTH, SESSION, THROTTLE
from petkit_local.app_api.auth import SESSION_IDLE_LIFETIME, PasswordRejected
from petkit_local.app_api.handlers._common import json_body
from petkit_local.app_api.middleware import ApiError, credentials
from petkit_local.app_api.throttle import client_address

log = logging.getLogger(__name__)


def _too_many(wait: float) -> ApiError:
    seconds = max(1, math.ceil(wait))
    return ApiError(429, "too_many_attempts",
                    f"too many wrong passwords; try again in {seconds} s",
                    {"Retry-After": str(seconds)})


async def _check_password(request: web.Request, password: object, what: str) -> None:
    """Verify `password` under the throttle, or raise the refusal.

    Raises:
        ApiError: 429 while the address is locked out, 401 for a wrong one
            (with the lockout it earned in `Retry-After`).
    """
    auth = request.app[AUTH]
    throttle = request.app[THROTTLE]
    address = client_address(request, auth.trusted_networks)
    wait = throttle.retry_after(address)
    if wait > 0:
        raise _too_many(wait)
    if not await auth.verify_password(password):
        delay = throttle.failure(address)
        log.warning("app API: wrong password on %s from %s (locked out for %.0f s)",
                    what, address or "?", delay)
        raise ApiError(401, "invalid_password", "wrong password",
                       {"Retry-After": str(max(1, math.ceil(delay)))})
    throttle.success(address)


@credentials
async def login(request: web.Request) -> web.Response:
    """`POST /app/v1/auth/login {password, device_name}` -> a session token.

    The token is returned once and never again; the server keeps only its
    hash. It stays valid until logout, revocation from the panel, a password
    change, or 90 days without use.
    """
    body = await json_body(request)
    await _check_password(request, body.get("password"), "login")
    token, session = await request.app[AUTH].create_session(body.get("device_name"))
    log.info("app API: %r signed in (session %s)", session.name, session.id)
    return web.json_response({
        "token": token,
        "token_type": "Bearer",
        "idle_timeout_sec": SESSION_IDLE_LIFETIME,
        "session": session.public(),
    })


async def logout(request: web.Request) -> web.Response:
    """`POST /app/v1/auth/logout` — revoke the calling session, and only it."""
    session = request[SESSION]
    await request.app[AUTH].revoke(session.id)
    log.info("app API: %r signed out (session %s)", session.name, session.id)
    return web.json_response({"ok": True})


async def change_password(request: web.Request) -> web.Response:
    """`POST /app/v1/auth/password {current_password, new_password}`.

    Needs the CURRENT password as well as the session, so a stolen token alone
    cannot lock the owner out. Every other session is revoked; the caller's
    survives.
    """
    body = await json_body(request)
    await _check_password(request, body.get("current_password"), "password change")
    session = request[SESSION]
    try:
        revoked = await request.app[AUTH].set_password(body.get("new_password"),
                                                       keep_session=session.id)
    except PasswordRejected as e:
        raise ApiError(400, "password_rejected", str(e)) from None
    log.info("app API: password changed from %r; %d other session(s) revoked",
             session.name, revoked)
    return web.json_response({"ok": True, "revoked_sessions": revoked})
