"""Who may call what on the app API, and what a refusal looks like.

ONE middleware, and its order of checks is the access policy:

1. **No password set → 401 `password_not_set`, for every request**, public
   routes and unknown paths included. The listener exists from boot, and until
   the owner has chosen a password it must not even say what it is.
2. A route marked `@public` (`info`, `openapi.yaml`) is served.
3. A route marked `@credentials` (login) is served without a session: it
   authenticates by password, under the login throttle.
4. Everything else — every other route AND every unknown path — needs
   `Authorization: Bearer <token>` naming a live session, else 401. An
   unauthenticated caller cannot even map which paths exist.

The mark is read from the matched route's own handler
(`request.match_info.route.handler`), never from the path, so a route cannot
become public by being spelled differently.

Every refusal is JSON, `{"error": <code>, "message": <text>}`, aiohttp's own
404/405/413 included — a client generated from the OpenAPI file parses one
error shape, not two.
"""
from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import web

from petkit_local.app_api.appkeys import AUTH, SESSION

log = logging.getLogger(__name__)

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]

_ACCESS_ATTR = "_app_api_access"
PUBLIC = "public"
CREDENTIALS = "credentials"

#: Headers a refusal keeps when it is re-rendered as JSON.
_KEPT_HEADERS = ("Allow", "Retry-After", "WWW-Authenticate")

_STATUS_CODES = {
    400: "bad_request", 401: "unauthorized", 403: "forbidden", 404: "not_found",
    405: "method_not_allowed", 413: "payload_too_large", 415: "unsupported_media_type",
    429: "too_many_requests", 500: "internal_error", 503: "unavailable",
}


def public(handler: Handler) -> Handler:
    """Mark a route as served without a session (still not before a password exists)."""
    setattr(handler, _ACCESS_ATTR, PUBLIC)
    return handler


def credentials(handler: Handler) -> Handler:
    """Mark a route that authenticates by password itself (login)."""
    setattr(handler, _ACCESS_ATTR, CREDENTIALS)
    return handler


def access_of(handler: Any) -> str:
    """`public`, `credentials`, or `session` (the default) for a route handler."""
    return getattr(handler, _ACCESS_ATTR, "session")


class ApiError(Exception):
    """A refusal with the app API's JSON body; raise it from a handler.

    A plain exception the middleware renders, rather than an aiohttp
    `HTTPException`, because those are one class per status and this needs
    any status with a machine-readable code beside it.
    """

    def __init__(self, status: int, code: str, message: str,
                 headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.headers = headers or {}


def _json_refusal(status: int, code: str, message: str,
                  headers: dict[str, str] | None = None) -> web.Response:
    return web.json_response({"error": code, "message": message}, status=status,
                             headers=headers)


def bearer_token(request: web.Request) -> str | None:
    """The token of an `Authorization: Bearer ...` header, or None."""
    header = request.headers.get("Authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer":
        return None
    token = token.strip()
    return token or None


def _unauthorized(code: str, message: str) -> web.Response:
    return _json_refusal(401, code, message, {"WWW-Authenticate": 'Bearer realm="petkit-local"'})


async def _guarded(request: web.Request, handler: Handler) -> web.StreamResponse:
    auth = request.app[AUTH]
    if not auth.password_set:
        return _unauthorized(
            "password_not_set",
            "The app API is closed until a password is set in the petkit-local panel "
            "(Setup -> Phone app access).")

    access = access_of(request.match_info.route.handler)
    if access in (PUBLIC, CREDENTIALS):
        return await handler(request)

    session = await auth.authenticate(bearer_token(request))
    if session is None:
        return _unauthorized("invalid_token",
                             "Send Authorization: Bearer <token> from POST /app/v1/auth/login. "
                             "Tokens end on logout, revocation, a password change, or "
                             "after 90 days unused.")
    request[SESSION] = session
    return await handler(request)


@web.middleware
async def app_api_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    """Enforce the access policy above, and render every refusal as JSON."""
    try:
        return await _guarded(request, handler)
    except ApiError as exc:
        return _json_refusal(exc.status, exc.code, exc.message, exc.headers)
    except web.HTTPException as exc:
        if exc.status < 400:
            raise
        headers = {k: exc.headers[k] for k in _KEPT_HEADERS if k in exc.headers}
        return _json_refusal(exc.status, _STATUS_CODES.get(exc.status, "error"),
                             exc.reason or "refused", headers)
    except Exception:
        log.exception("app API: %s %s failed", request.method, request.path)
        return _json_refusal(500, "internal_error", "internal error")
