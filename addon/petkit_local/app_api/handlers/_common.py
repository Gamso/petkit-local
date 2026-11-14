"""Request parsing every app API handler starts with.

Each helper either returns a clean value or raises `ApiError`, which the
middleware renders as the API's JSON refusal. Nothing request-supplied reaches
a handler unparsed.
"""
from __future__ import annotations

from typing import Any

from aiohttp import web

from petkit_local.app_api.middleware import ApiError
from petkit_local.utils.coerce import to_int


async def json_body(request: web.Request) -> dict[str, Any]:
    """The request body as a JSON object.

    Raises:
        ApiError: 415 when not declared as JSON, 400 when not a JSON object.
    """
    if request.content_type != "application/json":
        raise ApiError(415, "unsupported_media_type",
                       "send JSON with Content-Type: application/json")
    try:
        body = await request.json()
    except Exception:
        raise ApiError(400, "bad_json", "the body is not valid JSON") from None
    if not isinstance(body, dict):
        raise ApiError(400, "bad_json", "the body must be a JSON object")
    return body


def path_int(request: web.Request, name: str, what: str) -> int:
    """A numeric path segment. A non-number is a 404: no such `what` can exist."""
    value = to_int(request.match_info.get(name), None)
    if value is None or str(value) != request.match_info.get(name, "").strip():
        raise ApiError(404, f"{what}_not_found", f"no such {what}")
    return value


def query_int(request: web.Request, name: str, *, default: int | None = None,
              minimum: int | None = None, maximum: int | None = None) -> int | None:
    """An integer query parameter, clamped; absent gives `default`.

    Raises:
        ApiError: 400 when present but not an integer.
    """
    raw = request.query.get(name)
    if raw is None or raw == "":
        return default
    value = to_int(raw, None)
    if value is None or isinstance(value, bool):
        raise ApiError(400, "bad_parameter", f"{name} must be an integer")
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value
