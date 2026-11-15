"""What the API says about itself: `/info` and the OpenAPI file.

Both are public once a password exists — the app asks `/info` before it has a
token, to check it reached a petkit-local at all — so both say as little as
they can. `/info` gives the server's version and session only to a caller whose
bearer token is valid: a version number is what a scanner looks for.
"""
from __future__ import annotations

import time
from pathlib import Path

from aiohttp import web

from petkit_local.app_api.appkeys import AUTH
from petkit_local.app_api.middleware import bearer_token, public
from petkit_local.utils.const import VERSION

#: The major version of this API, which is also its path (`/app/v1`).
API_VERSION = 1
OPENAPI_PATH = Path(__file__).resolve().parent.parent / "openapi.yaml"


@public
async def info(request: web.Request) -> web.Response:
    """`GET /app/v1/info`. A valid bearer token adds the version and its session."""
    session = await request.app[AUTH].authenticate(bearer_token(request))
    return web.json_response({
        "api_version": API_VERSION,
        "server_time": time.time(),
        "authenticated": session is not None,
        "server_version": VERSION if session is not None else None,
        "session": session.public() if session is not None else None,
    })


@public
async def openapi(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/openapi.yaml` — the contract the app's client is generated from."""
    return web.FileResponse(OPENAPI_PATH, headers={"Content-Type": "application/yaml"})
