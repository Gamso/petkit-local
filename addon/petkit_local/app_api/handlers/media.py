"""`/app/v1/media/...`: recordings, photos and their thumbnails.

Authenticated like every other route — by the same `Authorization: Bearer`
header, not by a signed URL. Android's image and video loaders (Coil, Glide,
Media3) all take request headers through the OkHttp client the app already
has, so a header costs the app nothing; a URL with a credential in it, by
contrast, ends up in the reverse proxy's access log, Cloudflare's, and
anywhere a link is copied to, and would need a second secret and an expiry
scheme of its own.

Path resolution is `media/serving.py`'s, shared with the panel: everything
goes through `utils/paths.safe_join`, and missing and refused answer the same
404, so a probe learns nothing about what lies outside the media tree.
`web.FileResponse` handles `Range`, which is what lets a player seek.
"""
from __future__ import annotations

from aiohttp import web

from petkit_local.app_api.appkeys import DATA_DIR, MEDIA_ROOT
from petkit_local.app_api.middleware import ApiError
from petkit_local.media.serving import resolve_media_file, thumbnail_file


def _resolve(request: web.Request) -> str:
    path = resolve_media_file(request.app[MEDIA_ROOT], request.match_info.get("path", ""))
    if path is None:
        raise ApiError(404, "media_not_found", "no such media file")
    return path


async def media_file(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/media/{path}` — one file from the media tree."""
    return web.FileResponse(_resolve(request))


async def media_thumbnail(request: web.Request) -> web.StreamResponse:
    """`GET /app/v1/media/thumb/{path}` — a still for a photo or a video."""
    thumb, _why = await thumbnail_file(_resolve(request), request.app[DATA_DIR])
    if thumb is None:
        raise ApiError(404, "thumbnail_unavailable", "no thumbnail for this file")
    return web.FileResponse(thumb)
