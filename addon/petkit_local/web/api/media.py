"""The panel's media routes: one file, or one thumbnail, from the friendly tree.

What a relative path resolves to, how a thumbnail is made and which of an
episode's uploads fills which slot all live in `media/serving.py`, because the
app API (`app_api/`) serves the same tree and must not answer differently. What
stays here is the panel's side of it: its config keys and its JSON 404s.

The underscore names below are kept as aliases of the shared functions so the
Timeline module and the existing tests read them where they always did.
"""
from __future__ import annotations

import logging

from aiohttp import web

from petkit_local.media.serving import (
    STITCH_QUIET as _STITCH_QUIET,
    generate_video_thumb as _generate_video_thumb,
    has_media as _has_media,
    resolve_media_file,
    session_media_slots as _session_media_urls,
    thumbnail_file,
)
from petkit_local.web.appkeys import CFG

log = logging.getLogger(__name__)

__all__ = [
    "_STITCH_QUIET", "_generate_video_thumb", "_has_media", "_safe_media_path",
    "_session_media_urls", "api_media_file", "api_media_thumb",
]


def _safe_media_path(request: web.Request, rel_path: str) -> str | None:
    """Resolve `rel_path` under the panel's media root, or None.

    See `media/serving.py::resolve_media_file`; a rejection is a 404, never an
    exception.
    """
    return resolve_media_file(request.app[CFG].get("media_root", ""), rel_path)


async def api_media_file(request: web.Request) -> web.StreamResponse:
    """Serve one file from the friendly media tree.

    The path is relative to the media root — the same form the timeline hands
    out. 404 covers both "missing" and "rejected", so a probe cannot tell the
    two apart.
    """
    p = _safe_media_path(request, request.match_info["path"])
    if not p:
        return web.json_response({"error": "not found"}, status=404)
    return web.FileResponse(p)


async def api_media_thumb(request: web.Request) -> web.StreamResponse:
    """Serve a thumbnail for a media path: the image itself for stills, or a
    cached frame grab for video (`media/serving.py::thumbnail_file`).

    Every "no thumbnail" outcome answers 404, including a failed grab: that is
    the only status the UI has a fallback for (`hide-on-error`), and a 500
    leaves a broken tile where the poster still would have gone.
    """
    p = _safe_media_path(request, request.match_info["path"])
    if not p:
        return web.json_response({"error": "not found"}, status=404)
    thumb, why = await thumbnail_file(p, request.app[CFG].get("data_dir", "/data"))
    if thumb is None:
        return web.json_response({"error": why}, status=404)
    return web.FileResponse(thumb)
