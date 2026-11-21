"""`/app/v1/settings/...`: the server settings a phone may read and change.

Only one so far, `pets-source`: whose pets a device recognises while proxy
mode is on (`config.PETS_SOURCES`), so the app can offer it beside its pet
management. It is the panel's own live setting (`proxy_pets_source`), read
from and written to the same shared config dict the device-facing side reads
per request, and persisted to the same overrides file — so a change from either
place takes effect at the device's next `dev_discern_pic` and survives a
restart. Proxy mode itself stays panel-only: a phone can say whose pets, never
whether the cloud is reached.
"""
from __future__ import annotations

import logging
from typing import Any

from aiohttp import web

from petkit_local.app_api.appkeys import LIVE_CONFIG, SESSION, SETTINGS_PATH
from petkit_local.app_api.handlers._common import json_body
from petkit_local.app_api.middleware import ApiError
from petkit_local.config import PETS_SOURCE_LOCAL, PETS_SOURCES, pets_source, save_panel_overrides

log = logging.getLogger(__name__)

#: The key in the shared live config, and in `settings_overrides.json`.
PETS_SOURCE_KEY = "proxy_pets_source"


def _live(request: web.Request) -> dict[str, Any]:
    live = request.app.get(LIVE_CONFIG)
    if live is None:
        raise ApiError(503, "unavailable", "server settings are not available")
    return live


def _pets_source(live: dict[str, Any]) -> dict[str, Any]:
    """The `PetsSource` body: the setting, and what devices actually get now."""
    chosen = pets_source(live)
    proxy = bool(live.get("proxy_mode"))
    return {"pets_source": chosen,
            "proxy_mode": proxy,
            # With proxy mode off nothing is forwarded, so every device gets ours.
            "effective": chosen if proxy else PETS_SOURCE_LOCAL,
            "options": list(PETS_SOURCES)}


async def get_pets_source(request: web.Request) -> web.Response:
    """`GET /app/v1/settings/pets-source` -> `PetsSource`."""
    return web.json_response(_pets_source(_live(request)))


async def patch_pets_source(request: web.Request) -> web.Response:
    """`PATCH /app/v1/settings/pets-source {pets_source}` -> `PetsSource`.

    Applied at once and persisted. A file that cannot be written is logged, and
    the change still holds until the next restart, as in the panel.
    """
    live = _live(request)
    body = await json_body(request)
    value = body.get("pets_source")
    if value not in PETS_SOURCES:
        raise ApiError(400, "bad_parameter",
                       f"pets_source must be one of: {', '.join(PETS_SOURCES)}")
    live[PETS_SOURCE_KEY] = value
    path = request.app.get(SETTINGS_PATH)
    if path:
        try:
            save_panel_overrides(path, {PETS_SOURCE_KEY: value})
        except OSError as e:
            log.warning("app API: could not persist %s: %s", PETS_SOURCE_KEY, e)
    log.info("app API: %r set pets for recognition to %s", request[SESSION].name, value)
    return web.json_response(_pets_source(live))
