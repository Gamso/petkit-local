"""Setup -> Phone notifications: where ntfy notifications go, and which ones.

The only place these settings are made (`app_api/notify/settings.py` says why
they are never an add-on option). The access token goes in and never comes back
out: every answer carries it masked, so a panel left open on a screen does not
show it, and nothing here logs it.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aiohttp import web

from petkit_local.app_api.notify.settings import SettingsRejected
from petkit_local.web.api._common import _json_body, _refuse
from petkit_local.web.appkeys import NOTIFIER

if TYPE_CHECKING:  # pragma: no cover - typing only
    from petkit_local.app_api.notify.notifier import Notifier

log = logging.getLogger(__name__)


def _notifier(request: web.Request) -> Notifier:
    notifier = request.app.get(NOTIFIER)
    if notifier is None:
        raise _refuse(web.HTTPServiceUnavailable, "notifications are not running")
    return notifier


async def api_app_notify(request: web.Request) -> web.Response:
    """`GET` the settings (token masked); `POST` a partial update of them.

    The update takes any of `enabled`, `server`, `token` (`""` removes it; left
    out, it is kept), `topic` and `kinds` (`{kind: bool}`). Enabling with no
    topic generates one.
    """
    notifier = _notifier(request)
    if request.method == "POST":
        body = await _json_body(request)
        try:
            settings = await notifier.config.update(body)
        except SettingsRejected as e:
            raise _refuse(web.HTTPBadRequest, str(e)) from None
        log.info("ntfy: notifications %s from the panel",
                 "enabled" if settings.enabled else "disabled")
    return web.json_response(notifier.config.settings.public())


async def api_app_notify_topic(request: web.Request) -> web.Response:
    """`POST` — a fresh random topic. The phone must subscribe to the new one."""
    notifier = _notifier(request)
    settings = await notifier.config.regenerate_topic()
    log.info("ntfy: topic regenerated from the panel")
    return web.json_response(settings.public())


async def api_app_notify_test(request: web.Request) -> web.Response:
    """`POST` — publish a test notification now and say how it went."""
    ok, reason = await _notifier(request).send_test()
    return web.json_response({"ok": ok, "error": reason or None})
