"""The app API's application object and its whole route table.

`create_app_api` builds the one `web.Application` that `main/lifecycle.py`
serves on `app_port` (8097). It registers ONLY `/app/v1/...`: nothing of the
panel and nothing of the device API is mounted here, and nothing of this is
mounted there. Three separate applications on three ports is what lets a
reverse proxy be pointed at this one without exposing either of the others —
`tests/test_app_api.py` holds that line from both sides.

`ROUTES` is the table; `openapi.yaml` describes exactly these entries, and a
test fails the moment the two differ. Writes (phase 2) are ordinary rows:
every route needs a session unless marked otherwise, so a write is
authenticated by default. The WebSocket (phase 3) is one too — the bearer
check runs on the upgrade request like any other — fed by `live.LiveFeed`.
Extension points for the later phases:

* phase 4 (ntfy) — no route here: a publisher fed by the `EventHub`, with its
  settings in the panel;
* phase 5 (live video) — a `GET /app/v1/devices/{id}/live` row proxying that
  device's go2rtc stream name only.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from aiohttp import web

from petkit_local.app_api.appkeys import (
    AUTH, COMMAND_SINK, DATA_DIR, EVENT_STORE, HA_PUBLISHER, HUB, LIVE, MEDIA_ROOT,
    PET_REGISTRY, REGISTRY, THROTTLE,
)
from petkit_local.app_api.handlers import (
    auth, controls, devices, feeding, live, media, meta, pets, timeline,
)
from petkit_local.app_api.live import LiveFeed
from petkit_local.app_api.middleware import app_api_middleware
from petkit_local.app_api.throttle import LoginThrottle

if TYPE_CHECKING:
    from petkit_local.ai.pets import PetRegistry
    from petkit_local.app_api.auth import AppAuth
    from petkit_local.devices.registry import DeviceRegistry
    from petkit_local.events.store import EventStore
    from petkit_local.ha.publisher import HAPublisher
    from petkit_local.mqtt.bridge import MQTTBridge

PREFIX = "/app/v1"

#: The largest thing the app sends is a feeding plan, a few kilobytes at most.
MAX_BODY = 64 * 1024

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]

#: `(method, path, handler)`. Order matters where a literal segment shares a
#: position with a `{path:.*}` one: `media/thumb/...` is registered before
#: `media/...`, which would otherwise swallow it.
ROUTES: tuple[tuple[str, str, Handler], ...] = (
    ("GET", "/info", meta.info),
    ("GET", "/openapi.yaml", meta.openapi),
    ("POST", "/auth/login", auth.login),
    ("POST", "/auth/logout", auth.logout),
    ("POST", "/auth/password", auth.change_password),
    ("GET", "/devices", devices.list_devices),
    ("GET", "/devices/{id}", devices.device_detail),
    ("POST", "/devices/{id}/actions/{action}", controls.run_action),
    ("PUT", "/devices/{id}/entities/{key}", controls.set_entity),
    ("GET", "/devices/{id}/schedule", feeding.get_schedule),
    ("PUT", "/devices/{id}/schedule", feeding.put_schedule),
    ("GET", "/devices/{id}/deferred-feeds", feeding.list_deferred_feeds),
    ("POST", "/devices/{id}/deferred-feeds", feeding.add_deferred_feed),
    ("DELETE", "/devices/{id}/deferred-feeds/{feed_id}", feeding.delete_deferred_feed),
    ("GET", "/timeline", timeline.timeline),
    ("GET", "/timeline/{event_id}", timeline.timeline_event),
    ("GET", "/media/thumb/{path:.*}", media.media_thumbnail),
    ("GET", "/media/{path:.*}", media.media_file),
    ("GET", "/pets", pets.list_pets),
    ("GET", "/pets/{id}", pets.pet_detail),
    ("GET", "/pets/{id}/photo", pets.pet_photo),
    ("GET", "/ws", live.live),
)


def create_app_api(app_auth: AppAuth, registry: DeviceRegistry, *,
                   event_store: EventStore | None = None,
                   pet_registry: PetRegistry | None = None,
                   media_root: str = "", data_dir: str = "",
                   throttle: LoginThrottle | None = None,
                   command_sink: MQTTBridge | None = None,
                   ha_publisher: HAPublisher | None = None,
                   hub: Any = None) -> web.Application:
    """Build the app API application.

    Everything after `registry` is optional so a test can build one without
    the full stack; a handler whose collaborator is missing answers 503.
    Without a `command_sink` every write is queued for the device's heartbeat;
    without a `hub` (the panel's `EventHub`) the WebSocket still works, on its
    periodic sweep alone.
    """
    app = web.Application(client_max_size=MAX_BODY, middlewares=[app_api_middleware])
    app[AUTH] = app_auth
    app[THROTTLE] = throttle if throttle is not None else LoginThrottle()
    app[REGISTRY] = registry
    app[EVENT_STORE] = event_store
    app[PET_REGISTRY] = pet_registry
    app[MEDIA_ROOT] = media_root
    app[DATA_DIR] = data_dir
    app[COMMAND_SINK] = command_sink
    app[HA_PUBLISHER] = ha_publisher
    app[HUB] = hub
    app[LIVE] = LiveFeed(app_auth, registry, hub, event_store=event_store,
                         pet_registry=pet_registry, media_root=media_root)
    app.on_shutdown.append(_close_live_sockets)
    for method, path, handler in ROUTES:
        # `add_get` would also register HEAD; the OpenAPI file lists GET only.
        app.router.add_route(method, PREFIX + path, handler)
    return app


async def _close_live_sockets(app: web.Application) -> None:
    """Close every WebSocket (1001) BEFORE the runner's bounded drain starts.

    An open socket is a request that never finishes on its own, so left to the
    drain it would hold shutdown for the whole `shutdown_timeout` and then be
    cut without a close frame.
    """
    await app[LIVE].shutdown()
