"""The app API's `app[...]` and `request[...]` contract, in one place.

A third vocabulary beside `http/appkeys.py` (device API) and `web/appkeys.py`
(panel). Same rule as theirs: an `AppKey` is an object, so a reader finds a
value only through the key it was written with, and a handler on this app that
reached for a panel key would find nothing — which is part of what keeps the
two listeners apart (`tests/test_app_api.py`).

======================  ===========================================  ==========
key                     what it holds                                set by
======================  ===========================================  ==========
`AUTH`                  `AppAuth` — password, sessions, trusted      create_app_api
                        proxies (`app_api/auth.py`)
`THROTTLE`              `LoginThrottle` — failed-login lockouts      create_app_api
`REGISTRY`              `DeviceRegistry`                             create_app_api
`EVENT_STORE`           `EventStore`, or None                        create_app_api
`PET_REGISTRY`          `PetRegistry`, or None                       create_app_api
`MEDIA_ROOT`            the friendly media tree ("" serves nothing)  create_app_api
`DATA_DIR`              where thumbnails and face photos live        create_app_api
`COMMAND_SINK`          `MQTTBridge` the writes publish through,     create_app_api
                        or None (they are queued for the heartbeat)
`HA_PUBLISHER`          `HAPublisher`, told of each write so Home    create_app_api
                        Assistant shows it at once; or None
`HUB`                   the panel's `EventHub`, typed `Any` here     create_app_api
                        (`web` is off limits): writes narrate into it
======================  ===========================================  ==========

`SESSION` is set on each request by the auth middleware once a bearer token
has been accepted, and is absent on the routes that need none.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from petkit_local.ai.pets import PetRegistry
    from petkit_local.app_api.auth import AppAuth, AppSession
    from petkit_local.app_api.throttle import LoginThrottle
    from petkit_local.devices.registry import DeviceRegistry
    from petkit_local.events.store import EventStore
    from petkit_local.ha.publisher import HAPublisher
    from petkit_local.mqtt.bridge import MQTTBridge

AUTH: web.AppKey[AppAuth] = web.AppKey("app_auth")
THROTTLE: web.AppKey[LoginThrottle] = web.AppKey("app_throttle")
REGISTRY: web.AppKey[DeviceRegistry] = web.AppKey("app_registry")
EVENT_STORE: web.AppKey[EventStore | None] = web.AppKey("app_event_store")
PET_REGISTRY: web.AppKey[PetRegistry | None] = web.AppKey("app_pet_registry")
MEDIA_ROOT: web.AppKey[str] = web.AppKey("app_media_root")
DATA_DIR: web.AppKey[str] = web.AppKey("app_data_dir")
COMMAND_SINK: web.AppKey[MQTTBridge | None] = web.AppKey("app_command_sink")
HA_PUBLISHER: web.AppKey[HAPublisher | None] = web.AppKey("app_ha_publisher")
HUB: web.AppKey[Any] = web.AppKey("app_hub")

#: The session the bearer token belongs to.
SESSION: web.RequestKey[AppSession] = web.RequestKey("app_session")
