"""The panel application's `app[...]` contract, in one place.

`create_panel_app` builds one `web.Application` and hangs every collaborator on
it; each handler in `web/api/` then reads what it needs back out. That contract
used to exist only as a string typed in eleven places and read in ten modules,
which is exactly the kind of thing that drifts silently — a misspelt read is a
`KeyError` at request time and a misspelt `.get` is a `None` that reads as "not
wired".

Two of the fourteen keys are set from OUTSIDE the constructor, appear in no
signature, and are the reason this file lists all of them rather than only what
`create_panel_app` writes.

===================  ==========================================  ===============
key                  what it holds                               set by
===================  ==========================================  ===============
`registry`           `DeviceRegistry` — every real device        create_panel_app
`ble_registry`       `BLERegistry` — the BLE accessories, or     create_panel_app
                     None when there is none
`hub`                `EventHub` — the live log ring, the         create_panel_app
                     WebSocket fan-out and the diagnostics
`cfg`                the static panel config dict (`api_url`,    create_panel_app
                     `media_root`, `capture_dir`, `data_dir`,
                     `settings_path`, ...), read per request
`bridge`             `MQTTBridge` — the device-facing publish    create_panel_app
                     path, or None with `--no-mqtt`
`live_config`        the SAME dict the device-facing HTTP        create_panel_app
                     handlers read, so a settings change from
                     the panel needs no restart. `{}` in tests
`event_store`        `EventStore`, or None                       create_panel_app
`retention_config`   `RetentionConfig`, or None                  create_panel_app
`pet_registry`       `PetRegistry`, or None                      create_panel_app
`ha_publisher`       `HAPublisher`, or None (`--no-ha`)          create_panel_app
`app_auth`           `AppAuth` — the app API's password and          create_panel_app
                     sessions, or None when it is not running
`background_tasks`   the strong references to in-flight          create_panel_app
                     background tasks (see `_spawn_background`)
`mqtt_broker`        the embedded amqtt broker, for the          main/lifecycle.py
                     delivery view. Injected because the
                     broker starts after the panel is built
`go2rtc`             the camera sidecar, for a device's RTSP     main/lifecycle.py
                     URL. Injected so a test that builds a
                     panel need not know about it
===================  ==========================================  ===============

Both injected keys are set BEFORE `runner.setup()`, which freezes the
Application: a key added after that is a DeprecationWarning today and an error
under aiohttp 4.

Every key is a `web.AppKey`, and an `AppKey` is an object, not its name: a read
finds a value only through the SAME object it was written with, so every writer
and reader — `main/lifecycle.py` and the tests included — imports its key from
here. A string spelling of one of these keys finds nothing. The device API has
a vocabulary of its own in `http/appkeys.py`; where a name appears in both
(`registry`, `event_store`, ...) they are still two objects, because they are
two applications, and a handler that reads the other app's key gets nothing
(`tests/test_app_keys.py`).

The value types are annotations rather than `AppKey(name, t)` arguments, so
this module imports none of the classes it names at runtime and can be
imported from anywhere without a cycle.
"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from amqtt.broker import Broker

    from petkit_local.ai.pets import PetRegistry
    from petkit_local.app_api.auth import AppAuth
    from petkit_local.devices.ble import BLERegistry
    from petkit_local.devices.registry import DeviceRegistry
    from petkit_local.events.store import EventStore
    from petkit_local.ha.publisher import HAPublisher
    from petkit_local.media.go2rtc import Go2rtc
    from petkit_local.media.retention import RetentionConfig
    from petkit_local.mqtt.bridge import MQTTBridge
    from petkit_local.web.hub import EventHub

REGISTRY: web.AppKey[DeviceRegistry] = web.AppKey("registry")
BLE_REGISTRY: web.AppKey[BLERegistry | None] = web.AppKey("ble_registry")
HUB: web.AppKey[EventHub] = web.AppKey("hub")
CFG: web.AppKey[dict[str, Any]] = web.AppKey("cfg")
BRIDGE: web.AppKey[MQTTBridge | None] = web.AppKey("bridge")
LIVE_CONFIG: web.AppKey[dict[str, Any]] = web.AppKey("live_config")
EVENT_STORE: web.AppKey[EventStore | None] = web.AppKey("event_store")
RETENTION_CONFIG: web.AppKey[RetentionConfig | None] = web.AppKey("retention_config")
PET_REGISTRY: web.AppKey[PetRegistry | None] = web.AppKey("pet_registry")
HA_PUBLISHER: web.AppKey[HAPublisher | None] = web.AppKey("ha_publisher")
APP_AUTH: web.AppKey[AppAuth | None] = web.AppKey("app_auth")

# The strong references to in-flight background tasks (see _spawn_background).
# A set here; the device app's key of the same name holds a list.
BACKGROUND_TASKS: web.AppKey[set[asyncio.Task[Any]]] = web.AppKey("background_tasks")

#: Set from `main/lifecycle.py`, not from `create_panel_app`.
MQTT_BROKER: web.AppKey[Broker] = web.AppKey("mqtt_broker")
GO2RTC: web.AppKey[Go2rtc] = web.AppKey("go2rtc")
