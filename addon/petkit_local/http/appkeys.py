"""The device-facing applications' `app[...]` contract, in one place.

Two applications live in `http/`: the device API (`server.create_app`) and the
local bucket (`bucket.create_bucket_app`). Both hang their collaborators on the
`web.Application` and each handler reads what it needs back out. Every key is a
`web.AppKey`, and an `AppKey` is an object, not its name: a read finds a value
only through the SAME object it was written with. So every writer and every
reader imports its key from here, and a string spelling of one of these keys
finds nothing — which is the point: a misspelt key is an `ImportError` at
import time instead of a `None` at request time that reads as "not wired".

The panel has its own vocabulary in `web/appkeys.py` (`cfg` and `hub` where the
device API says `config` and `event_hub`). Those are different objects even
where the names agree, because they are different applications: a handler that
reads the other app's key gets nothing, exactly as it did with strings
(`tests/test_app_keys.py`).

The value types are annotations, not `AppKey(name, t)` arguments: `http/` may
import the panel's `EventHub` and the HA publisher only under `TYPE_CHECKING`
(ARCHITECTURE.md, "Layering"), and the annotation is what a type checker reads.

Device API (`create_app`)
=========================  ============================================  ==============
key                        what it holds                                 set by
=========================  ============================================  ==============
`REGISTRY`                 `DeviceRegistry` — every real device          create_app
`CONFIG`                   the runtime config dict the handlers read     create_app
                           (`Config.to_app_config()`); the panel holds
                           the SAME dict as `live_config`
`BLE_REGISTRY`             `BLERegistry`                                 main/wiring.py
`EVENT_HUB`                `EventHub` — the panel's live log             main/wiring.py
`EVENT_STORE`              `EventStore`                                  main/wiring.py
`PET_REGISTRY`             `PetRegistry`                                 main/wiring.py
`HA_PUBLISHER`             `HAPublisher`, or None (`--no-ha`)            main/wiring.py
`PROXY_UPSTREAM_CREDS`     `UpstreamCredentials` — what proxy mode       main/wiring.py
                           learns from a proxied `dev_iot_device_info`
`ON_STATE_REPORT`          hook: a parsed state report -> HA (only       main/wiring.py
                           with HA publishing on)
`ON_SIGNUP`                hook: a device registered -> HA discovery     main/wiring.py
`ON_DEVICE_SEEN`           hook: any HTTP contact -> HA availability     main/wiring.py
`BACKGROUND_TASKS`         every long-lived task `_spawn` started        main/wiring.py
`MQTT_BROKER`              the embedded amqtt broker                     main/lifecycle.py
`PANEL_RUNNER`             the panel's `AppRunner`                       main/lifecycle.py
`BUCKET_RUNNER`            the bucket's `AppRunner`                      main/lifecycle.py
`APP_API_RUNNER`           the app API's `AppRunner`, when it bound      main/lifecycle.py
=========================  ============================================  ==============

Every handler reads all but `REGISTRY` and `CONFIG` with `.get` and degrades
without them, so a test app built by `create_app` alone still answers.
`main/lifecycle.py` writes its four keys from the device app's own
`on_startup` hook, which runs before `AppRunner.setup()` freezes that app.

Bucket (`create_bucket_app`): `MEDIA_ROOT`, `LOG_ROOT`, `DATA_DIR`, and the two
it shares with the device API, `REGISTRY` and `EVENT_HUB`.

The request keys at the bottom are the same contract one level down: what the
device API's middleware leaves on each `web.Request` for the handlers (and for
the middleware after it) to read, as `web.RequestKey`s.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from amqtt.broker import Broker

    from petkit_local.ai.pets import PetRegistry
    from petkit_local.devices.base import Device
    from petkit_local.devices.ble import BLERegistry
    from petkit_local.devices.registry import DeviceRegistry
    from petkit_local.events.store import EventStore
    from petkit_local.ha.publisher import HAPublisher
    from petkit_local.mqtt.upstream import UpstreamCredentials
    from petkit_local.web.hub import EventHub

# --- device API --------------------------------------------------------------

REGISTRY: web.AppKey[DeviceRegistry] = web.AppKey("registry")
CONFIG: web.AppKey[dict[str, Any]] = web.AppKey("config")

BLE_REGISTRY: web.AppKey[BLERegistry | None] = web.AppKey("ble_registry")
EVENT_HUB: web.AppKey[EventHub | None] = web.AppKey("event_hub")
EVENT_STORE: web.AppKey[EventStore | None] = web.AppKey("event_store")
PET_REGISTRY: web.AppKey[PetRegistry | None] = web.AppKey("pet_registry")
HA_PUBLISHER: web.AppKey[HAPublisher | None] = web.AppKey("ha_publisher")
PROXY_UPSTREAM_CREDS: web.AppKey[UpstreamCredentials] = web.AppKey("proxy_upstream_creds")

ON_STATE_REPORT: web.AppKey[Callable[[Device, dict], Awaitable[None]]] = (
    web.AppKey("on_state_report"))
ON_SIGNUP: web.AppKey[Callable[[Device], Awaitable[None]]] = web.AppKey("on_signup")
ON_DEVICE_SEEN: web.AppKey[Callable[[Device], Awaitable[None]]] = web.AppKey("on_device_seen")

#: Populated by `main/lifecycle.py::_spawn` once the loop is up, and created by
#: `main/wiring.py` before `AppRunner` freezes the app.
BACKGROUND_TASKS: web.AppKey[list[asyncio.Task[Any]]] = web.AppKey("background_tasks")

#: Set from `main/lifecycle.py::start_background`.
MQTT_BROKER: web.AppKey[Broker] = web.AppKey("mqtt_broker")
PANEL_RUNNER: web.AppKey[web.AppRunner] = web.AppKey("panel_runner")
BUCKET_RUNNER: web.AppKey[web.AppRunner] = web.AppKey("bucket_runner")
APP_API_RUNNER: web.AppKey[web.AppRunner] = web.AppKey("app_api_runner")

# --- bucket ------------------------------------------------------------------

MEDIA_ROOT: web.AppKey[str] = web.AppKey("media_root")
LOG_ROOT: web.AppKey[str | None] = web.AppKey("log_root")
DATA_DIR: web.AppKey[str] = web.AppKey("data_dir")

# --- request storage (device API) ---------------------------------------------
# Set by `middleware/device.py::device_middleware` when derivable, and read with
# `request.get(...)` everywhere, because none is guaranteed.

#: The parsed `X-Device` header, only when it carries an id.
X_DEVICE: web.RequestKey[dict[str, Any]] = web.RequestKey("x_device")
#: The urlencoded POST body.
FORM: web.RequestKey[dict[str, Any]] = web.RequestKey("form")
#: From the path, `/<api_version>/<device_type>/...`.
API_VERSION: web.RequestKey[str] = web.RequestKey("api_version")
#: From the path, else the `X-Device` header's `type`, lowercased.
DEVICE_TYPE: web.RequestKey[str] = web.RequestKey("device_type")
#: Where `proxy_middleware` leaves what it did, for `logging_middleware`.
PROXY_OUTCOME: web.RequestKey[dict[str, Any]] = web.RequestKey("proxy_outcome")
