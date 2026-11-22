"""`/app/v1/accessories/{id}/...`: what the phone needs to sync a fountain itself (#141).

A CTW3's drink history is read only over a DIRECT phone-to-fountain Bluetooth
link: through its parent's relay the fountain answers reads and nothing else
(#140). So the phone app runs PetKit's own session (`codes.BLE_OPCODES` 84,
86, 212 and the stream after it) and hands the records here.

* `GET .../ble-sync` gives it what that session needs: the MAC to connect to,
  the pairing `secret` the 86 carries, and the cursor of what is already held.
  The secret is a credential. It is served only to an authenticated session,
  with `Cache-Control: no-store`, and never logged. When none is held, the
  answer says how to get one; nothing here asks PetKit for it, because the
  account is asked only when somebody presses a button (the panel's *Import
  from PetKit*, `web/api/ble.py::api_ble_import`).
* `POST .../drinks` takes the records and stores them through
  `devices/ble/history.py::store_drink_records`, the relay's own path, so a
  record that ever arrives both ways is one row.

An id the `DeviceRegistry` holds is a WiFi device, not an accessory, and is
not found here, as everywhere an accessory is resolved (`inventory.accessory`).
"""
from __future__ import annotations

import json
import logging
from typing import Any

from aiohttp import web

from petkit_local.app_api import inventory
from petkit_local.app_api.appkeys import BLE_REGISTRY, EVENT_STORE, HUB, REGISTRY, SESSION
from petkit_local.app_api.handlers._common import path_int
from petkit_local.app_api.middleware import ApiError
from petkit_local.devices.ble import (
    CTW3_CLOCK_EPOCH, BLEDevice, announce_drinks, ctw3_secret_hex, drink_record,
    history_cursor, store_drink_records,
)
from petkit_local.events.codes import BLE_HISTORY_SYNC_TYPES

log = logging.getLogger(__name__)

#: Records one upload may carry. A record is 6 bytes on the fountain and a real
#: history tens to hundreds of them; this is the relay's per-session cap
#: (`mqtt/ble_relay.py::HISTORY_MAX_RECORDS_PER_SESSION`), and a longer history
#: is sent in several uploads.
MAX_DRINK_RECORDS = 5000
#: The largest upload body, in bytes. A record is about 40 bytes of JSON, so
#: 5000 fit with room for whitespace; the application's own cap
#: (`server.MAX_BODY`, 64 KiB) is sized for a feeding plan and would refuse a
#: long history, so this route reads its body under its own.
MAX_DRINKS_BODY = 512 * 1024

#: The one `source` an upload may name today.
SOURCE_PHONE_BLE = "phone-ble"

_SECRET_UNKNOWN = (
    "petkit-local holds no pairing secret for this fountain, and the fountain refuses a "
    "direct Bluetooth session without it. In the petkit-local panel, open the device "
    "this fountain is paired to and press \"Import from PetKit\" (PetKit's account is "
    "asked once, then), or type the secret into the accessory's settings there. "
    "Nothing is fetched from PetKit until that button is pressed.")
_SECRET_INVALID = (
    "The pairing secret petkit-local holds for this fountain is not 8 bytes of hex, so "
    "the fountain would refuse it. Import it again from PetKit in the petkit-local panel "
    "(\"Import from PetKit\" on the device this fountain is paired to), or correct it "
    "in the accessory's settings there.")


def _accessory_or_404(request: web.Request) -> BLEDevice:
    """The Bluetooth accessory the `{id}` segment names.

    Raises:
        ApiError: 404 `accessory_not_found`, a WiFi device's id included;
            503 when there is no accessory registry at all.
    """
    node_id = path_int(request, "id", "accessory")
    ble = request.app.get(BLE_REGISTRY)
    found = inventory.accessory(request.app[REGISTRY], ble, node_id)
    if found is None:
        raise ApiError(404, "accessory_not_found", "no such Bluetooth accessory")
    return found


def _wire_mac(mac: str) -> str:
    """The MAC as Android's `BluetoothAdapter.getRemoteDevice` takes it: `AA:BB:...`.

    Stored canonical, twelve hex digits and no separators (`normalize_mac`);
    anything else is handed back as stored rather than guessed at.
    """
    if len(mac) == 12 and all(c in "0123456789ABCDEF" for c in mac):
        return ":".join(mac[i:i + 2] for i in range(0, 12, 2))
    return mac


def _cursor(ble: BLEDevice) -> int | None:
    """The accessory's history cursor as the API's whole Unix seconds; None for none."""
    cursor = history_cursor(ble)
    return int(cursor) if cursor > 0 else None


async def ble_sync(request: web.Request) -> web.Response:
    """`GET /app/v1/accessories/{id}/ble-sync` -> `BleSync`.

    For an accessory whose history the phone cannot sync (anything but a
    CTW3), `sync_supported` is false and nothing else is handed out: no
    secret, no cursor.

    Raises:
        ApiError: 404 `accessory_not_found`; 409 `secret_unknown` for a CTW3
            with no pairing secret held, 409 `secret_invalid` for one whose
            secret cannot be what its 86 carries.
    """
    acc = _accessory_or_404(request)
    supported = acc.ble_type in BLE_HISTORY_SYNC_TYPES
    body: dict[str, Any] = {
        "id": acc.petkit_id, "type": acc.ble_type, "mac": _wire_mac(acc.mac),
        "sync_supported": supported, "secret": None, "history_cursor": None,
        "clock_epoch": CTW3_CLOCK_EPOCH,
    }
    if supported:
        if not isinstance(acc.secret, str) or not acc.secret.strip():
            raise ApiError(409, "secret_unknown", _SECRET_UNKNOWN)
        secret = ctw3_secret_hex(acc.secret)
        if secret is None:
            raise ApiError(409, "secret_invalid", _SECRET_INVALID)
        body["secret"] = secret
        body["history_cursor"] = _cursor(acc)
        session = request.get(SESSION)
        # Who was handed it, never what: the value is a credential.
        log.info("app API: Bluetooth sync details of %s %d handed to %r",
                 acc.ble_type.upper(), acc.petkit_id, session.name if session else "?")
    return web.json_response(body, headers={"Cache-Control": "no-store"})


async def _read_upload(request: web.Request) -> dict[str, Any]:
    """The upload's JSON object, read under `MAX_DRINKS_BODY`.

    Read from the stream in chunks, as a face photo is: `request.json()` is
    bound by the application's `client_max_size`, and a `Content-Length` is
    not a promise, so the cap holds while reading.

    Raises:
        ApiError: 415 when not declared as JSON, 413 `payload_too_large`,
            400 `bad_json`.
    """
    if request.content_type != "application/json":
        raise ApiError(415, "unsupported_media_type",
                       "send JSON with Content-Type: application/json")
    too_large = ApiError(413, "payload_too_large",
                         f"an upload may be at most {MAX_DRINKS_BODY // 1024} KiB; "
                         "send a long history in several")
    if (request.content_length or 0) > MAX_DRINKS_BODY:
        raise too_large
    data = bytearray()
    async for chunk in request.content.iter_chunked(64 * 1024):
        data += chunk
        if len(data) > MAX_DRINKS_BODY:
            raise too_large
    try:
        body = json.loads(bytes(data))
    except (ValueError, RecursionError):
        raise ApiError(400, "bad_json", "the body is not valid JSON") from None
    if not isinstance(body, dict):
        raise ApiError(400, "bad_json", "the body must be a JSON object")
    return body


async def upload_drinks(request: web.Request) -> web.Response:
    """`POST /app/v1/accessories/{id}/drinks` -> `DrinkUploadResult`.

    Body `{records: [{work_time, stay_time}], source: "phone-ble"}`. Each
    record is decoded as the relay decodes one (`history.drink_record`); one
    that is not a plausible record is counted in `skipped` and the rest are
    stored. Records are not filtered by the cursor: one already held is
    counted in `duplicates` and rewritten identically, so an upload can be
    retried, or sent in any order, without losing or doubling anything.

    Raises:
        ApiError: 404 `accessory_not_found`; 400 `sync_unsupported` for an
            accessory that is not a CTW3, 400 `bad_parameter` for `records`
            that is not a list or a `source` that is not `phone-ble`;
            413 `too_many_records` / `payload_too_large`; 415; 503 with no
            event store.
    """
    acc = _accessory_or_404(request)
    if acc.ble_type not in BLE_HISTORY_SYNC_TYPES:
        raise ApiError(400, "sync_unsupported",
                       f"a {acc.ble_type.upper()} has no drink history to upload")
    store = request.app.get(EVENT_STORE)
    if store is None:
        raise ApiError(503, "unavailable", "the event store is not running")
    body = await _read_upload(request)
    source = body.get("source", SOURCE_PHONE_BLE)
    if source != SOURCE_PHONE_BLE:
        raise ApiError(400, "bad_parameter", f"source must be \"{SOURCE_PHONE_BLE}\"")
    items = body.get("records")
    if not isinstance(items, list):
        raise ApiError(400, "bad_parameter", "records must be a list")
    if len(items) > MAX_DRINK_RECORDS:
        raise ApiError(413, "too_many_records",
                       f"at most {MAX_DRINK_RECORDS} records per upload; "
                       "send a long history in several")

    records = []
    skipped = 0
    for item in items:
        record = (drink_record(item.get("work_time"), item.get("stay_time"))
                  if isinstance(item, dict) else None)
        if record is None:
            skipped += 1
        else:
            records.append(record)

    result = await store_drink_records(store, acc, records)
    if result.failed:
        raise ApiError(500, "drinks_not_stored",
                       "the records could not be stored; nothing changed, send them again")
    if result.cursor_moved:
        ble = request.app.get(BLE_REGISTRY)
        if ble is not None:
            ble.mark_dirty()
    announce_drinks(request.app.get(HUB), acc, result,
                    parent_id=acc.link_with or None, source=SOURCE_PHONE_BLE)
    session = request.get(SESSION)
    log.info("app API: %d drink records of %s %d from %r: %d new, %d already held, "
             "%d skipped", len(items), acc.ble_type.upper(), acc.petkit_id,
             session.name if session else "?", result.stored, result.duplicates, skipped)
    return web.json_response({"stored": result.stored, "duplicates": result.duplicates,
                              "skipped": skipped, "cursor": _cursor(acc)})
