"""A CTW3's drink history synced by the phone itself (#141).

The parent's relay passes a fountain's reads and never its history (#140), so
the phone app runs the direct Bluetooth session and the app API serves it and
takes the result. What is held here:

* `GET /accessories/{id}/ble-sync` hands an authenticated session the MAC, the
  pairing secret (8 bytes of hex, padded as the 86 carries it) and the cursor;
  with no secret held it refuses with `secret_unknown` and names the panel
  button, and it never fetches one; the secret is never logged;
* `POST /accessories/{id}/drinks` stores each record exactly as the relay
  stores one, through the same function -- same uid, so a record seen both
  ways is one row -- and moves the same cursor;
* bad records are counted and skipped, never raised on; the body is bounded;
* anything that is not a CTW3 accessory is 404 or 400;
* a new card reaches `/ws` as a `timeline` message.
"""
from __future__ import annotations

import base64
import json
import logging
import time
from pathlib import Path
from typing import Any

import pytest

from petkit_local.app_api.handlers.accessories import MAX_DRINK_RECORDS, MAX_DRINKS_BODY
from petkit_local.app_api.server import PREFIX
from petkit_local.devices.ble import CTW3_CLOCK_EPOCH, ctw3_secret_hex, drink_record
from petkit_local.events import codes, ingest
from petkit_local.mqtt.bridge import MQTTBridge
from tests.test_app_api import PASSWORD, _Schemas
from tests.test_app_api_accessories import CTW3_ID, K3_ID, AccessoryEnv
from tests.test_app_api_updates import _quiet
from tests.test_ble_accessories import _FakeClient

SECRET = "0123456789abcdef"
#: A whole hour ago, on the second: inside every plausibility window.
START = int(time.time()) - 3600


def _work(start: float) -> int:
    """The fountain's `workTime` for a Unix time: seconds since 2000."""
    return int(start) - CTW3_CLOCK_EPOCH


def _rec(start: float, stay: int) -> dict[str, int]:
    return {"work_time": _work(start), "stay_time": stay}


@pytest.fixture
async def a(tmp_path: Path, event_store, pet_registry):
    e = AccessoryEnv(tmp_path, event_store, pet_registry)
    await e.start()
    await e.auth.set_password(PASSWORD)
    e.token = await e.login()
    yield e
    await e.client.close()


async def _drinks(a: AccessoryEnv, device_id: int = CTW3_ID) -> list[dict[str, Any]]:
    return [r for r in await a.store.query_timeline(device_id=device_id, limit=6000)
            if r["event_type"] == codes.BLE_DRINK_EVENT]


async def _upload(a: AccessoryEnv, records: list[Any], **extra: Any):
    return await a.call("POST", f"/accessories/{CTW3_ID}/drinks",
                        {"records": records, "source": "phone-ble", **extra})


async def _refused(r, status: int, code: str) -> dict[str, Any]:
    assert r.status == status, await r.text()
    body = await r.json()
    assert body["error"] == code
    return body


# --- the secret's form ------------------------------------------------------------

@pytest.mark.parametrize(("stored", "wire"), [
    (SECRET, SECRET),
    ("0123456789AB", "00000123456789ab"),       # short: left-padded, as the app does
    (" 0123456789abcdef ", SECRET),
])
def test_a_secret_is_8_bytes_of_hex_padded_on_the_left(stored, wire):
    assert ctw3_secret_hex(stored) == wire


@pytest.mark.parametrize("stored", ["", "   ", "abc", "zz", "00" * 9, None, 12])
def test_a_secret_the_86_cannot_carry_is_none(stored):
    assert ctw3_secret_hex(stored) is None


def test_the_86_row_and_the_secret_width_are_graded():
    assert codes.BLE_SECRET_LEN == 8 and codes.BLE_SECRET_GRADE == codes.INFERRED
    for cmd in (84, 86):
        op = codes.BLE_OPCODES[cmd]
        assert op.grade == codes.INFERRED and not op.send and "13.8.1" in op.source


# --- GET ble-sync -------------------------------------------------------------------

async def test_a_ctw3_with_a_secret_gets_everything_its_session_needs(a: AccessoryEnv):
    a.ctw3.secret = SECRET
    a.ctw3.history = {"cursor": float(START)}
    r = await a.call("GET", f"/accessories/{CTW3_ID}/ble-sync")
    assert r.status == 200, await r.text()
    assert r.headers["Cache-Control"] == "no-store"
    body = await r.json()
    _Schemas().check(body, "BleSync")
    assert body == {"id": CTW3_ID, "type": "ctw3", "mac": "AA:BB:CC:DD:EE:02",
                    "sync_supported": True, "secret": SECRET, "history_cursor": START,
                    "clock_epoch": CTW3_CLOCK_EPOCH}


async def test_a_ctw3_with_no_history_has_no_cursor(a: AccessoryEnv):
    a.ctw3.secret = SECRET
    body = await (await a.call("GET", f"/accessories/{CTW3_ID}/ble-sync")).json()
    assert body["history_cursor"] is None


async def test_no_secret_is_a_409_that_names_the_panel_button(a: AccessoryEnv, monkeypatch):
    """And nothing asks PetKit: the account is asked on a button press only."""
    import aiohttp

    def no_network(*args, **kwargs):
        raise AssertionError("the app API must never reach PetKit on its own")

    monkeypatch.setattr(aiohttp, "ClientSession", no_network)
    assert a.ctw3.secret == ""
    body = await _refused(await a.call("GET", f"/accessories/{CTW3_ID}/ble-sync"),
                          409, "secret_unknown")
    assert "Import from PetKit" in body["message"]


async def test_a_secret_the_fountain_would_refuse_is_a_409_too(a: AccessoryEnv):
    a.ctw3.secret = "not hex at all"
    body = await _refused(await a.call("GET", f"/accessories/{CTW3_ID}/ble-sync"),
                          409, "secret_invalid")
    assert "Import from PetKit" in body["message"]
    assert "not hex at all" not in body["message"]


async def test_an_accessory_with_no_history_sync_gets_no_secret(a: AccessoryEnv):
    a.k3.secret = SECRET
    r = await a.call("GET", f"/accessories/{K3_ID}/ble-sync")
    assert r.status == 200
    body = await r.json()
    _Schemas().check(body, "BleSync")
    assert body["sync_supported"] is False
    assert body["secret"] is None and body["history_cursor"] is None


@pytest.mark.parametrize("node", ["12345", "300", "100", "abc", "-1"])
async def test_what_is_not_an_accessory_is_not_found(a: AccessoryEnv, node):
    """300 and 100 are WiFi devices: they are no accessory here either."""
    await _refused(await a.call("GET", f"/accessories/{node}/ble-sync"),
                   404, "accessory_not_found")
    await _refused(await a.call("POST", f"/accessories/{node}/drinks", {"records": []}),
                   404, "accessory_not_found")


async def test_both_routes_need_a_session(a: AccessoryEnv):
    a.ctw3.secret = SECRET
    for method, path in (("GET", "ble-sync"), ("POST", "drinks")):
        r = await a.client.request(method, f"{PREFIX}/accessories/{CTW3_ID}/{path}",
                                   json={"records": [_rec(START, 5)]})
        assert r.status == 401
        assert SECRET not in await r.text()
    assert await _drinks(a) == []


async def test_the_secret_is_never_logged(a: AccessoryEnv, caplog):
    caplog.set_level(logging.DEBUG)
    a.ctw3.secret = "0123456789AB"
    r = await a.call("GET", f"/accessories/{CTW3_ID}/ble-sync")
    assert (await r.json())["secret"] == "00000123456789ab"
    assert (await _upload(a, [_rec(START, 5)])).status == 200
    assert "ble-sync" in caplog.text or "Bluetooth sync" in caplog.text
    text = caplog.text.lower()
    assert "0123456789ab" not in text and "00000123456789ab" not in text


# --- POST drinks ----------------------------------------------------------------------

async def test_records_are_stored_as_the_accessorys_drinks(a: AccessoryEnv):
    r = await _upload(a, [_rec(START, 42), _rec(START + 600, 7)])
    assert r.status == 200, await r.text()
    body = await r.json()
    _Schemas().check(body, "DrinkUploadResult")
    assert body == {"stored": 2, "duplicates": 0, "skipped": 0, "cursor": START + 600}

    rows = await _drinks(a)
    assert sorted((r["ts"], r["event_kind"], r["device_type"], r["event_uid"]) for r in rows) == [
        (float(START), codes.KIND_DRINKING, "ctw3",
         f"ble{CTW3_ID}_{_work(START)}:{codes.BLE_DRINK_EVENT}"),
        (float(START + 600), codes.KIND_DRINKING, "ctw3",
         f"ble{CTW3_ID}_{_work(START + 600)}:{codes.BLE_DRINK_EVENT}")]
    # Byte for byte the row the relay builds from the same record.
    relay_row = ingest.from_ble_drink_record(CTW3_ID, "ctw3", _work(START), float(START), 42)
    stored = next(r for r in rows if r["ts"] == START)
    assert json.loads(stored["content_json"]) == json.loads(relay_row["content_json"])
    assert stored["source"] == relay_row["source"]
    # None on the parent: the drink happened at the fountain.
    assert await _drinks(a, 300) == []


async def test_a_record_sent_twice_is_one_drink(a: AccessoryEnv):
    await _upload(a, [_rec(START, 42)])
    body = await (await _upload(a, [_rec(START, 42), _rec(START, 42)])).json()
    assert (body["stored"], body["duplicates"]) == (0, 2)
    assert len(await _drinks(a)) == 1


def _relay(a: AccessoryEnv) -> MQTTBridge:
    a.t4.mqtt_connected = True
    bridge = MQTTBridge(a.registry, None, a.ble, event_store=a.store)
    bridge._client = _FakeClient()
    return bridge


async def _relay_record(bridge: MQTTBridge, a: AccessoryEnv, start: float, stay: int):
    record = _work(start).to_bytes(4, "big") + stay.to_bytes(2, "big", signed=True)
    content = {"device": {"type": 24, "mac": "aabbccddee02"},
               "payload": [{"cmd": 68, "data": base64.b64encode(record).decode()}]}
    await bridge._handle_event(a.t4, "ble_response",
                               {"params": {"event_id": "300_1", "content": json.dumps(content)}})


async def test_a_record_the_relay_already_stored_is_a_duplicate(a: AccessoryEnv):
    bridge = _relay(a)
    await _relay_record(bridge, a, START, 42)
    assert len(await _drinks(a)) == 1
    body = await (await _upload(a, [_rec(START, 42), _rec(START + 60, 3)])).json()
    assert (body["stored"], body["duplicates"]) == (1, 1)
    assert len(await _drinks(a)) == 2


async def test_a_record_the_phone_already_uploaded_is_one_row_for_the_relay(a: AccessoryEnv):
    await _upload(a, [_rec(START, 42)])
    await _relay_record(_relay(a), a, START, 42)
    (row,) = await _drinks(a)
    assert row["ts"] == START


async def test_the_cursor_moves_forward_only_and_never_past_now(a: AccessoryEnv):
    body = await (await _upload(a, [_rec(START, 4)])).json()
    assert body["cursor"] == START == int(a.ctw3.history["cursor"])

    # Older records are still stored -- the cursor filters nothing -- and it stays.
    body = await (await _upload(a, [_rec(START - 600, 4)])).json()
    assert (body["stored"], body["cursor"]) == (1, START)

    # A clock an hour fast is believed (within a day), the cursor stops at now.
    before = time.time()
    body = await (await _upload(a, [_rec(time.time() + 3600, 4)])).json()
    assert body["stored"] == 1
    assert before - 1 <= body["cursor"] <= time.time()

    r = await a.call("GET", f"/accessories/{CTW3_ID}/ble-sync")
    assert r.status == 409          # no secret, so nothing else is handed out
    a.ctw3.secret = SECRET
    sync = await (await a.call("GET", f"/accessories/{CTW3_ID}/ble-sync")).json()
    assert sync["history_cursor"] == body["cursor"]


async def test_the_relay_skips_what_the_phone_moved_the_cursor_past(a: AccessoryEnv):
    """One cursor, whichever side moved it."""
    await _upload(a, [_rec(START, 4)])
    writes = []
    real = a.store.record_event

    async def counting(row):
        writes.append(row["ts"])
        return await real(row)

    a.store.record_event = counting
    try:
        await _relay_record(_relay(a), a, START, 4)
    finally:
        a.store.record_event = real
    assert writes == []


BAD = [
    None, 7, "x", [1, 2], {}, {"work_time": _work(START)}, {"stay_time": 5},
    {"work_time": str(_work(START)), "stay_time": 5},
    {"work_time": float(_work(START)), "stay_time": 5},
    {"work_time": True, "stay_time": 5},
    {"work_time": _work(START), "stay_time": False},
    {"work_time": _work(START), "stay_time": -1},
    {"work_time": _work(START), "stay_time": 40000},
    {"work_time": -1, "stay_time": 5},
    {"work_time": 2 ** 32, "stay_time": 5},
    {"work_time": 5, "stay_time": 5},                      # a clock never set: 2000
    {"work_time": _work(time.time() + 3 * 86400), "stay_time": 5},   # days ahead
]


async def test_records_that_are_not_plausible_are_skipped_and_counted(a: AccessoryEnv):
    r = await _upload(a, [*BAD, _rec(START, 9)])
    assert r.status == 200, await r.text()
    assert await r.json() == {"stored": 1, "duplicates": 0, "skipped": len(BAD),
                              "cursor": START}
    assert len(await _drinks(a)) == 1


def test_an_uploaded_record_decodes_as_a_relayed_one():
    record = drink_record(_work(START), 42)
    assert (record.raw_time, record.start, record.seconds) == (_work(START), START, 42)
    # A parent that set the clock in Unix time: read the same way the relay reads it.
    assert drink_record(START, 42).start == START


async def test_an_empty_upload_stores_nothing(a: AccessoryEnv):
    body = await (await _upload(a, [])).json()
    assert body == {"stored": 0, "duplicates": 0, "skipped": 0, "cursor": None}


async def test_too_many_records_are_refused_whole(a: AccessoryEnv):
    records = [_rec(START - i, 1) for i in range(MAX_DRINK_RECORDS + 1)]
    await _refused(await _upload(a, records), 413, "too_many_records")
    assert await _drinks(a) == []


async def test_the_most_records_allowed_are_stored(a: AccessoryEnv):
    records = [_rec(START - i, 1) for i in range(MAX_DRINK_RECORDS)]
    body = await (await _upload(a, records)).json()
    assert body["stored"] == MAX_DRINK_RECORDS


async def test_a_body_over_the_size_bound_is_refused(a: AccessoryEnv):
    pad = "x" * (MAX_DRINKS_BODY + 1)
    r = await a.call("POST", f"/accessories/{CTW3_ID}/drinks",
                     {"records": [_rec(START, 1)], "pad": pad})
    await _refused(r, 413, "payload_too_large")
    assert await _drinks(a) == []


async def test_a_body_over_the_apps_own_cap_but_under_the_routes_is_read(a: AccessoryEnv):
    """64 KiB is the application's JSON cap; a long history is bigger than that."""
    records = [_rec(START - i, 1) for i in range(2000)]
    assert len(json.dumps({"records": records})) > 64 * 1024
    assert (await _upload(a, records)).status == 200


@pytest.mark.parametrize(("body", "status", "code"), [
    ({"records": "nope"}, 400, "bad_parameter"),
    ({}, 400, "bad_parameter"),
    ({"records": [], "source": "cloud"}, 400, "bad_parameter"),
    ([1, 2], 400, "bad_json"),
])
async def test_a_malformed_body_is_refused(a: AccessoryEnv, body, status, code):
    r = await a.call("POST", f"/accessories/{CTW3_ID}/drinks", body)
    await _refused(r, status, code)


async def test_a_body_that_is_not_json_is_refused(a: AccessoryEnv):
    headers = {"Authorization": f"Bearer {a.token}"}
    r = await a.client.post(f"{PREFIX}/accessories/{CTW3_ID}/drinks", data=b"{nope",
                            headers={**headers, "Content-Type": "application/json"})
    await _refused(r, 400, "bad_json")
    r = await a.client.post(f"{PREFIX}/accessories/{CTW3_ID}/drinks", data=b"x",
                            headers={**headers, "Content-Type": "text/plain"})
    await _refused(r, 415, "unsupported_media_type")


async def test_only_a_ctw3_takes_drinks(a: AccessoryEnv):
    r = await a.call("POST", f"/accessories/{K3_ID}/drinks", {"records": [_rec(START, 1)]})
    await _refused(r, 400, "sync_unsupported")
    assert await _drinks(a, K3_ID) == []


async def test_a_failing_store_loses_nothing_and_says_so(a: AccessoryEnv, monkeypatch):
    async def broken(row):
        raise RuntimeError("disk full")

    monkeypatch.setattr(a.store, "record_event", broken)
    await _refused(await _upload(a, [_rec(START, 1)]), 500, "drinks_not_stored")
    assert "cursor" not in a.ctw3.history


# --- live updates ---------------------------------------------------------------------

async def test_an_upload_is_announced_on_the_hub_as_old_news(a: AccessoryEnv):
    q = a.hub.subscribe()
    await _upload(a, [_rec(START, 4), _rec(START + 60, 4)])
    event = q.get_nowait()
    assert event["kind"] == "event" and event["device_id"] == CTW3_ID
    assert event["detail"] == {"event_kind": codes.KIND_DRINKING, "device_id": CTW3_ID,
                               "device_type": "ctw3", "is_new": False, "parent_id": 300,
                               "source": "phone-ble"}
    # Nothing new, nothing said.
    await _upload(a, [_rec(START, 4)])
    assert q.empty()


async def test_a_recent_drink_reaches_every_socket_as_a_timeline_card(a: AccessoryEnv):
    ws = await a.client.ws_connect(f"{PREFIX}/ws",
                                   headers={"Authorization": f"Bearer {a.token}"})
    try:
        await _quiet(ws, 0.3)
        recent = int(time.time()) - 120
        await _upload(a, [_rec(recent, 30)])
        cards = [m["item"] for m in await _quiet(ws, 1.0) if m["type"] == "timeline"]
        assert [(c["device_id"], c["event_kind"], c["duration_sec"]) for c in cards] == [
            (CTW3_ID, codes.KIND_DRINKING, 30)]
    finally:
        await ws.close()
