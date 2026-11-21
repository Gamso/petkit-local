"""A CTW3's drink history, asked for over its parent's relay and stored locally.

What is held here, all of it from PetKit's app 13.8.1 and none of it yet seen
from a real fountain (`codes.BLE_OPCODES` 212 and 67-82 grade it):

* the 212 request is the app's own bytes, an empty frame, and it goes out only
  once a CTW3 has answered its link-up reads -- once per session, once per
  `HISTORY_SYNC_INTERVAL_SECONDS`, and never with no store to keep records in;
* its reply announces a big-endian byte count in DATA bytes 1-4; a history
  packet (68 or 82, relayed bare or in its `FA FC FE` stream frame) is whole
  6-byte records, `workTime` then a signed `stayTime`, big-endian;
* every plausible record becomes ONE drinking row on the accessory's own id,
  timed by its own span, deduplicated by the fountain's timestamp, and the
  cursor that skips them next time is persisted;
* nothing past the request is ever sent, and nothing a fountain sends raises;
* the rows reach the timeline, its drinking chip and the app API.
"""
from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any

import pytest

from petkit_local.devices.ble import (
    BLE_STREAM_HEADER, CTW3_CLOCK_EPOCH, BLERegistry, _ble_unframe, ctw3_record_time,
    decode_ctw3_records, decode_ctw3_sync_reply, parse_ctw3_history,
)
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events import codes, ingest
from petkit_local.mqtt import ble_relay
from petkit_local.mqtt.bridge import MQTTBridge
from tests.test_app_api import PASSWORD
from tests.test_app_api_accessories import CTW3_ID, AccessoryEnv
from tests.test_ble_accessories import CTW3_CMD230, RELAY, _FakeClient, _decode_frame

MAC = "a4c138aabbcc"
#: A whole hour ago, on the second: inside every plausibility window.
START = float(int(time.time()) - 3600)


def _record(start: float, seconds: int, *, unix: bool = False) -> bytes:
    raw = int(start) if unix else int(start) - CTW3_CLOCK_EPOCH
    return raw.to_bytes(4, "big") + seconds.to_bytes(2, "big", signed=True)


def _stream_frame(cmd: int, data: bytes, index: int = 0, total: int = 1) -> bytes:
    return bytes([*BLE_STREAM_HEADER, cmd, 3, index, total,
                  len(data) & 0xFF, len(data) >> 8, *data, 0xFB])


def _content(*items: tuple[int, bytes]) -> dict:
    return {"device": {"type": 24, "mac": MAC},
            "payload": [{"cmd": cmd, "data": base64.b64encode(data).decode()}
                        for cmd, data in items]}


def _response(*items: tuple[int, bytes]) -> dict:
    return {"params": {"event_id": "100000001_1794949774",
                       "content": json.dumps(_content(*items))}}


# --- the decoders ---------------------------------------------------------------

def test_a_record_is_seconds_since_2000_then_a_signed_stay():
    data = _record(START, 42) + _record(START + 600, 7)
    records, rejected = decode_ctw3_records(data)
    assert rejected == 0
    assert [(r.start, r.seconds) for r in records] == [(START, 42), (START + 600, 7)]
    assert records[0].raw_time == int(START) - CTW3_CLOCK_EPOCH


def test_a_clock_counting_unix_time_reads_the_same():
    """Which clock a parent sets is not known; the two cannot be confused."""
    (record,), _ = decode_ctw3_records(_record(START, 30, unix=True))
    assert record.start == START


@pytest.mark.parametrize("raw", [0, 5, 0xFFFFFFFF])
def test_a_time_no_drink_could_have_is_rejected(raw):
    """A clock never set counts from 2000; 0xFFFFFFFF is from the future."""
    assert ctw3_record_time(raw) is None
    records, rejected = decode_ctw3_records(raw.to_bytes(4, "big") + b"\x00\x05")
    assert records == [] and rejected == 1


def test_a_stay_that_overflows_the_signed_short_is_rejected():
    raw = (int(START) - CTW3_CLOCK_EPOCH).to_bytes(4, "big")
    records, rejected = decode_ctw3_records(raw + b"\xff\xff")
    assert records == [] and rejected == 1


def test_a_broken_tail_costs_only_itself():
    records, rejected = decode_ctw3_records(_record(START, 9) + b"\x01\x02\x03")
    assert len(records) == 1 and rejected == 1


def test_the_212_reply_count_is_bytes_1_to_4_big_endian():
    assert decode_ctw3_sync_reply(bytes([0x09, 0, 0, 0x01, 0x2C])) == 300
    assert decode_ctw3_sync_reply(bytes([0, 0, 0, 0])) is None


def test_a_stream_frame_is_split_by_its_own_header_and_length():
    body = _record(START, 5)
    cmd, data = _ble_unframe(_stream_frame(68, body, index=3, total=4))
    assert (cmd, data) == (68, body)


def test_history_frames_bare_or_framed_decode_alike():
    body = _record(START, 11)
    for content in (_content((68, body)),
                    {"device": {"mac": MAC},
                     "payload": [{"cmd": None, "data": base64.b64encode(
                         _stream_frame(82, body)).decode()}]}):
        out = parse_ctw3_history(content)
        assert [(r.start, r.seconds) for r in out.records] == [(START, 11)]
        assert out.data_bytes == 6


def test_stream_control_is_named_not_answered():
    out = parse_ctw3_history(_content((67, b"\x00"), (69, b"")))
    assert out.control == [67, 69] and not out.records


@pytest.mark.parametrize("content", [
    None, "x", [1], {"payload": "nope"}, {"payload": [{"cmd": [1], "data": "AA=="}]},
    {"payload": [{"cmd": 68, "data": "%%%not base64%%%"}]},
    {"payload": [{"cmd": 212, "data": ""}]}, {"payload": [{"cmd": 68, "data": 7}]},
])
def test_garbage_is_skipped_never_raised(content):
    out = parse_ctw3_history(content)
    assert not out.records and out.pending is None


def test_the_row_is_the_accessorys_timed_drink():
    row = ingest.from_ble_drink_record(700, "ctw3", 123, START, 42)
    assert row["device_id"] == 700 and row["device_type"] == "ctw3"
    assert row["event_type"] == codes.BLE_DRINK_EVENT
    assert row["event_kind"] == codes.KIND_DRINKING
    assert row["ts"] == START
    assert row["event_uid"] == f"ble700_123:{codes.BLE_DRINK_EVENT}"
    content = json.loads(row["content_json"])
    assert content["event_end"] - content["event_start"] == 42


# --- over the relay ------------------------------------------------------------

def _relay(event_store=None):
    reg = DeviceRegistry()
    reg.get_or_create(petkit_id=10, device_type="t4", serial_number="SN10").mqtt_connected = True
    ble = BLERegistry()
    ble.register(ble_type="ctw3", petkit_id=700, mac=MAC.upper(), secret="s",
                 interval=240, link_with=10)
    bridge = MQTTBridge(reg, None, ble, event_store=event_store)
    bridge._client = _FakeClient()
    return bridge, reg, ble


def _ble_frames(bridge):
    out = []
    for topic, body in bridge._client.sent:
        if topic.endswith("/thing/service/ble"):
            payload = json.loads(body)["params"]["payload"]
            out.append((payload["cmd"], _decode_frame(payload["data"])))
    return out


def _connects(bridge):
    return [json.loads(body)["params"]["connect_action"]
            for topic, body in bridge._client.sent if topic.endswith("/thing/service/connect")]


async def _link_up_and_answer(bridge, reg):
    """A link-up, then the status and the settings its reads ask for."""
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    status = base64.b64decode(CTW3_CMD230)[:30]
    settings = bytes.fromhex("0303007804b0010300000100")
    await bridge._handle_event(reg.get(10), "ble_response", _response((210, status)))
    await bridge._handle_event(reg.get(10), "ble_response", _response((211, settings)))


async def _stored(store, device_id=700):
    return [r for r in await store.query_timeline(device_id=device_id, limit=100)
            if r["event_type"] == codes.BLE_DRINK_EVENT]


async def test_a_ctw3_that_answered_its_reads_is_asked_for_its_history(event_store):
    bridge, reg, ble = _relay(event_store)
    await _link_up_and_answer(bridge, reg)

    frames = _ble_frames(bridge)
    assert [cmd for cmd, _ in frames] == [210, 211, 212]
    # The app's bytes exactly: an empty request, the next sequence number.
    assert frames[2][1] == bytes.fromhex("fafcfdd401020000fb")
    # Not hung up: the 212 is now the read the session waits on.
    assert _connects(bridge) == []
    assert ble.get(700).history["asked_at"] > 0


async def test_a_status_push_after_the_request_does_not_hang_up_on_it(event_store):
    """The owner's CTW3 on 2026-11-21 (#126): right after the 212 it sent two
    252s and an unasked 230, and that 230 cleared the pending 212 -- the relay
    hung up 0.3 s after asking, before any history could come back."""
    bridge, reg, ble = _relay(event_store)
    await _link_up_and_answer(bridge, reg)
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [210, 211, 212]

    await bridge._handle_event(reg.get(10), "ble_response", _response((252, b"\x00\x01")))
    await bridge._handle_event(reg.get(10), "ble_response", _response((252, b"\x00\x01")))
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((230, base64.b64decode(CTW3_CMD230))))
    assert _connects(bridge) == [], "hung up before the 212 was answered"

    # The reply still ends it the normal way.
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((212, bytes([0, 0, 0, 0, 6]))))
    assert _connects(bridge) == []
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((68, _record(START, 42))))
    assert len(await _stored(event_store)) == 1
    assert _connects(bridge) == [0]


async def test_records_land_on_the_accessory_and_the_session_ends_when_all_are_in(event_store):
    bridge, reg, ble = _relay(event_store)
    await _link_up_and_answer(bridge, reg)

    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((212, bytes([0, 0, 0, 0, 12]))))
    assert _connects(bridge) == [], "hung up on the announcement"
    assert ble.get(700).history["pending_bytes"] == 12

    body = _record(START, 42) + _record(START + 600, 7)
    await bridge._handle_event(reg.get(10), "ble_response", _response((68, body)))
    rows = await _stored(event_store)
    assert sorted((r["ts"], r["event_kind"], r["device_type"]) for r in rows) == [
        (START, codes.KIND_DRINKING, "ctw3"), (START + 600, codes.KIND_DRINKING, "ctw3")]
    # None on the parent: the drink happened at the fountain.
    assert await _stored(event_store, device_id=10) == []
    assert ble.get(700).history["cursor"] == START + 600
    # All 12 announced bytes are in: now it hangs up.
    assert _connects(bridge) == [0]
    # And nothing beyond the request was ever sent.
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [210, 211, 212]


async def test_a_record_sent_twice_is_one_drink(event_store):
    bridge, reg, _ = _relay(event_store)
    body = _record(START, 42)
    await bridge._handle_event(reg.get(10), "ble_response", _response((68, body)))
    await bridge._handle_event(reg.get(10), "ble_response", _response((82, body)))
    assert len(await _stored(event_store)) == 1


async def test_records_nobody_here_asked_for_are_kept_without_hanging_up(event_store):
    """In proxy mode the real cloud may run the exchange itself: its records
    are drinks too, and a hang-up from here would cut its stream off."""
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((68, _record(START, 42))))
    assert len(await _stored(event_store)) == 1
    assert _connects(bridge) == []


async def test_the_cursor_skips_what_a_previous_session_stored(event_store, monkeypatch):
    bridge, reg, ble = _relay(event_store)
    ble.get(700).history = {"cursor": START + 600}
    writes = []
    real = event_store.record_event

    async def counting(row):
        writes.append(row["ts"])
        return await real(row)

    monkeypatch.setattr(event_store, "record_event", counting)
    body = _record(START, 42) + _record(START + 600, 7) + _record(START + 900, 3)
    await bridge._handle_event(reg.get(10), "ble_response", _response((68, body)))
    assert writes == [START + 900]
    assert ble.get(700).history["cursor"] == START + 900


async def test_nothing_waiting_ends_the_session_at_once(event_store):
    bridge, reg, _ = _relay(event_store)
    await _link_up_and_answer(bridge, reg)
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((212, bytes(5))))
    assert _connects(bridge) == [0]


async def test_a_history_is_asked_for_once_per_interval(event_store, monkeypatch):
    bridge, reg, _ = _relay(event_store)
    clock = [1794949774.0]
    monkeypatch.setattr(ble_relay.time, "time", lambda: clock[0])
    await _link_up_and_answer(bridge, reg)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_closed"])

    for advance, event_id in ((240, "100000001_1794950014"), (900, "100000001_1794950914")):
        clock[0] += advance
        later = json.loads(json.dumps(RELAY["over_link_up"]))
        later["params"]["event_id"] = event_id
        await bridge._handle_event(reg.get(10), "ble_relay_over", later)
        status = base64.b64decode(CTW3_CMD230)
        await bridge._handle_event(reg.get(10), "ble_response", _response((230, status)))

    # The first link-up asks; 240 s later it does not; 1140 s later it does.
    assert [cmd for cmd, _ in _ble_frames(bridge)].count(212) == 2


async def test_no_store_means_no_request(event_store):
    """A history with nowhere to go is not worth holding a session for."""
    bridge, reg, _ = _relay(None)
    await _link_up_and_answer(bridge, reg)
    assert 212 not in [cmd for cmd, _ in _ble_frames(bridge)]
    assert _connects(bridge) == [0]


async def test_no_broker_sends_nothing_and_raises_nothing(event_store):
    bridge, reg, ble = _relay(event_store)
    bridge._client = None
    await _link_up_and_answer(bridge, reg)
    assert "asked_at" not in ble.get(700).history


async def test_only_a_ctw3_is_asked(event_store):
    bridge, reg, ble = _relay(event_store)
    ble.remove(700)
    ble.register(ble_type="w5", petkit_id=701, mac=MAC.upper(), link_with=10)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    w5_status = bytes(18)
    await bridge._handle_event(reg.get(10), "ble_response", _response((210, w5_status)))
    assert 212 not in [cmd for cmd, _ in _ble_frames(bridge)]


@pytest.mark.parametrize("payload", [
    [{"cmd": 68, "data": base64.b64encode(b"\x01\x02\x03").decode()}],
    [{"cmd": 68, "data": base64.b64encode(bytes(12)).decode()}],
    [{"cmd": [68], "data": "AAAA"}],
    [{"cmd": 212, "data": "AA=="}],
    "garbage",
])
async def test_garbled_history_is_ignored(event_store, payload):
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_response", {"params": {
        "content": json.dumps({"device": {"mac": MAC}, "payload": payload})}})
    assert await _stored(event_store) == []


async def test_a_failing_store_loses_the_batch_not_the_relay(event_store, monkeypatch):
    bridge, reg, ble = _relay(event_store)

    async def broken(row):
        raise RuntimeError("disk full")

    monkeypatch.setattr(event_store, "record_event", broken)
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((68, _record(START, 42))))
    assert "cursor" not in ble.get(700).history


async def test_the_session_end_reports_a_stream_that_never_came(event_store, caplog):
    bridge, reg, _ = _relay(event_store)
    await _link_up_and_answer(bridge, reg)
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((212, bytes([0, 0, 0, 0, 60]))))
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_closed"])
    assert "announced 60 bytes of drink history and 0 arrived" in caplog.text
    assert bridge._ble_relay._history == {}


async def test_the_cursor_persists_with_the_accessory(tmp_path):
    ble = BLERegistry(tmp_path / "ble.json")
    dev = ble.register(ble_type="ctw3", petkit_id=700, mac=MAC.upper(), link_with=10)
    dev.history = {"cursor": START, "asked_at": START}
    ble.save()
    assert BLERegistry(tmp_path / "ble.json").get(700).history == {"cursor": START,
                                                                   "asked_at": START}


# --- where the rows show up ----------------------------------------------------

async def test_the_timeline_shows_each_record_as_a_timed_drinking_card(event_store):
    for raw, start, secs in ((1, START, 42), (2, START + 600, 7)):
        await event_store.record_event(
            ingest.from_ble_drink_record(700, "ctw3", raw, start, secs))
    sessions = ingest.group_sessions(await event_store.query_timeline(device_id=700))
    assert sorted((s["event_kind"], s["duration_sec"], s["display_ts"]) for s in sessions) == [
        (codes.KIND_DRINKING, 7, START + 600), (codes.KIND_DRINKING, 42, START)]
    assert ingest.filter_counts(sessions)["drinking"] == 2


@pytest.fixture
async def acc(tmp_path: Path, event_store, pet_registry):
    e = AccessoryEnv(tmp_path, event_store, pet_registry)
    await e.start()
    await e.auth.set_password(PASSWORD)
    e.token = await e.login()
    yield e
    await e.client.close()


async def test_the_app_api_lists_the_fountains_drinks_for_its_day(acc: Any):
    await acc.store.record_event(
        ingest.from_ble_drink_record(CTW3_ID, "ctw3", 1, START, 42))
    day = time.strftime("%Y-%m-%d", time.localtime(START))
    page = await acc.json(f"/timeline?device={CTW3_ID}&kind=drinking&day={day}")
    (item,) = page["items"]
    assert item["device_id"] == CTW3_ID and item["device_type"] == "ctw3"
    assert item["event_kind"] == codes.KIND_DRINKING and item["filters"] == ["drinking"]
    assert item["duration_sec"] == 42 and item["display_ts"] == START
    assert item["label"] == "Drinking done"
