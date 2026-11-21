"""A CTW3's drink history, asked for over its parent's relay and stored locally.

What is held here, all of it from PetKit's app 13.8.1 and none of it yet seen
from a real fountain (`codes.BLE_OPCODES` 212 and 67-82 grade it):

* a CTW3 whose history is due gets PetKit's whole direct-link session through
  its parent (#140): 213, the secret (86), the clock (84), 200, 201, 66, 210,
  211, 215, 216, 212, then 80 and the stream -- one frame in flight at a time,
  each step timed out on its own, once per `HISTORY_SYNC_INTERVAL_SECONDS`, and
  never with no store or no secret;
* every status a CTW3 pushes (230) is answered with the app's own ack;
* its reply announces a big-endian byte count in DATA bytes 1-4; a history
  packet (68 or 82, relayed bare or in its `FA FC FE` stream frame) is whole
  6-byte records, `workTime` then a signed `stayTime`, big-endian;
* every plausible record becomes ONE drinking row on the accessory's own id,
  timed by its own span, deduplicated by the fountain's timestamp, and the
  cursor that skips them next time is persisted;
* the stream's 67 and 69 are answered as the app answers them, and nothing a
  fountain sends raises;
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

#: A placeholder for the owner's pairing secret: 16 hex digits, as the cloud
#: hands it out in `dev_ble_device`.
SECRET = "0123456789abcdef"
#: The clock the session's 84 is built from, and the parent's zone (UTC+1).
NOW = 1794949774.0


def _relay(event_store=None, secret=SECRET):
    reg = DeviceRegistry()
    parent = reg.get_or_create(petkit_id=10, device_type="t4", serial_number="SN10")
    parent.mqtt_connected = True
    parent.config["timezone"] = 1.0
    ble = BLERegistry()
    ble.register(ble_type="ctw3", petkit_id=700, mac=MAC.upper(), secret=secret,
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


async def _stored(store, device_id=700):
    return [r for r in await store.query_timeline(device_id=device_id, limit=100)
            if r["event_type"] == codes.BLE_DRINK_EVENT]


#: What a fountain that answers every step says, per cmd. Byte 0 of 86, 84 and
#: 80 is the `01` the app checks for; the rest is the shape the app reads.
ANSWERS = {
    213: (400066264).to_bytes(8, "big") + b"20260101C30001",
    86: b"\x01",
    84: b"\x01",
    200: b"\x01\x6f",
    201: bytes(24),
    66: bytes([0x10, 0x68, 0x55]),
    210: base64.b64decode(CTW3_CMD230)[:30],
    211: bytes.fromhex("0303007804b0010300000100"),
    215: b"\x01\x00",
    216: b"\x00\x00",
    212: bytes([0, 0, 0, 0, 12]),
    80: b"\x01",
}


async def _fake_fountain(bridge, reg, answers=ANSWERS, stop_at=None):
    """Answer each frame the relay sends, one at a time, until it stops sending
    or `stop_at` is reached (that one goes unanswered)."""
    answered = 0
    for _ in range(40):
        frames = _ble_frames(bridge)
        if len(frames) == answered:
            return
        cmd, _ = frames[answered]
        answered += 1
        if cmd == stop_at or cmd not in answers:
            return
        await bridge._handle_event(reg.get(10), "ble_response", _response((cmd, answers[cmd])))


@pytest.fixture
def clock(monkeypatch):
    now = [NOW]
    monkeypatch.setattr(ble_relay.time, "time", lambda: now[0])
    return now


#: The frames of a whole session, hex, with `SECRET` and `NOW` at UTC+1. The
#: 84 is `00 | 328f7f0e` (NOW - 2000-01-01, seconds) `| 0d` (1 + 12).
SESSION_FRAMES = [
    (213, "fafcfdd501000000fb"),
    (86, "fafcfd56010108000123456789abcdeffb"),
    (84, "fafcfd540102060000328f7f0e0dfb"),
    (200, "fafcfdc801030000fb"),
    (201, "fafcfdc901040000fb"),
    (66, "fafcfd4201050000fb"),
    (210, "fafcfdd201060000fb"),
    (211, "fafcfdd301070000fb"),
    (215, "fafcfdd701080000fb"),
    (216, "fafcfdd801090000fb"),
    (212, "fafcfdd4010a0000fb"),
    (80, "fafcfd50010b08000000002000000014fb"),
]


async def test_a_due_ctw3_gets_the_whole_direct_link_session_in_order(event_store, clock):
    """#140: PetKit's app reads a CTW3's history only over its own Bluetooth,
    after 213, the secret (86), the clock (84), 200, 201, 66, 210, 211, 215,
    216. Each frame goes once the one before it is answered."""
    bridge, reg, ble = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    # One frame in flight: the link-up sends the 213 and nothing else.
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [213]

    await _fake_fountain(bridge, reg)
    assert [(cmd, data.hex()) for cmd, data in _ble_frames(bridge)] == SESSION_FRAMES
    assert _connects(bridge) == [], "hung up before the stream"
    assert ble.get(700).history["asked_at"] == NOW
    relay = bridge._ble_relay
    assert relay._ctw3_sessions[700].stage == ble_relay.SESSION_STREAM

    # The stream: two records in one packet, the batch check, the end.
    # An hour before the session's own clock, so inside every plausibility window.
    start = NOW - 3600
    body = _record(start, 42) + _record(start + 600, 7)
    await bridge._handle_event(reg.get(10), "ble_response", _response((68, body)))
    await bridge._handle_event(reg.get(10), "ble_response", _response((67, b"")))
    await bridge._handle_event(reg.get(10), "ble_response", _response((69, b"")))

    rows = await _stored(event_store)
    assert sorted(r["ts"] for r in rows) == [start, start + 600]
    assert ble.get(700).history["cursor"] == start + 600
    tail = [(cmd, data.hex()) for cmd, data in _ble_frames(bridge)][len(SESSION_FRAMES):]
    assert tail == [
        # One packet held: bit 31 set (`checkStreamData`), type 2, seq 0.
        (67, "fafcfd4302000400" + "80000000" + "fb"),
        # The end, answered empty, type 2.
        (69, "fafcfd4502000000fb"),
    ]
    assert _connects(bridge) == [0]
    assert relay._ctw3_sessions == {} and relay._history == {}


async def test_each_step_and_its_reply_is_logged(event_store, clock, caplog):
    import logging

    caplog.set_level(logging.INFO, logger="petkit_local.mqtt.ble_relay")
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await _fake_fountain(bridge, reg)
    assert "session step 1/11 (cmd 213 (Get device id)) -> sent" in caplog.text
    assert "session step 1/11 (cmd 213 (Get device id)) <- reply 0000000017d8" in caplog.text
    assert "session step 3/11 (cmd 84 (Set clock)) -> sent, seq 2, payload 00328f7f0e0d" \
        in caplog.text
    # The secret itself never reaches the log.
    assert SECRET not in caplog.text and "<8-byte secret>" in caplog.text


async def test_a_silent_step_is_skipped_and_only_a_silent_212_stops_the_session(
        event_store, clock, monkeypatch, caplog):
    """Through a T4 the owner's fountain did not answer the 213 (2026-11-21).
    A silent step before the 212 is skipped, so one session maps which steps
    the parent passes; only a silent 212 ends it."""
    import asyncio
    import logging

    caplog.set_level(logging.INFO, logger="petkit_local.mqtt.ble_relay")
    monkeypatch.setattr(ble_relay, "CTW3_STEP_TIMEOUT_SECONDS", 0.01)
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await _fake_fountain(bridge, reg, stop_at=86)
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [213, 86]
    assert _connects(bridge) == []

    await asyncio.sleep(0.5)
    assert "step 2/11 (cmd 86 (Security check)): no answer within 0.01 s, skipping it" in caplog.text
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [
        213, 86, 84, 200, 201, 66, 210, 211, 215, 216, 212]
    assert "session STOPPED: no answer to step 11/11 (cmd 212" in caplog.text
    assert _connects(bridge) == [0]
    assert bridge._ble_relay._ctw3_sessions == {}


async def test_a_refused_secret_stops_the_session(event_store, clock, caplog):
    """The app stops when the 86's reply is not `01` (\"CMD_PETKIT_BLE_V fail\")."""
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await _fake_fountain(bridge, reg, answers={**ANSWERS, 86: b"\x00"})
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [213, 86]
    assert "cmd 86 (Security check) was refused (00; the app needs 01)" in caplog.text
    assert _connects(bridge) == [0]


async def test_the_owners_252s_do_not_move_the_session(event_store, clock):
    """The owner's fountain says `252 0001` at link-up and after frames (#140).
    It is no step's reply: the session keeps waiting, and a 252 does not start
    a second one."""
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await bridge._handle_event(reg.get(10), "ble_response", RELAY["response_252"])
    await bridge._handle_event(reg.get(10), "ble_response", _response((252, b"\x00\x01")))
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [213]
    assert _connects(bridge) == []
    bridge._ble_relay._cancel_timer(bridge._ble_relay._ctw3_sessions[700])


async def test_a_reply_with_another_sequence_is_not_the_steps(event_store, clock):
    """When the parent passes a whole frame through, its sequence must match."""
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    wrong = bytes([0xFA, 0xFC, 0xFD, 213, 2, 9, 8, 0, *ANSWERS[213][:8], 0xFB])
    await bridge._handle_event(reg.get(10), "ble_response", _response((213, wrong)))
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [213]
    right = bytes([0xFA, 0xFC, 0xFD, 213, 2, 0, 8, 0, *ANSWERS[213][:8], 0xFB])
    await bridge._handle_event(reg.get(10), "ble_response", _response((213, right)))
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [213, 86]
    bridge._ble_relay._cancel_timer(bridge._ble_relay._ctw3_sessions[700])


async def test_a_stream_frame_with_its_header_gives_its_index_and_seq(event_store, clock):
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await _fake_fountain(bridge, reg)
    packet = _stream_frame(68, _record(START, 5), index=1, total=2)
    await bridge._handle_event(reg.get(10), "ble_response", {"params": {
        "content": json.dumps({"device": {"mac": MAC}, "payload": [
            {"cmd": None, "data": base64.b64encode(packet).decode()}]})}})
    check = bytes([0xFA, 0xFC, 0xFD, 67, 1, 7, 0, 0, 0xFB])
    await bridge._handle_event(reg.get(10), "ble_response", _response((67, check)))
    cmd, frame = _ble_frames(bridge)[-1]
    # Packet 1 of 2 held, packet 0 missing: bit 30 only, the request's seq 7.
    assert (cmd, frame.hex()) == (67, "fafcfd4302070400" + "40000000" + "fb")
    bridge._ble_relay._cancel_timer(bridge._ble_relay._ctw3_sessions[700])


async def test_nothing_waiting_ends_the_session_at_once(event_store, clock):
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await _fake_fountain(bridge, reg, answers={**ANSWERS, 212: bytes(5)})
    assert 80 not in [cmd for cmd, _ in _ble_frames(bridge)]
    assert _connects(bridge) == [0]


async def test_a_status_push_mid_session_is_acked_and_moves_nothing(event_store, clock):
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((230, base64.b64decode(CTW3_CMD230))))
    frames = [(cmd, data.hex()) for cmd, data in _ble_frames(bridge)]
    assert frames == [(213, "fafcfdd501000000fb"), (230, "fafcfde60200010001fb")]
    assert _connects(bridge) == []
    bridge._ble_relay._cancel_timer(bridge._ble_relay._ctw3_sessions[700])


async def test_a_pushed_status_is_answered_with_the_apps_ack(event_store):
    """`CTW3DataConvertor.getDeviceUpdateResponse`: cmd 230, type 2, data 01,
    sequence 0 -- the answer the app sends to every 230 a CTW3 pushes."""
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_response",
                               _response((230, base64.b64decode(CTW3_CMD230))))
    acks = [data for cmd, data in _ble_frames(bridge) if cmd == 230]
    assert acks == [bytes.fromhex("fafcfde60200010001fb")]


async def test_a_w5_status_is_not_acked(event_store):
    """The ack is the CTW3 client's; the W5 family's is not in the app."""
    bridge, reg, ble = _relay(event_store)
    ble.remove(700)
    ble.register(ble_type="w5", petkit_id=701, mac=MAC.upper(), link_with=10)
    await bridge._handle_event(reg.get(10), "ble_response", _response((230, bytes(18))))
    assert 230 not in [cmd for cmd, _ in _ble_frames(bridge)]


async def test_a_session_runs_once_per_interval(event_store, clock):
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await _fake_fountain(bridge, reg, answers={**ANSWERS, 212: bytes(5)})

    for advance, event_id in ((240, "100000001_1794950014"), (900, "100000001_1794950914")):
        clock[0] += advance
        later = json.loads(json.dumps(RELAY["over_link_up"]))
        later["params"]["event_id"] = event_id
        await bridge._handle_event(reg.get(10), "ble_relay_over", later)
        await _fake_fountain(bridge, reg, answers={**ANSWERS, 212: bytes(5)})

    sent = [cmd for cmd, _ in _ble_frames(bridge)]
    # The first link-up runs it; 240 s later only the reads go; 1140 s later
    # it runs again.
    assert sent.count(213) == 2 and sent.count(212) == 2


async def test_no_secret_means_the_plain_reads(event_store, caplog):
    import logging

    caplog.set_level(logging.INFO, logger="petkit_local.mqtt.ble_relay")
    bridge, reg, ble = _relay(event_store, secret="")
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [210, 211]
    assert "no usable pairing secret" in caplog.text
    assert "asked_at" not in ble.get(700).history


async def test_no_store_means_no_session(event_store):
    """A history with nowhere to go is not worth holding a session for."""
    bridge, reg, _ = _relay(None)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [210, 211]


async def test_no_broker_sends_nothing_and_raises_nothing(event_store):
    bridge, reg, ble = _relay(event_store)
    bridge._client = None
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    assert "asked_at" not in ble.get(700).history


async def test_only_a_ctw3_gets_a_session(event_store):
    bridge, reg, ble = _relay(event_store)
    ble.remove(700)
    ble.register(ble_type="w5", petkit_id=701, mac=MAC.upper(), secret=SECRET, link_with=10)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [210, 211]


async def test_the_parent_closing_mid_session_ends_it(event_store, clock, caplog):
    import logging

    caplog.set_level(logging.INFO, logger="petkit_local.mqtt.ble_relay")
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await _fake_fountain(bridge, reg, stop_at=84)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_closed"])
    assert "the parent closed the link at step 3/11 (cmd 84 (Set clock))" in caplog.text
    assert bridge._ble_relay._ctw3_sessions == {}


async def test_the_session_end_reports_a_stream_that_never_came(event_store, clock, caplog):
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await _fake_fountain(bridge, reg, answers={**ANSWERS, 212: bytes([0, 0, 0, 0, 60])})
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_closed"])
    assert "announced 60 bytes of drink history and 0 arrived" in caplog.text
    assert bridge._ble_relay._history == {}


@pytest.mark.parametrize("payload", [
    "garbage", [{"cmd": [86], "data": "AA=="}], [{"cmd": 999, "data": "AQ=="}],
    [{"cmd": 86}], [{"cmd": "x", "data": "AQ=="}], [{"cmd": 67, "data": 5}],
])
async def test_garbage_mid_session_is_skipped(event_store, clock, payload):
    bridge, reg, _ = _relay(event_store)
    await bridge._handle_event(reg.get(10), "ble_relay_over", RELAY["over_link_up"])
    await bridge._handle_event(reg.get(10), "ble_response", _response((213, ANSWERS[213])))
    await bridge._handle_event(reg.get(10), "ble_response", {"params": {
        "content": json.dumps({"device": {"mac": MAC}, "payload": payload})}})
    assert [cmd for cmd, _ in _ble_frames(bridge)] == [213, 86]
    bridge._ble_relay._cancel_timer(bridge._ble_relay._ctw3_sessions[700])


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


async def test_the_cursor_persists_with_the_accessory(tmp_path):
    ble = BLERegistry(tmp_path / "ble.json")
    dev = ble.register(ble_type="ctw3", petkit_id=700, mac=MAC.upper(), link_with=10)
    dev.history = {"cursor": START, "asked_at": START}
    ble.save()
    assert BLERegistry(tmp_path / "ble.json").get(700).history == {"cursor": START,
                                                                   "asked_at": START}


# --- the session's payloads -------------------------------------------------------

def test_the_secret_is_hex_decoded_and_left_padded_to_8():
    from petkit_local.devices.ble import ctw3_secret_payload

    assert ctw3_secret_payload(SECRET) == bytes.fromhex(SECRET)
    assert ctw3_secret_payload("0a0b") == bytes(6) + b"\x0a\x0b"
    for bad in ("", "s", "abc", "zz", None, 7):
        assert ctw3_secret_payload(bad) is None


def test_the_clock_is_seconds_since_2000_and_the_zone_plus_12():
    from petkit_local.devices.ble import ctw3_time_payload

    assert ctw3_time_payload(NOW, 1.0).hex() == "00" + "328f7f0e" + "0d"
    assert ctw3_time_payload(NOW, -5.0)[-1] == 7
    assert ctw3_time_payload(NOW, 5.75)[-1] == 18


def test_the_stream_check_sets_bit_31_minus_i():
    from petkit_local.devices.ble import ctw3_stream_check_payload

    assert ctw3_stream_check_payload({0, 1}, 2) == (bytes.fromhex("c0000000"), True)
    assert ctw3_stream_check_payload({1}, 2) == (bytes.fromhex("40000000"), False)
    assert ctw3_stream_check_payload(set(range(32)), 32) == (b"\xff" * 4, True)


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
