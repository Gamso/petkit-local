"""The frame layer both fountain families share, in both directions.

One header, one command numbering, one encoding — and the handful of protocol
facts that are not per-model either. The DATA layout inside a frame is the part
that differs, and it lives in `w5.py` and `ctw3.py`; nothing here may import
them, because both import this.
"""
from __future__ import annotations

import base64
import urllib.parse
from collections.abc import Iterator
from typing import Any

from petkit_local.events.codes import (
    BLE_OPCODES, BLE_SEND_ACK, BLE_SEND_READ, BLE_SEND_SESSION, BLE_SEND_WRITE,
    BLE_STATUS_ACK,
)

# The framing both fountain families share, carried as urlencode(base64(...))
# inside a relayed `ble_response`:
#
#     FA FC FD | cmd | type | seq | len_lo | len_hi | payload | FB
#
# Only the DATA layout inside it differs per model, so the decode/unframe
# helpers below are shared and the offset tables are not.
#
# The length is a 16-bit LITTLE-endian field, and getting that wrong is not a
# cosmetic detail: emit a single length byte and the accessory reads the first
# byte of our PAYLOAD as the high half of the length. A 4-byte mode write then
# announces 4 bytes and delivers 3 plus the trailer; a 12-byte config write
# announces 0x030C = 780 and is still being waited for when the session closes.
# Both fail in silence.
#
# Three sources agree on the 8-byte header: mr-ransel's W5 protocol notes,
# aavdberg/ha-petkit's builder and parser, and `_ble_unframe` immediately
# below, which reads the payload from offset 8.
BLE_FRAME_HEADER = bytes([0xFA, 0xFC, 0xFD])
BLE_FRAME_TRAILER = 0xFB
BLE_FRAME_HEADER_LEN = 8

# The OTHER header, for the packets of a history stream (`codes.BLE_OPCODES`
# 67-82). PetKit's app parses both in one constructor (`PetkitBleMsg.<init>([B)`,
# app 13.8.1, STREAM_HEADER at @0x00c0, parsed @0x007b-0x00b7):
#
#     FA FC FE | cmd | type | index | total | len_lo | len_hi | data ...
#
# A packet has an index within its batch and the batch's size where a command
# frame has its sequence byte, so the header is one byte longer. The length is
# little-endian like the command frame's, and the data is read by it, not by
# the trailer.
BLE_STREAM_HEADER = bytes([0xFA, 0xFC, 0xFE])
BLE_STREAM_HEADER_LEN = 9

#: The `type` byte: 1 request, 2 response, 3 request wanting no answer. We
#: send requests, and one response: the ack a pushed 230 is owed
#: (`codes.BLE_STATUS_ACK`).
BLE_TYPE_REQUEST = 0x01
BLE_TYPE_RESPONSE = 0x02

# The commands, in the one numbering that runs in both directions. `cmd` and
# the opcode inside the frame are THE SAME NUMBER — 220 is 0xDC — which is
# worth stating because mapping one to the other as though they were different
# numbers makes 222 look like a value nobody has.
#
# What each one means, how sure that is and where it came from is graded in
# `events/codes.py::BLE_OPCODES` (NS8); these names are only the handles the
# decoders and builders use, and a test holds every one of them to a row there.
CMD_GET_STATE = 210         # short status block, on request
CMD_GET_CONFIG = 211        # settings block. A CTW3 answers it over the relay.
CMD_SET_MODE = 220          # power / mode. CTW3 adds a suspend byte.
CMD_SET_CONFIG = 221        # the settings block, written whole
CMD_RESET_FILTER = 222      # filter life back to 100%
CMD_DEVICE_STATUS = 230     # status the accessory pushes unasked
CMD_SYNC_HISTORY = 212      # CTW3: "send me your drink history"
CMD_GET_LIGHT_SCHEDULE = 215  # CTW3: lamp-ring schedule
CMD_GET_DND_SCHEDULE = 216  # CTW3: do-not-disturb schedule
CMD_SYNC_BATTERY = 66       # CTW3: battery, first after the security check
CMD_GET_DEVICE_ID = 213     # the first frame of a direct-link session
CMD_SECURITY_CHECK = 86     # the pairing secret, 8 bytes
CMD_SYNC_TIME = 84          # the clock: seconds since 2000 and the zone
CMD_GET_VERSION = 200       # hardware and firmware
CMD_GET_DEVICE_LOG = 201    # ends the security check
CMD_STREAM_SETTING = 80     # starts the history stream
CMD_STREAM_CHECK = 67       # "which packets arrived?"
CMD_STREAM_DATA = 68        # history records, in a stream frame
CMD_STREAM_DATA_82 = 82     # the same, by a branch the app does not tell apart
CMD_STREAM_END = 69         # "that was all"

#: The WRITES we know how to build a frame for. Anything else is refused
#: rather than sent as a frame whose length or payload we would be inventing.
BLE_SENDABLE = frozenset(
    cmd for cmd, op in BLE_OPCODES.items() if op.send == BLE_SEND_WRITE)

#: The READS: an empty payload, asking the accessory to report. Kept apart from
#: `BLE_SENDABLE` because the two travel differently — a write opens its own
#: session (`mqtt/ble_relay.py`), a read is sent into one the parent has just
#: reported up, and opening another there would restart the link it asks over.
BLE_READABLE = frozenset(
    cmd for cmd, op in BLE_OPCODES.items() if op.send == BLE_SEND_READ)

#: What the CTW3 direct-link session may send beyond the reads: the frames
#: whose payload it builds itself, and the type-2 answers it owes.
BLE_SESSION_SENDABLE = frozenset(
    cmd for cmd, op in BLE_OPCODES.items()
    if op.send in (BLE_SEND_READ, BLE_SEND_SESSION, BLE_SEND_ACK))

#: The mode values a W5-family fountain really runs in. Anything else is the
#: device saying "not running right now" in a field that has no way to say it,
#: and it must not be stored — `ble_command_for` rebuilds the mode frame from
#: the last reading, so a latched 0 is a command to switch off.
#:
#: A W5, W4, W4X or CTW2 reports 0 whenever it is powered off (aavdberg/ha-petkit
#: #106): the mode select renders blank, and switching the fountain back on
#: silently puts it in normal mode because the smart it was in reads as nothing.
#:
#: A CTW3 has a third mode, 3 on battery, and its own graded table
#: (`codes.CTW3_MODES`); only its suspected smart-cycle 0 (their #57) is
#: dropped there for the same reason as here.
FOUNTAIN_MODES = (1, 2)


def _ble_decode_data(blob: Any) -> bytes | None:
    """Decode one `ble_response` frame's `data` field into raw bytes.

    The field is urlencode(base64(bytes)) — localkit's W5/Device.php reads it as
    `base64_decode(urldecode(payload))`. Returns None for anything that is
    neither that nor the observed hex fallback, so a garbled frame is skipped
    rather than decoded into nonsense.
    """
    if isinstance(blob, (bytes, bytearray)):
        return bytes(blob)
    if not isinstance(blob, str):
        return None
    s = urllib.parse.unquote(blob).strip()
    try:
        return base64.b64decode(s)
    except Exception:
        # Not every payload is base64; the hex form is the observed fallback.
        try:
            return bytes.fromhex(s)
        except ValueError:
            return None


def _ble_unframe(raw: bytes) -> tuple[int | None, bytes]:
    """Split a W5 BLE frame into `(cmd, data_bytes)`.

    Handles both a full `FA FC FD` frame and the pre-split case where `data`
    already IS the DATA payload; in the latter case the command is None and the
    caller falls back to the `cmd` the JSON carried alongside it.

    A history-stream packet (`FA FC FE`, `BLE_STREAM_HEADER`) is split too, by
    its own header and length; its index and batch size are dropped, because
    every packet's records stand on their own. A relayed frame has so far
    always arrived with the header already stripped, so this is the case for
    a parent that passes one through whole.
    """
    if len(raw) >= BLE_FRAME_HEADER_LEN + 1 and raw[:3] == BLE_FRAME_HEADER:
        return raw[3], raw[BLE_FRAME_HEADER_LEN:-1]
    if len(raw) >= BLE_STREAM_HEADER_LEN and raw[:3] == BLE_STREAM_HEADER:
        length = raw[7] | (raw[8] << 8)
        return raw[3], raw[BLE_STREAM_HEADER_LEN:BLE_STREAM_HEADER_LEN + length]
    return None, raw


def _iter_ble_raw(content: Any) -> Iterator[tuple[Any, bytes]]:
    """`(json cmd, raw bytes)` for every decodable entry of a `ble_response`."""
    if not isinstance(content, dict):
        return
    payload = content.get("payload")
    items = payload if isinstance(payload, list) else []
    for alt in ("data", "value", "frame"):
        if alt in content and content[alt] is not None:
            items = items + [{"cmd": None, "data": content[alt]}]
    for item in items:
        if not isinstance(item, dict) or "data" not in item:
            continue
        raw = _ble_decode_data(item.get("data"))
        if raw is not None:
            yield item.get("cmd"), raw


def ble_frame_header(raw: bytes) -> dict[str, int] | None:
    """What a WHOLE relayed frame's header says, or None for a bare DATA block.

    `{"cmd", "type", "seq"}` for a command frame, `{"cmd", "type", "index",
    "total"}` for a stream packet. The relay has so far always stripped the
    header, so this is for a parent that does not -- and it is the only way a
    sequence byte or a packet index can reach us.
    """
    if len(raw) >= BLE_FRAME_HEADER_LEN + 1 and raw[:3] == BLE_FRAME_HEADER:
        return {"cmd": raw[3], "type": raw[4], "seq": raw[5]}
    if len(raw) >= BLE_STREAM_HEADER_LEN and raw[:3] == BLE_STREAM_HEADER:
        return {"cmd": raw[3], "type": raw[4], "index": raw[5], "total": raw[6]}
    return None


def _iter_ble_frames(content: Any) -> Iterator[tuple[Any, bytes]]:
    """Yield `(cmd, data_bytes)` for every frame in a `ble_response` content.

    The content shape is `{device: {mac}, payload: [{cmd, data}, ...]}`; a
    single loose blob under `data`/`value`/`frame` is accepted too, because the
    proxying firmware does not always wrap one frame in a list. Undecodable
    entries are skipped silently — a BLE accessory is a best-effort extra, and
    one bad frame must not cost the parent's whole report.
    """
    for json_cmd, raw in _iter_ble_raw(content):
        framed_cmd, data = _ble_unframe(raw)
        yield (framed_cmd if framed_cmd is not None else json_cmd, data)


# --- writing to an accessory -------------------------------------------------
#
# The other direction of the same relay: we publish `thing/service/ble` to the
# PARENT, which forwards the bytes over its open BLE session and does not
# interpret them.
#
# Every write here restates a whole block. The device has no "set one field"
# frame, so each builder starts from the accessory's last decoded status and
# replaces one value — and refuses outright when that status has never
# arrived. Filling the unknown half with zeros would turn the light off and
# reset both intervals as a side effect of changing the brightness.


def build_ble_frame(cmd: int, seq: int, payload: bytes,
                    frame_type: int = BLE_TYPE_REQUEST) -> str:
    """One outbound BLE frame, encoded the way `thing/service/ble` carries it.

    `FA FC FD | cmd | type | seq | len_lo | len_hi | payload | FB`, then base64,
    then urlencode — the exact inverse of what `_ble_decode_data` and
    `_ble_unframe` undo on the way in.

    `seq` wraps at a byte. Nothing has been observed rejecting a repeat, but it
    is a sequence number and sending a constant would be the kind of detail
    that works until it does not.
    """
    body = bytes([*BLE_FRAME_HEADER, cmd & 0xFF, frame_type & 0xFF, seq & 0xFF,
                  len(payload) & 0xFF, (len(payload) >> 8) & 0xFF,
                  *payload, BLE_FRAME_TRAILER])
    return urllib.parse.quote(base64.b64encode(body).decode())


def ble_command_frame(cmd: int, seq: int, payload: bytes) -> str | None:
    """The encoded frame for one command, or None if it is not one we send."""
    if cmd not in BLE_SENDABLE:
        return None
    return build_ble_frame(cmd, seq, payload)


def ble_read_frame(cmd: int, seq: int) -> str | None:
    """The encoded frame for one read, or None if it is not a read we send.

    Always an empty payload: `fa fc fd d2 01 00 00 00 fb` is a 210 at seq 0.
    """
    if cmd not in BLE_READABLE:
        return None
    return build_ble_frame(cmd, seq, b"")


def ble_session_frame(cmd: int, seq: int, payload: bytes,
                      frame_type: int = BLE_TYPE_REQUEST) -> str | None:
    """The encoded frame for one step of the CTW3 direct-link session.

    None for a cmd the session has no business sending
    (`BLE_SESSION_SENDABLE`): a write never goes out this way.
    """
    if cmd not in BLE_SESSION_SENDABLE:
        return None
    return build_ble_frame(cmd, seq, payload, frame_type)


def ble_status_ack_frame() -> str:
    """The encoded answer to a pushed 230: `fa fc fd e6 02 00 01 00 01 fb`.

    Type 2, sequence 0, one byte `01` -- PetKit's app's own
    `getDeviceUpdateResponse`, graded in `codes.BLE_STATUS_ACK`.
    """
    cmd, frame_type, data = BLE_STATUS_ACK
    return build_ble_frame(cmd, 0, data, frame_type)
