"""What a CTW3 frame's DATA block means, in both directions.

The EverSweet Max Cordless speaks the framing in `framing.py` and nothing else
in common with the W5 family: different block lengths, different offsets,
different field names. Its decoder and its payload builders are therefore its
own, and reading one family's block with the other's offsets is the mistake the
separation exists to prevent (`w5.py` holds the other half).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from petkit_local.devices.base import Refused
from petkit_local.devices.ble.framing import (
    CMD_DEVICE_STATUS, CMD_GET_CONFIG, CMD_GET_STATE, CMD_SET_CONFIG, CMD_SET_MODE,
    CMD_STREAM_CHECK, CMD_STREAM_END, CMD_STREAM_SETTING, CMD_SYNC_HISTORY,
    _iter_ble_frames,
)
from petkit_local.events.codes import BLE_HISTORY_DATA, CTW3_BATTERY_MODE, CTW3_MODES
from petkit_local.utils.coerce import to_int

if TYPE_CHECKING:
    from petkit_local.devices.ble.registry import BLEDevice

log = logging.getLogger(__name__)

# --- CTW3 (EverSweet Max Cordless) ------------------------------------------
#
# A different DATA layout in the same framing, contributed with the hardware in
# hand (issue #4, firmware 111). Do NOT read it with the W5 offsets: the blocks
# are 30 bytes (cmd 210) or 42 (cmd 230, with a 12-byte config tail), against
# the W5's much shorter one, and the multi-byte values here are big-endian.
#
# The names are PetKit's own, taken from the account API's `kv` for this model
# rather than translated into the W5's vocabulary — `runStatus` not
# `runningStatus`, `lackWarning` not `warningWaterMissing`. Where the two
# families disagree, matching the cloud is what lets a report be compared with
# what the PetKit app shows.

#: Everything the decoder names fits in 26 bytes, and 26 is what an older
#: firmware sends — aavdberg/ha-petkit accepts that length from real hardware.
#: Demanding more drops such a block whole rather than reading the part of it
#: this decoder understands.
_CTW3_MIN_STATUS_LEN = 26

#: Single-byte fields, offset into the DATA block.
_CTW3_BYTE_FIELDS = {
    "powerStatus": 0,
    # Inverted against the English: 0 is paused/sleeping, 1 is working.
    "suspendStatus": 1,
    # 1 normal (continuous), 2 smart (intermittent), 3 battery: `codes.CTW3_MODES`.
    "mode": 2,
    # NOT a boolean. 2 is the AC path — the fountain is on mains.
    "electricStatus": 3,
    "noDisturbingSwitch": 4,
    "breakdownWarning": 5,
    "lackWarning": 6,
    "lowBattery": 7,
    "filterWarning": 8,
    "filterPercent": 13,
    "runStatus": 14,
    # 0 is nobody; anything else (0x02 observed) is a pet at the bowl.
    "detectStatus": 19,
    "batteryPercent": 24,
    # Undecoded. Kept because it is the last byte of the short block and a
    # value nobody can see is a value nobody can explain.
    "moduleStatus": 25,
}

#: Multi-byte fields as `(offset, width)`, big-endian.
_CTW3_INT_FIELDS = {
    "waterPumpRunTime": (9, 4),
    "todayPumpRunTime": (15, 4),
    "supplyVoltage": (20, 2),
    "batteryVoltage": (22, 2),
}

#: Where the 12-byte config tail starts in a cmd-230 block.
CTW3_CONFIG_OFFSET = 30
CTW3_CONFIG_LEN = 12


def decode_ctw3_config(tail: bytes) -> dict[str, int]:
    """The 12-byte config tail of a cmd-230 status, as named fields.

    Everything a cmd-221 write has to restate comes from here, which is what
    makes changing one setting possible without inventing the rest.

    SETTLED, and it was briefly unsettled for no good reason.

    PetKit's own app builds this exact block for a cmd-221 write, field by
    field (`CTW3DataConvertor.changeSmartMode` / `changeBatteryMode`, app
    13.8.1): smart working time, smart sleep time, battery working time and
    battery sleep time as big-endian shorts, then lamp ring switch, lamp ring
    brightness, do-not-disturb, child lock, and two inductive switches. Twelve
    bytes, in the order below.

    So the status tail and the write payload ARE one layout, which is what the
    real cmd-230 frame from issue #4 already said — 1, 3, 0 at bytes 6, 7, 8 is
    a fountain with the ring on at high and quiet hours off.
    aavdberg/ha-petkit reads those three as do-not-disturb / light / brightness
    and writes ten bytes rather than twelve; 1.6.0 followed them for the write
    and was wrong to. The app is the other end of that conversation and does
    not need interpreting.
    """
    if len(tail) < CTW3_CONFIG_LEN:
        return {}
    return {
        "smartWorkingTime": tail[0],
        "smartSleepTime": tail[1],
        "energyInterval": int.from_bytes(tail[2:4], "big"),
        "sleepTime": int.from_bytes(tail[4:6], "big"),
        "lightSwitch": tail[6],
        "brightness": tail[7],          # 1 low, 2 medium, 3 high
        "noDisturbingSwitch": tail[8],
        # Written by the app only on hardware+firmware/100 >= 1.35
        # (`CTW3Utils.isSupportLockVersion`), and left at 0 below it.
        "childLock": tail[9],
        "smartInductiveSwitch": tail[10],
        "batteryInductiveSwitch": tail[11],
    }


def _decode_ctw3_status(data: bytes) -> dict[str, dict[str, int]]:
    """Decode a CTW3 cmd-210/230 DATA block.

    Refuses anything shorter than the 26 bytes the short form is defined to be,
    rather than emitting the handful of fields that happen to fit — a one-byte
    ACK read permissively becomes a confident `powerStatus`. The block length is
    known, so a short one is a broken frame and is dropped.
    """
    if len(data) < _CTW3_MIN_STATUS_LEN:
        log.warning("CTW3 status frame is %d bytes, expected at least %d - dropped",
                    len(data), _CTW3_MIN_STATUS_LEN)
        return {"states": {}, "consumables": {}}

    states: dict[str, int] = {name: data[off] for name, off in _CTW3_BYTE_FIELDS.items()}
    for name, (off, width) in _CTW3_INT_FIELDS.items():
        states[name] = int.from_bytes(data[off:off + width], "big")

    # 1 and 2 are the two modes PetKit's app selects, and 3 is the one the
    # fountain enters by itself on battery (`codes.CTW3_MODES`, graded there).
    # 3 used to be dropped here with everything else outside 1/2, as "the smart
    # cycle's sleep half" -- which no capture ever showed: that suspicion is
    # about 0 (aavdberg/ha-petkit #57), and the one real frame we hold reads 1.
    # Dropping 3 left an unplugged fountain showing the mode it had on mains.
    #
    # Anything else is still dropped rather than stored: the caller merges, so
    # absence keeps the last real mode, and `ctw3_mode_payload` restates that
    # mode on every power and pause write -- a latched 0 would send one.
    mode = states.get("mode")
    if mode in CTW3_MODES:
        # Its own field, so an entity can ask a yes/no question of it (a binary
        # sensor tests for truth, not for a value). Set whenever `mode` is, so
        # the two never disagree.
        states["batteryMode"] = int(mode == CTW3_BATTERY_MODE)
    else:
        log.debug("CTW3 reported mode=%s - keeping the last real one", mode)
        states.pop("mode", None)

    # `filterPercent` is the one field a human treats as a consumable rather
    # than a state, and the entity reads it from there.
    consumables = {"filterPercent": states.pop("filterPercent")}

    if len(data) >= CTW3_CONFIG_OFFSET + CTW3_CONFIG_LEN:
        tail = data[CTW3_CONFIG_OFFSET:CTW3_CONFIG_OFFSET + CTW3_CONFIG_LEN]
        # `noDisturbingSwitch` appears in both halves; the tail is the one a
        # config write round-trips, so let it win.
        states.update(decode_ctw3_config(tail))

    return {"states": states, "consumables": consumables}


def parse_ctw3_ble_response(content: Any) -> dict[str, dict[str, Any]]:
    """Decode a CTW3 `ble_response` into the state its entities read.

    Same contract as `parse_w5_ble_response`: an empty dict means nothing was
    decodable, and the caller leaves the previous state alone.

    cmd 211 is the settings block, as on the W5 family, and a CTW3 does answer
    it over the relay — see the capture in the branch below. Its settings also
    arrive as the tail of a long cmd-230, so both paths decode the same block.
    """
    result: dict[str, dict[str, Any]] = {"states": {}, "consumables": {}}
    for cmd, data in _iter_ble_frames(content):
        if not data:
            continue
        if cmd in (CMD_DEVICE_STATUS, CMD_GET_STATE):
            dec = _decode_ctw3_status(data)
            result["states"].update(dec["states"])
            result["consumables"].update(dec["consumables"])
        elif cmd == CMD_GET_CONFIG:
            # A CTW3 DOES answer 211 — over the relay, at least. It is silent
            # to a direct GATT client on firmware 111, which is why
            # aavdberg/ha-petkit skips the command entirely, and why nothing
            # here ever asked. The reply is the same 12-byte block as a 221
            # write and the tail of a long 230, so it decodes with no new
            # offsets: `03 03 00 78 04 b0 01 03 00 00 01 00` off a live one.
            result["states"].update(decode_ctw3_config(data))
    return {k: v for k, v in result.items() if v}


def ctw3_mode_payload(power: int, suspend: int, mode: int) -> bytes:
    """cmd 220 on a CTW3 — the three settings that share one frame.

    All three travel together, so changing one means restating the other two.
    `suspend` is 1 for WORKING and 0 for paused, inverted against its name; a
    fountain being switched off has nothing to suspend, so 0 is forced there
    rather than left at whatever the last reading held.

    Three bytes, matching aavdberg/ha-petkit's `build_ctw3_mode_payload`. A
    leading zero does not belong here: that byte is the frame header's `len_hi`,
    and putting it in the payload compensates for a framing bug in the wrong
    place (see `framing.py` on the 16-bit length).
    """
    if not power:
        suspend = 0
    return bytes([power & 0xFF, suspend & 0xFF, mode & 0xFF])


def ctw3_config_payload(state: dict[str, Any]) -> bytes | None:
    """cmd 221 on a CTW3 — the settings block, rebuilt from the last status.

    Twelve bytes, the same layout `decode_ctw3_config` reads and the same one
    PetKit's app writes. A third-party capture shows ten bytes in a different
    order; where the two disagree the app's own write is what the accessory is
    known to accept.

    Returns None when the accessory has never reported a long status.
    """
    needed = ("smartWorkingTime", "smartSleepTime", "energyInterval", "sleepTime",
              "lightSwitch", "brightness", "noDisturbingSwitch")
    if any(k not in state for k in needed):
        return None
    return bytes([
        int(state["smartWorkingTime"]) & 0xFF,
        int(state["smartSleepTime"]) & 0xFF,
        *int(state["energyInterval"]).to_bytes(2, "big"),
        *int(state["sleepTime"]).to_bytes(2, "big"),
        int(state["lightSwitch"]) & 0xFF,
        int(state["brightness"]) & 0xFF,
        int(state["noDisturbingSwitch"]) & 0xFF,
        int(state.get("childLock", 0)) & 0xFF,
        int(state.get("smartInductiveSwitch", 0)) & 0xFF,
        int(state.get("batteryInductiveSwitch", 0)) & 0xFF,
    ])


#: Entity key -> the field of `states` it sets, per frame. A block restates
#: every field it carries, so which frame a key belongs to decides what else
#: has to be read back out of the last status.
_CTW3_MODE_FIELDS = {
    "ctw3_power": "powerStatus",
    "ctw3_working": "suspendStatus",
    "ctw3_mode": "mode",
}
_CTW3_CONFIG_FIELDS = {
    "ctw3_light": "lightSwitch",
    "ctw3_brightness": "brightness",
    "ctw3_dnd": "noDisturbingSwitch",
    "ctw3_child_lock": "childLock",
    "ctw3_energy_interval": "energyInterval",
    "ctw3_sleep_time": "sleepTime",
    "ctw3_smart_work": "smartWorkingTime",
    "ctw3_smart_sleep": "smartSleepTime",
}

CTW3_WRITABLE = (frozenset(_CTW3_MODE_FIELDS) | frozenset(_CTW3_CONFIG_FIELDS)
                 | {"ctw3_reset_filter"})


class OnBattery(Refused):
    """A mode write to a CTW3 that reports it is on battery.

    Its own class so the app API can answer with a code a phone can act on
    (`on_battery`) rather than a sentence it can only display.
    """

    code = "on_battery"


#: What a refused mode write says, wherever it surfaces: the HA log, the
#: panel's toast, the app API's `message`.
ON_BATTERY_REASON = ("the fountain is on battery and runs its battery mode until it "
                     "is plugged in again; its battery working and sleep times are "
                     "what can be set meanwhile")


def _ctw3_command_for(ble_dev: BLEDevice, key: str, value: int) -> tuple[int, bytes]:
    """`(cmd, payload)` for one CTW3 entity. See `ble_command_for`.

    Raises:
        OnBattery: a mode write while the fountain reports mode 3. PetKit's app
            skips both of its mode buttons then (`CTW3HomeActivity.onClick`,
            13.8.1), and so does this: nothing is sent.
        Refused: a mode that cannot be selected, or a block never reported.
    """
    states = dict(ble_dev.state.get("states") or {})

    if key in _CTW3_MODE_FIELDS:
        if key == "ctw3_mode":
            mode = CTW3_MODES.get(value)
            if mode is None or not mode.selectable:
                raise Refused(f"mode {value} is not one a CTW3 can be switched to")
            if states.get("mode") == CTW3_BATTERY_MODE:
                raise OnBattery(ON_BATTERY_REASON)
        # Power and pause are NOT refused on battery: PetKit's Pause button
        # restates the mode the fountain reports, 3 included
        # (`CTW3HomePresenter.changePauseState`), and so does this frame.
        states[_CTW3_MODE_FIELDS[key]] = value
        if key == "ctw3_mode":
            # Picking a mode means "run in it": PetKit's app sends power 1 and
            # pause 1 with the chosen mode and reads nothing back
            # (`CTW3HomePresenter.changeDeviceMode`, app 13.8.1). Taking power
            # from the last status instead sends 0 whenever the fountain was
            # caught in the sleep half of its smart cycle, leaving the pump off
            # and the select looking like it did nothing (aavdberg/ha-petkit
            # issue #54) — they fixed the power half and derived pause from the
            # mode, which the app does not do.
            states["powerStatus"] = 1
            states["suspendStatus"] = 1
        missing = [f for f in ("powerStatus", "suspendStatus", "mode") if f not in states]
        if missing:
            raise Refused(f"no reading yet for {', '.join(missing)}")
        return CMD_SET_MODE, ctw3_mode_payload(
            states["powerStatus"], states["suspendStatus"], states["mode"])

    states[_CTW3_CONFIG_FIELDS[key]] = value
    payload = ctw3_config_payload(states)
    if payload is None:
        raise Refused("no full status reported yet - the settings block is "
                      "written whole, and the rest of it is not known")
    return CMD_SET_CONFIG, payload


# --- the drink history ---------------------------------------------------------
#
# What a CTW3 sends back for a 212 (`codes.BLE_OPCODES`, graded there). Two
# shapes, both from PetKit's app 13.8.1 and neither yet seen from a real
# fountain:
#
# * the 212 reply: DATA byte 0 unread, bytes 1-4 a big-endian count of the
#   history bytes waiting (`CTW3BleClient.onReceiveCustomData` @0x0054-0x006c,
#   `ByteUtil.bytes2Int`, which is big-endian);
# * each history packet (68, or 82): whole 6-byte records, read per packet with
#   no carry-over between packets (`CTW3DataConvertor.processStreamData`
#   @0x0022-0x0032 hands each packet's data to `parseWorkData` on its own).
#
# A record is `workTime` (4 bytes, `bytes2Int`) then `stayTime` (2 bytes,
# `bytes2Short` -- a SIGNED Java short), big-endian like everything else here
# (`CTW3DataConvertor.parseWorkData` @0x0006-0x002c, into
# `CTW3WorkData(short stayTime, long workTime)`). The app shows `stayTime` as
# seconds (`CTW3Utils.calcStayTimeStr` divides by 3600 and 60).

#: Bytes per record.
CTW3_RECORD_LEN = 6

#: The accessory's clock counts seconds from 2000-01-01 UTC. INFERRED from how
#: PetKit's app SETS it: cmd 84 carries `(now_ms - BASE_TIME_2000_MS) / 1000`,
#: shifted to local time only for an "AQ" device
#: (`BaseDataConvertor.securityCheckAndSyncData` @0x0035-0x0051), and
#: `CTW3BleClient.isAqDevice` returns false. Over a relay the parent sets that
#: clock, and with what is not known -- so `ctw3_record_time` also accepts a
#: plain Unix count, which no plausible date can be confused with until 2050.
CTW3_CLOCK_EPOCH = 946684800

#: No record is believed from before 2020: a fountain whose clock was never set
#: counts from 2000, and storing its drinks in 2000 would put them on no day
#: anybody looks at while hiding that the clock is wrong.
_RECORD_EARLIEST = 1577836800.0
#: Nor from more than a day ahead -- the slack is for a clock set in a
#: timezone rather than in UTC.
_RECORD_FUTURE_SLACK = 86400.0


@dataclass(frozen=True)
class DrinkRecord:
    """One CTW3 history record, decoded.

    `raw_time` is the fountain's own value, kept because it is the record's only
    identity: two records with the same one are the same visit.
    """

    raw_time: int
    start: float
    seconds: int


@dataclass
class HistoryFrames:
    """What one `ble_response` carried of the history exchange."""

    #: The 212 reply's byte count, or None when this content held none.
    pending: int | None = None
    records: list[DrinkRecord] = field(default_factory=list)
    #: Records dropped as implausible (a clock never set, a stay that
    #: overflowed the signed short), and bytes that were not a whole record.
    rejected: int = 0
    #: History bytes received -- what `pending` counts down against.
    data_bytes: int = 0
    #: Stream control frames the accessory sent (67, 69, 80): proof it is
    #: waiting on a step of the stream nothing here takes.
    control: list[int] = field(default_factory=list)

    def __bool__(self) -> bool:
        return (self.pending is not None or bool(self.records) or bool(self.rejected)
                or bool(self.data_bytes) or bool(self.control))


def ctw3_record_time(raw: int, now: float | None = None) -> float | None:
    """The Unix time of a record's `workTime`, or None if it is not plausible.

    Seconds from 2000 first (`CTW3_CLOCK_EPOCH`), then a plain Unix count; the
    two ranges cannot overlap while either is a plausible date, so this is a
    choice the value makes, not a guess.
    """
    now = time.time() if now is None else now
    latest = now + _RECORD_FUTURE_SLACK
    for candidate in (raw + CTW3_CLOCK_EPOCH, raw):
        if _RECORD_EARLIEST <= candidate <= latest:
            return float(candidate)
    return None


def decode_ctw3_sync_reply(data: bytes) -> int | None:
    """The byte count a 212 reply announces, or None for a reply too short."""
    if len(data) < 5:
        return None
    return int.from_bytes(data[1:5], "big")


def decode_ctw3_records(data: bytes, now: float | None = None) -> tuple[list[DrinkRecord], int]:
    """Every plausible record in one history packet, and how many were not.

    A trailing part-record counts as one rejected, never as a record: the app
    reads `len // 6` and would drop it in silence, which is the same thing but
    without saying so.
    """
    now = time.time() if now is None else now
    records: list[DrinkRecord] = []
    rejected = 1 if len(data) % CTW3_RECORD_LEN else 0
    for off in range(0, len(data) - CTW3_RECORD_LEN + 1, CTW3_RECORD_LEN):
        raw = int.from_bytes(data[off:off + 4], "big")
        stay = int.from_bytes(data[off + 4:off + 6], "big", signed=True)
        start = ctw3_record_time(raw, now)
        if start is None or stay < 0:
            rejected += 1
            continue
        records.append(DrinkRecord(raw_time=raw, start=start, seconds=stay))
    return records, rejected


def parse_ctw3_history(content: Any, now: float | None = None) -> HistoryFrames:
    """The history half of a CTW3 `ble_response`: falsy when it held none.

    Device input: anything malformed is skipped, never raised on -- a 212
    reply too short to carry its count is reported as "no count", a packet that
    is not whole records loses only its broken tail.
    """
    out = HistoryFrames()
    for raw_cmd, data in _iter_ble_frames(content):
        # The JSON `cmd` is device input: a list there must not reach a set
        # lookup, where it would raise instead of being skipped.
        cmd = to_int(raw_cmd, None)
        if cmd == CMD_SYNC_HISTORY:
            count = decode_ctw3_sync_reply(data)
            if count is not None:
                out.pending = count
            else:
                log.info("CTW3 212 reply too short to carry a count: %s",
                         bytes(data).hex() or "(empty)")
        elif cmd in BLE_HISTORY_DATA:
            records, rejected = decode_ctw3_records(data, now)
            out.records.extend(records)
            out.rejected += rejected
            out.data_bytes += len(data)
        elif cmd in (CMD_STREAM_CHECK, CMD_STREAM_END, CMD_STREAM_SETTING):
            out.control.append(cmd)
    return out
