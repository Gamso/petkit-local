"""The BLE relay: accessories talked to through the WiFi device that relays
for them.

A K3 spray or an EverSweet fountain has no network of its own (`devices/ble/`).
It pairs to a mains-powered litter box or feeder, and everything it says
arrives inside that parent's MQTT traffic — as `ble_response` frames, or
piggybacked on the parent's own `property/post`. Nothing arrives unprompted:
the parent opens its radio only while the server holds a session open with
`thing/service/connect`, which is why this both polls and decodes.

A session that is open is not yet a reading. The parent reports the link up
(`ble_relay_over`, `action 1, result 0`), and a fountain may then say nothing
of its own: a CTW3 behind a T4 sent one 252 and fell silent on every link-up
observed. So each link-up is followed by reads of the fountain's status and
settings (`codes.BLE_LINK_UP_READS`), once, into the session already open.

A CTW3 whose drink history is due gets more than the reads: the WHOLE session
PetKit's app runs on a CTW3 over its own Bluetooth (`codes.CTW3_SESSION_STEPS`:
213, the secret, the clock, 200, 201, 66, 210, 211, 215, 216, 212, then the 80
and the stream), one frame in flight at a time, every step and its reply logged
(#140). PetKit's app never asks for the history through a parent; the bet is
that the fountain wants the security check and the clock first, and the parent
forwards our frames verbatim. At most once per `HISTORY_SYNC_INTERVAL_SECONDS`.
Every record that comes back is stored as a drinking row on the ACCESSORY's own
id. And every status a CTW3 pushes (230) is answered as the app answers it
(`codes.BLE_STATUS_ACK`).

It owns no broker connection. Every frame goes out through the bridge
(`mqtt/bridge.py`), which holds the one client to the embedded broker, and
every path here checks that connection first — a session opened for a publish
that cannot leave costs a sequence number and a throttle slot and delivers
nothing.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from petkit_local.devices.ble import (
    BLE_TYPE_RESPONSE, CMD_DEVICE_STATUS, CMD_SECURITY_CHECK, CMD_STREAM_CHECK,
    CMD_STREAM_END, CMD_STREAM_SETTING, CMD_SYNC_TIME, CTW3_RECORD_LEN, HistoryFrames,
    ble_command_frame, ble_frame_header, ble_read_frame, ble_session_frame,
    ble_status_ack_frame, ctw3_secret_payload, ctw3_stream_check_payload,
    ctw3_stream_setting_payload, ctw3_time_payload, decode_ctw3_sync_reply,
    parse_ctw3_history, parser_for,
)
from petkit_local.devices.ble.framing import _ble_unframe, _iter_ble_frames, _iter_ble_raw
from petkit_local.events import ingest
from petkit_local.events.codes import (
    BLE_HISTORY_DATA, BLE_HISTORY_SYNC_CMD, BLE_HISTORY_SYNC_TYPES, BLE_LINK_NOTICES,
    BLE_LINK_UP_READS, BLE_STATUS_ACK, BLE_STATUS_ACK_TYPES, CTW3_SESSION_CHECKED,
    CTW3_SESSION_STEPS, CTW3_STREAM_BATCH, CTW3_STREAM_MTU, KIND_DRINKING,
    ble_opcode_label,
)
from petkit_local.utils.coerce import to_float, to_int
from petkit_local.utils.dicts import dig

if TYPE_CHECKING:
    from petkit_local.devices.base import Device
    from petkit_local.devices.ble import BLEDevice, BLERegistry
    from petkit_local.devices.registry import DeviceRegistry
    from petkit_local.events.store import EventStore
    from petkit_local.ha.publisher import HAPublisher
    from petkit_local.mqtt.bridge import MQTTBridge
    from petkit_local.web.hub import EventHub

log = logging.getLogger(__name__)

#: Seconds to ask a parent to hold a BLE relay session open (`connect.time`).
#: Confirmed working at 30 on a CTW3 behind a D4SH; the unit is inferred.
BLE_SESSION_HOLD_SECONDS = 30

#: Two link-up reports for one accessory this close together are one link-up.
#:
#: The parent reports a link twice over — `ble_relay_over` and, ~0.3 s later, a
#: 252 sharing its `event_id` — and either may arrive alone. The `event_id`
#: settles it when both carry one; this window is for when they do not, and is
#: far shorter than any poll interval, so it never swallows the next session.
LINK_UP_DEDUP_SECONDS = 10.0

#: How long a relay session is taken to hold its parent when nothing says it
#: ended. The parent ends one with `ble_relay_over action 0`, and a T4 asked to
#: hold for `BLE_SESSION_HOLD_SECONDS` held one 133 s (`relay.start` to
#: `relay.over_closed` in `tests/fixtures/t4_ble_relay_and_data_get.json`),
#: others 100-130 s. 180 is clearly past that and still under the 240 s
#: default poll interval, so a lost `action 0` costs one deferred tick.
BLE_SESSION_TIMEOUT_SECONDS = 180.0

#: How often a CTW3 is asked for its drink history, at most. The first link-up
#: after it is due asks; every other one does not. Fifteen minutes is far
#: slower than the 240 s poll and still puts a drink on the timeline the same
#: quarter-hour -- and a fountain whose history arrives in a form nothing here
#: reads (a reply announcing bytes, then no records) costs one held session
#: per interval rather than one per poll.
HISTORY_SYNC_INTERVAL_SECONDS = 900.0

#: Records stored from one relay session, at most. A record is 6 bytes and
#: PetKit's app takes a stream in batches of 32 packets, so a real session
#: carries tens to hundreds; this is far above that and only bounds what a
#: stream of garbage could write.
HISTORY_MAX_RECORDS_PER_SESSION = 5000


#: How long one step of the CTW3 session waits for its reply. A relayed round
#: trip (broker, parent, radio and back) takes well under a second when it
#: works; a step that has not answered in this long is where the fountain
#: stopped, and the session ends there rather than holding the parent's radio.
CTW3_STEP_TIMEOUT_SECONDS = 10.0

#: How long the stream may go quiet before the session gives up on it.
CTW3_STREAM_IDLE_SECONDS = 30.0

#: Where a CTW3 session stands: its steps, the 80 it sent, the stream after.
SESSION_STEPS = "steps"
SESSION_SETTING = "setting"
SESSION_STREAM = "stream"


@dataclass
class _Ctw3Session:
    """One run of `codes.CTW3_SESSION_STEPS` through one parent (#140)."""

    device: Device
    ble: BLEDevice
    stage: str = SESSION_STEPS
    #: Index into `CTW3_SESSION_STEPS` of the step in flight.
    step: int = 0
    #: The cmd whose reply is awaited, and the sequence it went out with.
    waiting: int | None = None
    seq: int = 0
    #: This batch's stream packets: their indexes when a header carried them,
    #: and how many arrived (the fallback when none did).
    held: set[int] = field(default_factory=set)
    arrived: int = 0
    batch_total: int = 0
    timer: asyncio.Task | None = None

    def where(self) -> str:
        """The step in flight, for a log line."""
        if self.stage == SESSION_STEPS:
            return (f"step {self.step + 1}/{len(CTW3_SESSION_STEPS)} "
                    f"({ble_opcode_label(self.waiting)})")
        if self.stage == SESSION_SETTING:
            return f"the stream setting ({ble_opcode_label(CMD_STREAM_SETTING)})"
        return "the stream"


@dataclass
class _HistorySync:
    """One relay session's drink-history exchange with one accessory."""

    #: The cursor when this session began: a record at or before it is one a
    #: previous session already stored. Snapshotted, not live, because records
    #: of one session can arrive out of order (a resent packet).
    since: float
    #: The byte count the 212 reply announced; None until one has.
    pending: int | None = None
    received: int = 0
    stored: int = 0
    duplicates: int = 0
    skipped: int = 0
    rejected: int = 0
    #: The accessory said the stream is over (69).
    ended: bool = False
    #: Stream control the accessory sent (67, 69, 80).
    control: tuple[int, ...] = ()

    @property
    def streaming(self) -> bool:
        """Records announced and not all in yet: the session is not done."""
        return bool(self.pending) and self.received < (self.pending or 0) and not self.ended


def _relay_content(params: object) -> dict:
    """The decoded `content` of a relay event, or {} for anything unreadable.

    Device input: a `content` that is not a JSON object is treated as empty
    rather than raised on.
    """
    content = params.get("content", {}) if isinstance(params, dict) else {}
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except (ValueError, TypeError):
            content = {}
    return content if isinstance(content, dict) else {}


def _event_id(params: object) -> str:
    """The `event_id` a relay event carries, as text, or "" when it has none."""
    value = params.get("event_id") if isinstance(params, dict) else None
    return value if isinstance(value, str) else ""


def update_linked_k3(device: "Device", params: dict,
                     ble_registry: "BLERegistry | None") -> "BLEDevice | None":
    """Merge K3 consumable fields piggybacked on the parent's state report.

    Transport-agnostic: the K3 spray is BLE-only and never posts anything of
    its own, so its `battery` and `liquid` ride along on the parent litter
    box's continuous state — MQTT `property/post`, HTTP `dev_state_report`,
    and the embedded `state` block of every `dev_event_report`. All three
    carry the same top-level keys, so ONE extractor covers every transport,
    for the reason `_extract_feeder_next_gen` records: a mapping added to
    only one of them works on whichever frames happen to carry it and
    silently does nothing on the other.

    A reading that arrives here is the K3's reading, so it moves the K3's own
    `last_seen` — the only clock it has, since it never sends a frame of its own
    and so is never "heard" (`BLEDevice.reports_via_parent`).

    Returns:
        The K3 BLEDevice if anything changed (the caller re-publishes it to
        HA), else None.
    """
    if not ble_registry:
        return None
    k3 = ble_registry.get_linked_k3(device.petkit_id)
    if not k3:
        return None

    updated = False
    if "battery" in params:
        k3.state.setdefault("consumables", {})["battery"] = params["battery"]
        updated = True
    if "liquid" in params:
        k3.state.setdefault("consumables", {})["liquid"] = params["liquid"]
        updated = True

    if not updated:
        return None
    k3.last_seen = time.time()
    ble_registry.mark_dirty()
    log.info("Updated K3 (id=%d) from parent device %d: battery=%s liquid=%s",
             k3.petkit_id, device.petkit_id,
             params.get("battery", "?"), params.get("liquid", "?"))
    return k3

class BLERelay:
    """Reads and writes BLE accessories through their parent's MQTT session.

    Constructed with the bridge rather than with a broker client, because both
    of the things it needs from that connection change over its lifetime: the
    client is rebound on every reconnect, and whether there is one at all
    decides whether a method does anything before it does it.

    The BLE registry and the HA publisher are both optional and both checked
    before use — an install can have no accessories and no Home Assistant.
    """

    def __init__(self, bridge: MQTTBridge, registry: DeviceRegistry,
                 ble_registry: BLERegistry | None = None,
                 ha_publisher: HAPublisher | None = None,
                 event_store: EventStore | None = None,
                 hub: EventHub | None = None) -> None:
        self._bridge = bridge
        self._registry = registry
        self._ble_registry = ble_registry
        self._ha_publisher = ha_publisher
        #: Where a CTW3's drink records go; None stores nothing (and asks for
        #: nothing -- a history with nowhere to go is not worth a session).
        self._event_store = event_store
        self._hub = hub
        #: Per accessory, the history exchange of the session open now.
        self._history: dict[int, _HistorySync] = {}
        self._ble_poll_ts: dict[int, float] = {}
        #: Per-accessory BLE frame sequence, wrapping at a byte.
        self._ble_seq: dict[int, int] = {}
        #: Per accessory, the link-up last read on: `(event_id, when)`.
        self._link_reads: dict[int, tuple[str, float]] = {}
        #: Per accessory, the reads sent on the current link that have not been
        #: answered yet. The session is closed only once this is empty, so a
        #: status reply does not hang up on the settings reply behind it.
        self._pending_reads: dict[int, set[int]] = {}
        #: Per parent, the relay session it holds: `(accessory id, since)`.
        #: One per parent, whichever accessory it is for: see `_open_session`.
        self._sessions: dict[int, tuple[int, float]] = {}
        #: Per accessory, the CTW3 direct-link session running now (#140).
        self._ctw3_sessions: dict[int, _Ctw3Session] = {}

    def _open_session(self, device: Device) -> tuple[int, float] | None:
        """The relay session `device` still holds, as `(accessory id, since)`.

        A `connect` sent while one is open was the last thing a T4 (ESP32,
        1.652) was sent before it re-signed up as after a reboot (#106): the
        240 s poll opened a second session on a fountain whose first had never
        been reported closed. That is a suspicion from timing, not a proven
        cause, and whether the firmware takes two sessions at once, for one
        accessory or two, is unknown — so this assumes it takes one.

        A session ends with the parent's `ble_relay_over action 0`, after
        `BLE_SESSION_TIMEOUT_SECONDS` without one, or when the parent has made
        a new MQTT connection since it opened — a reconnect is a restart of the
        whole stack, radio included.
        """
        held = self._sessions.get(device.petkit_id)
        if held is None:
            return None
        _ble_id, since = held
        if (time.time() - since >= BLE_SESSION_TIMEOUT_SECONDS
                or (device.mqtt_connected_at or 0.0) > since):
            del self._sessions[device.petkit_id]
            return None
        return held

    def _next_seq(self, ble: BLEDevice) -> int:
        """This accessory's next frame sequence number, wrapping at a byte."""
        seq = self._ble_seq.get(ble.petkit_id, 0)
        self._ble_seq[ble.petkit_id] = (seq + 1) & 0xFF
        return seq

    async def _publish_frame(self, device: Device, ble: BLEDevice,
                             cmd: int, data: str) -> None:
        """One `thing/service/ble` publish carrying an encoded frame."""
        now = int(time.time())
        envelope = {
            "method": "thing.service.ble",
            "id": str(now),
            "params": {
                "device": {"type": ble.ble_type_int, "mac": ble.wire_mac},
                "payload": {"cmd": cmd, "data": data},
                "timestamp": now,
            },
            "version": "1.0.0",
        }
        await self._bridge.publish_service(device, "ble", envelope)

    async def publish_ble_command(self, device: Device, ble: BLEDevice,
                                 cmd: int, payload: bytes) -> bool:
        """Write one framed command to an accessory, through its parent.

        The mirror of `_handle_ble_response`: `thing/service/ble` carries the
        MQTT `cmd` alongside the same `FA FC FD ... FB` frame the accessory
        answers with. The parent forwards the bytes; it does not interpret them.

        A write needs a session that is already open. Published on its own it is
        accepted by the parent, forwarded, and does nothing — confirmed on a
        CTW3 by @strxno, who saw the same frame acknowledged with a session held
        and silently ignored without one. So the session is opened first, on
        every write, and left for `_handle_ble_response` to close once a reading
        comes back.

        A session the parent already holds for THIS accessory is written into
        as it is: a second `connect` is what `_open_session` exists to prevent.
        One it holds for ANOTHER accessory refuses the write until it ends.

        Returns False when nothing could be sent, so a caller does not report
        success for a command that never left.
        """
        if not self._bridge.connected or not device.mqtt_connected:
            return False
        held = self._open_session(device)
        if held is None:
            if not await self._ble_connect(device, ble, action=1):
                return False
        elif held[0] != ble.petkit_id:
            log.info("BLE cmd %d -> %s held back: parent %d is still relaying accessory %d",
                     cmd, ble.ble_type, device.petkit_id, held[0])
            return False
        data = ble_command_frame(cmd, self._next_seq(ble), payload)
        if data is None:
            log.warning("No BLE opcode known for cmd %s (%s id=%d)",
                        cmd, ble.ble_type, ble.petkit_id)
            return False
        await self._publish_frame(device, ble, cmd, data)
        log.info("BLE cmd %d -> %s (mac=%s) via parent %d",
                 cmd, ble.ble_type, ble.mac, device.petkit_id)
        return True

    async def publish_ble_read(self, device: Device, ble: BLEDevice, cmd: int) -> bool:
        """Ask an accessory to report, over the session its parent holds NOW.

        The write path's twin with one difference that is the whole point: it
        does NOT open a session first. It is sent because the parent has just
        reported the link up, and a `connect` pushed into that link would ask
        the parent to bring it up again rather than use it.

        Returns False when nothing could be sent (no broker, or `cmd` is not a
        read — `framing.BLE_READABLE`).
        """
        if not self._bridge.connected:
            return False
        data = ble_read_frame(cmd, self._next_seq(ble))
        if data is None:
            log.warning("cmd %s is not a BLE read (%s id=%d)",
                        cmd, ble.ble_type, ble.petkit_id)
            return False
        await self._publish_frame(device, ble, cmd, data)
        log.info("BLE read %s -> %s (mac=%s) via parent %d", ble_opcode_label(cmd),
                 ble.ble_type, ble.mac, device.petkit_id)
        return True

    async def _read_on_link_up(self, device: Device, ble: BLEDevice,
                               event_id: str) -> None:
        """Ask a fountain for its status and its settings, once per link-up.

        Only an accessory with a frame decoder is asked: that is the fountains,
        and a reply nothing here can read would only be logged. Bounded twice —
        two reads per link-up and no retry, so a fountain that does not answer
        costs two frames per poll interval rather than a parent's radio held
        busy (see `_handle_ble_response` on why a retry here would race the
        next poll).

        A CTW3 whose drink history is due gets the whole direct-link session
        instead (`_start_ctw3_session`), and a link-up notice arriving while
        one runs starts nothing.
        """
        if parser_for(ble.ble_type) is None or ble.petkit_id in self._ctw3_sessions:
            return
        now = time.time()
        last_event, last_at = self._link_reads.get(ble.petkit_id, ("", 0.0))
        if (event_id and event_id == last_event) or now - last_at < LINK_UP_DEDUP_SECONDS:
            return
        self._link_reads[ble.petkit_id] = (event_id, now)
        # A new link is a new session: whatever the last one left unfinished
        # is reported now rather than carried into this one.
        self._finish_history(ble)
        # A CTW3 whose history is due gets the whole direct-link session
        # instead, whose 210 and 211 are these reads.
        if await self._start_ctw3_session(device, ble):
            self._pending_reads.pop(ble.petkit_id, None)
            return
        sent = set()
        for cmd in BLE_LINK_UP_READS:
            if await self.publish_ble_read(device, ble, cmd):
                sent.add(cmd)
        self._pending_reads[ble.petkit_id] = sent

    async def _handle_relay_over(self, device: Device, params: dict) -> None:
        """React to the parent reporting a BLE link up, failed, or closed.

        `ble_relay_over` is the completion of the CONNECT, not of the data
        exchange (see `_handle_ble_response`): `action 1, result 0` is the link
        up and ready, which is when the reads go out. `result 6` on `action 1`
        is a link that never came up; `action 0` is the session ending, which
        abandons whatever read was still unanswered.
        """
        if not self._ble_registry:
            return
        content = _relay_content(params)
        mac = dig(content, "device", "mac", default="")
        ble_dev = self._ble_registry.get_by_mac(mac) if isinstance(mac, str) and mac else None
        if ble_dev is None:
            return
        action = to_int(content.get("action"), None)
        result = to_int(content.get("result"), None)
        if action == 1 and result == 0:
            await self._read_on_link_up(device, ble_dev, _event_id(params))
        elif action == 1:
            log.info("%s (id=%d): parent %d could not bring the BLE link up (result %s)",
                     ble_dev.ble_type.upper(), ble_dev.petkit_id, device.petkit_id, result)
        elif action == 0:
            self._pending_reads.pop(ble_dev.petkit_id, None)
            session = self._ctw3_sessions.pop(ble_dev.petkit_id, None)
            if session is not None:
                self._cancel_timer(session)
                log.info("%s (id=%d) session: the parent closed the link at %s",
                         ble_dev.ble_type.upper(), ble_dev.petkit_id, session.where())
            self._finish_history(ble_dev)
            held = self._sessions.get(device.petkit_id)
            if held is not None and held[0] == ble_dev.petkit_id:
                del self._sessions[device.petkit_id]

    async def _handle_relay_start(self, device: Device, params: dict) -> None:
        """Note a relay session the parent reports opening (`action 1`).

        Ours are noted when the `connect` goes out; this also catches one we
        did not ask for (proxy mode relays the real cloud's `connect` down),
        and restarts the clock at the parent's own start.
        """
        if not self._ble_registry:
            return
        content = _relay_content(params)
        if to_int(content.get("action"), None) != 1:
            return
        mac = dig(content, "device", "mac", default="")
        ble_dev = self._ble_registry.get_by_mac(mac) if isinstance(mac, str) and mac else None
        if ble_dev is not None:
            self._sessions[device.petkit_id] = (ble_dev.petkit_id, time.time())

    async def request_ble_reading(self, device: Device, ble: BLEDevice) -> bool:
        """Ask the parent for a reading from this accessory, right now.

        Bypasses the `interval` throttle on purpose: this exists for the moment
        somebody is standing in front of the panel asking "is it alive", and
        making them wait up to four minutes for the timer is the opposite of an
        answer. The throttle is then re-armed so the periodic loop does not
        immediately ask again.

        Held to the poll's own rule (`non_k3_for_parent`): only an accessory in
        this parent's relay list is asked for. A K3 is in no relay list — a
        `connect` for one names `type: 0`, a scan for a device the parent was
        never told about — and it reports inside its parent's state anyway.

        And to `_open_session`: a session already open for this accessory is
        the reading on its way, so nothing is sent and the answer is yes; one
        open for another accessory is a no until it ends.
        """
        if not self._bridge.connected or not self._ble_registry or not device.mqtt_connected:
            return False
        relayed = {b.petkit_id for b in self._ble_registry.non_k3_for_parent(device.petkit_id)}
        if ble.petkit_id not in relayed:
            return False
        held = self._open_session(device)
        if held is not None:
            return held[0] == ble.petkit_id
        if not await self._ble_connect(device, ble, action=1):
            return False
        self._ble_poll_ts[ble.petkit_id] = time.time()
        return True

    async def poll_ble_loop(self, period: float = 30.0) -> None:
        """Ask every paired accessory's parent for a reading, forever.

        An accessory reports only when the server tells its parent to open a
        BLE session -- the parent never does it unprompted. Until this existed
        the only thing that asked was the handler for the parent's own
        `property/post`, which meant an accessory paired to a FEEDER never
        reported at all: a feeder does not send that topic. Its owner saw an
        accessory that paired, appeared in Home Assistant, and stayed unknown.

        A timer rather than a reaction, because that is what the cloud does and
        because it is the only trigger that does not depend on the parent
        having something of its own to say. `_poll_ble_accessories` still holds
        each accessory's `interval`, so this loop can tick often without
        talking often; `period` is only how finely those intervals are honoured.
        """
        while True:
            try:
                await asyncio.sleep(period)
                if not self._bridge.connected or not self._ble_registry:
                    continue
                for ble in self._ble_registry.all():
                    if not ble.link_with:
                        continue
                    parent = self._registry.get(ble.link_with)
                    if parent is not None:
                        await self._poll_ble_accessories(parent)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one bad poll must not end the loop
                log.exception("BLE poll loop")

    async def _ble_connect(self, device: Device, ble: BLEDevice, action: int) -> bool:
        """Open (`action=1`) or close (`action=0`) the parent's BLE session.

        `thing.service.connect` is the only way an accessory reports at all:
        the parent does not poll on its own initiative, so nothing arrives
        until this is pushed. Closing matters too — an accessory whose session
        is never ended keeps the parent's radio busy between readings.

        Nothing is sent to a parent that is not on MQTT: it is subscribed to
        nothing, so the publish would vanish. Nor is a session opened while the
        parent still holds one (`_open_session`); closing ours does not end it
        here either — the parent's own `ble_relay_over action 0` does.

        Returns:
            True if the frame went out.
        """
        if not self._bridge.connected or not device.mqtt_connected:
            return False
        if action and self._open_session(device) is not None:
            return False
        now = int(time.time())
        envelope = {
            "method": "thing.service.connect",
            "id": str(now),
            "params": {
                "connect_action": action,
                # How long to HOLD the session open, in seconds. Without it the
                # parent opens the radio and lets it go again before the
                # accessory has said anything useful: a CTW3 answers a bare
                # open with a short 251/252 and only then does its own run-info
                # pass, so the status arrives after the window we were giving
                # it. 30 is what @strxno confirmed on hardware; the unit is
                # inferred from the value working, not from a capture of the
                # field being varied.
                **({"time": BLE_SESSION_HOLD_SECONDS} if action else {}),
                "device": {"type": ble.ble_type_int, "mac": ble.wire_mac},
                "timestamp": now,
            },
            "version": "1.0.0",
        }
        if not await self._bridge.publish_service(device, "connect", envelope):
            return False
        if action:
            self._sessions[device.petkit_id] = (ble.petkit_id, time.time())
        log.info("BLE %s -> %s (mac=%s) via parent %d",
                 "connect" if action else "disconnect",
                 ble.ble_type, ble.mac, device.petkit_id)
        return True

    async def publish_relay_update(self, device: Device) -> bool:
        """Tell a parent its accessory list has changed, so it refetches now.

        `dev_ble_device` answers with `nextTick: 3600`, and the parent honours
        it: pair an accessory and it is an HOUR before that parent knows to scan
        for the MAC. Everything downstream looks broken for that hour — the poll
        pushes `thing/service/connect` for an accessory the parent was never
        told about, and nothing comes back, which is indistinguishable from a
        wrong scan type or a bad MAC.

        `update_action: 1` is the trigger, and any other value (or no field at
        all) is logged by the firmware and ignored. Confirmed on a T5: the
        publish is followed immediately by a `GET /6/t5/dev_ble_device`.
        Reported by @strxno.

        Returns False when nothing could be sent, so a caller does not report a
        refresh that never left.
        """
        if not self._bridge.connected:
            return False
        now = int(time.time())
        envelope = {
            "method": "thing.service.ble_relay_update",
            "id": str(now),
            "params": {"update_action": 1},
            "version": "1.0.0",
        }
        await self._bridge.publish_service(device, "ble_relay_update", envelope)
        log.info("BLE relay list refresh -> parent %d", device.petkit_id)
        return True

    async def _poll_ble_accessories(self, device: Device) -> None:
        """Ask the parent to open a BLE relay session for each linked accessory
        so it starts posting ble_response frames. Without this poll the parent
        never relays W5 data. Throttled per accessory `interval`.

        Mirrors localkit ServiceConnect (thing/service/connect, connect_action=1).

        An accessory whose `connect` was held back (`_ble_connect`) keeps its
        old timestamp, so the next tick tries again rather than a whole
        interval later.
        """
        if not self._bridge.connected or not self._ble_registry or not device.mqtt_connected:
            return
        # `non_k3_for_parent`, not `get_linked`: a K3 is never in the relay list
        # at all (it travels inside the parent's own device_info), so asking a
        # parent to open a BLE session for one sent a `connect` with `type: 0`.
        linked = self._ble_registry.non_k3_for_parent(device.petkit_id)
        if not linked:
            return
        now = time.time()
        for ble in linked:
            interval = getattr(ble, "interval", 240) or 240
            if now - self._ble_poll_ts.get(ble.petkit_id, 0) < interval:
                continue
            if await self._ble_connect(device, ble, action=1):
                self._ble_poll_ts[ble.petkit_id] = now

    def _update_linked_k3(self, device: Device, params: dict) -> BLEDevice | None:
        """Merge K3 consumable fields piggybacked on the parent's property post.

        Thin wrapper around :func:`update_linked_k3` — every transport now
        reaches the same helper, this is only kept for the MQTT call site's
        readability. See the free function for the full contract.
        """
        return update_linked_k3(device, params, self._ble_registry)

    async def _handle_ble_response(self, device: Device, params: dict) -> None:
        """Apply one relayed BLE frame to the accessory it came from.

        The accessory is identified by MAC, not by the relaying parent: the
        same W5 can in principle be reachable through more than one WiFi
        device, and the MAC is the only stable key in the frame. That is not
        hypothetical: one D4SH in the 2026-08-11 capture relays two separate
        type-14 fountains.

        TWO THINGS THIS DOES NOT DO, both visible in that capture and neither
        fixed here because neither can be tested without the hardware:

        * `ble_relay_over` is the completion of the CONNECT, not of the data
          exchange. The order is `connect(1)` -> `ble_relay_start(action=1)`
          -> `ble_relay_over(result=0)` -> and only THEN the response frames.
          Anything that treats `relay_over` as the end of a session would cut
          off everything it was opened for.
        * A failed write is not retried. `result=6` on `action=1` means the
          link never came up (73 of 1081 attempts), and the one `cmd 221`
          write in that capture hit it: the frame was correct, the session
          died 38 seconds later, and all 237 subsequent reads still showed the
          setting unchanged. Nothing noticed. A retry belongs here, but it has
          to be bounded -- this loop already opens a session per accessory per
          poll, and a retry that races the next poll would leave the parent's
          radio permanently busy.
        """
        if not self._ble_registry:
            return

        content = _relay_content(params)

        ble_mac = dig(content, "device", "mac", default="")

        ble_dev = (self._ble_registry.get_by_mac(ble_mac)
                   if isinstance(ble_mac, str) and ble_mac else None)
        if not ble_dev:
            log.debug("BLE response with no matching accessory (mac=%s)", ble_mac)
            return

        # Heard, whatever it said: the frame came from this MAC through its
        # parent, which is proof of the pairing even when nothing decodes.
        ble_dev.last_heard = time.time()
        self._ble_registry.mark_dirty()

        # Coerced here, once: the JSON `cmd` is device input, and a list there
        # reached a set lookup below and raised for the whole bridge.
        frames = [(to_int(cmd, None), data) for cmd, data in _iter_ble_frames(content)]
        parse = parser_for(ble_dev.ble_type)
        fragment = parse(content) if parse is not None else {}
        history = (parse_ctw3_history(content) if ble_dev.ble_type in BLE_HISTORY_SYNC_TYPES
                   else HistoryFrames())
        if history:
            await self._apply_history(device, ble_dev, history)
        if fragment:
            for section, vals in fragment.items():
                ble_dev.state.setdefault(section, {}).update(vals)
            ble_dev.last_seen = time.time()
            self._ble_registry.mark_dirty()
            log.info("Decoded %s (id=%d) from parent %d: %s",
                     ble_dev.ble_type.upper(), ble_dev.petkit_id,
                     device.petkit_id, json.dumps(fragment)[:120])
        elif not history:
            # Not a status frame, or one we cannot read. Either way, name what
            # came back: a write is answered with a bare `01` on its own cmd,
            # or a short 251/252, and dropping those in silence left "the
            # switch flipped back" as the only symptom of a frame the accessory
            # did not accept.
            replies = [(cmd, bytes(data).hex() or "(empty)") for cmd, data in frames]
            if replies:
                log.info("%s (id=%d) replied: %s", ble_dev.ble_type.upper(),
                         ble_dev.petkit_id,
                         ", ".join(f"{ble_opcode_label(c)} {d}" for c, d in replies))
            else:
                log.info("%s ble_response not decodable yet (id=%d) - turn capture on in the "
                         "panel (Setup -> Settings) to collect frames",
                         ble_dev.ble_type.upper(), ble_dev.petkit_id)

        # A CTW3 pushes its status as a REQUEST and is owed an answer: PetKit's
        # app sends one for every 230 it gets (`codes.BLE_STATUS_ACK`). Sent
        # before anything below can hang up, into the session the push came on.
        if (ble_dev.ble_type in BLE_STATUS_ACK_TYPES
                and any(cmd == BLE_STATUS_ACK[0] for cmd, _ in frames)):
            await self._ack_status(device, ble_dev)

        # A CTW3 session in progress decides for itself what goes next and
        # when the link is hung up.
        session = self._ctw3_sessions.get(ble_dev.petkit_id)
        if session is not None:
            await self._advance_ctw3_session(session, content)
            if self._ha_publisher:
                await self._ha_publisher.publish_ble_discovery(ble_dev)
                await self._ha_publisher.publish_ble_state(ble_dev)
            return

        # A 251/252 is the accessory's side of a link-up. It shares its
        # `event_id` with the parent's `ble_relay_over`, so whichever of the two
        # arrives first sends the reads and the other is recognised as the same
        # link-up.
        if any(cmd in BLE_LINK_NOTICES for cmd, _ in frames):
            await self._read_on_link_up(device, ble_dev, _event_id(params))

        # Hang up only once a reading is actually in. A session is also
        # answered with frames that are not status — a bare `01` write ack, a
        # short 251/252 — and closing on those cut the radio before the
        # accessory got to its run-info pass, which is most of why a CTW3 was so
        # hard to get anything out of. The hold above bounds the session, so
        # leaving it open costs a fixed number of seconds rather than for ever.
        #
        # And not while a read sent on this link is still unanswered: the
        # status and the settings come back as separate replies, and hanging up
        # on the first loses the second. A 230 carries both blocks.
        #
        # The history is the CTW3 session's (`_start_ctw3_session`), never
        # asked for here.
        pending = self._pending_reads.get(ble_dev.petkit_id)
        answered = {cmd for cmd, _ in frames}
        if pending and fragment:
            pending -= answered
            if CMD_DEVICE_STATUS in answered:
                pending.clear()
        # History progress ends a session only once a 212 reply has said how
        # much to expect. Records nobody here announced are a sync somebody
        # else runs -- the real cloud's, in proxy mode -- and hanging up on
        # those would cut it off halfway.
        sync = self._history.get(ble_dev.petkit_id)
        announced = sync is not None and sync.pending is not None
        progressed = bool(fragment) or (announced and bool(history))
        if progressed and not pending:
            if sync is not None and sync.streaming:
                pass
            else:
                self._pending_reads.pop(ble_dev.petkit_id, None)
                await self._ble_connect(device, ble_dev, action=0)

        if self._ha_publisher:
            await self._ha_publisher.publish_ble_discovery(ble_dev)
            await self._ha_publisher.publish_ble_state(ble_dev)

    # --- the drink history -----------------------------------------------------

    async def _start_ctw3_session(self, device: Device, ble: BLEDevice) -> bool:
        """Begin `codes.CTW3_SESSION_STEPS` on a link that has just come up.

        Only for a CTW3 (`BLE_HISTORY_SYNC_TYPES`), only with a store to keep
        its records in, only once per `HISTORY_SYNC_INTERVAL_SECONDS`
        (persisted as `asked_at`, so a restart does not ask again at once), and
        only with a pairing secret the 86 can carry. Anything else falls back
        to the plain link-up reads.

        Returns:
            True if the first step went out.
        """
        if (ble.ble_type not in BLE_HISTORY_SYNC_TYPES or self._event_store is None
                or not self._bridge.connected):
            return False
        now = time.time()
        history = ble.history if isinstance(ble.history, dict) else {}
        asked_at = to_float(history.get("asked_at"), 0.0) or 0.0
        if 0.0 <= now - asked_at < HISTORY_SYNC_INTERVAL_SECONDS:
            return False
        name = f"{ble.ble_type.upper()} (id={ble.petkit_id})"
        if ctw3_secret_payload(ble.secret) is None:
            log.info("%s: no usable pairing secret (ble_devices.json), so its drink "
                     "history cannot be asked for; reading its status only", name)
            return False
        self._history[ble.petkit_id] = _HistorySync(
            since=to_float(history.get("cursor"), 0.0) or 0.0)
        ble.history = {**history, "asked_at": now}
        if self._ble_registry is not None:
            self._ble_registry.mark_dirty()
        session = self._ctw3_sessions[ble.petkit_id] = _Ctw3Session(device=device, ble=ble)
        log.info("%s session: starting the direct-link sequence through parent %d "
                 "(%s)", name, device.petkit_id,
                 " -> ".join(str(c) for c in (*CTW3_SESSION_STEPS, CMD_STREAM_SETTING)))
        if not await self._send_session_step(session):
            await self._end_ctw3_session(session, "the first step could not be sent")
            return True
        return True

    def _session_payload(self, session: _Ctw3Session, cmd: int) -> bytes:
        """What one step carries: the secret, the clock, or nothing."""
        if cmd == CMD_SECURITY_CHECK:
            return ctw3_secret_payload(session.ble.secret) or b""
        if cmd == CMD_SYNC_TIME:
            return ctw3_time_payload(time.time(), session.device.timezone_offset)
        if cmd == CMD_STREAM_SETTING:
            return ctw3_stream_setting_payload(CTW3_STREAM_BATCH, CTW3_STREAM_MTU)
        return b""

    async def _send_session_step(self, session: _Ctw3Session,
                                 cmd: int | None = None) -> bool:
        """Send the step in flight (or `cmd`), and wait for its reply.

        Logged at INFO with the frame's hex, so a capture of one session shows
        exactly where the fountain stops answering -- except the secret, which
        is logged as its length only.
        """
        ble = session.ble
        cmd = CTW3_SESSION_STEPS[session.step] if cmd is None else cmd
        seq = self._next_seq(ble)
        payload = self._session_payload(session, cmd)
        data = ble_session_frame(cmd, seq, payload)
        if data is None or not self._bridge.connected:
            return False
        await self._publish_frame(session.device, ble, cmd, data)
        session.waiting, session.seq = cmd, seq
        shown = (f"<{len(payload)}-byte secret>" if cmd == CMD_SECURITY_CHECK
                 else payload.hex() or "(empty)")
        log.info("%s (id=%d) session %s -> sent, seq %d, payload %s",
                 ble.ble_type.upper(), ble.petkit_id, session.where(), seq, shown)
        self._arm_timer(session, CTW3_STEP_TIMEOUT_SECONDS)
        return True

    async def _send_session_answer(self, session: _Ctw3Session, cmd: int, seq: int,
                                   payload: bytes) -> None:
        """A type-2 answer the stream is owed (67, 69)."""
        data = ble_session_frame(cmd, seq, payload, BLE_TYPE_RESPONSE)
        if data is None or not self._bridge.connected:
            return
        await self._publish_frame(session.device, session.ble, cmd, data)
        log.info("%s (id=%d) session: answered %s, seq %d, payload %s",
                 session.ble.ble_type.upper(), session.ble.petkit_id,
                 ble_opcode_label(cmd), seq, payload.hex() or "(empty)")

    def _arm_timer(self, session: _Ctw3Session, seconds: float) -> None:
        self._cancel_timer(session)
        session.timer = asyncio.get_running_loop().create_task(
            self._session_timeout(session, seconds))

    @staticmethod
    def _cancel_timer(session: _Ctw3Session) -> None:
        timer, session.timer = session.timer, None
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()

    async def _session_timeout(self, session: _Ctw3Session, seconds: float) -> None:
        await asyncio.sleep(seconds)
        if self._ctw3_sessions.get(session.ble.petkit_id) is not session:
            return
        session.timer = None
        sync = self._history.get(session.ble.petkit_id)
        if (session.stage == SESSION_STREAM and sync is not None and sync.pending
                and sync.received >= sync.pending):
            await self._end_ctw3_session(session, "every announced byte is in")
            return
        await self._end_ctw3_session(
            session, f"no answer to {session.where()} within {seconds:g} s",
            stopped=True)

    async def _end_ctw3_session(self, session: _Ctw3Session, reason: str,
                                stopped: bool = False) -> None:
        """Close a CTW3 session, say why, and hang the link up."""
        ble = session.ble
        if self._ctw3_sessions.get(ble.petkit_id) is session:
            del self._ctw3_sessions[ble.petkit_id]
        self._cancel_timer(session)
        (log.warning if stopped else log.info)(
            "%s (id=%d) session %s: %s", ble.ble_type.upper(), ble.petkit_id,
            "STOPPED" if stopped else "done", reason)
        self._finish_history(ble)
        self._pending_reads.pop(ble.petkit_id, None)
        await self._ble_connect(session.device, ble, action=0)

    async def _advance_ctw3_session(self, session: _Ctw3Session, content: dict) -> None:
        """Take one `ble_response` through a running CTW3 session.

        Device input: anything malformed is skipped, never raised on. The
        records themselves were already stored by `_apply_history`; this only
        decides what to send next.
        """
        ble = session.ble
        name = f"{ble.ble_type.upper()} (id={ble.petkit_id})"
        for json_cmd, raw in _iter_ble_raw(content):
            framed_cmd, data = _ble_unframe(raw)
            cmd = to_int(framed_cmd if framed_cmd is not None else json_cmd, None)
            if cmd is None:
                continue
            header = ble_frame_header(raw) or {}
            if self._ctw3_sessions.get(ble.petkit_id) is not session:
                return
            if session.stage == SESSION_STREAM:
                await self._session_stream_frame(session, cmd, data, header)
                continue
            if cmd != session.waiting:
                continue
            if "seq" in header and header["seq"] != session.seq:
                log.info("%s session: %s came back with seq %d, not %d -- still "
                         "waiting", name, ble_opcode_label(cmd), header["seq"], session.seq)
                continue
            log.info("%s session %s <- reply %s", name, session.where(),
                     bytes(data).hex() or "(empty)")
            if cmd in CTW3_SESSION_CHECKED and (not data or data[0] != 1):
                await self._end_ctw3_session(
                    session, f"{ble_opcode_label(cmd)} was refused "
                    f"({bytes(data).hex() or 'empty'}; the app needs 01)", stopped=True)
                return
            if session.stage == SESSION_SETTING:
                session.stage, session.waiting = SESSION_STREAM, None
                log.info("%s session: the stream has started", name)
                self._arm_timer(session, CTW3_STREAM_IDLE_SECONDS)
                continue
            if cmd == BLE_HISTORY_SYNC_CMD:
                count = decode_ctw3_sync_reply(bytes(data))
                if not count:
                    await self._end_ctw3_session(
                        session, "the 212 reply announced no drink history"
                        if count == 0 else "the 212 reply was too short to read")
                    return
                session.stage = SESSION_SETTING
                if not await self._send_session_step(session, CMD_STREAM_SETTING):
                    await self._end_ctw3_session(session, "the 80 could not be sent",
                                                 stopped=True)
                    return
                continue
            session.step += 1
            if not await self._send_session_step(session):
                await self._end_ctw3_session(
                    session, f"step {session.step + 1} could not be sent", stopped=True)
                return

    async def _session_stream_frame(self, session: _Ctw3Session, cmd: int,
                                    data: bytes, header: dict[str, int]) -> None:
        """One frame of the history stream: a packet, a check, or the end."""
        if cmd in BLE_HISTORY_DATA:
            session.arrived += 1
            if "index" in header:
                session.held.add(header["index"])
                session.batch_total = session.batch_total or header.get("total", 0)
            self._arm_timer(session, CTW3_STREAM_IDLE_SECONDS)
        elif cmd == CMD_STREAM_CHECK:
            # Without the stream header the indexes never reached us: take the
            # packets that arrived to be 0..n-1, which is a guess.
            held = session.held or set(range(min(session.arrived, 32)))
            total = session.batch_total or (len(held) if not session.held else 32)
            bitmap, complete = ctw3_stream_check_payload(held, total)
            await self._send_session_answer(session, CMD_STREAM_CHECK,
                                            header.get("seq", 0), bitmap)
            if complete:
                session.held, session.arrived, session.batch_total = set(), 0, 0
            self._arm_timer(session, CTW3_STREAM_IDLE_SECONDS)
        elif cmd == CMD_STREAM_END:
            await self._send_session_answer(session, CMD_STREAM_END,
                                            header.get("seq", 0), b"")
            await self._end_ctw3_session(session, "the fountain ended its stream (69)")

    async def _ack_status(self, device: Device, ble: BLEDevice) -> None:
        """Answer a CTW3's pushed 230 the way PetKit's app does.

        `codes.BLE_STATUS_ACK`: a type-2 frame, sequence 0, data `01`. Not
        numbered from this accessory's sequence -- the app's ack carries none
        -- and never a reason to open a session: it goes into the one the push
        arrived on.
        """
        if not self._bridge.connected:
            return
        await self._publish_frame(device, ble, BLE_STATUS_ACK[0], ble_status_ack_frame())
        log.debug("BLE status ack -> %s (mac=%s) via parent %d",
                  ble.ble_type, ble.mac, device.petkit_id)

    async def _apply_history(self, device: Device, ble: BLEDevice,
                             frames: HistoryFrames) -> None:
        """Store what one `ble_response` carried of a CTW3's history.

        Records arrive whether or not we asked -- in proxy mode the real cloud
        may run the exchange itself -- and are stored either way: a drink is a
        drink. Each becomes one drinking row on the ACCESSORY's id
        (`ingest.from_ble_drink_record`), deduplicated by the fountain's own
        timestamp, and a record at or before the cursor this session started
        from is skipped without a write. The cursor then moves to the newest
        record stored, never past now (a clock set a day ahead must not hide
        tomorrow's real drinks).

        Never raises: a store that fails loses this batch, logged, and the
        relay carries on.
        """
        sync = self._history.get(ble.petkit_id)
        if sync is None:
            history = ble.history if isinstance(ble.history, dict) else {}
            sync = self._history[ble.petkit_id] = _HistorySync(
                since=to_float(history.get("cursor"), 0.0) or 0.0)
        name = f"{ble.ble_type.upper()} (id={ble.petkit_id})"

        if frames.pending is not None:
            sync.pending = frames.pending
            ble.history = {**(ble.history or {}), "pending_bytes": frames.pending}
            if self._ble_registry is not None:
                self._ble_registry.mark_dirty()
            log.info("%s: %d bytes of drink history waiting (about %d records)",
                     name, frames.pending, frames.pending // CTW3_RECORD_LEN)
        sync.received += frames.data_bytes
        sync.rejected += frames.rejected
        if frames.control:
            sync.control += tuple(frames.control)
            sync.ended = sync.ended or CMD_STREAM_END in frames.control
            log.info("%s sent history stream control %s",
                     name, ", ".join(ble_opcode_label(c) for c in frames.control))

        fresh = [r for r in frames.records if r.start > sync.since]
        sync.skipped += len(frames.records) - len(fresh)
        room = max(HISTORY_MAX_RECORDS_PER_SESSION - sync.stored - sync.duplicates, 0)
        if len(fresh) > room:
            log.warning("%s: %d drink records past this session's cap of %d dropped",
                        name, len(fresh) - room, HISTORY_MAX_RECORDS_PER_SESSION)
            fresh = fresh[:room]
        if not fresh or self._event_store is None:
            return

        new = 0
        newest = 0.0
        try:
            async with self._event_store.transaction() as store:
                for record in fresh:
                    row = ingest.from_ble_drink_record(
                        ble.petkit_id, ble.ble_type, record.raw_time, record.start,
                        record.seconds)
                    _, inserted = await store.record_event(row)
                    new += int(inserted)
                    newest = max(newest, record.start)
        except Exception:  # noqa: BLE001 - a lost batch must not take the relay down
            log.exception("%s: storing %d drink records failed", name, len(fresh))
            return
        sync.stored += new
        sync.duplicates += len(fresh) - new
        cursor = min(newest, time.time())
        history = ble.history if isinstance(ble.history, dict) else {}
        if cursor > (to_float(history.get("cursor"), 0.0) or 0.0):
            ble.history = {**history, "cursor": cursor}
            if self._ble_registry is not None:
                self._ble_registry.mark_dirty()
        log.info("%s: %d drink records stored (%d already held)", name, new,
                 len(fresh) - new)
        if new and self._hub is not None:
            # One line per batch, not per record: a first sync can carry
            # hundreds. It is also what tells the app's live feed to re-read
            # this accessory. `is_new` False keeps the phone notifier off it --
            # a drink hours old is history, not news.
            self._hub.publish("event", ble.petkit_id,
                              f"{len(fresh)} drink records from history ({new} new)",
                              detail={"event_kind": KIND_DRINKING, "device_id": ble.petkit_id,
                                      "device_type": ble.ble_type, "is_new": False,
                                      "parent_id": device.petkit_id})

    def _finish_history(self, ble: BLEDevice) -> None:
        """Close this session's history exchange, saying how it ended.

        The one place a reply announcing records that never came is reported:
        that is a fountain waiting on the rest of the stream, and what a capture
        of this session would show is exactly what finishes the protocol.
        """
        sync = self._history.pop(ble.petkit_id, None)
        if sync is None:
            return
        name = f"{ble.ble_type.upper()} (id={ble.petkit_id})"
        if sync.pending and sync.received < sync.pending and not sync.ended:
            log.warning(
                "%s announced %d bytes of drink history and %d arrived before the "
                "session ended. The session log above shows the last step the "
                "fountain answered; a capture of it (Setup -> Settings) is what "
                "would settle the rest.", name, sync.pending, sync.received)
        elif sync.stored or sync.duplicates or sync.skipped or sync.rejected:
            log.info("%s history session: %d stored, %d already held, %d before the "
                     "cursor, %d rejected", name, sync.stored, sync.duplicates,
                     sync.skipped, sync.rejected)
