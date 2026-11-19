"""The `Device` model: one registered PetKit device, its identity and its state.

Created on `dev_signup` and persisted by `DeviceRegistry`, this is the object
every other part of the add-on identifies a device by. What the device is
*served* is built from it next door: the wire bodies in `devices/payloads.py`,
and the per-category seed data those bodies fall back on in
`devices/defaults.py`.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from petkit_local.utils.const import (
    AI_GENERATIONS,
    DEVICE_TYPES_AI,
    DEVICE_TYPES_BLE_ONLY,
    DEVICE_TYPES_CAMERA,
    DEVICE_TYPES_FEEDER,
    DEVICE_TYPES_LITTER,
    DEVICE_TYPES_NEXT_GEN,
    DEVICE_TYPES_PURIFIER,
    DEVICE_TYPES_WATER_FOUNTAIN,
    device_display_name,
    generation_from_hardware,
    valid_generation,
)
from petkit_local.utils.coerce import to_bool, to_float
from petkit_local.utils.crypto import generate_device_secret, generate_product_key
from petkit_local.utils.timeutil import local_offset_hours, offset_hours_for_locale

#: `config` key holding the last UTC offset, in hours, that was PUSHED to the
#: device as a `property.set{timezone}` — by the panel's override endpoint or
#: by the DST loop (`ha/timezone_sync.py`). Kept apart from `reported_timezone`
#: (what the device SAID at signup) because the device's own readback is a
#: lossy `%.1f`, so "what we told it" cannot be recovered from "what it says".
#: Persisted with the rest of `config`, so a restart does not re-push.
TIMEZONE_PUSHED_KEY = "timezone_pushed"

#: `config` key holding a pushed offset that a LATER signup showed the device
#: not holding — it came back from a restart on another value. That offset is
#: not pushed again until the zone has moved off it (`ha/timezone_sync.py`), so
#: a device that restarts after a push cannot be pushed into a loop. Persisted
#: like the push itself.
TIMEZONE_NOT_KEPT_KEY = "timezone_not_kept"

#: `config` key holding the generation the OWNER chose in the panel (an int,
#: e.g. 2 for a Pura Max 2). Absent means "Automatic". It always wins over a
#: learned value: it exists to correct a wrong one, and for installs that never
#: learn one at all (no proxy mode, a model whose `hardware` means nothing).
GENERATION_KEY = "generation"
#: `config` key holding each LEARNED generation by where it came from,
#: `{"typeCode": 2, "hardware": 2}` -- kept apart so two sources that disagree
#: cannot overwrite each other on every report, and so the panel can say which
#: one is in force.
GENERATION_LEARNED_KEY = "generation_learned"
#: Learned sources, most authoritative first. `typeCode` is PetKit's own
#: record of what was sold (`ha/learn.py::learn_generation`, proxy mode only);
#: `hardware` is the device's own report, where `utils/const.py` grades it as
#: meaning the generation.
GENERATION_SOURCES = ("typeCode", "hardware")


def resolve_generation(code: str, owner: object, learned: object) -> tuple[int | None, str]:
    """`(generation, source)` from an owner's choice and the learned values.

    The one precedence rule, shared by a WiFi device (`Device.generation`) and
    a Bluetooth accessory (`devices/ble/registry.py::BLEDevice.generation`),
    which store the two in different places: the owner first, then `learned`
    in `GENERATION_SOURCES` order; `(None, "")` when nothing says. Every value
    is re-checked against `utils/const.py::MODEL_GENERATIONS` for `code`, so one
    hand-edited into a JSON file or left by an older table is ignored rather
    than trusted. `source` is `owner`, a `GENERATION_SOURCES` entry, or `""`.
    """
    chosen = valid_generation(code, owner)
    if chosen is not None:
        return chosen, "owner"
    if isinstance(learned, dict):
        for source in GENERATION_SOURCES:
            value = valid_generation(code, learned.get(source))
            if value is not None:
                return value, source
    return None, ""


def learned_generations(code: str, learned: object) -> dict[str, int]:
    """The learned values that are still a generation of `code`, by source."""
    if not isinstance(learned, dict):
        return {}
    return {k: v for k, v in learned.items()
            if k in GENERATION_SOURCES and valid_generation(code, v) is not None}


def with_learned_generation(code: str, learned: object, source: str,
                            value: object) -> dict[str, Any] | None:
    """`learned` with `value` recorded under `source`, or None if nothing changes.

    Only a generation this codename has is kept (`valid_generation`), from a
    known source: anything else from the cloud or the device is dropped, never
    raised on. A stored value that is not a dict is replaced, not merged into.
    """
    generation = valid_generation(code, value)
    if generation is None or source not in GENERATION_SOURCES:
        return None
    current = learned if isinstance(learned, dict) else {}
    if current.get(source) == generation:
        return None
    return {**current, source: generation}


class Refused(ValueError):
    """The write was understood and rejected, and nothing was changed.

    Distinct from a `None` return, which means "applied, but there is nothing to
    send to the device" -- a capability toggle, a schedule write. Collapsing the
    two into one outcome has the panel answer `{"ok": true}` to a value it just
    threw away, which is the silent failure refusing exists to avoid.

    Lives here rather than in `ha/commands.py` because a BLE accessory refuses
    writes too (`devices/ble/`), and `devices/` cannot import `ha/`. Two
    classes of one name reaching the same `except` in `web/api/` is the
    trap this avoids.
    """


def encode_multi_range(key: str, value: Any) -> str:
    """Encode one schedule range the way PetKit encodes it: doubly.

    The value of a `*MultiRange` field is a JSON STRING that wraps its own key
    again — `"{\\"distrubMultiRange\\":[[1425,585]]}"` — and it is that shape in
    BOTH directions. It is what the real cloud puts in a `dev_multi_config`
    reply, and it is what the cloud sends when the app sets a do-not-disturb
    period, captured going through this add-on to a T5 on 2026-08-09.

    So there is one encoder, called from `payloads.to_multi_config` on the way
    out and from the panel's schedule write on the way in. Two copies of a shape
    this surprising is two chances for one of them to be right.

    `schedule` is NOT this shape, despite travelling the same way: it is a
    plain JSON string of the array, with no wrapping key. See
    `ha/schedules.py::save_schedule`.
    """
    return json.dumps({key: value}, separators=(",", ":"))


def split_bucket_authority(bucket_endpoint: str) -> tuple[str, str] | None:
    """Split our bucket address into the `(bucketName, endPoint)` pair the
    device's `logUpload` concatenates back with a literal dot.

    `dev_upload_log_token` gives the firmware no way to express a plain host:
    the upload URL comes from `https://%s.%s%s/%s`, so the authority is always
    `{bucketName}.{endPoint}`. To be reachable at all, our own address has to be
    cut at a dot and handed over in two pieces:

        https://192.0.2.199:9000  ->  ("192", "0.2.199:9000")

    The cut is at the FIRST dot rather than the last, which matters for one
    reason: the device rebuilds the same string either way, but a real OSS
    bucket name may not contain a dot, so `192` is a plausible `bucketName`
    where `192.0.2` is not. If a firmware ever sanity-checks that field, this
    is the form that survives.

    The port stays on `endPoint`, which is safe here and would NOT be in
    `to_oss_sts`: `primaryDomain` there goes through
    `sscanf("https://%[^/]/%s")` into `getaddrinfo`, which a port breaks. This
    value is only ever sprintf'd into a URL and handed to curl, which parses a
    port correctly. Should that turn out to be wrong on hardware, moving the
    bucket to 443 and dropping the suffix is a change to this function alone.

    Returns:
        The two pieces, or None when no valid authority can be formed — an
        empty address (there is no add-on option for `bucket_endpoint`, so an
        install where the Supervisor host-IP lookup failed has none), a
        single-label host like `localhost:9000`, an IPv6 literal, or anything
        carrying userinfo. The caller answers `{"result": {}}` for all of them.
    """
    if not bucket_endpoint:
        return None
    authority = bucket_endpoint.split("//", 1)[-1].split("/", 1)[0]
    # `@` and `[` would each need a URL-building rule of their own, and neither
    # can appear in an address this add-on generates for itself.
    if not authority or "@" in authority or "[" in authority:
        return None
    head, dot, tail = authority.partition(".")
    if not dot or not head or not tail:
        return None
    return head, tail


def aliyun_broker_name(product_key: str) -> str:
    """The broker name an ESP32 model dials, built from its product key.

    A T4 and two D4s handed PetKit's own `dev_iot_device_info` ignored its
    `mqttHost` and each dialled `{productKey}.iot-as-mqtt.eu-central-1.
    aliyuncs.com` instead (2026-10-22, seen by DNS); issue #35 reports the same
    of a T3. localkit builds the same name for the same reason.
    """
    return f"{product_key}.iot-as-mqtt.eu-central-1.aliyuncs.com"


log = logging.getLogger(__name__)

#: Private marker `Device.queue_command` puts on a queued command dict: the
#: `time.monotonic()` at which it was asked for. Never sent to a device.
QUEUED_AT_KEY = "_queued_at"

#: The most commands one device may have waiting. Far above anything a person
#: or a scene produces at once (a device has a few dozen settable entities),
#: and small enough that a retrying automation cannot grow it for days.
MAX_QUEUED_COMMANDS = 64


@dataclass
class Device:
    """One registered PetKit device: its identity, credentials and live state.

    Created on `dev_signup` and persisted to `devices.json` by `DeviceRegistry`.
    Only identity, credentials and `config` survive a restart (`to_dict`);
    `state`, `command_queue` and the liveness timestamps are rebuilt from the
    device's next contact, so nothing transient may become the only copy of
    something we cannot re-derive.

    The MQTT triple (`mqtt_product_key`, `mqtt_device_name`,
    `mqtt_device_secret`) is the exception: it is minted here exactly once and
    handed to the device by `to_iot_device_info*`. Losing it leaves the device
    connecting with credentials the broker no longer knows, which is why
    `DeviceRegistry` writes a new device through synchronously. The one change
    it ever sees: a patched ESP32's product key becomes the shared
    `payloads.PATCHED_PRODUCT_KEY` at its first CONNECT under it (`mqtt/auth.py`).
    """

    device_type: str
    petkit_id: int
    serial_number: str = ""
    mac: str = ""
    firmware: str = ""
    #: The `type` value this device puts in its own `X-Device` header, recorded
    #: verbatim from live traffic. Not the same thing as `device_type`, which is
    #: our lowercase codename taken from the URL: the header's spelling is one
    #: of the five fields hashed into that header's `sign`, so asking PetKit a
    #: question AS this device needs the device's own (`http/cloud_fetch.py`).
    wire_type: str = ""

    mqtt_product_key: str = field(default_factory=generate_product_key)
    mqtt_device_name: str = ""
    mqtt_device_secret: str = field(default_factory=generate_device_secret)

    #: The credential the device signs its HTTP requests with, handed to it by
    #: `to_signup`. Empty means "use `mqtt_device_secret`", which is what every
    #: device had before proxy mode could learn a real one — see
    #: `signing_secret`.
    api_secret: str = ""
    #: The HTTP heartbeat this device last sent — method, path, query, body and
    #: the session headers — recorded from live traffic
    #: (`http/presence.py::remember_heartbeat`). Replayed to PetKit, signed as
    #: the device, by the opt-in keep-online heartbeat once the device is on
    #: MQTT and has stopped sending it. Persisted, because a device on MQTT
    #: does not heartbeat over HTTP again until it reboots: a restart that lost
    #: this would fall back to a guessed shape. Empty until the first heartbeat.
    heartbeat_request: dict[str, Any] = field(default_factory=dict)

    state: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    # Entries are either a command dict or an already-JSON-encoded string
    # (`patchers/common.py` queues the encoded form); the heartbeat handler
    # passes both through unchanged.
    command_queue: list[Any] = field(default_factory=list)

    last_heartbeat: float = 0.0
    last_state_report: float = 0.0
    last_seen: float = 0.0  # any HTTP contact from the device
    #: Last frame received from the device over MQTT. A device that gets onto
    #: the broker STOPS polling `poll/{type}/heartbeat` — confirmed on a T5,
    #: which went quiet over HTTP some 40s after its CONNECT — so without this
    #: the liveness check sees only silence and calls a perfectly healthy device
    #: offline (`devices/liveness.py::device_is_stale`).
    last_mqtt: float = 0.0
    #: Every LAN address recently named for this device, `{kind: {ip: when}}` —
    #: see `devices/addresses.py`. `state["ip"]` is one unauthenticated claim;
    #: this is what the patchers check it against before fetching from it.
    addresses_seen: dict[str, dict[str, float]] = field(default_factory=dict)
    #: Topic filters in force for this device's current MQTT session — the ones
    #: it asked for, plus the ones subscribed on its behalf
    #: (`mqtt/auth.py::_server_subscribe`, which is the only source for a T5:
    #: it sends no SUBSCRIBE at all). Live state, reset per session by
    #: `_mark_mqtt_connected`. Exists because a publish to an unsubscribed
    #: topic succeeds silently, so this is the only way to tell a command that
    #: was delivered from one that went nowhere.
    mqtt_subscriptions: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    online: bool = False
    mqtt_connected: bool = False
    #: When `mqtt_connected` last went up. The heartbeat's `iotStatus` is the
    #: backstop that takes it down again, and the device samples that value
    #: before it sends the request — so a poll already in flight when the
    #: session came up arrives reporting the state from just before it. Without
    #: this timestamp that stale zero cancels the connect that beat it by
    #: milliseconds; `http/handlers/heartbeat.py::note_iot_status` uses it to
    #: tell a lagging report from a real loss.
    mqtt_connected_at: float = 0.0
    #: The last `timestamp` a heartbeat handed this device with a command. The
    #: firmware drops an entry whose stamp is not greater than the last one it
    #: consumed, and a batch is stamped one second apart from now, so the next
    #: poll must start after this rather than at its own now
    #: (`http/handlers/heartbeat.py`). Live state, like the rest of this block.
    last_heartbeat_stamp: int = 0

    def __post_init__(self) -> None:
        """Derive the MQTT device name when the caller did not supply one."""
        if not self.mqtt_device_name:
            sn = self.serial_number or str(self.petkit_id)
            self.mqtt_device_name = f"d_{self.device_type}_{sn}"

    # The category predicates below are plain reads of the type sets in
    # `utils/const.py`; they are properties because the payload builders in
    # `devices/payloads.py` branch on them constantly and a set lookup reads
    # badly inline.

    @property
    def is_camera(self) -> bool:
        """Whether the model has a camera, and so media uploads and STS capabilities."""
        return self.device_type in DEVICE_TYPES_CAMERA

    @property
    def is_next_gen(self) -> bool:
        """Whether this is an embedded-Linux model rather than an ESP32 one."""
        return self.device_type in DEVICE_TYPES_NEXT_GEN

    @property
    def is_litter(self) -> bool:
        """Whether this is a litter box, which selects the litter settings block."""
        return self.device_type in DEVICE_TYPES_LITTER

    @property
    def is_feeder(self) -> bool:
        """Whether this is a feeder, which selects the feeder settings block."""
        return self.device_type in DEVICE_TYPES_FEEDER

    @property
    def feed_schedule_active(self) -> bool:
        """Whether a feeder is served its recurring meals, or they are paused.

        A pause is ours alone: the firmware has no "schedule enabled" field a
        server can write, only the plan itself, so pausing means answering
        with a plan that has no meals in it while the real one stays stored.
        That makes this the single question every place that hands a device
        its plan has to ask -- `dev_feed_get` over either transport and both
        `property.set{feed}` pushes -- because one that forgets hands the
        meals straight back.

        Kept under `config["local"]` with the other values that are our intent
        and never a device setting; absent means nobody has paused anything.
        """
        local = self.config.get("local")
        if not isinstance(local, dict):
            return True
        return to_bool(local.get("feedScheduleActive"), True)

    @property
    def is_water_fountain(self) -> bool:
        """Whether this is a fountain, which selects the fountain settings block."""
        return self.device_type in DEVICE_TYPES_WATER_FOUNTAIN

    @property
    def is_purifier(self) -> bool:
        """Whether this is an air purifier (K2/K3, BLE-only in practice)."""
        return self.device_type in DEVICE_TYPES_PURIFIER

    @property
    def is_ble_only(self) -> bool:
        """Whether this model has no network of its own.

        True means this `Device` should not exist: the real thing pairs over
        BLE to a WiFi device that relays for it (`devices/ble/`), so it can
        neither sign up nor hold credentials nor poll. Read by
        `registry.get_or_create`, which warns rather than refuses -- a device
        is never told no. Nothing else branches on it: one that cannot arrive
        needs no special handling once it somehow has.
        """
        return self.device_type in DEVICE_TYPES_BLE_ONLY

    @property
    def generation(self) -> int | None:
        """Which generation of its codename this is -- 2 for a Pura Max 2.

        The owner's choice (`GENERATION_KEY`) first, then what was learned, in
        `GENERATION_SOURCES` order; None when nothing says. Every stored value
        is re-checked against `utils/const.py::MODEL_GENERATIONS`, so a value
        hand-edited into `devices.json` or left by an older table is ignored
        rather than trusted.
        """
        return self.generation_and_source[0]

    @property
    def generation_and_source(self) -> tuple[int | None, str]:
        """`(generation, source)`: `owner`, a `GENERATION_SOURCES` entry, or `""`."""
        return resolve_generation(self.device_type, self.config.get(GENERATION_KEY),
                                  self.config.get(GENERATION_LEARNED_KEY))

    @property
    def model_name(self) -> str:
        """The product name, generation included: "Pura Max 2", "YumShare Solo".

        `utils/const.py::device_display_name` with this device's generation, so
        an unknown one reads as the codename's own name, as before.
        """
        return device_display_name(self.device_type, self.generation)

    def record_generation(self, source: str, value: object) -> bool:
        """Store a LEARNED generation from `source`; True if it changed.

        Only a generation this codename has is kept (`valid_generation`):
        anything else from the cloud or the device is dropped, never raised on.
        The owner's choice is a different key and is never touched here.
        """
        learned = with_learned_generation(
            self.device_type, self.config.get(GENERATION_LEARNED_KEY), source, value)
        if learned is None:
            return False
        before = self.model_name
        self.config[GENERATION_LEARNED_KEY] = learned
        log.info("Device %d: learned generation %d from %s (%s -> %s)",
                 self.petkit_id, learned[source], source, before, self.model_name)
        return True

    def note_hardware(self, report: object) -> bool:
        """Learn the generation from a state report's own `hardware`, where it means one.

        Only a codename `utils/const.py::HARDWARE_GENERATIONS` grades as usable
        (the D4SH); on a T4 `hardware` is not the generation. Returns whether a
        stored value changed, so a caller already persisting knows to.
        """
        if not isinstance(report, dict):
            return False
        generation = generation_from_hardware(self.device_type, report.get("hardware"))
        return generation is not None and self.record_generation("hardware", generation)

    @property
    def supports_ai(self) -> bool:
        """Whether the device's own NPU runs facial recognition (`dev_discern_pic`).

        The codename set is only a seed. A device that does recognition asks for
        `dev_discern_config` at every boot and roughly hourly after that, and one
        that cannot never asks at all — so the device answers this question
        better than any list of product names can. (The reference capture shows
        122 of these in three hours, but that was a device boot-looping on a
        cloud error: 117 of the gaps are under ten seconds and the count tracks
        `dev_signup` almost exactly. It is a per-boot poll, not a fast one.)

        That matters most for the feeders, where PetKit ships two generations
        under one codename and only the newer has an NPU — see
        `utils/const.py::DEVICE_TYPES_AI`.

        `ai_observed` is set by `http/handlers/discern.py` and only ever turns
        ON: a device being offline, asleep, or simply not having polled yet is
        not evidence that it lacks the hardware.

        A known generation answers it up front for those feeders: a YumShare
        Solo 2 or Dual-Hopper 2 (`utils/const.py::AI_GENERATIONS`) does
        recognition before it has asked for anything.
        """
        return (self.device_type in DEVICE_TYPES_AI
                or self.generation in AI_GENERATIONS.get(self.device_type, ())
                or to_bool(self.config.get("ai_observed"), False))

    # Media capabilities are the STS `capability[]` entries (see to_oss_sts) —
    # the upload types the device is allowed to ask for. Toggling one off
    # drops it from the next STS response, so the device stops uploading that
    # type at the source (no bandwidth/disk wasted on a discarded upload).
    CAPABILITY_TYPES = ("fullVideo", "eventImage", "highLight", "dynamicVideo")

    def enabled_capabilities(self) -> set[str]:
        """Which of `CAPABILITY_TYPES` this device may currently upload.

        Empty for a camera-less model, which has no media pipeline at all. A
        camera model with nothing stored under `config["capabilities"]` has
        never been configured, and defaults to all types on.
        """
        if not self.is_camera:
            return set()
        stored = self.config.get("capabilities")
        if stored is None:
            return set(self.CAPABILITY_TYPES)  # default: all on
        return {ct for ct in self.CAPABILITY_TYPES if stored.get(ct, True)}

    @property
    def signing_secret(self) -> str:
        """What `secret` to hand the device — the real one if we have learned it.

        The firmware signs every request with this: the `X-Device` header carries
        `id`, `nonce`, `timestamp`, `type` and an MD5 `sign` computed from them
        and this secret. We never verify that signature, so locally any value
        works — but PetKit does verify it, and answers `{"error": {"code": 704}}`
        to a signature made with a secret it does not know, which is every
        signature made with one WE generated.

        Proxy mode's whole point is seeing real cloud replies, so when a proxied
        `dev_signup` reveals the device's genuine secret it is adopted here
        (`http/middleware/proxy.py::_remember_upstream_credentials`) and handed on
        unchanged. The device then signs with a credential PetKit accepts, and
        we never had to reproduce the signature algorithm — the device computes
        it. Note this is NOT `mqtt_device_secret`: the real cloud issues two
        different values, 16 hex here and 32 for the broker.

        Falls back to `mqtt_device_secret` so a device that has never been
        proxied behaves exactly as before.
        """
        return self.api_secret or self.mqtt_device_secret

    @property
    def timezone_offset(self) -> float:
        """Hours east of UTC to report to the device, e.g. 2.0 for CEST.

        Four sources, most specific first:

        1. the manual override (`config["timezone"]`), for an install whose box
           lives in a different zone than its Home Assistant;
        2. the offset derived from the zone NAME the device reports
           (`config["locale"]`, e.g. `Europe/Warsaw`) — DST-correct and the
           device's own statement of where it is;
        3. the numeric offset the device reported at signup
           (`reported_timezone`), for a device that sent no usable locale;
        4. the container's clock, which the Supervisor sets from Home Assistant.

        The device is the authority on where IT is — it was handed a zone over
        BLE at provisioning and burns it into its video watermarks — so the two
        device-sourced values (2, 3) sit above the server's. But the NAME wins
        over the number, because a device that only ever got a locale and never
        a numeric offset reports that offset as 0: a box plainly in
        `Europe/Warsaw` telling us it is in UTC, which then had us serve UTC
        back and its scheduled feeds fire two hours late. The name it also sent
        is unambiguous and recovers the real offset. A hardcoded constant is not
        an option: any fixed offset is wrong for half the year under DST — which
        is the same reason the name beats the frozen number even when both are
        present.

        Answering this correctly does not by itself fix the device's video
        watermarks — the firmware does not take its clock from this response.
        It CAN be fixed without re-provisioning, though: a `property.set`
        carrying `timezone` as a JSON STRING moves it, verified on a live T5.
        `web/api/devices.py::api_timezone` is the path that does both, and the
        string is not a detail — see the note there. The device does no DST of
        its own, so a locale-derived value is pushed again whenever its zone
        changes offset (`ha/timezone_sync.py`).
        """
        override = to_float(self.config.get("timezone"), None)
        if override is not None:
            return override
        from_locale = offset_hours_for_locale(self.config.get("locale"))
        if from_locale is not None:
            return from_locale
        reported = to_float(self.config.get("reported_timezone"), None)
        if reported is not None:
            return reported
        return local_offset_hours()

    @property
    def aliyun_mqtt_host(self) -> str:
        """The Aliyun-format MQTT hostname for this device's product key.

        Only a fallback for when we cannot derive our own broker host. It is
        NOT unresolvable, as this once said: Aliyun answers every
        `*.iot-as-mqtt.eu-central-1.aliyuncs.com` (checked 2026-10-22, made-up
        keys included), so a device handed it reaches PetKit's real broker
        rather than failing over to the HTTP heartbeat.
        """
        return aliyun_broker_name(self.mqtt_product_key)

    def resolve_mqtt_host(self, real_host: str) -> str:
        """Which `mqttHost` to hand this device.

        Always our own broker (`real_host`): a patched `ctrl` connects to it
        over MQTT, and an unpatched one simply keeps heartbeating over HTTP — it
        does NOT crash, confirmed on-device. `aliyun_mqtt_host` is used only
        when we could not determine our own address at all.
        """
        return real_host or self.aliyun_mqtt_host

    def queue_command(self, cmd: Any, service_suffix: str = "") -> None:
        """Queue a command for the next heartbeat, remembering when it was asked.

        The stamp is what lets `http/handlers/heartbeat.py::still_wanted` tell
        a command the device is collecting on its next poll from one that sat
        here for a day while it was unplugged. Callers that append to
        `command_queue` directly (the patchers, a firmware job's trigger) queue
        unstamped entries, which are delivered whenever the device returns.

        Args:
            service_suffix: The `thing/service/{suffix}` the envelope was built
                for; recorded for the heartbeat's translation.
        """
        if isinstance(cmd, dict):
            if service_suffix:
                cmd["_service_suffix"] = service_suffix
            # Monotonic: only ever compared within this process (the queue is
            # not persisted), and a wall clock stepped by NTP must not age a
            # command it did not wait for.
            cmd[QUEUED_AT_KEY] = time.monotonic()
        self.command_queue.append(cmd)
        # Nothing drains the queue of a device that never comes back, so an
        # automation retrying against it would grow this without bound.
        while len(self.command_queue) > MAX_QUEUED_COMMANDS:
            stamped = [c for c in self.command_queue
                       if isinstance(c, dict) and QUEUED_AT_KEY in c]
            if not stamped:
                break
            oldest = stamped[0]
            self.command_queue[:] = [c for c in self.command_queue if c is not oldest]
            log.warning("Device %d: command queue is full (%d), dropping the oldest "
                        "undelivered command: %s", self.petkit_id, MAX_QUEUED_COMMANDS,
                        oldest.get("_service_suffix") or oldest.get("method") or "?")

    def pop_commands(self) -> list[Any]:
        """Drain the queue and return everything in it, oldest first.

        Destructive on purpose: the heartbeat response is the only delivery
        attempt a command gets, so leaving entries behind would re-send them on
        every subsequent poll.
        """
        cmds = list(self.command_queue)
        self.command_queue.clear()
        return cmds

    @property
    def last_ip(self) -> str:
        """The LAN address as it is persisted -- see `to_dict`."""
        return str(self.state.get("ip") or "")

    def to_dict(self) -> dict[str, Any]:
        """The persisted form: identity, MQTT credentials and `config` only.

        Live state (`state`, `command_queue`, the liveness timestamps and flags)
        is deliberately excluded — it is re-derived from the device's next
        contact, and persisting it would resurrect a stale "online" after a
        restart. One exception, `last_ip`, is explained where it is written.
        """
        return {
            "device_type": self.device_type,
            "petkit_id": self.petkit_id,
            "serial_number": self.serial_number,
            "mac": self.mac,
            "firmware": self.firmware,
            # Signing input, so it has to survive a restart like any credential:
            # without it, the first cloud fetch after a reboot signs with our
            # lowercase codename and is refused (`http/cloud_fetch.py`).
            "wire_type": self.wire_type,
            "mqtt_product_key": self.mqtt_product_key,
            "mqtt_device_name": self.mqtt_device_name,
            "mqtt_device_secret": self.mqtt_device_secret,
            "api_secret": self.api_secret,
            # What the keep-online heartbeat replays (`http/presence.py`). Kept
            # for the same reason as `wire_type`: the device on MQTT will not
            # send it again before it reboots.
            "heartbeat_request": self.heartbeat_request,
            "config": self.config,
            "created_at": self.created_at,
            # The one piece of live state that does survive, as a HINT rather
            # than a fact: the LAN address gates the stream probe, the Patchers
            # tab and two-way talk, and a YumShare Dual-Hopper reports it only
            # with a full state report, which it sends rarely. After a restart
            # the add-on used to sit without an IP for as long as that took --
            # half an hour was observed -- with no stream, no camera in Home
            # Assistant and no talk. `media/go2rtc.py::probe_stream` verifies
            # the address before anything is advertised, so a stale one costs a
            # five-second probe and nothing else.
            "last_ip": self.last_ip,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Device:
        """Rebuild a device from its `to_dict` form.

        Raises:
            KeyError: `device_type` or `petkit_id` is missing. Everything else
                falls back to a freshly generated default, but those two cannot
                be invented — `DeviceRegistry._restore` drops such an entry
                rather than letting one bad record cost every device its
                credentials.
        """
        d = cls(
            device_type=data["device_type"],
            petkit_id=data["petkit_id"],
            serial_number=data.get("serial_number", ""),
            mac=data.get("mac", ""),
            firmware=data.get("firmware", ""),
            wire_type=data.get("wire_type", ""),
        )
        d.mqtt_product_key = data.get("mqtt_product_key", d.mqtt_product_key)
        d.mqtt_device_name = data.get("mqtt_device_name", d.mqtt_device_name)
        d.mqtt_device_secret = data.get("mqtt_device_secret", d.mqtt_device_secret)
        d.api_secret = data.get("api_secret", d.api_secret)
        heartbeat = data.get("heartbeat_request")
        d.heartbeat_request = heartbeat if isinstance(heartbeat, dict) else {}
        d.config = data.get("config", {})
        d.created_at = data.get("created_at", d.created_at)
        last_ip = str(data.get("last_ip") or "")
        # Only a plausible IPv4 literal is seeded (see `to_dict`): the value
        # came from `request.remote` in some paths and could be anything.
        if last_ip.count(".") == 3 and all(p.isdigit() and int(p) < 256 for p in last_ip.split(".")):
            d.state["ip"] = last_ip
        return d
