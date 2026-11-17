"""Keeping a device on local MQTT "online" in PetKit's app — opt-in, device-paced.

PetKit's app judges a litter box online by its HTTP heartbeat. In proxy mode
that heartbeat used to reach PetKit because `middleware/proxy.py` forwards
everything a device sends — but a device that gets onto our broker STOPS
polling the heartbeat (AGENTS.md), so the moment it moves to local MQTT PetKit
hears nothing and the app calls it "disconnected from Wi-Fi", even with the
upstream MQTT bridge holding an Aliyun session for it the whole time.

This module sends that heartbeat in the device's place, and is the one
exception to "nothing reaches PetKit without a button or a device request":

* **Opt-in.** `proxy_keep_online`, a panel switch, off by default, and only
  while proxy mode is on (`blocker`).
* **Device-paced, never a timer of ours.** A heartbeat is only ever sent from
  `CloudPresence.note_activity`, which `mqtt/auth.py::on_mqtt_packet_received`
  calls for a packet the device itself sent — PINGREQ included. A device that
  goes quiet stops the heartbeats with it; nothing here wakes up on its own.
  At most one per device per `MIN_INTERVAL_SECONDS`, and none while the
  device is still sending its own over HTTP.
* **The device's own request.** The heartbeat the device last sent over HTTP is
  recorded (`remember_heartbeat`, from `handlers/heartbeat.py`) and replayed —
  method, path, query and body — with a fresh `X-Device` signed exactly as
  `http/cloud_fetch.py` signs as the device: its `wire_type`, its adopted real
  `signing_secret`. Only a device whose real API secret has been adopted is
  eligible; anything else would be a 704 every time.
* **The reply is never acted on.** Whatever PetKit answers — commands queued
  for the device, settings, addresses — is redacted, counted and recorded for
  the panel exactly like a proxied reply, and then dropped. Nothing reaches the
  device, nothing is learned, no credential is adopted. A command PetKit wants
  delivered goes the way it already would: the upstream MQTT bridge, under the
  existing guards.

Fire-and-forget: a send runs as its own task with a timeout, off the MQTT hot
path, and nothing in here raises into the broker.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import parse_qsl, urlencode

from petkit_local.http.cloud_fetch import USER_AGENT, x_device_header
from petkit_local.http.proxy import (
    _warn_unverified, breaker_is_open, get_proxy_session, resolve_upstream,
    verifies_certificate,
)
from petkit_local.http.redact import cloud_error, redact_body
from petkit_local.utils.capture import capture_record

if TYPE_CHECKING:  # pragma: no cover - typing only
    from aiohttp import web

    from petkit_local.devices.base import Device
    from petkit_local.events.store import EventStore
    from petkit_local.http.redact import RedactionPolicy
    from petkit_local.web.hub import EventHub

log = logging.getLogger(__name__)

#: The panel switch. Panel-only, like every proxy setting (`config.PANEL_LIVE_KEYS`).
SETTING = "proxy_keep_online"

#: The least time between two heartbeats sent for one device.
#:
#: The device's own pace over HTTP was one heartbeat every ~10-15s, so 30s is
#: at most half of what PetKit was already receiving from it — this can never
#: make the device look busier than it was. It is also comfortably under one
#: MQTT keep-alive (62s on a T5, about the same on a T4): an idle device's only
#: traffic is its PINGREQ, so in practice the heartbeat follows that, and 30s
#: just keeps a chatty one (a burst of `property/post`s) from turning every
#: frame into a request. Being device-paced, the real interval is
#: max(30s, the gap between the device's own packets).
MIN_INTERVAL_SECONDS = 30.0

#: How long one send may take before it is abandoned. Under
#: `MIN_INTERVAL_SECONDS`, so a send never overlaps the next one's slot.
SEND_TIMEOUT_SECONDS = 10.0

#: A failing upstream is logged at debug per attempt and at warning at most
#: this often per device, so a dead upstream costs one line per ten minutes.
WARN_INTERVAL_SECONDS = 600.0

#: The heartbeat request headers worth replaying. The `X-Device` is rebuilt per
#: request; the rest is what `http/proxy.py` forwards for the same endpoint.
_REPLAYED_HEADERS = ("X-Session", "F-Session", "User-Agent", "Content-Type")

#: A heartbeat body is a small form, if it has one at all. Anything bigger is
#: not one we want to persist on every device record.
_MAX_BODY = 4096


def remember_heartbeat(device: Device, request: web.Request, body: bytes) -> bool:
    """Record the shape of the HTTP heartbeat this device just sent.

    What `CloudPresence` replays once the device has moved to MQTT and stopped
    sending it. Recorded on every heartbeat so it is always the device's latest
    — the firmware's own path, query and body, which nothing else in this repo
    documents for every model.

    Returns:
        True when the SHAPE changed (method, path, query keys, content type) —
        the caller then persists the registry. A value that changes from poll
        to poll (`iotStatus`, say) is kept in memory without a write per poll.
    """
    try:
        text = body.decode("utf-8") if len(body) <= _MAX_BODY else ""
    except UnicodeDecodeError:
        text = ""
    shape = {
        "method": request.method,
        "path": request.path,
        "query": request.query_string,
        "body": text,
        "headers": {h: request.headers[h] for h in _REPLAYED_HEADERS
                    if h in request.headers},
    }
    old = device.heartbeat_request or {}
    device.heartbeat_request = shape
    return _shape_key(old) != _shape_key(shape)


def _shape_key(shape: dict[str, Any]) -> tuple:
    """What of a recorded heartbeat is worth a write to disk when it changes."""
    query = shape.get("query") or ""
    return (shape.get("method"), shape.get("path"),
            tuple(sorted(k for k, _ in parse_qsl(query, keep_blank_values=True))),
            (shape.get("headers") or {}).get("Content-Type"))


def heartbeat_request(device: Device) -> dict[str, Any]:
    """The heartbeat to send for `device`: `{method, path, query, body, headers, shape}`.

    `shape` is `"device"` when this is the device's own recorded request and
    `"default"` when none has been seen yet — a device that was already on
    MQTT when this feature arrived sends no HTTP heartbeat until it next boots.
    The default is `POST /6/poll/{type}/heartbeat`, the route our own handler
    serves the T4 on, with no query and no body; it is a guess, labelled as
    one in the panel, and replaced by the real thing the first time the device
    heartbeats over HTTP.

    `iotStatus`, when the device's request carries it, is sent as `1`. The
    device fills it in with the state of its MQTT session at the time it
    polls, and it only stopped polling BECAUSE that session is up; replaying a
    `0` captured during boot would tell PetKit the opposite of the truth.
    """
    recorded = device.heartbeat_request or {}
    if recorded.get("path"):
        pairs = parse_qsl(recorded.get("query") or "", keep_blank_values=True)
        pairs = [(k, "1" if k == "iotStatus" else v) for k, v in pairs]
        return {
            "method": recorded.get("method") or "POST",
            "path": recorded["path"],
            "query": urlencode(pairs),
            "body": recorded.get("body") or "",
            "headers": dict(recorded.get("headers") or {}),
            "shape": "device",
        }
    return {
        "method": "POST",
        "path": f"/6/poll/{device.device_type}/heartbeat",
        "query": "",
        "body": "",
        "headers": {},
        "shape": "default",
    }


def blocker(config: dict[str, Any], device: Device) -> str | None:
    """Why no heartbeat would be sent for `device` right now, or None if one would.

    Shared with the panel, so what it shows and what this module does cannot
    disagree. Interval and in-flight checks are not here: those are pacing,
    not eligibility.
    """
    if not config.get(SETTING):
        return "switched off"
    if not config.get("proxy_mode"):
        return "proxy mode is off"
    if not device.mqtt_connected:
        return "not on local MQTT"
    if not device.api_secret:
        return "real API secret not adopted yet"
    return None


def withheld_commands(raw: bytes) -> int:
    """How many entries of a heartbeat reply would have been executed.

    Read from PetKit's own body — the one the device never gets. The idle
    filler (`{"time": ms}`) does not count; anything else does, recognised or
    not, because "the cloud wanted something done" is the useful fact.
    """
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return 0
    result = data.get("result") if isinstance(data, dict) else None
    if not isinstance(result, list):
        return 0
    return sum(1 for e in result
               if not (isinstance(e, dict) and set(e) <= {"time", "timestamp"}))


class CloudPresence:
    """Sends each eligible device's heartbeat to PetKit, paced by the device.

    One per process, built in `main/wiring.py` and handed to the broker's auth
    plugin, which calls `note_activity` for every packet a device sends.
    """

    def __init__(self, app: web.Application, live_config: dict[str, Any], *,
                 policy_factory: Callable[[Device], RedactionPolicy],
                 hub: EventHub | None = None,
                 event_store: EventStore | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        """Wire the collaborators; nothing is sent until a device is active.

        Args:
            app: The device-facing application, whose pooled upstream session
                (`proxy.get_proxy_session`) and breaker this shares, so a dead
                upstream is not tried any harder from here than from a proxied
                request.
            policy_factory: The redaction policy for one device, rebuilt per
                reply from the live config — used to RECORD the reply the way a
                proxied one is recorded, never to serve it.
        """
        self._app = app
        self._config = live_config
        self._policy_factory = policy_factory
        self._hub = hub
        self._event_store = event_store
        self._clock = clock
        self._last_attempt: dict[int, float] = {}
        self._last_warning: dict[int, float] = {}
        self._inflight: dict[int, asyncio.Task[None]] = {}

    def note_activity(self, device: Device) -> None:
        """A packet from `device` just arrived on our broker; heartbeat if due.

        Synchronous and cheap — it runs for every packet the broker reads — and
        never raises: a failure here would surface in the broker's packet loop.
        """
        try:
            if blocker(self._config, device) is not None:
                return
            petkit_id = device.petkit_id
            task = self._inflight.get(petkit_id)
            if task is not None and not task.done():
                return
            if time.time() - device.last_heartbeat < MIN_INTERVAL_SECONDS:
                # Still heartbeating over HTTP for itself — the ~40s after a
                # CONNECT before it goes quiet there — and proxy mode forwards
                # those. Sending ours as well would outpace the device.
                return
            now = self._clock()
            last = self._last_attempt.get(petkit_id)
            if last is not None and now - last < MIN_INTERVAL_SECONDS:
                return
            self._last_attempt[petkit_id] = now
            task = asyncio.get_running_loop().create_task(
                self._send(device), name=f"cloud-presence-{petkit_id}")
            self._inflight[petkit_id] = task
            task.add_done_callback(lambda t, pid=petkit_id: self._done(pid, t))
        except Exception:
            log.debug("Presence heartbeat not scheduled for device %s",
                      getattr(device, "petkit_id", "?"), exc_info=True)

    def _done(self, petkit_id: int, task: asyncio.Task[None]) -> None:
        """Forget a finished send, and swallow whatever it raised."""
        if self._inflight.get(petkit_id) is task:
            del self._inflight[petkit_id]
        if not task.cancelled() and task.exception() is not None:
            log.debug("Presence heartbeat task failed for device %d", petkit_id,
                      exc_info=task.exception())

    async def close(self) -> None:
        """Cancel any send still in flight. Idempotent."""
        tasks = list(self._inflight.values())
        self._inflight.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except BaseException:  # noqa: BLE001 - cancelled or failed, both expected
                pass

    async def _send(self, device: Device) -> None:
        """Send one heartbeat as `device` and record what came back."""
        # Checked again: the switch may have flipped while this was scheduled.
        # "Off stops it at once" is the contract the panel states.
        if blocker(self._config, device) is not None:
            return
        if breaker_is_open(self._app):
            log.debug("Presence heartbeat for device %d skipped: upstream breaker "
                      "is open", device.petkit_id)
            return

        req = heartbeat_request(device)
        upstream = resolve_upstream(self._config.get("proxy_upstream", ""))
        url = f"{upstream}{req['path']}" + (f"?{req['query']}" if req["query"] else "")
        headers = {"User-Agent": USER_AGENT, **req["headers"],
                   "X-Device": x_device_header(device)}
        verify = verifies_certificate(upstream)
        if not verify:
            _warn_unverified(self._app, upstream)

        try:
            session = get_proxy_session(self._app, self._config.get("proxy_dns", ""))
            status, raw = await asyncio.wait_for(
                self._request(session, req["method"], url, headers,
                              req["body"].encode() if req["body"] else None, verify),
                SEND_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - every failure is just "not sent"
            self._failed(device, url, e)
            return

        self._record(device, req, url, status, raw)

    @staticmethod
    async def _request(session: Any, method: str, url: str, headers: dict[str, str],
                       body: bytes | None, verify: bool) -> tuple[int, bytes]:
        """One HTTP round trip, as `(status, body)`."""
        async with session.request(method=method, url=url, headers=headers,
                                   data=body, ssl=verify) as resp:
            return resp.status, await resp.read()

    def _failed(self, device: Device, url: str, error: BaseException) -> None:
        """Count an attempt that got no answer, and say so without flooding."""
        reason = "timeout" if isinstance(error, asyncio.TimeoutError) else type(error).__name__
        self._note(device, status=None, outcome=reason)
        log.debug("Presence heartbeat for device %d to %s failed: %r",
                  device.petkit_id, url, error)
        now = self._clock()
        last = self._last_warning.get(device.petkit_id)
        if last is None or now - last >= WARN_INTERVAL_SECONDS:
            self._last_warning[device.petkit_id] = now
            log.warning("PROXY: could not send the keep-online heartbeat for device "
                        "%d (%s); retrying on its next MQTT activity", device.petkit_id,
                        reason)

    def _record(self, device: Device, req: dict[str, Any], url: str,
                status: int, raw: bytes) -> None:
        """Record PetKit's reply for the panel, exactly as far as that goes.

        Redacted with the live policy so the panel and the blocked-attempt
        table see it the way they see a proxied reply — and then discarded.
        Nothing here touches the device: no command is queued, no setting
        learned, no credential adopted.
        """
        try:
            error = cloud_error(raw)
            withheld = withheld_commands(raw)
            outcome = ("ok" if 200 <= status < 300 and error is None
                       else f"error_{error.get('code')}" if error
                       else f"http_{status}")
            self._note(device, status=status, outcome=outcome,
                       error=error.get("code") if error else None,
                       withheld=withheld, shape=req["shape"])
            log.debug("Presence heartbeat for device %d -> %s [%d] %s",
                      device.petkit_id, url, status, outcome)

            redacted = redact_body(raw, endpoint=req["path"],
                                   policy=self._policy_factory(device))
            if self._hub is not None:
                if withheld:
                    self._hub.publish(
                        "blocked", device.petkit_id,
                        f"PetKit's reply to the keep-online heartbeat carried "
                        f"{withheld} command(s) — not delivered",
                        detail={"endpoint": req["path"], "upstream": url,
                                "reply": raw[:2048].decode("utf-8", "replace")})
                for record in redacted.records:
                    self._hub.record_redaction(
                        device.petkit_id, record.rule,
                        f"{record.rule} on {req['path']} (keep-online heartbeat)",
                        detail={"rule": record.rule, "path": record.path,
                                "endpoint": req["path"], "upstream": url,
                                "original": record.original, "note": record.note},
                        blocked=record.blocking)
            blocked = [r for r in redacted.records if r.blocking]
            if self._event_store is not None and blocked:
                task = asyncio.get_running_loop().create_task(
                    self._event_store.add_blocked_attempts([{
                        "device_id": device.petkit_id,
                        "kind": r.rule,
                        "transport": "http",
                        "endpoint": req["path"],
                        "upstream": url,
                        "field_path": r.path,
                        "payload_json": r.original,
                        "detail_json": {"note": r.note, "status": status,
                                        "presence": True},
                    } for r in blocked]))
                task.add_done_callback(_log_task_failure)
            if self._config.get("capture"):
                capture_record(self._config.get("capture_dir", "/data/capture"),
                               "proxy_http", {
                                   "presence": True,
                                   "method": req["method"],
                                   "path": req["path"],
                                   "query": req["query"],
                                   "req_body": req["body"] or None,
                                   "upstream_url": url,
                                   "upstream_status": status,
                                   "upstream_body": raw.decode("utf-8", "replace"),
                                   "redactions": [r.rule for r in redacted.records],
                               })
        except Exception:
            log.exception("PROXY: could not record the keep-online heartbeat "
                          "reply for device %d", device.petkit_id)

    def _note(self, device: Device, **fields: Any) -> None:
        """Hand one attempt's outcome to the hub's per-device counter."""
        if self._hub is not None:
            self._hub.record_presence(device.petkit_id, **fields)


def _log_task_failure(task: asyncio.Task[Any]) -> None:
    """Swallow, at debug, a background write that failed."""
    if not task.cancelled() and task.exception() is not None:
        log.debug("Could not persist a blocked attempt from a keep-online reply",
                  exc_info=task.exception())
