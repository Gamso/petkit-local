"""Live video for the app: go2rtc's fMP4 WebSocket and its HLS, behind the session token.

go2rtc's API stays on loopback (`media/go2rtc.py::API_ADDR`). The app reaches
two things of it, both through `/app/v1/devices/{id}/live...`, and both only
for the stream of the device named in the path:

* **fMP4 over a WebSocket** (go2rtc's "MSE" mode) — the primary path, because it
  crosses a Cloudflare tunnel (WebRTC media is UDP and does not) and has no
  segment to wait for. Protocol, checked against go2rtc 1.9.14 in the add-on
  image: the client sends `{"type":"mse","value":"<codecs it can play>"}`;
  go2rtc answers ONE text `{"type":"mse","value":"video/mp4; codecs=\\"...\\""}`
  and then binary messages, the first `ftyp`+`moov` (the init segment), every
  later one `moof`+`mdat`. A stream go2rtc does not have is a text
  `{"type":"error","value":"mse: stream not found"}` on an open socket.
* **HLS** — the fallback for networks where the socket keeps dropping. go2rtc
  opens an HLS session on `/api/stream.m3u8?src=` and serves its files under
  `/api/hls/...?id=<session>`; that session dies after ~5 s without a request.

What this module refuses, and why:

* **The stream name is never the client's.** It is resolved from the device id
  server-side (`device_stream`), and only a device go2rtc is configured to serve
  has one. go2rtc treats an unknown `src` as a SOURCE URL and dials it, so a
  `src` from the client would be a way to make the add-on open connections.
* **Nothing the client sends is forwarded as sent.** On the socket exactly one
  `mse` request is relayed, re-serialised by us: go2rtc closes the socket on a
  text frame that is not JSON, and every further `mse` would add a consumer.
* **go2rtc's error text stays here.** It is ffmpeg's stderr — internal
  addresses, the camera's LAN address — so the phone gets a fixed message and
  the original goes to the log (`from_go2rtc_text`).
* **An HLS session id is only good for the phone and device it was opened for.**
  go2rtc's ids are global; one taken from another device's playlist, or from
  another phone's, is a 404 here (`VideoSessions.hls_slot`).
* **A slow client is dropped, not buffered for.** go2rtc's output is read
  as it comes so the camera's other viewers never wait on this one, into a
  queue capped at `CLIENT_BACKLOG_BYTES`; a client that lets it fill is closed
  with 1013.
* **Viewers are capped** — `MAX_LIVE_PER_DEVICE` per camera and
  `MAX_LIVE_TOTAL` in all, whichever transport. Each viewer is a separate
  copy of the video through the tunnel and the camera has one connection to
  give (`media/go2rtc.py`), so this is a budget, not a formality. A phone
  holds at most one slot per camera: opening the same camera again replaces
  the older view rather than waiting for it to time out — what a phone
  reconnecting after a network change needs.

A relay ends when either side closes, when the phone's session ends (logout,
revocation, password change; checked every `SESSION_RECHECK_SECONDS`), and at
shutdown (`close_all`, an `on_shutdown` hook), well inside the bounded drain.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urljoin, urlsplit

import aiohttp
from aiohttp import web

from petkit_local.app_api.middleware import ApiError
from petkit_local.media.go2rtc import API_ADDR, stream_name

if TYPE_CHECKING:  # pragma: no cover - typing only
    from petkit_local.devices.base import Device

log = logging.getLogger(__name__)

#: Viewers of one camera at once, both transports together.
MAX_LIVE_PER_DEVICE = 2
#: Viewers of all cameras at once.
MAX_LIVE_TOTAL = 4
#: go2rtc output queued for one client before it counts as too slow. About
#: ten seconds of a 3 Mbit/s stream: far more than a phone on a working link
#: ever holds, and a hard ceiling on what one stalled phone can cost.
CLIENT_BACKLOG_BYTES = 4 * 1024 * 1024
#: How often a running relay re-checks that its phone is still signed in.
SESSION_RECHECK_SECONDS = 2.0
#: WebSocket ping interval towards the phone: a phone that vanished without a
#: close (a dead mobile link) is noticed, and its slot freed, within this.
CLIENT_HEARTBEAT_SECONDS = 20.0
#: A phone sends one small JSON request; nothing bigger is accepted.
CLIENT_MAX_MESSAGE = 4096
#: An HLS viewer that has requested nothing for this long no longer counts.
#: go2rtc forgets the session itself after ~5 s idle (measured on 1.9.14).
HLS_IDLE_SECONDS = 10.0
#: How long go2rtc gets to accept the socket or answer an HLS request.
UPSTREAM_TIMEOUT = 5.0
#: A playlist is a few hundred bytes; this is a ceiling, not an estimate.
MAX_PLAYLIST_BYTES = 256 * 1024
#: One fMP4 fragment from go2rtc; a keyframe of a 1080p stream is well below.
UPSTREAM_MAX_MESSAGE = 16 * 1024 * 1024
_CHUNK = 64 * 1024

#: WebSocket close codes the app can act on (documented in openapi.yaml).
CLOSE_STREAM_ENDED = 1011       # go2rtc closed or failed: retry after a few seconds
CLOSE_SHUTDOWN = 1001           # the server is stopping
CLOSE_TOO_SLOW = 1013           # the backlog filled: the link cannot carry the stream
CLOSE_SESSION_ENDED = 4401      # logout, revocation, password change: sign in again
CLOSE_REPLACED = 4409           # the same phone opened this camera again

#: The files of a go2rtc HLS session this API serves, and the type it gives
#: each. Fixed here rather than copied from go2rtc's reply. `segment.ts` is
#: deliberately absent: go2rtc emits it only for a session opened without
#: `mp4`, which this module never asks for.
HLS_FILES = {
    "playlist.m3u8": "application/vnd.apple.mpegurl",
    "init.mp4": "video/mp4",
    "segment.m4s": "video/iso.segment",
}
_HLS_ID = re.compile(r"[A-Za-z0-9]{1,64}")
_DIGITS = re.compile(r"[0-9]{1,10}")
#: Codec strings, as an MSE `isTypeSupported` list: `avc1.640029,mp4a.40.2,...`.
_CODECS = re.compile(r"[A-Za-z0-9.,_ -]{0,512}")


def device_stream(device: Device, go2rtc: Any) -> str | None:
    """go2rtc's stream name for `device`, or None when there is none to watch.

    Only a camera that go2rtc is configured to serve right now has one — the
    same set `Go2rtc.reconcile` renders into go2rtc's config, so a device the
    probe found no stream on, or one with no address, has none.
    """
    if go2rtc is None or not device.is_camera:
        return None
    name = stream_name(device)
    return name if name in go2rtc.desired_streams() else None


def mse_request(text: str) -> str | None:
    """The `mse` request to forward for a client's text frame, or None.

    Re-serialised from the parsed value, so nothing of the client's frame
    reaches go2rtc but a codec list of plain characters.
    """
    try:
        msg = json.loads(text)
    except ValueError:
        return None
    if not isinstance(msg, dict) or msg.get("type") != "mse":
        return None
    value = msg.get("value", "")
    if not isinstance(value, str) or not _CODECS.fullmatch(value):
        return None
    return json.dumps({"type": "mse", "value": value})


def _error(text: str) -> str:
    """A refusal in go2rtc's own shape, so a go2rtc player handles it as one."""
    return json.dumps({"type": "error", "value": text})


#: What the phone is told when go2rtc reports an error. go2rtc's own text is
#: ffmpeg's stderr — internal addresses, the camera's LAN address, the
#: producer chain — and this socket faces the internet, so it goes to the log.
STREAM_ERROR = ("the camera stream could not be opened; try again in a few seconds "
                "(a camera accepts a new viewer only some seconds after the last one left)")


def from_go2rtc_text(text: str, device_id: int) -> str:
    """go2rtc's text frame as the phone gets it: `mse` verbatim, anything else generic."""
    try:
        msg = json.loads(text)
    except ValueError:
        msg = None
    if isinstance(msg, dict) and msg.get("type") == "mse" and isinstance(msg.get("value"), str):
        return text
    log.info("live: go2rtc reported for device %s: %s", device_id, text[:500])
    return _error(STREAM_ERROR)


@dataclass(eq=False)
class _Slot:
    """One viewer: a phone watching a camera, over one transport."""

    session_id: str
    device_id: int
    kind: str                       # "ws" or "hls"
    expires: float = float("inf")   # HLS only: idle deadline
    hls_id: str = ""                # HLS only: go2rtc's session id
    close_code: int = 0
    close_reason: str = ""
    stopped: asyncio.Event = field(default_factory=asyncio.Event)

    def stop(self, code: int, reason: str) -> None:
        """Ask the relay to end with `code`. The first reason given wins."""
        if not self.stopped.is_set():
            self.close_code, self.close_reason = code, reason
            self.stopped.set()


class _Backlog:
    """go2rtc's messages waiting for a slow client, bounded in bytes."""

    def __init__(self, limit: int) -> None:
        self._items: deque[str | bytes] = deque()
        self._size = 0
        self._limit = limit
        self._ready = asyncio.Event()

    def put(self, item: str | bytes) -> bool:
        """Queue `item`; False when that would pass the limit.

        An empty backlog takes any one message, so a single large fragment is
        never mistaken for a slow client.
        """
        size = len(item)
        if self._items and self._size + size > self._limit:
            return False
        self._items.append(item)
        self._size += size
        self._ready.set()
        return True

    async def get(self) -> str | bytes:
        while not self._items:
            self._ready.clear()
            await self._ready.wait()
        item = self._items.popleft()
        self._size -= len(item)
        return item


class VideoSessions:
    """The live viewers, their caps, and the relays they run.

    One per app API. Admission is synchronous — no await between counting the
    slots and taking one — so two requests racing for the last slot cannot
    both get it.
    """

    def __init__(self, *, go2rtc_api: str = API_ADDR,
                 per_device: int = MAX_LIVE_PER_DEVICE, total: int = MAX_LIVE_TOTAL,
                 backlog_bytes: int = CLIENT_BACKLOG_BYTES,
                 recheck_seconds: float = SESSION_RECHECK_SECONDS,
                 hls_idle_seconds: float = HLS_IDLE_SECONDS,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.go2rtc_api = go2rtc_api
        self.per_device = per_device
        self.total = total
        self.backlog_bytes = backlog_bytes
        self.recheck_seconds = recheck_seconds
        self.hls_idle_seconds = hls_idle_seconds
        self._clock = clock
        self._slots: dict[tuple[str, int], _Slot] = {}

    # --- slots ----------------------------------------------------------

    def _prune(self) -> None:
        now = self._clock()
        for key, slot in list(self._slots.items()):
            if slot.kind == "hls" and slot.expires <= now:
                del self._slots[key]

    def viewers(self, device_id: int | None = None) -> int:
        """Live viewers, of one camera or of all."""
        self._prune()
        return sum(1 for s in self._slots.values()
                   if device_id is None or s.device_id == device_id)

    def admit(self, session_id: str, device_id: int, kind: str) -> _Slot:
        """Take a viewer slot, replacing this phone's earlier view of this camera.

        Raises:
            ApiError: 429 `live_limit_reached` when the camera or the server
                is at its cap.
        """
        self._prune()
        key = (session_id, device_id)
        previous = self._slots.pop(key, None)
        if previous is not None:
            previous.stop(CLOSE_REPLACED, "replaced by a newer live view")
        if self.viewers(device_id) >= self.per_device:
            reason = f"this camera already has {self.per_device} live viewers"
        elif self.viewers() >= self.total:
            reason = f"the server already has {self.total} live viewers"
        else:
            slot = _Slot(session_id, device_id, kind,
                         expires=self._clock() + self.hls_idle_seconds
                         if kind == "hls" else float("inf"))
            self._slots[key] = slot
            return slot
        raise ApiError(429, "live_limit_reached",
                       f"{reason}; close one and try again", {"Retry-After": "5"})

    def release(self, slot: _Slot) -> None:
        """Give `slot` back, unless it was already replaced."""
        key = (slot.session_id, slot.device_id)
        if self._slots.get(key) is slot:
            del self._slots[key]

    def hls_slot(self, session_id: str, device_id: int, hls_id: str) -> _Slot | None:
        """The live HLS slot `hls_id` belongs to, for this phone and camera only.

        Each request to it moves its idle deadline.
        """
        self._prune()
        slot = self._slots.get((session_id, device_id))
        if slot is None or slot.kind != "hls" or not hls_id or slot.hls_id != hls_id:
            return None
        slot.expires = self._clock() + self.hls_idle_seconds
        return slot

    async def close_all(self, _app: web.Application | None = None) -> None:
        """Shutdown hook: end every relay now, so none holds the drain."""
        for slot in list(self._slots.values()):
            slot.stop(CLOSE_SHUTDOWN, "server shutting down")
        self._slots.clear()

    # --- the WebSocket relay -------------------------------------------

    async def relay(self, slot: _Slot, request: web.Request, client: web.WebSocketResponse,
                    upstream: aiohttp.ClientWebSocketResponse,
                    signed_in: Callable[[], bool]) -> None:
        """Pump go2rtc <-> phone until something ends it. Never raises."""
        backlog = _Backlog(self.backlog_bytes)

        async def from_go2rtc() -> None:
            code, reason = CLOSE_STREAM_ENDED, "the stream ended"
            try:
                async for msg in upstream:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        ok = backlog.put(from_go2rtc_text(msg.data, slot.device_id))
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        ok = backlog.put(msg.data)
                    else:
                        break
                    if not ok:
                        code, reason = CLOSE_TOO_SLOW, "client too slow for the stream"
                        break
            finally:
                slot.stop(code, reason)

        async def to_client() -> None:
            try:
                while True:
                    item = await backlog.get()
                    if isinstance(item, bytes):
                        await client.send_bytes(item)
                    else:
                        await client.send_str(item)
            finally:
                slot.stop(CLOSE_STREAM_ENDED, "the connection to the phone failed")

        async def from_client() -> None:
            requested = False
            try:
                async for msg in client:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue  # binary has no meaning here; pings are aiohttp's
                    request_text = None if requested else mse_request(msg.data)
                    if request_text is None:
                        backlog.put(_error(
                            "only one {\"type\":\"mse\",\"value\":\"<codecs>\"} request "
                            "is accepted per connection"))
                        continue
                    requested = True
                    await upstream.send_str(request_text)
            finally:
                slot.stop(1000, "closed by the phone")

        async def watch_session() -> None:
            while True:
                await asyncio.sleep(self.recheck_seconds)
                if not signed_in():
                    slot.stop(CLOSE_SESSION_ENDED, "session ended; sign in again")
                    return

        tasks = [asyncio.create_task(c) for c in
                 (from_go2rtc(), to_client(), from_client(), watch_session())]
        try:
            await slot.stopped.wait()
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            with contextlib.suppress(Exception):
                await upstream.close()
            await self._close_client(request, client, slot)

    @staticmethod
    async def _close_client(request: web.Request, client: web.WebSocketResponse,
                            slot: _Slot) -> None:
        """Close the phone's socket with the slot's code, bounded in time.

        A client too slow to take the stream is too slow to take a close
        frame as well, so after a second the connection is cut instead.
        """
        try:
            await asyncio.wait_for(
                client.close(code=slot.close_code or 1000,
                             message=slot.close_reason.encode()[:120]), 1.0)
        except (asyncio.TimeoutError, Exception):
            transport = request.transport
            if transport is not None:
                transport.abort()

    # --- go2rtc HTTP (HLS) ---------------------------------------------

    async def hls_open(self, session: aiohttp.ClientSession, name: str) -> tuple[str, str]:
        """Open a go2rtc HLS session on `name`: `(its id, its master playlist)`.

        Raises:
            ApiError: 503 when go2rtc does not answer, 502 when its answer
                is not a master playlist naming one session.
        """
        text = await self._get_playlist(
            session, f"/api/stream.m3u8?src={name}&mp4",
            missing=ApiError(503, "live_unavailable",
                             "go2rtc could not open this stream; try again in a few seconds"))
        hls_id = ""
        for uri in playlist_uris(text):
            parts = urlsplit(urljoin("/api/", uri))
            ids = parse_qs(parts.query).get("id", [])
            if parts.path == "/api/hls/playlist.m3u8" and len(ids) == 1 \
                    and _HLS_ID.fullmatch(ids[0]):
                hls_id = ids[0]
        if not hls_id:
            raise ApiError(502, "bad_upstream", "go2rtc did not open an HLS session")
        return hls_id, text

    async def _get_playlist(self, session: aiohttp.ClientSession, path: str,
                            missing: ApiError | None = None) -> str:
        try:
            async with session.get(f"http://{self.go2rtc_api}{path}",
                                   timeout=aiohttp.ClientTimeout(total=UPSTREAM_TIMEOUT)) as r:
                if r.status == 404:
                    raise missing or ApiError(
                        404, "hls_session_expired",
                        "this HLS session is gone; request playlist.m3u8 "
                        "without an id to start another")
                if r.status != 200:
                    raise ApiError(502, "bad_upstream", f"go2rtc answered {r.status}")
                body = await r.content.read(MAX_PLAYLIST_BYTES + 1)
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError):
            raise ApiError(503, "live_unavailable", "go2rtc did not answer") from None
        if len(body) > MAX_PLAYLIST_BYTES:
            raise ApiError(502, "bad_upstream", "go2rtc's playlist is too large")
        return body.decode("utf-8", "replace")

    async def hls_playlist(self, session: aiohttp.ClientSession, hls_id: str) -> str:
        """go2rtc's media playlist for session `hls_id`, rewritten (`rewrite_playlist`)."""
        text = await self._get_playlist(session, f"/api/hls/playlist.m3u8?id={hls_id}")
        return rewrite_playlist(text, hls_id)

    async def hls_file(self, request: web.Request, session: aiohttp.ClientSession,
                       file: str, hls_id: str, n: str | None) -> web.StreamResponse:
        """Stream one HLS file of session `hls_id` through, chunk by chunk."""
        query = f"id={hls_id}" + (f"&n={n}" if n is not None else "")
        url = f"http://{self.go2rtc_api}/api/hls/{file}?{query}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(
                    total=None, sock_connect=UPSTREAM_TIMEOUT,
                    sock_read=UPSTREAM_TIMEOUT * 2)) as upstream:
                if upstream.status == 404:
                    raise ApiError(404, "hls_session_expired", "no such segment")
                if upstream.status != 200:
                    raise ApiError(502, "bad_upstream", f"go2rtc answered {upstream.status}")
                response = web.StreamResponse(headers={
                    "Content-Type": HLS_FILES[file], "Cache-Control": "no-store"})
                await response.prepare(request)
                with contextlib.suppress(aiohttp.ClientError, ConnectionError,
                                         asyncio.TimeoutError):
                    async for chunk in upstream.content.iter_chunked(_CHUNK):
                        await response.write(chunk)
                    await response.write_eof()
                return response
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError):
            raise ApiError(503, "live_unavailable", "go2rtc did not answer") from None


def playlist_uris(text: str) -> list[str]:
    """Every URI an m3u8 playlist references: its URI lines and `URI="..."` attributes."""
    uris = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            uris += re.findall(r'URI="([^"]*)"', line)
        else:
            uris.append(line)
    return uris


def _local_uri(uri: str, hls_id: str) -> str | None:
    """`uri` (relative to go2rtc's `/api/hls/`) as a URI under our route, or None.

    Only a file of `HLS_FILES` of THIS session, with nothing in its query but
    the session id and a segment number, survives — rebuilt from those two
    values, so what the phone is told to fetch is relative to the route it
    fetched the playlist from and cannot point anywhere else.
    """
    parts = urlsplit(urljoin("/api/hls/", uri))
    if parts.scheme or parts.netloc or not parts.path.startswith("/api/hls/"):
        return None
    file = parts.path.removeprefix("/api/hls/")
    if file not in HLS_FILES or file == "playlist.m3u8":
        return None
    query = parse_qs(parts.query, keep_blank_values=True)
    if set(query) - {"id", "n"} or query.get("id") != [hls_id]:
        return None
    if file == "segment.m4s":
        n = query.get("n", [])
        if len(n) != 1 or not _DIGITS.fullmatch(n[0]):
            return None
        return f"{file}?id={hls_id}&n={n[0]}"
    return None if "n" in query else f"{file}?id={hls_id}"


def rewrite_playlist(text: str, hls_id: str) -> str:
    """go2rtc's media playlist, every URI rewritten to stay under our route.

    Fails closed: a URI that is not a file of this session is a 502, never
    passed through.

    Raises:
        ApiError: 502 `bad_upstream` on a URI this API would not serve.
    """
    out = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            local = _local_uri(stripped, hls_id)
            if local is None:
                raise ApiError(502, "bad_upstream", "go2rtc's playlist names a foreign URI")
            out.append(local)
            continue

        def attr(match: re.Match[str]) -> str:
            local = _local_uri(match.group(1), hls_id)
            if local is None:
                raise ApiError(502, "bad_upstream", "go2rtc's playlist names a foreign URI")
            return f'URI="{local}"'
        out.append(re.sub(r'URI="([^"]*)"', attr, stripped))
    return "\n".join(out) + "\n"


def master_playlist(text: str, hls_id: str) -> str:
    """go2rtc's master playlist with its one variant pointed at our route."""
    out = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            out.append(f"playlist.m3u8?id={hls_id}")
        elif stripped and "URI=" not in stripped:
            out.append(stripped)
    return "\n".join(out) + "\n"


def valid_hls_id(value: str | None) -> bool:
    return bool(value) and bool(_HLS_ID.fullmatch(value or ""))


def valid_segment_number(value: str | None) -> bool:
    return bool(value) and bool(_DIGITS.fullmatch(value or ""))
