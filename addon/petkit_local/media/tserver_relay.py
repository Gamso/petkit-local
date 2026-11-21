"""A loopback relay between go2rtc and a tserver that must never see a FIN.

A Dual-Hopper 2's tserver (`patchers/camera.py::RELAYED_MODELS`, issue #122)
serves one client at a time and never closes its side when that client leaves
normally: the socket sits in CLOSE_WAIT and every later connection waits behind
it for ~15-20 minutes. A client that leaves with a TCP RESET frees it. go2rtc
cannot be told to reset (Go's `net/http` closes with a FIN), and neither can
`aiohttp`, so for those models go2rtc's source is this relay instead of the
device, and this relay is the only thing that ever dials the device:

* **Every device connection is closed with a reset.** The socket is created by
  hand with `SO_LINGER` on and a linger of 0 BEFORE it connects, and is driven
  with `loop.sock_*` rather than handed to an asyncio transport, so the close
  is `socket.close()` and nothing else: no transport that might `shutdown()`
  first (the Windows proactor transport does) and send the FIN we exist to
  avoid. A linger of 0 makes that close abortive — the kernel answers with RST.
* **A silent device is retried.** Measured: after a reset, a connection
  sometimes gets no answer at all within 8-10 s, and the next one works. So
  each attempt has `HEADER_TIMEOUT` to produce a response header (connect
  included), a failed one is reset, and there are `ATTEMPTS` in all. go2rtc
  waits: its HTTP client sets no response-header timeout (go2rtc 1.9.14,
  `pkg/tcp/request.go`).
* **One device connection per device, ever.** A second live request for a
  device the relay is already streaming is refused with 503 rather than opening
  a second connection — tserver would only queue it, and queueing is the
  lock-up. go2rtc opens one producer per stream however many viewers attach,
  so this refuses nothing in normal use. The availability probe takes the same
  slot (`probing`), so it never races a viewer; a viewer arriving mid-probe
  waits for it (bounded by `CLAIM_WAIT`) instead of being refused.
* **What go2rtc reads is exactly what the device sent** — the response header
  and the FLV after it, byte for byte, so go2rtc sees the stream it read when
  it dialled the device itself. The request the device gets is the one
  go2rtc's Go client sends (`device_request`).
* **A stream that stops is resumed, not ended** (issue #128). The camera
  pipeline restarts every ~82 s and tserver never re-attaches an open
  connection to it, while a new connection works. So when the device sends
  nothing for `STALL_SECONDS` the relay resets it, reopens it with the same
  retries, and splices the new FLV onto the one go2rtc is reading
  (`FlvSplicer`): the new file header goes, and every tag's timestamp is
  shifted to carry on from the last one sent. The first connection still
  passes through untouched; only a reopened one is rewritten. A reopen that
  fails, a stream the parser cannot follow, or more than `MAX_RESUMES` in
  `RESUME_WINDOW` ends the stream as before.

The relay listens on loopback only and dials nothing but a known device of a
relayed model, at the IPv4 address it reported and the stream path we chose —
never anything taken from the request but the device id.
"""
from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import logging
import re
import socket
import struct
import sys
from collections import deque
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from petkit_local.patchers.camera import is_relayed, main_stream_path

if TYPE_CHECKING:  # pragma: no cover - typing only
    from petkit_local.devices.base import Device
    from petkit_local.devices.registry import DeviceRegistry

log = logging.getLogger(__name__)

#: Where the relay listens: loopback, because go2rtc is its only client and
#: runs in this container. Next to go2rtc's own API port, and not one go2rtc,
#: the panel or any published mapping uses.
RELAY_HOST = "127.0.0.1"
RELAY_PORT = 1985
#: tserver's port (the camera patcher's).
DEVICE_STREAM_PORT = 80
#: Per attempt: connect, send, and receive the whole response header.
HEADER_TIMEOUT = 8.0
#: Attempts per live request, the first included.
ATTEMPTS = 3
#: Pause after an attempt that failed fast (refused, or closed empty), so the
#: next one is not just the same instant again.
RETRY_DELAY = 2.0
#: How long a live request waits for a probe to give the device back.
CLAIM_WAIT = 10.0
#: tserver's header is a few lines; past this it is not an HTTP header.
MAX_HEADER_BYTES = 16 * 1024
#: go2rtc's request to us: one line and a few Go defaults.
REQUEST_TIMEOUT = 5.0
#: Silence from the device, while go2rtc is still reading, after which the
#: connection is reset and reopened (issue #128). Measured 2026-11-21 on a
#: Dual-Hopper 2 through this relay: ~27 fps for 5-20 s, then nothing for over
#: a minute with the device's Send-Q at 0 — tserver had nothing to send. Its
#: camera pipeline (`media`) restarts every ~82 s and tserver never re-attaches
#: an open connection afterwards; a new connection works. At 27 fps three
#: seconds is ~80 frames missing, never jitter. Before a connection's first
#: FLV tag the allowance is `header_timeout` instead (a first keyframe may
#: take a GOP).
STALL_SECONDS = 3.0
#: Resumes allowed in `RESUME_WINDOW` seconds; one more ends the stream, so a
#: dead camera is not reopened forever. A healthy one needs one per ~82 s.
MAX_RESUMES = 3
RESUME_WINDOW = 60.0
#: Added after the last timestamp sent to place the first tag of a reopened
#: connection: one frame at 25 fps (~27 was measured).
FRAME_INTERVAL_MS = 40
_CHUNK = 64 * 1024
_TARGET = re.compile(r"/([0-9]{1,20})")

#: `struct linger {on, seconds}` with on=1, seconds=0: close() resets. Two
#: u_shorts on Windows, two ints elsewhere.
_LINGER_RST = struct.pack("HH" if sys.platform == "win32" else "ii", 1, 0)


def device_request(host: str, path: str) -> bytes:
    """The GET go2rtc's Go client sends, which is what tserver has always got."""
    return (f"GET /{path} HTTP/1.1\r\nHost: {host}\r\n"
            "User-Agent: Go-http-client/1.1\r\nAccept-Encoding: gzip\r\n\r\n").encode()


def _ipv4(value: Any) -> bool:
    try:
        return isinstance(value, str) and str(ipaddress.IPv4Address(value)) == value
    except ValueError:
        return False


def split_host_port(addr: str, default: int = DEVICE_STREAM_PORT) -> tuple[str, int] | None:
    """`host` or `host:port` as `(IPv4 host, port)`, or None when it is not one."""
    host, sep, port = (addr or "").partition(":")
    if not _ipv4(host):
        return None
    if not sep:
        return host, default
    if not port.isdigit() or not 0 < int(port) < 65536:
        return None
    return host, int(port)


def reset_socket() -> socket.socket:
    """A non-blocking TCP socket whose `close()` sends RST, never FIN."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setblocking(False)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, _LINGER_RST)
    except OSError:
        sock.close()
        raise
    return sock


async def open_stream(host: str, port: int, path: str,
                      timeout: float) -> tuple[socket.socket, bytes] | None:
    """Dial tserver, send the GET, and read up to the end of its header.

    Returns the socket (still open, linger 0) and everything read so far — the
    header and whatever of the body arrived with it — or None, the socket then
    already reset. Never raises: refused, timed out, closed empty and garbage
    are all None.
    """
    loop = asyncio.get_running_loop()
    try:
        sock = reset_socket()
    except OSError as e:
        log.info("tserver relay: no socket for %s: %s", host, e)
        return None
    buf = b""
    answered = False
    try:
        async with asyncio.timeout(timeout):
            await loop.sock_connect(sock, (host, port))
            await loop.sock_sendall(sock, device_request(host, path))
            while b"\r\n\r\n" not in buf and len(buf) < MAX_HEADER_BYTES:
                chunk = await loop.sock_recv(sock, _CHUNK)
                if not chunk:
                    break
                buf += chunk
    except (OSError, TimeoutError):
        pass
    else:
        answered = b"\r\n\r\n" in buf or len(buf) >= MAX_HEADER_BYTES
    finally:
        # Cancellation included: whatever ends this without an answer resets
        # the socket here, rather than leaving it to the garbage collector.
        if not answered:
            sock.close()
    return (sock, buf) if answered else None


def response_status(head: bytes) -> int:
    """The status code of a response's first line, 0 when there is none."""
    parts = head.split(b"\r\n", 1)[0].split()
    if len(parts) >= 2 and parts[0].startswith(b"HTTP/") and parts[1].isdigit():
        return int(parts[1])
    return 0


async def probe_with_reset(addr: str, path: str, signature: bytes, timeout: float,
                           attempts: int | None = None,
                           retry_delay: float | None = None) -> bool:
    """Whether `addr` answers `path` with 200 and a body starting `signature`.

    The relayed models' probe: same question as `go2rtc.probe_stream`, but the
    connection is reset when it is done, because a FIN is what locks their
    tserver — and a device that gives no response is tried again, up to
    `attempts` times with `timeout` each, as the relay does: one silent
    attempt after a reset is normal there, and a single "no" would withdraw
    the camera for a minute. A response that is not the stream is final.
    Never raises.
    """
    target = split_host_port(addr)
    if target is None:
        return False
    attempts = ATTEMPTS if attempts is None else attempts
    retry_delay = RETRY_DELAY if retry_delay is None else retry_delay
    loop = asyncio.get_running_loop()
    for attempt in range(1, attempts + 1):
        started = loop.time()
        verdict = await _probe_once(target, path, signature, timeout)
        if verdict is not None:
            return verdict
        if attempt < attempts and loop.time() - started < retry_delay:
            await asyncio.sleep(retry_delay)
    return False


async def _probe_once(target: tuple[str, int], path: str, signature: bytes,
                      timeout: float) -> bool | None:
    """One reset probe: True/False on a response, None on no response."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    opened = await open_stream(*target, path, timeout)
    if opened is None:
        return None
    sock, buf = opened
    try:
        if response_status(buf) != 200:
            return False
        body = buf.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in buf else b""
        async with asyncio.timeout_at(deadline):
            while len(body) < len(signature):
                chunk = await loop.sock_recv(sock, _CHUNK)
                if not chunk:
                    return False
                body += chunk
        return body[:len(signature)] == signature
    except (OSError, TimeoutError) as e:
        log.debug("tserver relay: probe of %s found no stream: %s", target[0], e)
        return False
    finally:
        sock.close()


#: FLV framing (Adobe FLV spec 10.1, annex E): a 9-byte file header (`FLV`,
#: version, flags, u32 header size), then per tag a u32 PreviousTagSize and an
#: 11-byte header — type, u24 data size, u24 timestamp, u8 timestamp bits
#: 31-24, u24 stream id — then the data. go2rtc reads it the same way: 4+11
#: bytes, then the data (go2rtc 1.9.14 `pkg/flv/producer.go::readPacket`).
_FLV_HEADER = 9
_TAG_PREFIX = 4 + 11
_TAG_TYPES = frozenset({8, 9, 18})  # audio, video, script
#: A file header longer than this is not tserver's.
_MAX_HEADER_EXTRA = 64 * 1024
_HEADER, _SKIP, _PREFIX, _DATA = range(4)


class FlvSplicer:
    """Follows the FLV the relay forwards, and splices a reopened one onto it.

    Fed every body byte the device sends. On the first connection `feed`
    returns its input unchanged — the same object — and only walks the tags,
    so it knows where they begin and the last timestamp sent. After `restart`
    (a new connection to the device) it rewrites: the new file header is
    dropped, the new `PreviousTagSize0` becomes the size of the last tag
    go2rtc got (or nothing, if that size already went out), and each tag's
    timestamp gets `offset` added — `last_ts + FRAME_INTERVAL_MS` minus the
    first new tag's — so time keeps increasing across the seam. Script tags
    and sequence headers are rebased and passed like any other: go2rtc 1.9.14
    reads codecs only while probing, and in `Producer.Start` skips a repeated
    AVC sequence header, AAC header or script tag (`continue` / no case), so
    a repeat is harmless; and a stall during go2rtc's probe still needs them.

    Never raises. Bytes it cannot follow set `broken`: on the first connection
    they still pass (only a resume becomes impossible); on a reopened one
    nothing more is returned, and the relay ends the stream.
    """

    def __init__(self, frame_ms: int = FRAME_INTERVAL_MS) -> None:
        self.frame_ms = frame_ms
        #: Why the stream could not be followed; None while it can.
        self.broken: str | None = None
        #: The highest timestamp go2rtc has been sent, in ms; None before a tag.
        self.last_ts: int | None = None
        #: This reopened connection's shift, in ms; None before its first tag
        #: (and always on the first connection).
        self.offset: int | None = None
        #: Tags read on the current connection.
        self.tags = 0
        self.rewriting = False
        self._state = _HEADER
        self._buf = bytearray()
        self._left = 0          # data or header-extra bytes still to go
        self._last_size: int | None = None
        # Where go2rtc's stream stands at the seam: how many bytes of the last
        # tag's PreviousTagSize it already has, and that size's value.
        self._seam_sent = 0
        self._seam_size = 0
        self._seam_open = False  # restarted, and no tag sent since

    def splice_blocker(self) -> str | None:
        """Why the stream go2rtc has cannot take a new connection now, or None.

        Only a tag boundary takes one: stopped inside a tag (or the first
        connection's file header), the next bytes go2rtc reads would be
        another tag's.
        """
        if self.broken is not None:
            return f"its FLV could not be followed ({self.broken})"
        if self._seam_open or (self._state == _PREFIX
                               and (self.rewriting or len(self._buf) <= 4)):
            return None
        return "it stopped inside a tag"

    def restart(self) -> None:
        """Expect a new connection's FLV, from its file header, and splice it
        on. Only valid when `splice_blocker()` is None."""
        if not self._seam_open:
            # On the first connection the buffered prefix bytes already went
            # out; on a reopened one they are held until the prefix completes.
            self._seam_sent = 0 if self.rewriting else len(self._buf)
            self._seam_size = 0 if self._last_size is None else 11 + self._last_size
            self._seam_open = True
        self.rewriting = True
        self.offset = None
        self.tags = 0
        self._state = _HEADER
        self._buf.clear()
        self._left = 0

    def feed(self, data: bytes) -> bytes:
        """The bytes to send go2rtc for `data` the device sent."""
        if self.broken is not None:
            return b"" if self.rewriting else data
        out = bytearray() if self.rewriting else None
        view = memoryview(data)
        i, n = 0, len(data)
        while i < n and self.broken is None:
            if self._state in (_DATA, _SKIP):
                take = min(self._left, n - i)
                if out is not None and self._state == _DATA:
                    out += view[i:i + take]
                self._left -= take
                i += take
                if not self._left:
                    self._state = _PREFIX
                continue
            unit = _FLV_HEADER if self._state == _HEADER else _TAG_PREFIX
            take = min(unit - len(self._buf), n - i)
            self._buf += view[i:i + take]
            i += take
            if len(self._buf) < unit:
                break
            if self._state == _HEADER:
                self._file_header()
            else:
                self._tag(out)
            self._buf.clear()
        if out is None:
            return data
        return b"" if self.broken is not None else bytes(out)

    def _file_header(self) -> None:
        head = self._buf
        size = int.from_bytes(head[5:9], "big")
        if head[:3] != b"FLV" or not _FLV_HEADER <= size <= _FLV_HEADER + _MAX_HEADER_EXTRA:
            self.broken = f"no FLV file header: {bytes(head[:9])!r}"
            return
        self._left = size - _FLV_HEADER
        self._state = _SKIP if self._left else _PREFIX

    def _tag(self, out: bytearray | None) -> None:
        prefix = self._buf
        kind, size = prefix[4], int.from_bytes(prefix[5:8], "big")
        if kind not in _TAG_TYPES:
            self.broken = f"tag type {kind} after {self.tags} tags"
            return
        ts = int.from_bytes(prefix[8:11], "big") | prefix[11] << 24
        if out is not None:
            if self.offset is None:
                target = ts if self.last_ts is None else self.last_ts + self.frame_ms
                self.offset = target - ts
            ts = min(max(ts + self.offset, 0), 0xFFFFFFFF)
            prefix[8:11] = (ts & 0xFFFFFF).to_bytes(3, "big")
            prefix[11] = ts >> 24
            if self._seam_open:
                out += self._seam_size.to_bytes(4, "big")[self._seam_sent:]
                out += prefix[4:]
                self._seam_open = False
            else:
                out += prefix
        self.last_ts = ts if self.last_ts is None else max(self.last_ts, ts)
        self.tags += 1
        self._last_size = size
        self._left = size
        self._state = _DATA if size else _PREFIX


class _StreamEnd(Exception):
    """The relay ends go2rtc's stream; the message says why."""


def _reply(code: int, reason: str, text: str) -> bytes:
    body = text.encode()
    return (f"HTTP/1.1 {code} {reason}\r\nContent-Type: text/plain\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body


class TserverRelay:
    """The loopback HTTP server go2rtc reads relayed models' streams from."""

    def __init__(self, registry: DeviceRegistry, *, host: str = RELAY_HOST,
                 port: int = RELAY_PORT, device_port: int = DEVICE_STREAM_PORT,
                 header_timeout: float = HEADER_TIMEOUT, attempts: int = ATTEMPTS,
                 retry_delay: float = RETRY_DELAY, claim_wait: float = CLAIM_WAIT,
                 stall_seconds: float = STALL_SECONDS, max_resumes: int = MAX_RESUMES,
                 resume_window: float = RESUME_WINDOW) -> None:
        self._registry = registry
        self.host = host
        self.port = port
        self.device_port = device_port
        self.header_timeout = header_timeout
        self.attempts = attempts
        self.retry_delay = retry_delay
        self.claim_wait = claim_wait
        self.stall_seconds = stall_seconds
        self.max_resumes = max_resumes
        self.resume_window = resume_window
        self._server: asyncio.Server | None = None
        self._handlers: set[asyncio.Task[Any]] = set()
        #: petkit_id -> "live" or "probe": who holds the device's one connection.
        self._busy: dict[int, str] = {}
        self._idle: dict[int, asyncio.Event] = {}

    @property
    def running(self) -> bool:
        return self._server is not None

    def url_for(self, device: Device) -> str:
        """go2rtc's source for `device`. Fixed per device for the relay's life,
        so neither an address change nor a go2rtc restart changes it."""
        return f"http://{self.host}:{self.port}/{device.petkit_id}"

    def busy(self, device: Device) -> bool:
        """Whether the device's one connection is held right now."""
        return device.petkit_id in self._busy

    # --- the one-connection guard ----------------------------------------

    def _try_claim(self, pid: int, kind: str) -> bool:
        if pid in self._busy:
            return False
        self._busy[pid] = kind
        self._idle.setdefault(pid, asyncio.Event()).clear()
        return True

    def _release(self, pid: int) -> None:
        self._busy.pop(pid, None)
        event = self._idle.get(pid)
        if event is not None:
            event.set()

    async def _claim_live(self, pid: int) -> bool:
        """Take the device for a viewer: at once, or after a probe finishes."""
        if self._try_claim(pid, "live"):
            return True
        if self._busy.get(pid) != "probe":
            return False
        try:
            await asyncio.wait_for(self._idle[pid].wait(), self.claim_wait)
        except TimeoutError:
            return False
        return self._try_claim(pid, "live")

    @contextlib.contextmanager
    def probing(self, device: Device) -> Iterator[bool]:
        """Hold the device's slot for a probe; yields False when it is taken.

        A viewer holding it is exactly when a probe must not run: it would
        be a second connection, which tserver queues.
        """
        claimed = self._try_claim(device.petkit_id, "probe")
        try:
            yield claimed
        finally:
            if claimed:
                self._release(device.petkit_id)

    # --- lifecycle --------------------------------------------------------

    async def start(self) -> bool:
        """Listen. Never raises: a port that cannot be bound is logged, and
        the caller falls back to dialling the device directly."""
        if self._server is not None:
            return True
        try:
            self._server = await asyncio.start_server(self._handle, self.host, self.port)
        except OSError as e:
            log.warning("tserver relay: cannot listen on %s:%d: %s", self.host, self.port, e)
            return False
        self.port = self._server.sockets[0].getsockname()[1]
        log.info("tserver relay listening on %s:%d", self.host, self.port)
        return True

    async def stop(self) -> None:
        """Stop listening and reset every device connection. Idempotent."""
        server, self._server = self._server, None
        if server is not None:
            server.close()
        for task in list(self._handlers):
            task.cancel()
        for task in list(self._handlers):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if server is not None:
            with contextlib.suppress(TimeoutError, Exception):
                await asyncio.wait_for(server.wait_closed(), 2)

    # --- one request ------------------------------------------------------

    async def _read_target(self, reader: asyncio.StreamReader) -> int | None:
        """The device id go2rtc asked for: `GET /<petkit_id>`, nothing else."""
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), REQUEST_TIMEOUT)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, OSError):
            return None
        parts = head.split(b"\r\n", 1)[0].decode("latin-1").split()
        if len(parts) != 3 or parts[0] != "GET":
            return None
        match = _TARGET.fullmatch(parts[1].split("?", 1)[0])
        return int(match.group(1)) if match else None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._handlers.add(task)
        try:
            await self._serve(reader, writer)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("tserver relay: request failed")
        finally:
            if task is not None:
                self._handlers.discard(task)
            writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(writer.wait_closed(), 1)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        pid = await self._read_target(reader)
        device = self._registry.get(pid) if pid is not None else None
        ip = (device.state or {}).get("ip", "") if device is not None else ""
        if device is None or not is_relayed(device) or not _ipv4(ip):
            writer.write(_reply(404, "Not Found", "no relayed camera by that id"))
            return
        if not await self._claim_live(pid):
            log.info("tserver relay: device %s is already streaming; refusing a second "
                     "connection", pid)
            writer.write(_reply(503, "Service Unavailable", "the camera is already streaming"))
            return
        try:
            path = main_stream_path(device)
            opened = await self._open_device(pid, ip, path)
            if opened is None:
                writer.write(_reply(503, "Service Unavailable", "the camera did not answer"))
                return
            await self._pump(pid, ip, path, *opened, reader, writer)
        finally:
            self._release(pid)

    async def _open_device(self, pid: int, ip: str,
                           path: str) -> tuple[socket.socket, bytes] | None:
        loop = asyncio.get_running_loop()
        for attempt in range(1, self.attempts + 1):
            started = loop.time()
            opened = await open_stream(ip, self.device_port, path, self.header_timeout)
            if opened is not None:
                log.info("tserver relay: device %s streaming %s (attempt %d, status %d)",
                         pid, path, attempt, response_status(opened[1]))
                return opened
            if attempt == self.attempts:
                break
            log.info("tserver relay: device %s gave no response on attempt %d of %d; "
                     "reset it, retrying", pid, attempt, self.attempts)
            # A timeout has already waited; only a fast failure needs a pause.
            if loop.time() - started < self.retry_delay:
                await asyncio.sleep(self.retry_delay)
        log.info("tserver relay: device %s gave no response in %d attempts", pid, self.attempts)
        return None

    async def _pump(self, pid: int, ip: str, path: str, sock: socket.socket, head: bytes,
                    reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Device -> go2rtc until either side ends; then reset the device.

        A device silent for `stall_seconds` while go2rtc still reads is reset
        and reopened, and its new FLV spliced on (`FlvSplicer`, issue #128).
        """
        loop = asyncio.get_running_loop()
        why = "the device closed the stream"
        splicer = FlvSplicer()
        resumes: deque[float] = deque()

        async def reopen() -> bytes:
            """Reset the silent connection and open a new one; its FLV body."""
            nonlocal sock
            blocker = splicer.splice_blocker()
            if blocker is not None:
                raise _StreamEnd(f"the device went silent and {blocker}")
            now = loop.time()
            while resumes and now - resumes[0] >= self.resume_window:
                resumes.popleft()
            if len(resumes) >= self.max_resumes:
                raise _StreamEnd(f"the device went silent again after {len(resumes)} "
                                 f"resumes in {self.resume_window:.0f} s")
            resumes.append(now)
            sock.close()  # linger 0: a reset
            log.info("tserver relay: device %s sent nothing for %.1f s; reset it, reopening",
                     pid, now - last_byte)
            opened = await self._open_device(pid, ip, path)
            if opened is None:
                raise _StreamEnd("the device did not answer a reopen")
            sock, buf = opened
            status = response_status(buf)
            _, sep, body = buf.partition(b"\r\n\r\n")
            if status != 200 or not sep:
                raise _StreamEnd(f"the device answered a reopen with status {status}")
            splicer.restart()
            return body

        async def down() -> None:
            nonlocal last_byte
            writer.write(head)
            await writer.drain()
            _, sep, body = head.partition(b"\r\n\r\n")
            if not sep:
                splicer.broken = "no response header"
            splicer.feed(body)
            resumed: float | None = None  # when the device went silent, until logged
            while True:
                # Before a connection's first tag it may be waiting for a keyframe.
                allowed = (self.stall_seconds if splicer.tags
                           else max(self.stall_seconds, self.header_timeout))
                try:
                    chunk = await asyncio.wait_for(loop.sock_recv(sock, _CHUNK), allowed)
                except TimeoutError:
                    resumed = resumed if resumed is not None else last_byte
                    chunk = await reopen()
                else:
                    if not chunk:
                        return
                last_byte = loop.time()
                out = splicer.feed(chunk)
                if splicer.rewriting and splicer.broken is not None:
                    raise _StreamEnd(f"the reopened stream could not be followed "
                                     f"({splicer.broken})")
                if out:
                    writer.write(out)
                    await writer.drain()
                if resumed is not None and splicer.offset is not None:
                    log.info("tserver relay: device %s resumed after a %.1f s gap; timestamps "
                             "shifted by %+d ms (resume %d in %.0f s)", pid,
                             last_byte - resumed, splicer.offset, len(resumes),
                             self.resume_window)
                    resumed = None

        async def up() -> None:
            # go2rtc sends nothing after its request; EOF means it left.
            while await reader.read(_CHUNK):
                pass

        last_byte = loop.time()
        tasks = {asyncio.create_task(down()): "device", asyncio.create_task(up()): "go2rtc"}
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            first = next(iter(done))
            if tasks[first] == "go2rtc" or first.exception() is not None:
                why = "go2rtc left" if tasks[first] == "go2rtc" else f"{first.exception()}"
        except asyncio.CancelledError:
            why = "the relay is stopping"
            raise
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            sock.close()
            log.info("tserver relay: device %s connection reset (%s)", pid, why)
