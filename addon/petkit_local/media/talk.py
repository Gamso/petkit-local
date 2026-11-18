"""Two-way talk: a microphone's audio, to the speaker of a camera.

The `talk` patcher (`patchers/talk.py`) installs an audio sink on the device: an
`nc` listener on `TALK_TCP_PORT` that pipes each connection into the firmware's
own `pktool play_aac`, which wants 16 kHz mono ADTS-AAC. This module is the
other end of it, for whatever a talk starts from: today the panel,
`/api/devices/{id}/talk` (`web/api/talk.py`), sending the browser's webm/opus,
which ffmpeg probes (`PROBED_INPUT`). `PCM_INPUT` is raw PCM, s16le/16 kHz/mono,
which a phone's `AudioRecord` produces with no encoder at all.

go2rtc's WebRTC backchannel is a third way to the same sink, and does not come
through here: it is go2rtc's own `exec:` producer
(`media/go2rtc.py::backchannel_source`).

A `TalkSession` is one ffmpeg that only TRANSCODES, stdin to stdout, and one
TCP connection to the sink, which is ours. ffmpeg used to dial `tcp://` itself;
the connection moved here for three reasons:

* **The sink is known to be there before anyone talks.** ffmpeg dials only
  once it has encoded its first packet, so a sink that was not listening was
  found out after the audio had started — and the panel then dropped it in
  silence. Now a session that cannot reach the sink never starts, and says so.
* **ffmpeg's stderr holds only decoding errors**, never a LAN address, so its
  reason can be shown to a phone on the internet (`transcoder_failed`).
* **It is one connection.** A separate probe would cost the sink its next
  accept: the device runs `while true; do nc -l ... -e <sink>; sleep 1; done`,
  so a probe that connects and hangs up runs the sink once and leaves the port
  closed for a second, refusing the real connection right behind it.

One session per device at a time, whoever opens it (`Talks.open`, synchronous,
so two racing opens cannot both win): two would be two `pktool play_aac`
feeding one speaker. A second is refused with `busy`; the first is never
disturbed. (The device's `nc` takes one connection at a time anyway, so a
go2rtc backchannel already holding the sink makes a session here fail with
`device_unreachable` rather than play over it.)

Every failure is a `TalkError` with a code from the list below; nothing else
escapes. `TalkSession.end()`, which every path runs, kills ffmpeg, closes the
sink and frees the device — in a task of its own, so that a request handler
cancelled when its client's connection drops cannot cut it short.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import deque
from collections.abc import Coroutine, Sequence
from typing import TYPE_CHECKING, Any

from petkit_local.devices.addresses import as_ipv4
from petkit_local.patchers.common import TALK_TCP_PORT

if TYPE_CHECKING:  # pragma: no cover - typing only
    from petkit_local.devices.base import Device

log = logging.getLogger(__name__)

#: The patcher whose sink this needs (`patchers/talk.py`'s id).
PATCHER_ID = "talk"

# --- availability: what a device's page says before anyone presses Talk -------

AVAILABLE = "available"
NEEDS_PATCHER = "needs_patcher"
DEVICE_UNREACHABLE = "device_unreachable"
UNSUPPORTED = "unsupported"

# --- why a session failed: the `code` a client is told --------------------------

NOT_A_CAMERA = "not_a_camera"
BUSY = "busy"
TRANSCODER_FAILED = "transcoder_failed"
IDLE_TIMEOUT = "idle_timeout"
# NEEDS_PATCHER and DEVICE_UNREACHABLE are codes too, spelt as above.

#: What each availability other than AVAILABLE refuses a session with.
_REFUSALS = {
    UNSUPPORTED: (NOT_A_CAMERA, "this device has no camera speaker to talk through"),
    NEEDS_PATCHER: (NEEDS_PATCHER, "the Two-Way Talk patcher is not applied to this device "
                                   "(petkit-local panel, Patchers tab)"),
    DEVICE_UNREACHABLE: (DEVICE_UNREACHABLE, "the device has not reported its network "
                                             "address yet; wait for it to come online"),
}

#: Raw PCM, signed 16-bit little-endian, 16 kHz, mono.
#: Nothing to probe and nothing to buffer, so none of the flags below — and
#: `-fflags nobuffer` must stay off it: with the raw PCM demuxer, ffmpeg 7.1
#: (Debian's, in the 32-bit ARM image) then encodes NOTHING, exiting 0 with an
#: empty output. `-ch_layout mono` rather than `-ac 1`, which works too but
#: has ffmpeg log a guessed layout on every talk.
PCM_INPUT: tuple[str, ...] = ("-f", "s16le", "-ar", "16000", "-ch_layout", "mono")
#: Anything ffmpeg can probe (the panel's webm/opus), probing as little as a
#: live stream allows so the first audio is not held back — the panel's flags
#: since talk began.
PROBED_INPUT: tuple[str, ...] = ("-fflags", "nobuffer", "-flags", "low_delay",
                                 "-probesize", "32", "-analyzeduration", "0")

#: How long the sink gets to accept the connection.
CONNECT_TIMEOUT = 5.0
#: How long a write to the sink may block before it counts as stalled. This is
#: what ffmpeg's `-rw_timeout 5000000` bounded when ffmpeg owned the socket:
#: the device's `nc` accepts and then never reads when `pktool` is not where
#: the sink script expects it.
WRITE_TIMEOUT = 5.0
#: How long a clean end waits for ffmpeg to flush its last frames to the sink.
FINISH_TIMEOUT = 6.0
#: ffmpeg's last stderr lines kept, for the reason given when it exits.
STDERR_TAIL = 5
#: The longest stderr reason a client is told.
REASON_MAX = 300


class TalkError(Exception):
    """Why a talk session could not start or could not go on."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def sink_ip(device: Device) -> str:
    """The device's address as an IPv4 dotted quad, or `""`.

    The recorded address comes from an unauthenticated report, so only
    something that IS one address is ever dialled (`as_ipv4`).
    """
    return as_ipv4((device.state or {}).get("ip"))


def availability(device: Device) -> str:
    """Whether a talk could start on `device` now — cheap, no network.

    `unsupported` for anything but a camera model the patchers can reach,
    `needs_patcher` until the `talk` patcher is recorded as applied,
    `device_unreachable` while no address is known, else `available`.
    `available` is not a promise: the sink itself is only tried when a
    session opens (`TalkSession.start`), which is where an unapplied-in-fact
    patch or a device that went away is found out.
    """
    if not (device.is_camera and device.is_next_gen):
        return UNSUPPORTED
    if PATCHER_ID not in (device.config.get("active_patchers") or ()):
        return NEEDS_PATCHER
    if not sink_ip(device):
        return DEVICE_UNREACHABLE
    return AVAILABLE


def transcoder_args(input_args: Sequence[str]) -> list[str]:
    """ffmpeg's arguments: `input_args` on stdin -> 16 kHz mono ADTS-AAC on stdout.

    The same output the panel has always sent (48 kbit/s AAC, the speaker's
    format). `-flush_packets 1` hands each frame to stdout as it is encoded
    instead of filling a buffer first: a talk is only as good as its delay.
    """
    return ["-hide_banner", "-nostats", "-loglevel", "warning",
            *input_args, "-i", "pipe:0",
            "-ar", "16000", "-ac", "1", "-c:a", "aac", "-b:a", "48k",
            "-flush_packets", "1", "-f", "adts", "pipe:1"]


class TalkSession:
    """One talk: ffmpeg transcoding into one connection to the device's sink.

    Made by `Talks.open`, which has already given it the device's one slot.
    `start()`, then `feed()` audio, then `end()` — graceful or not — on every
    path. `error` is set (and `failed` fires) the moment anything breaks —
    ffmpeg exiting, the sink closing or stalling — whether or not a `feed()`
    is in progress to notice.
    """

    def __init__(self, talks: Talks, device_id: int, ip: str,
                 input_args: Sequence[str]) -> None:
        self.talks = talks
        self.device_id = device_id
        self.ip = ip
        self.input_args = tuple(input_args)
        self.error: TalkError | None = None
        self.failed = asyncio.Event()
        #: Set by `Talks.close_all`: the server is stopping.
        self.shutdown = False
        self.bytes_in = 0
        self.bytes_out = 0
        self.last_audio = time.monotonic()
        self._proc: asyncio.subprocess.Process | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._stderr_task: asyncio.Task[None] | None = None
        self._out_task: asyncio.Task[None] | None = None
        self._tail: deque[str] = deque(maxlen=STDERR_TAIL)
        self._finishing = False
        self._ending: asyncio.Task[None] | None = None
        self.closed = False

    # --- life cycle -------------------------------------------------------

    async def start(self) -> None:
        """Start ffmpeg, then connect to the sink.

        ffmpeg first: it is local and cheap, and a server without it should
        not have touched the device. On failure what was started is left for
        `close()`, which the caller runs on every path.

        Raises:
            TalkError: `transcoder_failed` when ffmpeg cannot be started,
                `device_unreachable` when the sink does not accept.
        """
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *self.talks.ffmpeg, *transcoder_args(self.input_args),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
        except FileNotFoundError:
            raise TalkError(TRANSCODER_FAILED,
                            "ffmpeg is not installed on the server") from None
        except OSError as exc:
            raise TalkError(TRANSCODER_FAILED,
                            f"the server could not start ffmpeg ({exc.strerror or exc})") from None
        self._stderr_task = self._spawn(self._pump_stderr())
        try:
            reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.talks.port), self.talks.connect_timeout)
        except (OSError, TimeoutError) as exc:
            log.info("talk #%d: sink %s:%d did not accept: %r", self.device_id, self.ip,
                     self.talks.port, exc)
            raise TalkError(DEVICE_UNREACHABLE,
                            f"the device's talk sink did not answer on TCP {TALK_TCP_PORT}: "
                            "the device is off or unreachable, the Two-Way Talk patch is "
                            "not on it any more, or another talk is using it") from None
        self._out_task = self._spawn(self._pump_out())
        self._spawn(self._watch_sink(reader))
        self._spawn(self._watch_exit())
        log.info("talk #%d: session start, sink %s:%d", self.device_id, self.ip, self.talks.port)

    async def feed(self, data: bytes) -> None:
        """Hand a piece of the input stream to ffmpeg, with backpressure.

        Raises:
            TalkError: the session's failure, whenever there is one.
        """
        if self.error is not None:
            raise self.error
        proc = self._proc
        if proc is None or proc.stdin is None or proc.stdin.is_closing() or self._finishing:
            raise TalkError(TRANSCODER_FAILED, "the session is not taking audio")
        self.last_audio = time.monotonic()
        if not data:
            return
        try:
            proc.stdin.write(data)
            # Backpressure: a sink that stalls stalls ffmpeg, and this then
            # waits rather than buffering the client's stream without bound.
            # Longer than the sink's own bound, so that one reports first.
            await asyncio.wait_for(proc.stdin.drain(), self.talks.write_timeout * 2)
        except TimeoutError:
            self._fail(DEVICE_UNREACHABLE, "the device stopped taking the audio")
        except (OSError, RuntimeError):
            # ffmpeg is gone. `_watch_exit` is about to say why, from its
            # stderr; give it a moment rather than reporting a broken pipe.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.failed.wait(), 2.0)
            self._fail(TRANSCODER_FAILED, "ffmpeg stopped reading the audio")
        self.bytes_in += len(data)
        if self.error is not None:
            raise self.error

    def end(self, *, graceful: bool) -> asyncio.Task[None]:
        """End the session in a task of its own, and return that task.

        `graceful` lets the audio already sent reach the device first
        (`finish`); then `close`. The first call decides: a later one gets the
        same task. Await it under `asyncio.shield`: the request handler that
        asks may itself be cancelled when its client's connection drops (an
        `AppRunner` with `handler_cancellation`), and the end of a talk —
        ffmpeg reaped, the sink closed, the device given back — must not be
        cut short by that.
        """
        if self._ending is None:
            self._ending = self.talks._run_detached(self._end(graceful))
        return self._ending

    async def _end(self, graceful: bool) -> None:
        if graceful:
            await self.finish()
        await self.close()

    async def finish(self) -> None:
        """End cleanly: ffmpeg flushes its last frames and they reach the sink.

        Bounded by `FINISH_TIMEOUT`; `close()` kills whatever is left. Never
        raises — a failure on the way is in `error`.
        """
        proc = self._proc
        if proc is None or self._finishing or self.closed:
            return
        self._finishing = True
        if proc.stdin is not None and not proc.stdin.is_closing():
            with contextlib.suppress(Exception):
                proc.stdin.write_eof()
        try:
            await asyncio.wait_for(proc.wait(), FINISH_TIMEOUT)
            if self._out_task is not None:
                await asyncio.wait_for(asyncio.shield(self._out_task), FINISH_TIMEOUT)
        except TimeoutError:
            log.warning("talk #%d: ffmpeg did not finish within %.0fs", self.device_id,
                        FINISH_TIMEOUT)
        except Exception:
            log.debug("talk #%d: finish", self.device_id, exc_info=True)

    async def close(self) -> None:
        """Kill ffmpeg, close the sink, give the device back. Idempotent, never raises.

        The device is given back FIRST and synchronously, so not even a
        cancellation during the awaits below can leave it held.
        """
        if self.closed:
            return
        self.closed = True
        self.talks._release(self)
        proc, writer = self._proc, self._writer
        if proc is not None:
            if proc.stdin is not None:
                with contextlib.suppress(Exception):
                    proc.stdin.close()
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
        if writer is not None:
            writer.close()
        try:
            if proc is not None:
                with contextlib.suppress(TimeoutError, ProcessLookupError):
                    await asyncio.wait_for(proc.wait(), 5.0)
            if writer is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(writer.wait_closed(), 2.0)
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            log.info("talk #%d: session end, %d bytes in, %d to the device, ffmpeg exit %s%s",
                     self.device_id, self.bytes_in, self.bytes_out,
                     proc.returncode if proc is not None else None,
                     f" ({self.error.code}: {self.error.message})" if self.error else "")

    # --- the helpers that run for the life of the session -------------------

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._tasks.append(task)
        return task

    def _fail(self, code: str, message: str) -> None:
        """Record the first failure; later ones are consequences of it."""
        if self.error is None and not self.closed:
            self.error = TalkError(code, message)
            log.warning("talk #%d: %s: %s", self.device_id, code, message)
            self.failed.set()

    async def _pump_stderr(self) -> None:
        """Keep ffmpeg's last lines for the exit report, and log them.

        The pipe must be drained for the life of the process, or ffmpeg
        blocks on it once it fills.
        """
        proc = self._proc
        assert proc is not None and proc.stderr is not None
        while True:
            line = await proc.stderr.readline()
            if not line:
                return
            text = line.decode("utf-8", "replace").strip()
            if text:
                self._tail.append(text)
                log.warning("talk #%d: ffmpeg: %s", self.device_id, text)

    async def _pump_out(self) -> None:
        """Copy ffmpeg's ADTS frames to the sink as they come."""
        proc, writer = self._proc, self._writer
        assert proc is not None and proc.stdout is not None and writer is not None
        while True:
            chunk = await proc.stdout.read(4096)
            if not chunk:
                return
            try:
                writer.write(chunk)
                await asyncio.wait_for(writer.drain(), self.talks.write_timeout)
            except TimeoutError:
                self._fail(DEVICE_UNREACHABLE, "the device stopped taking the audio")
                return
            except (OSError, RuntimeError):
                self._fail(DEVICE_UNREACHABLE, "the device closed the talk connection")
                return
            self.bytes_out += len(chunk)

    async def _watch_sink(self, reader: asyncio.StreamReader) -> None:
        """Notice the device hanging up. The sink never sends anything; this
        reads only to see the end of the stream."""
        with contextlib.suppress(OSError):
            while await reader.read(1024):
                pass
        if not self._finishing:
            self._fail(DEVICE_UNREACHABLE, "the device closed the talk connection")

    async def _watch_exit(self) -> None:
        """Report ffmpeg dying under a live session, with its own last words."""
        proc = self._proc
        assert proc is not None
        rc = await proc.wait()
        if self._finishing:
            return
        if self._stderr_task is not None:
            # Its last lines arrive with the end of the pipe, just after the exit.
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.shield(self._stderr_task), 1.0)
        why = self._tail[-1] if self._tail else "no output"
        self._fail(TRANSCODER_FAILED, f"ffmpeg exited ({rc}): {why}"[:REASON_MAX])


class Talks:
    """Every talk in progress, one per device, and how a session is made.

    One per process (`TALKS`), shared by everything that talks, because a
    device has one speaker whoever is talking to it.
    """

    def __init__(self, *, ffmpeg: Sequence[str] = ("ffmpeg",), port: int = TALK_TCP_PORT,
                 connect_timeout: float = CONNECT_TIMEOUT,
                 write_timeout: float = WRITE_TIMEOUT) -> None:
        self.ffmpeg = tuple(ffmpeg)
        self.port = port
        self.connect_timeout = connect_timeout
        self.write_timeout = write_timeout
        self._active: dict[int, TalkSession] = {}
        # Strong references to the ending tasks (`TalkSession.end`), which
        # nothing else holds while they run.
        self._detached: set[asyncio.Task[None]] = set()

    def _run_detached(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._detached.add(task)
        task.add_done_callback(self._detached.discard)
        return task

    def active(self, device_id: int) -> TalkSession | None:
        """The session talking through `device_id`, if any."""
        return self._active.get(device_id)

    def open(self, device: Device, input_args: Sequence[str]) -> TalkSession:
        """A session for `device`, holding its one slot.

        Synchronous on purpose: nothing awaits between the check and the
        taking, so two opens racing for one device cannot both get it.

        Raises:
            TalkError: `not_a_camera`, `needs_patcher` or `device_unreachable`
                (`availability`), or `busy`.
        """
        status = availability(device)
        if status != AVAILABLE:
            raise TalkError(*_REFUSALS[status])
        if device.petkit_id in self._active:
            raise TalkError(BUSY, "someone is already talking through this device; "
                                  "try again when they stop")
        session = TalkSession(self, device.petkit_id, sink_ip(device), input_args)
        self._active[device.petkit_id] = session
        return session

    def _release(self, session: TalkSession) -> None:
        if self._active.get(session.device_id) is session:
            del self._active[session.device_id]

    async def close_all(self, _app: Any = None) -> None:
        """Shutdown hook: end every talk now, so none holds the bounded drain."""
        sessions = list(self._active.values())
        for session in sessions:
            session.shutdown = True
        await asyncio.gather(*(s.close() for s in sessions), return_exceptions=True)


#: The process's one `Talks`: every session excludes every other one on the
#: same device through it.
TALKS = Talks()
