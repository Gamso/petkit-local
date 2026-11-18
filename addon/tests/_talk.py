"""Stand-ins for what a talk reaches: the device's sink, and ffmpeg.

Shared by `test_talk.py` (the shared path and the panel) and
`test_app_api_talk.py` (the phone's socket). A module rather than
`conftest.py` for the reason `_fakes.py` gives.

The sink is a local TCP server recording each connection's bytes. ffmpeg is a
few lines of Python run by this interpreter: one passes its input through, so
what reaches the sink is exactly what was sent; one fails the way ffmpeg does.
Neither needs ffmpeg or a device.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import aiohttp

from petkit_local.devices.registry import DeviceRegistry

#: ffmpeg's stand-in: copies stdin to stdout until stdin ends.
PASSTHROUGH = """
import sys
i, o = sys.stdin.buffer, sys.stdout.buffer
while True:
    b = i.read1(65536)
    if not b:
        break
    o.write(b)
    o.flush()
"""
#: ffmpeg's stand-in failing on its input, the way ffmpeg reports it.
FAILING = """
import sys
sys.stdin.buffer.read1(1)
sys.stderr.write("[in#0 @ 0x55] Error opening input: Invalid data found when processing input\\n")
sys.stderr.flush()
sys.exit(1)
"""

HAVE_FFMPEG = shutil.which("ffmpeg") is not None


class FakeSink:
    """The device's talk sink: one record of bytes per connection."""

    def __init__(self, *, hang_up: bool = False) -> None:
        self.hang_up = hang_up
        self.received: list[bytearray] = []
        self.ended = 0
        self.server: asyncio.Server | None = None
        self.port = 0

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        self.received.append(buf)
        try:
            while not self.hang_up:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                buf += chunk
        except ConnectionError:
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            self.ended += 1

    def payloads(self) -> list[bytes]:
        return [bytes(b) for b in self.received]

    async def close(self) -> None:
        assert self.server is not None
        self.server.close()
        self.server.close_clients()
        await self.server.wait_closed()


def script(tmp: Path, name: str, body: str) -> tuple[str, ...]:
    """A command running `body` with this interpreter: ffmpeg's stand-in."""
    path = tmp / name
    path.write_text(body, encoding="utf-8")
    return (sys.executable, str(path))


def registry() -> DeviceRegistry:
    """Four devices, one of each availability: 100 can talk (its sink on
    127.0.0.1), 101 lacks the patcher, 102 its address, 200 is no camera."""
    reg = DeviceRegistry()
    cam = reg.get_or_create(petkit_id=100, device_type="t5", serial_number="SN100")
    cam.config["active_patchers"] = ["camera", "talk"]
    cam.state["ip"] = "127.0.0.1"
    bare = reg.get_or_create(petkit_id=101, device_type="t5", serial_number="SN101")
    bare.state["ip"] = "127.0.0.1"
    silent = reg.get_or_create(petkit_id=102, device_type="d4sh", serial_number="SN102")
    silent.config["active_patchers"] = ["talk"]
    reg.get_or_create(petkit_id=200, device_type="d4", serial_number="SN200")
    return reg


async def until(predicate, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition never became true"
        await asyncio.sleep(0.01)


async def next_json(ws, timeout: float = 10.0) -> dict[str, Any]:
    async with asyncio.timeout(timeout):
        return json.loads(await ws.receive_str())


async def closed_code(ws, timeout: float = 10.0) -> int | None:
    """Read until the socket closes; its close code."""
    async with asyncio.timeout(timeout):
        while True:
            msg = await ws.receive()
            if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                return ws.close_code


async def finished(talks: Any, sink: FakeSink, device: int = 100) -> None:
    """Every talk resource of `device` is gone: its slot, and the sink's connection."""
    await until(lambda: talks.active(device) is None)
    await until(lambda: sink.ended == len(sink.received))


async def webm_opus() -> bytes | None:
    """A second of webm/opus, as the panel's MediaRecorder sends; None if
    this ffmpeg cannot make it."""
    made = await asyncio.create_subprocess_exec(
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
        "-i", "sine=frequency=440:sample_rate=48000:duration=1", "-c:a", "libopus",
        "-f", "webm", "pipe:1", stdout=asyncio.subprocess.PIPE)
    out, _ = await made.communicate()
    return out if made.returncode == 0 and out else None


def assert_adts_16k_mono(out: bytes) -> None:
    """`out` starts with an ADTS frame header of 16 kHz mono AAC."""
    assert len(out) > 1000
    assert out[0] == 0xFF and out[1] & 0xF6 == 0xF0     # sync word, layer 0
    assert (out[2] >> 2) & 0x0F == 8                     # sampling index: 16000 Hz
    assert ((out[2] & 0x01) << 2) | (out[3] >> 6) == 1   # one channel
