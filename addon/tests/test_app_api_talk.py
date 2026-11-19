"""Two-way talk from the phone: `/app/v1/devices/{id}/talk` and the `talk` field.

On the fake sink and ffmpeg of `tests/_talk.py`; one test transcodes for real
when ffmpeg is installed. What is held here:

* the socket needs a session like every route, and only the generic refusals
  happen before the upgrade; every talk-specific one is an `error` message
  with its code, then close code 4000;
* a device's `talk` says whether a talk can start, without reaching the device;
* the phone's PCM reaches the sink as ffmpeg's output, and a stop (or a close)
  flushes it before the device is let go;
* one talk per device, from a phone or the panel: a second is `busy` and the
  first is not disturbed;
* an unreachable sink, a missing ffmpeg, ffmpeg exiting, the device hanging
  up and silence are each reported with their code — ffmpeg's with its reason;
* on every path ffmpeg is gone, the sink connection closed and the device
  free: on revocation, at shutdown, and when the phone vanishes mid-stream;
* the limits the OpenAPI file states are the code's.
"""
from __future__ import annotations

import asyncio
import base64
import json
import math
import os
import struct
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, unused_port

from petkit_local.app_api import serializers
from petkit_local.app_api.auth import FILENAME, AppAuth, ScryptParams
from petkit_local.app_api.handlers import talk as talk_handler
from petkit_local.app_api.handlers.meta import OPENAPI_PATH
from petkit_local.app_api.server import PREFIX, create_app_api
from petkit_local.devices.ble import BLEDevice
from petkit_local.media import talk
from petkit_local.media.talk import Talks
from petkit_local.web.api.talk import api_talk
from petkit_local.web.appkeys import REGISTRY as PANEL_REGISTRY
from tests._talk import (
    FAILING, HAVE_FFMPEG, PASSTHROUGH, FakeSink, assert_adts_16k_mono, closed_code, finished,
    next_json, registry, script, until,
)

FAST = ScryptParams(n=2 ** 10, r=8, p=1)
PASSWORD = "correct horse battery"
PCM = bytes(range(256)) * 8   # 2 KiB: what a phone sends in ~64 ms


class Env:
    """An app API over `registry()`, talking to a fake sink through a fake ffmpeg."""

    def __init__(self, tmp: Path, sink: FakeSink) -> None:
        self.sink = sink
        self.auth = AppAuth(tmp / FILENAME, scrypt_params=FAST)
        self.registry = registry()
        self.talks = Talks(ffmpeg=script(tmp, "ffmpeg_pass.py", PASSTHROUGH), port=sink.port,
                           connect_timeout=2.0, write_timeout=2.0)
        self.app = create_app_api(self.auth, self.registry, talks=self.talks)
        self.server = TestServer(self.app)
        self.client: TestClient | None = None

    async def start(self) -> None:
        self.client = TestClient(self.server)
        await self.client.start_server()
        await self.auth.set_password(PASSWORD)

    async def login(self, name: str = "Pixel") -> str:
        r = await self.client.post(f"{PREFIX}/auth/login",
                                   json={"password": PASSWORD, "device_name": name})
        assert r.status == 200
        return (await r.json())["token"]

    async def ws(self, token: str | None, device: int = 100):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return await self.client.ws_connect(f"{PREFIX}/devices/{device}/talk", headers=headers)

    async def finished(self, device: int = 100) -> None:
        await finished(self.talks, self.sink, device)


async def _make(tmp: Path, sink: FakeSink) -> Env:
    await sink.start()
    env = Env(tmp, sink)
    await env.start()
    return env


@pytest.fixture
async def env(tmp_path: Path):
    e = await _make(tmp_path, FakeSink())
    yield e
    await e.client.close()
    await e.sink.close()


# --- what the device page says ------------------------------------------------------

async def test_the_device_list_and_detail_say_whether_talk_is_available(env: Env):
    token = await env.login()
    auth = {"Authorization": f"Bearer {token}"}
    r = await env.client.get(f"{PREFIX}/devices", headers=auth)
    assert {d["id"]: d["talk"] for d in (await r.json())["devices"]} == {
        100: "available", 101: "needs_patcher", 102: "device_unreachable", 200: "unsupported"}
    r = await env.client.get(f"{PREFIX}/devices/100", headers=auth)
    assert (await r.json())["device"]["talk"] == "available"
    # Nothing reached the device to answer that.
    assert env.sink.received == []


def test_an_accessory_has_no_talk():
    accessory = BLEDevice(ble_type="w5", petkit_id=7)
    assert serializers.accessory_summary(accessory, None)["talk"] == "unsupported"


# --- refusals ----------------------------------------------------------------------

async def test_the_upgrade_needs_a_session(env: Env):
    for token in (None, "not-a-token"):
        with pytest.raises(aiohttp.WSServerHandshakeError) as e:
            await env.ws(token)
        assert e.value.status == 401
    assert env.sink.received == []


async def test_only_the_generic_refusals_come_before_the_upgrade(env: Env):
    token = await env.login()
    with pytest.raises(aiohttp.WSServerHandshakeError) as e:
        await env.ws(token, device=999)
    assert e.value.status == 404
    r = await env.client.get(f"{PREFIX}/devices/100/talk",
                             headers={"Authorization": f"Bearer {token}"})
    assert r.status == 426 and (await r.json())["error"] == "upgrade_required"


@pytest.mark.parametrize("device, code", [(200, "not_a_camera"), (101, "needs_patcher"),
                                          (102, "device_unreachable")])
async def test_a_device_that_cannot_talk_is_an_error_on_the_socket(env: Env, device, code):
    token = await env.login()
    ws = await env.ws(token, device=device)
    msg = await next_json(ws)
    assert msg["type"] == "error" and msg["code"] == code and msg["message"]
    assert await closed_code(ws) == talk_handler.CLOSE_ERROR
    assert env.sink.received == [] and env.talks.active(device) is None


async def test_a_sink_that_does_not_listen_is_device_unreachable(env: Env):
    env.talks.port = unused_port()
    token = await env.login()
    ws = await env.ws(token)
    msg = await next_json(ws)
    assert (msg["type"], msg["code"]) == ("error", "device_unreachable")
    assert "127.0.0.1" not in msg["message"]   # a LAN address never reaches the phone
    assert await closed_code(ws) == talk_handler.CLOSE_ERROR
    # Free when the phone hears of it, so a retry is never `busy`.
    assert env.talks.active(100) is None


async def test_a_missing_ffmpeg_is_transcoder_failed_and_the_device_is_never_dialled(env: Env):
    env.talks.ffmpeg = ("petkit-local-no-such-ffmpeg",)
    token = await env.login()
    ws = await env.ws(token)
    msg = await next_json(ws)
    assert (msg["code"], msg["message"]) == ("transcoder_failed",
                                             "ffmpeg is not installed on the server")
    assert await closed_code(ws) == talk_handler.CLOSE_ERROR
    assert env.sink.received == [] and env.talks.active(100) is None


# --- talking -----------------------------------------------------------------------------

async def test_the_audio_reaches_the_sink_and_a_stop_flushes_it(env: Env):
    token = await env.login()
    ws = await env.ws(token)
    assert await next_json(ws) == {"type": "ready", "format": "s16le", "sample_rate": 16000,
                                   "channels": 1}
    for _ in range(5):
        await ws.send_bytes(PCM)
    await ws.send_str(json.dumps({"type": "ping"}))   # ignored
    await ws.send_str(json.dumps({"type": "stop"}))
    assert await next_json(ws) == {"type": "stopped"}
    # The device is free by the time `stopped` arrives.
    assert env.talks.active(100) is None
    assert await closed_code(ws) == talk_handler.CLOSE_NORMAL
    await env.finished()
    # One connection, carrying all of the transcoder's output, then closed.
    assert env.sink.payloads() == [PCM * 5]


async def test_closing_the_socket_still_plays_what_was_sent(env: Env):
    token = await env.login()
    ws = await env.ws(token)
    await next_json(ws)
    await ws.send_bytes(PCM)
    await ws.close()
    await env.finished()
    assert env.sink.payloads() == [PCM]


async def test_a_second_talk_is_busy_and_the_first_goes_on(env: Env):
    token = await env.login()
    first = await env.ws(token)
    await next_json(first)
    await first.send_bytes(PCM)
    other = await env.login("Tablet")
    second = await env.ws(other)
    msg = await next_json(second)
    assert (msg["type"], msg["code"]) == ("error", "busy")
    assert await closed_code(second) == talk_handler.CLOSE_ERROR
    await first.send_bytes(PCM)
    await first.send_str(json.dumps({"type": "stop"}))
    assert await next_json(first) == {"type": "stopped"}
    await env.finished()
    assert env.sink.payloads() == [PCM * 2]
    third = await env.ws(other)
    assert (await next_json(third))["type"] == "ready"
    await third.close()
    await env.finished()


async def test_the_panel_and_the_app_never_talk_through_one_device_at_once(env: Env,
                                                                           monkeypatch):
    monkeypatch.setattr(talk, "TALKS", env.talks)   # what main/ gives both: the one TALKS
    app = web.Application()
    app[PANEL_REGISTRY] = env.registry
    app.router.add_get("/api/devices/{id}/talk", api_talk)
    panel = TestClient(TestServer(app))
    await panel.start_server()
    try:
        token = await env.login()
        phone = await env.ws(token)
        await next_json(phone)
        browser = await panel.ws_connect("/api/devices/100/talk")
        await browser.send_str(json.dumps({"type": "talk_start"}))
        msg = await next_json(browser)
        assert (msg["type"], msg["code"]) == ("error", "busy") and msg["msg"]
        await phone.send_str(json.dumps({"type": "stop"}))
        assert await next_json(phone) == {"type": "stopped"}
        await env.finished()
    finally:
        await panel.close()


async def test_ffmpeg_exiting_is_reported_with_its_reason(env: Env, tmp_path: Path):
    env.talks.ffmpeg = script(tmp_path, "ffmpeg_fail.py", FAILING)
    token = await env.login()
    ws = await env.ws(token)
    assert (await next_json(ws))["type"] == "ready"
    await ws.send_bytes(PCM)
    msg = await next_json(ws)
    assert msg["code"] == "transcoder_failed"
    assert "Invalid data found when processing input" in msg["message"]
    assert await closed_code(ws) == talk_handler.CLOSE_ERROR
    await env.finished()


async def test_the_device_hanging_up_is_device_unreachable(tmp_path: Path):
    env = await _make(tmp_path, FakeSink(hang_up=True))
    try:
        token = await env.login()
        ws = await env.ws(token)
        msg = await next_json(ws)
        if msg["type"] == "ready":
            msg = await next_json(ws)
        assert (msg["type"], msg["code"]) == ("error", "device_unreachable")
        assert await closed_code(ws) == talk_handler.CLOSE_ERROR
        await env.finished()
    finally:
        await env.client.close()
        await env.sink.close()


async def test_a_talk_with_no_audio_ends(env: Env, monkeypatch):
    monkeypatch.setattr(talk_handler, "IDLE_SECONDS", 0.3)
    token = await env.login()
    ws = await env.ws(token)
    await next_json(ws)
    msg = await next_json(ws)
    assert msg["code"] == "idle_timeout"
    assert await closed_code(ws) == talk_handler.CLOSE_ERROR
    await env.finished()


@pytest.mark.parametrize("how", ["revoke", "password"])
async def test_a_talk_ends_with_the_phones_session(env: Env, how):
    token = await env.login()
    ws = await env.ws(token)
    await next_json(ws)
    await ws.send_bytes(PCM)
    if how == "revoke":
        await env.auth.revoke(env.auth.sessions()[0].id)
    else:
        await env.auth.set_password("another password entirely")
    assert await closed_code(ws) == talk_handler.CLOSE_SESSION_ENDED
    await env.finished()


def _masked(opcode: int, data: bytes) -> bytes:
    """One client frame, as the protocol requires it: masked, FIN set."""
    assert len(data) < 126
    mask = os.urandom(4)
    return (bytes([0x80 | opcode, 0x80 | len(data)]) + mask
            + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))


async def test_a_phone_vanishing_mid_stream_frees_everything(env: Env):
    """No close frame: the connection just dies, as on a dropped mobile link."""
    token = await env.login()
    reader, writer = await asyncio.open_connection("127.0.0.1", env.server.port)
    key = base64.b64encode(os.urandom(16)).decode()
    writer.write((f"GET {PREFIX}/devices/100/talk HTTP/1.1\r\nHost: test\r\n"
                  "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                  f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
                  f"Authorization: Bearer {token}\r\n\r\n").encode())
    head = await reader.readuntil(b"\r\n\r\n")
    assert b" 101 " in head.split(b"\r\n", 1)[0]
    _, length = await reader.readexactly(2)
    assert json.loads(await reader.readexactly(length & 0x7F))["type"] == "ready"
    writer.write(_masked(0x2, PCM[:100]))
    await writer.drain()
    await until(lambda: env.sink.received and len(env.sink.received[0]) == 100)
    writer.transport.abort()
    await env.finished()


async def test_shutdown_ends_every_talk(tmp_path: Path):
    env = await _make(tmp_path, FakeSink())
    token = await env.login()
    async with aiohttp.ClientSession() as http:
        ws = await http.ws_connect(env.server.make_url(f"{PREFIX}/devices/100/talk"),
                                   headers={"Authorization": f"Bearer {token}"})
        await next_json(ws)
        assert env.talks.close_all in env.app.on_shutdown
        closing = asyncio.create_task(env.client.close())
        assert await closed_code(ws) == talk_handler.CLOSE_SHUTDOWN
        await closing
    assert env.talks.active(100) is None
    await env.sink.close()


@pytest.mark.skipif(not HAVE_FFMPEG, reason="needs ffmpeg")
async def test_real_ffmpeg_turns_the_phones_pcm_into_adts_aac(env: Env):
    env.talks.ffmpeg = ("ffmpeg",)
    tone = b"".join(struct.pack("<h", int(8000 * math.sin(2 * math.pi * 440 * n / 16000)))
                    for n in range(16000))   # one second of 440 Hz
    token = await env.login()
    ws = await env.ws(token)
    assert (await next_json(ws))["type"] == "ready"
    for i in range(0, len(tone), 3200):
        await ws.send_bytes(tone[i:i + 3200])
    await ws.send_str(json.dumps({"type": "stop"}))
    assert await next_json(ws) == {"type": "stopped"}
    await env.finished()
    assert_adts_16k_mono(env.sink.payloads()[0])


# --- the documentation is the code -------------------------------------------------

def test_the_documented_limits_are_the_real_ones():
    spec = OPENAPI_PATH.read_text(encoding="utf-8")
    assert f"up to {talk_handler.MAX_FRAME // 1024} KiB" in spec
    assert f"no audio for {talk_handler.IDLE_SECONDS:.0f} s" in spec
    assert f"closes with `{talk_handler.CLOSE_ERROR}`" in spec
    assert f"pings every {talk_handler.HEARTBEAT_SECONDS:.0f} s" in spec
    for code in ("not_a_camera", "needs_patcher", "device_unreachable", "busy",
                 "transcoder_failed", "idle_timeout"):
        assert f"`{code}`" in spec, code


def test_the_talk_socket_shares_the_other_sockets_close_codes():
    from petkit_local.app_api import video
    assert talk_handler.CLOSE_SESSION_ENDED == video.CLOSE_SESSION_ENDED
    assert talk_handler.CLOSE_SHUTDOWN == video.CLOSE_SHUTDOWN
