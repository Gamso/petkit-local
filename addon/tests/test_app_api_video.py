"""Live video on the app API (`app_api/video.py`): fMP4 over a WebSocket, and HLS.

go2rtc is played by an aiohttp test server speaking the protocol checked
against go2rtc 1.9.14 in the add-on image (see the module docstring there).
What is held here:

* the stream name comes from the device, never from the client, and only a
  camera go2rtc serves has one;
* the upgrade is authenticated like any route, and refused cleanly (JSON, no
  upgrade) when there is nothing to watch or go2rtc is not there;
* the relay carries go2rtc's frames to the phone in order, forwards exactly
  one re-serialised `mse` request, and hides go2rtc's error text;
* viewers are capped per camera and in total, and a phone reopening a
  camera replaces its older view;
* a running relay ends on revocation, logout, password change and shutdown,
  and a client too slow for the stream is dropped, not buffered for;
* HLS playlists only ever point under the route, and an HLS session id is
  good for one phone and one camera.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, unused_port

from petkit_local.app_api import video as video_mod
from petkit_local.app_api.appkeys import VIDEO_SESSIONS
from petkit_local.app_api.auth import FILENAME, AppAuth, ScryptParams
from petkit_local.app_api.handlers.meta import OPENAPI_PATH
from petkit_local.app_api.middleware import ApiError
from petkit_local.app_api.server import PREFIX, create_app_api
from petkit_local.app_api.video import (
    CLIENT_BACKLOG_BYTES, CLOSE_REPLACED, CLOSE_SESSION_ENDED, CLOSE_SHUTDOWN, CLOSE_TOO_SLOW,
    MAX_LIVE_PER_DEVICE, MAX_LIVE_TOTAL, STREAM_ERROR, LiveSessions, _Backlog, device_stream,
    master_playlist, mse_request, rewrite_playlist,
)
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.media.go2rtc import STREAM_AVAILABLE

FAST = ScryptParams(n=2 ** 10, r=8, p=1)
PASSWORD = "correct horse battery"
CODECS = "avc1.640029,avc1.64002A,mp4a.40.2,opus"
INIT = b"\x00\x00\x00\x10ftypiso5" + b"\x00" * 4 + b"\x00\x00\x00\x08moov"
FRAGMENT = b"\x00\x00\x00\x08moof" + b"\x00\x00\x00\x10mdat" + b"\x01" * 8
HLS_ID = "abcDEF12"


class FakeGo2rtc:
    """go2rtc's `/api/ws` and HLS endpoints, recording what reached them."""

    def __init__(self) -> None:
        self.ws_srcs: list[str] = []
        self.ws_received: list[str] = []
        self.ws_closed = asyncio.Event()
        self.m3u8_queries: list[dict[str, str]] = []
        self.hls_requests: list[str] = []
        self.media_playlist: str | None = None
        self.hls_gone = False
        self.flood = 0          # binary messages to push without waiting
        self.error_text: str | None = None
        self.open_ws: list[web.WebSocketResponse] = []
        app = web.Application()
        app.router.add_get("/api/ws", self.ws)
        app.router.add_get("/api/stream.m3u8", self.stream_m3u8)
        app.router.add_get("/api/hls/{file}", self.hls)
        self.server = TestServer(app)

    @property
    def addr(self) -> str:
        return f"127.0.0.1:{self.server.port}"

    async def ws(self, request: web.Request) -> web.WebSocketResponse:
        self.ws_srcs.append(request.query.get("src", ""))
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.open_ws.append(ws)
        try:
            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                self.ws_received.append(msg.data)
                if self.error_text is not None:
                    await ws.send_str(self.error_text)
                    continue
                await ws.send_str(json.dumps(
                    {"type": "mse", "value": 'video/mp4; codecs="avc1.640029"'}))
                await ws.send_bytes(INIT)
                if self.flood:
                    try:
                        for _ in range(self.flood):
                            if ws.closed:
                                break
                            await ws.send_bytes(b"\x00" * 65536)
                    except ConnectionError:
                        break
                else:
                    for _ in range(3):
                        await ws.send_bytes(FRAGMENT)
        finally:
            self.ws_closed.set()
        return ws

    async def drop_all(self) -> None:
        """Close every socket from go2rtc's side, as a dying go2rtc would."""
        for ws in self.open_ws:
            await ws.close()

    async def stream_m3u8(self, request: web.Request) -> web.Response:
        self.m3u8_queries.append(dict(request.query))
        return web.Response(
            text=('#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=192000,CODECS="avc1.640029"\n'
                  f"hls/playlist.m3u8?id={HLS_ID}"),
            content_type="application/vnd.apple.mpegurl")

    async def hls(self, request: web.Request) -> web.StreamResponse:
        self.hls_requests.append(f"{request.match_info['file']}?{request.query_string}")
        if self.hls_gone or request.query.get("id") != HLS_ID:
            return web.Response(status=404, text="404 page not found\n")
        file = request.match_info["file"]
        if file == "playlist.m3u8":
            text = self.media_playlist or (
                "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:1\n"
                f'#EXT-X-MAP:URI="init.mp4?id={HLS_ID}"\n'
                f"#EXTINF:0.500,\nsegment.m4s?id={HLS_ID}&n=0\n"
                f"#EXTINF:0.500,\nsegment.m4s?id={HLS_ID}&n=1")
            return web.Response(text=text, content_type="application/vnd.apple.mpegurl")
        body = INIT if file == "init.mp4" else FRAGMENT
        return web.Response(body=body, content_type="application/octet-stream")


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Env:
    def __init__(self, tmp: Path, fake: FakeGo2rtc, **live_kw: Any) -> None:
        self.auth = AppAuth(tmp / FILENAME, scrypt_params=FAST)
        self.registry = DeviceRegistry()
        self.cams = []
        for pid in (100, 101, 102):
            d = self.registry.get_or_create(petkit_id=pid, device_type="t5",
                                            serial_number=f"SN{pid}")
            d.state.update({"ip": f"192.168.1.{pid}", STREAM_AVAILABLE: True})
            self.cams.append(d)
        self.registry.get_or_create(petkit_id=200, device_type="d4", serial_number="SN200")
        cam = self.registry.get_or_create(petkit_id=300, device_type="t6", serial_number="SN300")
        cam.state.update({"ip": "192.168.1.30"})  # a camera the probe found no stream on
        self.go2rtc = SimpleNamespace(
            running=True,
            desired_streams=lambda: {str(d.petkit_id): f"http://{d.state['ip']}/main.flv"
                                     for d in self.registry.all()
                                     if d.state.get(STREAM_AVAILABLE)})
        self.clock = Clock()
        live_kw.setdefault("recheck_seconds", 0.05)
        self.live = LiveSessions(go2rtc_api=fake.addr, clock=self.clock, **live_kw)
        self.app = create_app_api(self.auth, self.registry, go2rtc=self.go2rtc,
                                  live_sessions=self.live)
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

    async def ws(self, token: str | None, device: int = 100, query: str = ""):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return await self.client.ws_connect(f"{PREFIX}/devices/{device}/live{query}",
                                            headers=headers)

    async def hls(self, token: str | None, path: str, device: int = 100):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return await self.client.get(f"{PREFIX}/devices/{device}/live/hls/{path}",
                                     headers=headers)


async def _make(tmp_path: Path, **live_kw: Any):
    fake = FakeGo2rtc()
    await fake.server.start_server()
    env = Env(tmp_path, fake, **live_kw)
    await env.start()
    return env, fake


@pytest.fixture
async def setup(tmp_path: Path):
    env, fake = await _make(tmp_path)
    yield env, fake
    await env.client.close()
    await fake.server.close()


async def _until(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition never became true"
        await asyncio.sleep(0.01)


async def _closed_code(ws, timeout: float = 5.0) -> int | None:
    """Read until the socket closes; its close code."""
    async with asyncio.timeout(timeout):
        while True:
            msg = await ws.receive()
            if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                return ws.close_code


# --- which stream a device has -------------------------------------------------

def test_only_a_camera_go2rtc_serves_has_a_stream(tmp_path: Path):
    registry = DeviceRegistry()
    cam = registry.get_or_create(petkit_id=100, device_type="t5", serial_number="A")
    feeder = registry.get_or_create(petkit_id=200, device_type="d4", serial_number="B")
    other = registry.get_or_create(petkit_id=300, device_type="t6", serial_number="C")
    go2rtc = SimpleNamespace(desired_streams=lambda: {"100": "http://x/main.flv",
                                                      "200": "http://y/main.flv"})
    assert device_stream(cam, go2rtc) == "100"
    assert device_stream(feeder, go2rtc) is None   # not a camera, whatever go2rtc says
    assert device_stream(other, go2rtc) is None    # a camera go2rtc does not serve
    assert device_stream(cam, None) is None


def test_only_a_plain_mse_request_is_forwarded_and_it_is_rebuilt():
    assert json.loads(mse_request(json.dumps({"type": "mse", "value": CODECS,
                                              "extra": "x"}))) == {"type": "mse", "value": CODECS}
    for bad in ("not json", "[]", '{"type":"webrtc/offer","value":"v=0"}',
                '{"type":"mse","value":"avc1\\"; rm"}', '{"type":"mse","value":5}',
                json.dumps({"type": "mse", "value": "a" * 600}), '{"type":"exec"}'):
        assert mse_request(bad) is None, bad


# --- refusals before the upgrade -------------------------------------------------

async def test_the_upgrade_needs_a_session(setup):
    env, fake = setup
    with pytest.raises(aiohttp.WSServerHandshakeError) as e:
        await env.ws(None)
    assert e.value.status == 401
    with pytest.raises(aiohttp.WSServerHandshakeError) as e:
        await env.ws("not-a-token")
    assert e.value.status == 401
    assert fake.ws_srcs == []


async def test_nothing_to_watch_is_404_and_no_go2rtc_is_503(setup):
    env, fake = setup
    token = await env.login()
    headers = {"Authorization": f"Bearer {token}", "Connection": "Upgrade",
               "Upgrade": "websocket", "Sec-WebSocket-Version": "13",
               "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ=="}
    for device, status, code in ((999, 404, "device_not_found"), (200, 404, "stream_not_found"),
                                 (300, 404, "stream_not_found"), (0, 404, "device_not_found")):
        r = await env.client.get(f"{PREFIX}/devices/{device}/live", headers=headers)
        assert r.status == status, device
        assert (await r.json())["error"] == code

    r = await env.client.get(f"{PREFIX}/devices/100/live",
                             headers={"Authorization": f"Bearer {token}"})
    assert r.status == 426 and (await r.json())["error"] == "upgrade_required"

    env.go2rtc.running = False
    r = await env.client.get(f"{PREFIX}/devices/100/live", headers=headers)
    assert r.status == 503 and (await r.json())["error"] == "live_unavailable"
    r = await env.hls(token, "playlist.m3u8")
    assert r.status == 503
    assert fake.ws_srcs == [] and fake.m3u8_queries == []
    assert env.live.viewers() == 0


async def test_an_unreachable_go2rtc_is_503_and_frees_the_slot(setup):
    env, _fake = setup
    env.live.go2rtc_api = f"127.0.0.1:{unused_port()}"
    token = await env.login()
    with pytest.raises(aiohttp.WSServerHandshakeError) as e:
        await env.ws(token)
    assert e.value.status == 503
    assert env.live.viewers() == 0


async def test_no_go2rtc_at_all_is_503(tmp_path: Path):
    auth = AppAuth(tmp_path / FILENAME, scrypt_params=FAST)
    await auth.set_password(PASSWORD)
    registry = DeviceRegistry()
    d = registry.get_or_create(petkit_id=100, device_type="t5", serial_number="A")
    d.state.update({"ip": "1.2.3.4", STREAM_AVAILABLE: True})
    client = TestClient(TestServer(create_app_api(auth, registry)))
    await client.start_server()
    try:
        r = await client.post(f"{PREFIX}/auth/login", json={"password": PASSWORD})
        token = (await r.json())["token"]
        r = await client.get(f"{PREFIX}/devices/100/live/hls/playlist.m3u8",
                             headers={"Authorization": f"Bearer {token}"})
        assert r.status == 503 and (await r.json())["error"] == "live_unavailable"
    finally:
        await client.close()


# --- the relay -------------------------------------------------------------------

async def test_the_relay_carries_the_stream_and_rebuilds_the_request(setup):
    env, fake = setup
    token = await env.login()
    # A `src` of the client's is ignored: the name comes from the device.
    ws = await env.ws(token, query="?src=rtsp://10.0.0.1/evil")
    await ws.send_str(json.dumps({"type": "mse", "value": CODECS, "src": "evil"}))
    first = await ws.receive_str()
    assert json.loads(first)["type"] == "mse"
    assert await ws.receive_bytes() == INIT
    for _ in range(3):
        assert await ws.receive_bytes() == FRAGMENT
    assert fake.ws_srcs == ["100"]
    assert fake.ws_received == [json.dumps({"type": "mse", "value": CODECS})]

    # A second request, garbage, or another type is answered, never forwarded.
    for extra in (json.dumps({"type": "mse", "value": CODECS}), "not json",
                  json.dumps({"type": "webrtc/offer", "value": "v=0"})):
        await ws.send_str(extra)
        reply = json.loads(await ws.receive_str())
        assert reply["type"] == "error"
    await ws.send_bytes(b"\x00binary")
    await asyncio.sleep(0.05)
    assert len(fake.ws_received) == 1
    assert env.live.viewers(100) == 1

    await ws.close()
    await asyncio.wait_for(fake.ws_closed.wait(), 5)
    await _until(lambda: env.live.viewers() == 0)


async def test_go2rtcs_error_text_is_not_shown_to_the_phone(setup):
    env, fake = setup
    fake.error_text = json.dumps({"type": "error", "value": (
        "mse: streams: 404 Not Found, exec/rtsp\nError opening input file "
        "http://192.168.1.100/main.flv?audio=1")})
    token = await env.login()
    ws = await env.ws(token)
    await ws.send_str(json.dumps({"type": "mse", "value": CODECS}))
    reply = json.loads(await ws.receive_str())
    assert reply == {"type": "error", "value": STREAM_ERROR}
    await ws.close()


async def test_go2rtc_closing_ends_the_relay(setup):
    env, fake = setup
    token = await env.login()
    ws = await env.ws(token)
    await asyncio.sleep(0.05)
    await fake.drop_all()
    assert await _closed_code(ws) == video_mod.CLOSE_STREAM_ENDED
    await _until(lambda: env.live.viewers() == 0)


async def test_a_slow_client_is_dropped_not_buffered_for(tmp_path: Path):
    env, fake = await _make(tmp_path, backlog_bytes=256 * 1024)
    try:
        fake.flood = 2000  # 125 MiB the client never reads
        codes = []
        close_client = env.live._close_client

        async def record(request, client, slot):
            codes.append(slot.close_code)
            await close_client(request, client, slot)
        env.live._close_client = record
        token = await env.login()
        ws = await env.ws(token)
        await ws.send_str(json.dumps({"type": "mse", "value": CODECS}))
        await asyncio.wait_for(fake.ws_closed.wait(), 30)
        await _until(lambda: env.live.viewers() == 0, 10)
        assert codes == [CLOSE_TOO_SLOW]
        await ws.close()
    finally:
        await env.client.close()
        await fake.server.close()


def test_the_backlog_is_bounded_in_bytes_but_takes_any_one_message():
    backlog = _Backlog(100)
    assert backlog.put(b"x" * 500)          # empty: one oversized message is fine
    assert not backlog.put(b"x")            # but nothing on top of it
    backlog = _Backlog(100)
    assert backlog.put(b"x" * 60) and not backlog.put(b"x" * 60)
    assert backlog.put(b"x" * 40)


# --- caps --------------------------------------------------------------------------

async def test_viewers_are_capped_per_camera_and_in_total(tmp_path: Path):
    env, _fake = await _make(tmp_path, per_device=2, total=3)
    try:
        phones = [await env.login(f"phone{i}") for i in range(4)]
        a = await env.ws(phones[0], 100)
        b = await env.ws(phones[1], 100)
        with pytest.raises(aiohttp.WSServerHandshakeError) as e:
            await env.ws(phones[2], 100)
        assert e.value.status == 429 and e.value.headers.get("Retry-After")
        # HLS counts against the same caps.
        r = await env.hls(phones[2], "playlist.m3u8", device=100)
        assert r.status == 429 and (await r.json())["error"] == "live_limit_reached"

        c = await env.ws(phones[2], 101)
        assert env.live.viewers() == 3
        with pytest.raises(aiohttp.WSServerHandshakeError) as e:
            await env.ws(phones[3], 102)
        assert e.value.status == 429

        await a.close()
        await _until(lambda: env.live.viewers() == 2)
        d = await env.ws(phones[3], 102)
        for ws in (b, c, d):
            await ws.close()
    finally:
        await env.client.close()
        await _fake.server.close()


async def test_a_phone_reopening_a_camera_replaces_its_older_view(tmp_path: Path):
    env, _fake = await _make(tmp_path, per_device=1, total=1)
    try:
        token = await env.login()
        old = await env.ws(token)
        new = await env.ws(token)   # not refused: the old view goes
        assert await _closed_code(old) == CLOSE_REPLACED
        assert env.live.viewers() == 1
        await new.close()
    finally:
        await env.client.close()
        await _fake.server.close()


def test_admission_has_no_await_between_counting_and_taking():
    live = LiveSessions(per_device=1, total=5)
    live.admit("s1", 100, "ws")
    with pytest.raises(ApiError) as e:
        live.admit("s2", 100, "ws")
    assert e.value.status == 429


def test_an_idle_hls_viewer_stops_counting():
    clock = Clock()
    live = LiveSessions(per_device=1, total=5, clock=clock, hls_idle_seconds=10)
    slot = live.admit("s1", 100, "hls")
    slot.hls_id = HLS_ID
    clock.now += 5
    assert live.hls_slot("s1", 100, HLS_ID) is slot   # a request moves the deadline
    clock.now += 9
    assert live.viewers(100) == 1
    clock.now += 2
    assert live.viewers(100) == 0
    live.admit("s2", 100, "hls")


# --- ending a relay -----------------------------------------------------------------

@pytest.mark.parametrize("how", ["revoke", "logout", "password", "clear"])
async def test_a_running_relay_ends_with_its_session(setup, how):
    env, fake = setup
    token = await env.login()
    ws = await env.ws(token)
    await ws.send_str(json.dumps({"type": "mse", "value": CODECS}))
    await ws.receive_str()
    if how == "revoke":
        await env.auth.revoke(env.auth.sessions()[0].id)
    elif how == "logout":
        r = await env.client.post(f"{PREFIX}/auth/logout",
                                  headers={"Authorization": f"Bearer {token}"})
        assert r.status == 200
    elif how == "password":
        await env.auth.set_password("another password entirely")
    else:
        await env.auth.clear_password()
    assert await _closed_code(ws) == CLOSE_SESSION_ENDED
    await asyncio.wait_for(fake.ws_closed.wait(), 5)
    await _until(lambda: env.live.viewers() == 0)


async def test_shutdown_ends_every_relay_at_once(tmp_path: Path):
    env, fake = await _make(tmp_path)
    token = await env.login()
    async with aiohttp.ClientSession() as http:
        ws = await http.ws_connect(env.server.make_url(f"{PREFIX}/devices/100/live"),
                                   headers={"Authorization": f"Bearer {token}"})
        await ws.send_str(json.dumps({"type": "mse", "value": CODECS}))
        await ws.receive_str()
        assert env.live.close_all in env.app.on_shutdown
        loop = asyncio.get_running_loop()
        started = loop.time()
        closing = asyncio.create_task(env.client.close())
        assert await _closed_code(ws) == CLOSE_SHUTDOWN
        await closing
        assert loop.time() - started < 2.0
    await fake.server.close()


def test_lifecycle_closes_live_relays_before_stopping_go2rtc():
    """Source pin: a phone is told 1001 rather than "stream ended" at shutdown."""
    source = (Path(__file__).resolve().parent.parent / "petkit_local" / "main"
              / "lifecycle.py").read_text(encoding="utf-8")
    body = source.split("async def cleanup_background(", 1)[1].split('"""')[2]
    assert body.index("close_all()") < body.index("await go2rtc.stop()")
    assert "go2rtc=services.go2rtc" in source


# --- HLS -------------------------------------------------------------------------------

async def test_hls_is_rewritten_to_stay_under_the_route(setup):
    env, fake = setup
    token = await env.login()
    assert (await env.hls(None, "playlist.m3u8")).status == 401

    r = await env.hls(token, "playlist.m3u8?src=evil")
    assert r.status == 200
    assert r.headers["Content-Type"].startswith("application/vnd.apple.mpegurl")
    master = await r.text()
    assert fake.m3u8_queries == [{"src": "100", "mp4": ""}]
    assert master.splitlines()[-1] == f"playlist.m3u8?id={HLS_ID}"
    assert "hls/" not in master

    r = await env.hls(token, f"playlist.m3u8?id={HLS_ID}")
    media = await r.text()
    assert r.status == 200
    assert f'#EXT-X-MAP:URI="init.mp4?id={HLS_ID}"' in media
    assert f"segment.m4s?id={HLS_ID}&n=1" in media

    r = await env.hls(token, f"init.mp4?id={HLS_ID}")
    assert r.status == 200 and await r.read() == INIT
    assert r.headers["Content-Type"] == "video/mp4"
    r = await env.hls(token, f"segment.m4s?id={HLS_ID}&n=1")
    assert r.status == 200 and await r.read() == FRAGMENT
    assert r.headers["Content-Type"] == "video/iso.segment"
    assert env.live.viewers(100) == 1


async def test_an_hls_id_is_good_for_one_phone_and_one_camera(setup):
    env, fake = setup
    mine, theirs = await env.login("mine"), await env.login("theirs")
    assert (await env.hls(mine, "playlist.m3u8")).status == 200
    seen = len(fake.hls_requests)
    for token, path, device in (
            (theirs, f"playlist.m3u8?id={HLS_ID}", 100),        # another phone
            (mine, f"playlist.m3u8?id={HLS_ID}", 101),          # another camera
            (mine, "playlist.m3u8?id=someoneelse", 100),        # an id we never issued
            (mine, "playlist.m3u8?id=../../api/config", 100),
            (mine, "init.mp4", 100)):                           # no id at all
        r = await env.hls(token, path, device)
        assert r.status == 404, path
        assert (await r.json())["error"] == "hls_session_not_found"
    for path in (f"segment.m4s?id={HLS_ID}&n=x", f"segment.m4s?id={HLS_ID}",
                 f"segment.ts?id={HLS_ID}&n=1", f"stream.mp4?id={HLS_ID}", "..%2Fstreams",
                 f"..%2F..%2Fstreams?id={HLS_ID}", "config"):
        r = await env.hls(mine, path)
        assert r.status == 404, path
    assert len(fake.hls_requests) == seen   # none of it reached go2rtc


async def test_an_expired_hls_session_is_404_and_frees_its_slot(setup):
    env, fake = setup
    token = await env.login()
    assert (await env.hls(token, "playlist.m3u8")).status == 200
    fake.hls_gone = True
    r = await env.hls(token, f"playlist.m3u8?id={HLS_ID}")
    assert r.status == 404 and (await r.json())["error"] == "hls_session_expired"
    assert env.live.viewers() == 0


async def test_a_playlist_naming_anything_else_is_refused(setup):
    env, fake = setup
    token = await env.login()
    await env.hls(token, "playlist.m3u8")
    fake.media_playlist = f"#EXTM3U\n#EXTINF:1,\nhttp://evil.example/segment.m4s?id={HLS_ID}&n=1"
    r = await env.hls(token, f"playlist.m3u8?id={HLS_ID}")
    assert r.status == 502 and (await r.json())["error"] == "bad_upstream"


@pytest.mark.parametrize("uri", [
    "http://evil.example/x.m4s", "//evil.example/segment.m4s?id=abcDEF12&n=1",
    "../streams?src=x", "/api/config", "segment.m4s?id=other&n=1",
    "segment.m4s?id=abcDEF12&n=1&src=rtsp://x", "segment.m4s?id=abcDEF12&n=-1",
    "segment.ts?id=abcDEF12&n=1", "playlist.m3u8?id=abcDEF12", "init.mp4?id=abcDEF12&n=1",
    "../hls/../ws?src=1",
])
def test_rewriting_fails_closed_on_any_uri_it_would_not_serve(uri):
    with pytest.raises(ApiError) as e:
        rewrite_playlist(f"#EXTM3U\n#EXTINF:1,\n{uri}\n", HLS_ID)
    assert e.value.status == 502
    with pytest.raises(ApiError):
        rewrite_playlist(f'#EXTM3U\n#EXT-X-MAP:URI="{uri}"\n', HLS_ID)


def test_rewriting_keeps_this_sessions_files_relative():
    text = rewrite_playlist(
        "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4?id=abcDEF12\"\n#EXTINF:0.5,\n"
        "segment.m4s?n=7&id=abcDEF12\n#EXTINF:0.5,\n/api/hls/segment.m4s?id=abcDEF12&n=8\n",
        HLS_ID)
    assert text.splitlines() == [
        "#EXTM3U", '#EXT-X-MAP:URI="init.mp4?id=abcDEF12"', "#EXTINF:0.5,",
        "segment.m4s?id=abcDEF12&n=7", "#EXTINF:0.5,", "segment.m4s?id=abcDEF12&n=8"]
    master = master_playlist('#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhls/playlist.m3u8?id=x\n',
                             HLS_ID)
    assert master.splitlines() == ["#EXTM3U", "#EXT-X-STREAM-INF:BANDWIDTH=1",
                                   f"playlist.m3u8?id={HLS_ID}"]


async def test_the_live_routes_are_on_the_app_listener_only(setup):
    env, _fake = setup
    assert VIDEO_SESSIONS in env.app
    paths = {r.resource.canonical for r in env.app.router.routes()}
    assert f"{PREFIX}/devices/{{id}}/live" in paths
    assert f"{PREFIX}/devices/{{id}}/live/hls/{{file}}" in paths


def test_the_documented_caps_are_the_real_ones():
    spec = OPENAPI_PATH.read_text(encoding="utf-8")
    assert (f"capped at {MAX_LIVE_PER_DEVICE} per camera and {MAX_LIVE_TOTAL} in total"
            in spec)
    assert f"more than {CLIENT_BACKLOG_BYTES // 2 ** 20} MiB" in spec
    docs = (Path(__file__).resolve().parent.parent / "DOCS.md").read_text(encoding="utf-8")
    assert (f"At most {MAX_LIVE_PER_DEVICE} viewers per camera and {MAX_LIVE_TOTAL} in all"
            in docs)
