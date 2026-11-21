"""The loopback relay in front of a Dual-Hopper 2's tserver (issue #122).

That tserver serves one client at a time and is locked for minutes by a client
that leaves with a FIN; a reset frees it. These tests stand a fake tserver up
on loopback and check what the relay does to it: retries a silent one, resets
every connection it is done with, and never holds two.
"""
import asyncio
import contextlib

import pytest

from petkit_local.devices.base import Device
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.media import go2rtc as g
from petkit_local.media import tserver_relay as r
from petkit_local.patchers import camera

FLV_HEAD = b"HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\n\r\n"
FLV_BODY = g.FLV_SIGNATURE + b"\x05\x00\x00\x00\x09" + b"\x00" * 64


class FakeTserver:
    """One scripted behaviour per incoming connection, and what each saw.

    `silent` reads the request and never answers; `serve` answers with an FLV
    header and body and keeps the connection; `empty` closes at once; `html`
    answers something that is not a stream. Every connection then waits for
    the client to go, and records whether it left with a reset or a FIN.
    """

    def __init__(self, script):
        self.script = list(script)
        self.requests: list[bytes] = []
        self.outcomes: list[str] = []
        self.connected = asyncio.Event()
        self.ended = asyncio.Condition()
        self.server = None
        self.port = 0

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def close(self):
        self.server.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.server.wait_closed(), 2)

    async def _handle(self, reader, writer):
        mode = self.script.pop(0) if self.script else "serve"
        outcome = "?"
        try:
            self.requests.append(await reader.readuntil(b"\r\n\r\n"))
            self.connected.set()
            if mode == "empty":
                outcome = "closed by us"
                return
            if mode == "serve":
                writer.write(FLV_HEAD + FLV_BODY)
            elif mode == "html":
                writer.write(b"HTTP/1.1 200 OK\r\n\r\n<html>")
            await writer.drain()
            while True:
                data = await reader.read(4096)
                if not data:
                    outcome = "fin"
                    break
        except ConnectionResetError:
            outcome = "reset"
        except (asyncio.IncompleteReadError, OSError) as e:
            outcome = f"error {e!r}"
        finally:
            writer.close()
            async with self.ended:
                self.outcomes.append(outcome)
                self.ended.notify_all()

    async def wait_outcomes(self, n, timeout=5):
        async with self.ended:
            await asyncio.wait_for(self.ended.wait_for(lambda: len(self.outcomes) >= n), timeout)
        return self.outcomes


def _registry(*devices):
    reg = DeviceRegistry()
    for d in devices:
        reg._devices[d.petkit_id] = d
    return reg


def _cam(pid=7, device_type="d4sh", ip="127.0.0.1"):
    d = Device(device_type=device_type, petkit_id=pid, serial_number=f"SN{pid}")
    d.state["ip"] = ip
    d.state[g.STREAM_AVAILABLE] = True
    return d


async def _relay(fake, *devices, **kw):
    kw.setdefault("header_timeout", 0.5)
    kw.setdefault("retry_delay", 0.05)
    relay = r.TserverRelay(_registry(*devices), port=0, device_port=fake.port, **kw)
    assert await relay.start()
    return relay


async def _get(relay, target="/7"):
    """go2rtc's side: GET the relay, return (reader, writer, response head)."""
    reader, writer = await asyncio.open_connection(relay.host, relay.port)
    writer.write(f"GET {target} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
    return reader, writer, head


@pytest.fixture
async def fake_factory():
    made = []

    async def make(*script):
        fake = await FakeTserver(script).start()
        made.append(fake)
        return fake

    yield make
    for fake in made:
        await fake.close()


# --- the relay --------------------------------------------------------------

def test_only_the_dual_hopper_2_is_relayed():
    """Every other model keeps today's behaviour exactly."""
    assert camera.RELAYED_MODELS == {"d4sh"}
    assert camera.is_relayed(_cam(device_type="d4sh"))
    for other in ("t5", "t4", "d4h", "t6"):
        assert not camera.is_relayed(_cam(device_type=other))


async def test_the_stream_passes_through_byte_for_byte(fake_factory):
    """go2rtc must read the same FLV it read from the device: header and body."""
    fake = await fake_factory("serve")
    relay = await _relay(fake, _cam())
    try:
        reader, writer, head = await _get(relay)
        body = await reader.readexactly(len(FLV_BODY))
        assert head == FLV_HEAD and body == FLV_BODY
        # The device got the GET go2rtc's own client sends, for the main stream.
        assert fake.requests[0].startswith(b"GET /main.flv?audio=1 HTTP/1.1\r\nHost: 127.0.0.1\r\n")
        writer.close()
    finally:
        await relay.stop()


async def test_a_silent_device_is_reset_and_retried(fake_factory):
    """Measured: after a reset, a connection sometimes gets no answer at all,
    and the next one works."""
    fake = await fake_factory("silent", "serve")
    relay = await _relay(fake, _cam())
    try:
        reader, writer, head = await _get(relay)
        assert head == FLV_HEAD
        assert await reader.readexactly(len(g.FLV_SIGNATURE)) == g.FLV_SIGNATURE
        outcomes = await fake.wait_outcomes(1)
        assert outcomes[0] == "reset", "the silent attempt must be reset, never FIN'd"
        writer.close()
    finally:
        await relay.stop()


async def test_an_empty_close_is_retried_too(fake_factory):
    fake = await fake_factory("empty", "serve")
    relay = await _relay(fake, _cam())
    try:
        _, writer, head = await _get(relay)
        assert head == FLV_HEAD
        writer.close()
    finally:
        await relay.stop()


async def test_three_silent_attempts_are_a_503_and_all_three_are_reset(fake_factory):
    fake = await fake_factory("silent", "silent", "silent", "serve")
    relay = await _relay(fake, _cam())
    try:
        reader, writer, head = await _get(relay)
        assert head.startswith(b"HTTP/1.1 503 ")
        assert await fake.wait_outcomes(3) == ["reset", "reset", "reset"]
        assert len(fake.requests) == 3, "no fourth attempt"
        writer.close()
    finally:
        await relay.stop()


async def test_the_device_is_reset_when_go2rtc_leaves(fake_factory):
    """The whole point: a FIN locks that tserver for minutes, a reset frees it."""
    fake = await fake_factory("serve")
    relay = await _relay(fake, _cam())
    try:
        reader, writer, _ = await _get(relay)
        await reader.readexactly(len(FLV_BODY))
        writer.close()
        assert await fake.wait_outcomes(1) == ["reset"]
        # And the slot is free again.
        for _ in range(50):
            if not relay.busy(_cam()):
                break
            await asyncio.sleep(0.02)
        assert not relay.busy(_cam())
    finally:
        await relay.stop()


async def test_stopping_the_relay_resets_the_device(fake_factory):
    fake = await fake_factory("serve")
    relay = await _relay(fake, _cam())
    reader, writer, _ = await _get(relay)
    await reader.readexactly(len(FLV_BODY))
    await relay.stop()
    assert await fake.wait_outcomes(1) == ["reset"]
    writer.close()


def test_the_device_socket_lingers_zero():
    """What makes `close()` a reset rather than a FIN."""
    import socket
    import struct
    import sys
    sock = r.reset_socket()
    try:
        raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_LINGER, 8)
        fmt = "HH" if sys.platform == "win32" else "ii"
        onoff, linger = struct.unpack(fmt, raw[:struct.calcsize(fmt)])
        assert onoff != 0 and linger == 0
    finally:
        sock.close()


async def test_a_second_viewer_never_opens_a_second_device_connection(fake_factory):
    """tserver would only queue it, and queueing is the lock-up."""
    fake = await fake_factory("serve", "serve")
    relay = await _relay(fake, _cam())
    try:
        reader, writer, head = await _get(relay)
        assert head == FLV_HEAD
        _, writer2, head2 = await _get(relay)
        assert head2.startswith(b"HTTP/1.1 503 ")
        assert len(fake.requests) == 1
        writer2.close()
        writer.close()
    finally:
        await relay.stop()


async def test_a_viewer_waits_for_a_probe_rather_than_racing_it(fake_factory):
    fake = await fake_factory("serve")
    cam = _cam()
    relay = await _relay(fake, cam)
    try:
        with relay.probing(cam) as free:
            assert free
            pending = asyncio.create_task(_get(relay))
            await asyncio.sleep(0.2)
            assert not fake.requests, "the viewer dialled while the probe held the device"
        _, writer, head = await pending
        assert head == FLV_HEAD
        with relay.probing(cam) as free:
            assert not free, "a probe must not run while a viewer holds the device"
        writer.close()
    finally:
        await relay.stop()


async def test_a_probe_that_outlasts_the_wait_is_a_503(fake_factory):
    fake = await fake_factory("serve")
    cam = _cam()
    relay = await _relay(fake, cam, claim_wait=0.1)
    try:
        with relay.probing(cam):
            _, writer, head = await _get(relay)
        assert head.startswith(b"HTTP/1.1 503 ")
        assert not fake.requests
        writer.close()
    finally:
        await relay.stop()


@pytest.mark.parametrize("target,device", [
    ("/8", _cam()),                                  # no such device
    ("/7", _cam(device_type="t5")),                  # not a relayed model
    ("/7", _cam(ip="192.0.2.7 -x")),                 # not an address
    ("/../7", _cam()),                               # not an id
])
async def test_only_a_known_relayed_camera_is_dialled(fake_factory, target, device):
    fake = await fake_factory("serve")
    relay = await _relay(fake, device)
    try:
        _, writer, head = await _get(relay, target)
        assert head.startswith(b"HTTP/1.1 404 ")
        assert not fake.requests
        writer.close()
    finally:
        await relay.stop()


async def test_garbage_from_the_client_never_raises(fake_factory):
    fake = await fake_factory("serve")
    relay = await _relay(fake, _cam())
    try:
        reader, writer = await asyncio.open_connection(relay.host, relay.port)
        writer.write(b"\x00\xff garbage\r\n\r\n")
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        assert head.startswith(b"HTTP/1.1 404 ")
        writer.close()
    finally:
        await relay.stop()


async def test_a_port_that_cannot_be_bound_degrades(fake_factory):
    fake = await fake_factory()
    relay = await _relay(fake, _cam())
    try:
        other = r.TserverRelay(_registry(), port=relay.port)
        assert await other.start() is False
        assert other.running is False
    finally:
        await relay.stop()


async def test_stop_is_idempotent():
    relay = r.TserverRelay(_registry(), port=0)
    await relay.stop()
    assert await relay.start()
    await relay.stop()
    await relay.stop()
    assert relay.running is False


# --- the probe, for a relayed model -----------------------------------------

async def test_the_reset_probe_reads_the_sub_stream_and_resets(fake_factory):
    fake = await fake_factory("serve")
    assert await g.probe_stream(f"127.0.0.1:{fake.port}", reset=True) is True
    assert fake.requests[0].startswith(b"GET /sub.flv?audio=1 HTTP/1.1\r\n")
    assert await fake.wait_outcomes(1) == ["reset"]


async def test_the_reset_probe_wants_the_flv_signature(fake_factory):
    fake = await fake_factory("html")
    assert await g.probe_stream(f"127.0.0.1:{fake.port}", reset=True) is False
    assert await fake.wait_outcomes(1) == ["reset"]


@pytest.fixture
def _fast_retries(monkeypatch):
    monkeypatch.setattr(r, "RETRY_DELAY", 0.01)


async def test_the_reset_probe_retries_a_silent_device(fake_factory, _fast_retries):
    """One silent attempt after a reset is normal on that tserver, and a
    single "no" would withdraw the camera for a minute."""
    fake = await fake_factory("silent", "serve")
    assert await g.probe_stream(f"127.0.0.1:{fake.port}", timeout=0.3, reset=True) is True
    assert await fake.wait_outcomes(2) == ["reset", "reset"]


async def test_the_reset_probe_gives_up_after_three_silent_attempts(fake_factory, _fast_retries):
    fake = await fake_factory("silent", "silent", "silent", "serve")
    assert await g.probe_stream(f"127.0.0.1:{fake.port}", timeout=0.3, reset=True) is False
    assert await fake.wait_outcomes(3) == ["reset", "reset", "reset"]
    assert len(fake.requests) == 3


async def test_a_response_that_is_not_a_stream_is_not_retried(fake_factory, _fast_retries):
    fake = await fake_factory("html", "serve")
    assert await g.probe_stream(f"127.0.0.1:{fake.port}", reset=True) is False
    assert len(fake.requests) == 1


@pytest.mark.parametrize("ip", ["127.0.0.1:1", "evil.example", "192.0.2.7 -x", "1.2.3.4:99999"])
async def test_the_reset_probe_never_raises(ip, _fast_retries):
    assert await g.probe_stream(ip, timeout=0.5, reset=True) is False


# --- go2rtc's view of it ------------------------------------------------------

@pytest.fixture(autouse=True)
def _binary_present(monkeypatch):
    monkeypatch.setattr(g, "_have_go2rtc_cache", True)
    yield
    g._have_go2rtc_cache = None


async def test_go2rtc_reads_a_d4sh_through_the_relay_and_nothing_else(tmp_path):
    d4sh = _cam(1, ip="192.0.2.10")
    t5 = _cam(2, device_type="t5", ip="192.0.2.11")
    d4h = _cam(3, device_type="d4h", ip="192.0.2.12")
    relay = r.TserverRelay(_registry(), port=0)
    s = g.Go2rtc(_registry(d4sh, t5, d4h), data_dir=str(tmp_path), relay=relay)
    # Before the relay listens, nothing changes (and a relay that cannot bind
    # leaves the old behaviour in place).
    assert s.desired_streams()["1"] == "http://192.0.2.10/main.flv?audio=1"
    await relay.start()
    try:
        assert s.desired_streams() == {
            "1": f"http://127.0.0.1:{relay.port}/1",
            "2": "http://192.0.2.11/main.flv?audio=1",
            "3": "http://192.0.2.12/main.ts?audio=1",
        }
        rendered = g.render_config(s.desired_streams(), "/data/go2rtc.log")
        assert f"    - http://127.0.0.1:{relay.port}/1\n" in rendered
    finally:
        await relay.stop()


async def test_a_new_address_does_not_change_the_relayed_source(tmp_path):
    """go2rtc restarts drop every viewer; the relay resolves the address itself."""
    d4sh = _cam(1, ip="192.0.2.10")
    relay = r.TserverRelay(_registry(), port=0)
    s = g.Go2rtc(_registry(d4sh), data_dir=str(tmp_path), relay=relay)
    await relay.start()
    try:
        before = s.desired_streams()
        d4sh.state["ip"] = "192.0.2.99"
        assert s.desired_streams() == before
    finally:
        await relay.stop()


async def test_the_relay_starts_only_for_a_relayed_camera(tmp_path):
    relay = r.TserverRelay(_registry(), port=0)
    s = g.Go2rtc(_registry(_cam(1, device_type="t5")), data_dir=str(tmp_path), relay=relay)
    await s._ensure_relay()
    assert not relay.running
    s._registry._devices[2] = _cam(2)
    try:
        await s._ensure_relay()
        assert relay.running
    finally:
        await s.shutdown()
    assert not relay.running


async def test_a_go2rtc_restart_leaves_the_relay_up(tmp_path):
    relay = r.TserverRelay(_registry(), port=0)
    s = g.Go2rtc(_registry(_cam(1)), data_dir=str(tmp_path), relay=relay)
    await s._ensure_relay()
    try:
        await s.stop()
        assert relay.running
    finally:
        await s.shutdown()


async def test_a_d4sh_is_probed_with_a_reset(tmp_path, monkeypatch):
    calls = []

    async def probe(ip, *a, **kw):
        calls.append((ip, kw.get("reset", False)))
        return True

    monkeypatch.setattr(g, "probe_stream", probe)
    s = g.Go2rtc(_registry(_cam(1, ip="192.0.2.10"),
                           _cam(2, device_type="t5", ip="192.0.2.11")),
                 data_dir=str(tmp_path), relay=r.TserverRelay(_registry(), port=0))
    await s.refresh_probes()
    assert sorted(calls) == [("192.0.2.10", True), ("192.0.2.11", False)]


async def test_a_d4sh_the_relay_is_streaming_is_not_probed(tmp_path, monkeypatch):
    """Even when go2rtc's API says nobody watches (it could be mid-reconnect):
    the relay's own guard is the authority on the device's one connection."""
    calls = []

    async def probe(ip, *a, **kw):
        calls.append(ip)
        return False

    async def nobody(self):
        return set()

    monkeypatch.setattr(g, "probe_stream", probe)
    monkeypatch.setattr(g.Go2rtc, "_watched_streams", nobody)
    cam = _cam(1, ip="192.0.2.10")
    relay = r.TserverRelay(_registry(), port=0)
    s = g.Go2rtc(_registry(cam), data_dir=str(tmp_path), relay=relay)
    assert relay._try_claim(cam.petkit_id, "live")
    await s.refresh_probes()
    assert calls == []
    assert cam.state[g.STREAM_AVAILABLE] is True, "a skipped probe keeps the verdict"
    relay._release(cam.petkit_id)
    await s.refresh_probes()
    assert calls == ["192.0.2.10"]


async def test_the_auto_candidate_looks_past_a_relay_url(tmp_path, monkeypatch):
    """The LAN route is found toward the device, not toward the loopback relay."""
    monkeypatch.setattr(g, "lan_ip", lambda toward: "192.168.1.9" if toward == "192.0.2.7" else "")
    relay = r.TserverRelay(_registry(), port=0)
    s = g.Go2rtc(_registry(_cam(1, ip="192.0.2.7")), data_dir=str(tmp_path),
                 host_candidate=g.AUTO_CANDIDATE, relay=relay)
    await relay.start()

    async def no_start():
        pass

    monkeypatch.setattr(s, "_start", no_start)
    try:
        await s.reconcile()
        import yaml
        doc = yaml.safe_load(s._rendered)
        assert doc["webrtc"]["candidates"] == [f"192.168.1.9:{g.WEBRTC_PORT}"]
    finally:
        await relay.stop()
