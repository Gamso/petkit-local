"""A device that reconnects under the SAME client id, against the real broker.

A patched ESP32's SDK sends a fixed `timestamp=2524608000000` in its client id,
so every reconnect that beats the broker to noticing the old connection is gone
is an MQTT take-over (issue #106). amqtt ended BOTH connections and the device
was marked off MQTT until its next boot. These drive `mqtt/broker.py::
DeviceBroker` over raw sockets — aiomqtt cannot run on the Windows proactor
loop, and a raw CONNECT is also the only way to pick the `clean_session` flag.
"""
from __future__ import annotations

import asyncio
import socket
import struct
import time

import pytest

from petkit_local.devices.payloads import PATCHED_PRODUCT_KEY
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.mqtt.auth import compute_aliyun_sign, internal_credentials
from petkit_local.mqtt.broker import delivery_view, start_broker

FIXED_TS = "2524608000000"   # what the ESP32 SDK sends, every time

#: Longer than one tick of amqtt's session monitor (1 s), which is what reaped
#: the new connection in #106.
SETTLE = 1.6


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _utf8(text: str) -> bytes:
    raw = text.encode()
    return struct.pack("!H", len(raw)) + raw


def _remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        digit, n = n % 128, n // 128
        out.append(digit | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _connect(client_id: str, username: str, password: str, *, clean: bool,
             keep_alive: int = 62) -> bytes:
    flags = 0xC0 | (0x02 if clean else 0x00)          # username + password
    body = (_utf8("MQTT") + bytes([4, flags]) + struct.pack("!H", keep_alive)
            + _utf8(client_id) + _utf8(username) + _utf8(password))
    return b"\x10" + _remaining_length(len(body)) + body


def _subscribe(packet_id: int, topic_filter: str) -> bytes:
    body = struct.pack("!H", packet_id) + _utf8(topic_filter) + bytes([0])
    return bytes([0x82]) + _remaining_length(len(body)) + body


def _publish(topic: str, payload: bytes) -> bytes:
    body = _utf8(topic) + payload
    return b"\x30" + _remaining_length(len(body)) + body


class _Conn:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 connack: bytes) -> None:
        self.reader, self.writer, self.connack = reader, writer, connack

    @property
    def accepted(self) -> bool:
        return self.connack[:2] == b"\x20\x02" and self.connack[3] == 0

    async def packet(self, timeout: float = 3.0) -> tuple[int, bytes]:
        """The next packet: (fixed-header byte, body). Raises on a closed socket."""
        first = await asyncio.wait_for(self.reader.readexactly(1), timeout)
        length, shift = 0, 0
        while True:
            byte = (await asyncio.wait_for(self.reader.readexactly(1), timeout))[0]
            length |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                break
        body = await asyncio.wait_for(self.reader.readexactly(length), timeout)
        return first[0], body

    async def alive(self) -> bool:
        """Whether the broker still answers a PINGREQ on this connection."""
        try:
            self.writer.write(b"\xc0\x00")
            await self.writer.drain()
            kind, _ = await self.packet(timeout=2.0)
        except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError):
            return False
        return kind == 0xD0

    async def send(self, data: bytes) -> None:
        self.writer.write(data)
        await self.writer.drain()

    def close(self) -> None:
        self.writer.close()


async def _open(port: int, client_id: str, username: str, password: str,
                *, clean: bool) -> _Conn:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(_connect(client_id, username, password, clean=clean))
    await writer.drain()
    connack = await asyncio.wait_for(reader.readexactly(4), 5)
    return _Conn(reader, writer, connack)


@pytest.fixture
async def esp32(tmp_path):
    """A running broker and a registered patched ESP32 on the shared key."""
    reg = DeviceRegistry()
    device = reg.get_or_create(100, "t4", serial_number="SN100")
    device.mqtt_product_key = PATCHED_PRODUCT_KEY
    port = _free_port()
    broker = await start_broker(port, reg, data_dir=str(tmp_path))
    pk, dn = PATCHED_PRODUCT_KEY, device.mqtt_device_name
    creds = {
        "client_id": f"{pk}.{dn}|timestamp={FIXED_TS},_v=sdk-c-3.2.0,securemode=2,"
                     "signmethod=hmacsha256,lan=C,_ss=1,gw=0,ext=0|",   # a T4's, verbatim
        "username": f"{dn}&{pk}",
        "password": compute_aliyun_sign(device.mqtt_device_secret, f"{pk}.{dn}",
                                        dn, pk, FIXED_TS),
    }
    conns: list[_Conn] = []

    async def connect(*, clean: bool, password: str | None = None,
                      client_id: str | None = None) -> _Conn:
        conn = await _open(port, client_id or creds["client_id"], creds["username"],
                           password or creds["password"], clean=clean)
        conns.append(conn)
        return conn

    try:
        yield broker, device, port, connect
    finally:
        for conn in conns:
            conn.close()
        await asyncio.wait_for(broker.shutdown(), 15)


@pytest.mark.parametrize("first_clean,second_clean",
                         [(False, False), (True, False), (True, True), (False, True)])
async def test_the_newest_connection_wins_and_stays_up(esp32, first_clean, second_clean):
    broker, device, port, connect = esp32
    old = await connect(clean=first_clean)
    assert old.accepted and device.mqtt_connected

    new = await connect(clean=second_clean)       # the old one is still open
    assert new.accepted
    await asyncio.sleep(SETTLE)

    assert await new.alive(), "the new connection was closed with the old one"
    assert not await old.alive(), "the replaced connection was left open"
    assert device.mqtt_connected, "a live device was taken off MQTT"
    (session, _handler), = broker.sessions.values()
    assert session.transitions.state == "connected"

    # Its packets keep it alive in the watchdog's eyes.
    device.last_mqtt = 0.0
    assert await new.alive()
    assert device.last_mqtt > time.time() - 5

    # And a command reaches it: the server-side subscriptions are on the
    # session the NEW connection is serving.
    user, secret = internal_credentials()
    bridge = await _open(port, "petkit-local-bridge-test", user, secret, clean=True)
    try:
        topic = (f"/sys/{device.mqtt_product_key}/{device.mqtt_device_name}"
                 "/thing/service/property/set")
        await bridge.send(_publish(topic, b'{"method":"thing.service.property.set"}'))
        kind, body = await new.packet()
        assert kind & 0xF0 == 0x30
        assert topic.encode() in body
    finally:
        bridge.close()


async def test_a_real_disconnect_after_a_take_over_still_ends_the_session(esp32):
    _broker, device, _port, connect = esp32
    await connect(clean=False)
    new = await connect(clean=False)
    await asyncio.sleep(SETTLE)
    assert device.mqtt_connected

    await new.send(b"\xe0\x00")                    # DISCONNECT
    new.close()
    for _ in range(50):
        if not device.mqtt_connected:
            break
        await asyncio.sleep(0.1)
    assert not device.mqtt_connected


async def test_a_refused_take_over_leaves_nothing_claiming_the_device(esp32):
    """amqtt closes the open connection BEFORE it looks at the new CONNECT's
    credentials, so a refused one still ends the session. Nothing may then
    claim the device is on MQTT: its commands would go to a broker with
    nobody on the other end."""
    broker, device, _port, connect = esp32
    for plugin in broker.plugins_manager._plugins:
        if hasattr(plugin, "set_strict"):
            plugin.set_strict(True)
    old = await connect(clean=False)
    assert device.mqtt_connected

    refused = await connect(clean=False, password="0" * 64)
    assert not refused.accepted
    await asyncio.sleep(SETTLE)
    assert not await old.alive()
    assert not device.mqtt_connected


async def _receives(conn: _Conn, topic: str) -> bool:
    """Whether a PUBLISH on `topic` reaches `conn`, skipping anything else."""
    try:
        while True:
            kind, body = await conn.packet(timeout=2.0)
            if kind & 0xF0 == 0x30 and topic.encode() in body:
                return True
    except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError):
        return False


@pytest.mark.parametrize("clean", [False, True])
async def test_three_take_overs_in_a_row_each_receive_what_we_publish(esp32, clean):
    """Issue #150, as the owner's T4 did it: a connection vanishes without a
    DISCONNECT (its socket is simply abandoned, so the broker still holds it
    open), the device comes back under the same client id, asks for its config
    with a `data_get`, and waits for our answer on `user/get`. Three times."""
    broker, device, port, connect = esp32
    pk, dn = device.mqtt_product_key, device.mqtt_device_name
    user_get = f"/{pk}/{dn}/user/get"
    service = f"/sys/{pk}/{dn}/thing/service/property/set"
    user, secret = internal_credentials()
    bridge = await _open(port, "petkit-local-bridge-test", user, secret, clean=True)
    await bridge.send(_subscribe(1, "#"))
    kind, _ = await bridge.packet()
    assert kind == 0x90
    try:
        first = await connect(clean=clean)
        assert first.accepted
        for _attempt in range(3):
            conn = await connect(clean=clean)     # the previous one is still open
            assert conn.accepted
            await asyncio.sleep(0.2)

            # Its own frames reach us...
            data_get = f"/{pk}/{dn}/user/update"
            await conn.send(_publish(data_get, b'{"method":"data_get"}'))
            assert await _receives(bridge, data_get)

            # ...and ours reach it, the way `MQTTBridge` publishes them.
            await bridge.send(_publish(user_get, b'{"dataType":"dev_device_info"}'))
            assert await _receives(conn, user_get), "the reply never reached the new connection"
            await bridge.send(_publish(service, b'{"method":"thing.service.property.set"}'))
            assert await _receives(conn, service)

            assert device.mqtt_connected
            view = delivery_view(broker, pk, dn)
            # One session for the device, connected: what the broadcast loop sends to.
            assert [s["state"] for s in view["sessions"]] == ["connected"]
            assert user_get in view["filters"]

        await asyncio.sleep(SETTLE)               # past a session-monitor tick
        await bridge.send(_publish(user_get, b"{}"))
        assert await _receives(conn, user_get)
        assert device.mqtt_connected
    finally:
        bridge.close()


async def test_a_device_take_over_gets_a_fresh_session_and_session_present_0(esp32):
    """Issue #150: the T4 reconnects with clean_session=0 (amqtt's "performing
    take-over" warning in #106's log fires only for a resumed session), and a
    resumed session answers session-present=1. A device is never given one."""
    broker, device, _port, connect = esp32
    first = await connect(clean=False)
    assert first.accepted and first.connack[2] & 0x01 == 0
    (session, _), = broker.sessions.values()
    seen = [session]
    for _ in range(3):
        conn = await connect(clean=False)          # the previous one is still open
        assert conn.accepted
        assert conn.connack[2] & 0x01 == 0, "a take-over answered session-present=1"
        (session, _), = broker.sessions.values()
        assert all(session is not old for old in seen), "a session was resumed"
        assert session.clean_session
        seen.append(session)
    await asyncio.sleep(SETTLE)
    assert await conn.alive()
    assert device.mqtt_connected


async def test_a_newer_sdk_device_still_resumes_its_session(esp32):
    """Scoped to the T4's SDK: a D4SH on sdk-c-4.1.0 given a fresh session on
    every CONNECT reported iotStatus=0 ten minutes in and reconnected every
    fifteen, so it keeps amqtt's resumed session and session-present=1."""
    broker, device, _port, connect = esp32
    pk, dn = PATCHED_PRODUCT_KEY, device.mqtt_device_name
    client_id = (f"{pk}.{dn}|timestamp={FIXED_TS},_ss=1,_v=sdk-c-4.1.0,securemode=2,"
                 "signmethod=hmacsha256,ext=3,_conn=tls|")   # a D4SH's shape
    first = await connect(clean=False, client_id=client_id)
    assert first.accepted and first.connack[2] & 0x01 == 0
    (session, _), = broker.sessions.values()
    second = await connect(clean=False, client_id=client_id)
    assert second.accepted and second.connack[2] & 0x01 == 1
    (resumed, _), = broker.sessions.values()
    assert resumed is session
    await asyncio.sleep(SETTLE)
    assert await second.alive()
    assert device.mqtt_connected


async def test_a_non_device_client_still_resumes_its_session(esp32):
    """Scoped to devices: the bridge's own client keeps amqtt's handling."""
    broker, _device, port, _connect = esp32
    user, secret = internal_credentials()
    first = await _open(port, "petkit-local-bridge-test", user, secret, clean=False)
    assert first.accepted and first.connack[2] & 0x01 == 0
    (session, _), = broker.sessions.values()
    try:
        second = await _open(port, "petkit-local-bridge-test", user, secret, clean=False)
        assert second.accepted and second.connack[2] & 0x01 == 1
        (resumed, _), = broker.sessions.values()
        assert resumed is session
        second.close()
    finally:
        first.close()
