"""Smoke test of the BUILT image, from the outside, the way a device sees it.

The suite tests the code; this tests the artefact. 2.3.0 passed every test and
then died on every start: amqtt 0.12 had changed an internal the broker's TLS
hook relies on, and nothing in CI ever started the broker with its TLS
listener on. This fails on that image within seconds.

It runs inside a second container that shares the add-on's network namespace
(`run.sh` starts both), so everything is 127.0.0.1 and nothing is published on
the host. It needs only the standard library and paho-mqtt, which the image
already has from the lock -- `run.sh` feeds it to the image's own python.

What it proves, in order:

1. the device API, the panel and the MQTT TLS port answer;
2. the TLS listener accepts the suites a device's mbedTLS offers -- RSA key
   exchange only, TLS 1.2 -- which is `mqtt/broker.py`'s hook on amqtt's
   `_create_ssl_context`, the code path 2.3.0 broke;
3. an MQTT 5.0 CONNECT is refused with a v5 CONNACK (`mqtt/protocol_compat.py`,
   which patches amqtt's packet classes);
4. a fake T5, end to end: signs up and fetches its MQTT credentials over HTTP,
   CONNECTs over TLS with the Aliyun signature in UPPERCASE hex as the real one
   does, and sends NO SUBSCRIBE. The broker must subscribe it on its behalf
   (`mqtt/auth.py::_server_subscribe`, amqtt's `add_subscription`), so a command
   sent from the panel reaches it, and the reply to its own `property/post`
   does too. The post's telemetry must show up in the panel's device state,
   with the `XDevice` credential stripped.

`run.sh` checks the rest from outside: the container never restarted, and its
log has no traceback and says the auth plugin was found.

Exits non-zero, with the step that failed, on any failure. Keep it under a
minute: every check waits on an event with a short timeout, never on a sleep.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Any

import paho.mqtt.client as mqtt

HOST = "127.0.0.1"
API = f"http://{HOST}:8080"
PANEL = f"http://{HOST}:8099"
TLS_PORT = 443

#: Our locally-minted id range (`http/handlers/signup.py::_LOCAL_ID_BASE`), so
#: this can never look like a real PetKit device.
DEVICE_ID = 412_345_678
SERIAL = "SMOKE0000000001"
X_DEVICE = (f"id={DEVICE_ID}&nonce=smoke&timestamp={int(time.time())}"
            f"&type=T5&sign=0&sn={SERIAL}")

#: What a device's mbedTLS offers: RSA key exchange, no (EC)DHE.
DEVICE_CIPHERS = "AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA256:AES128-SHA:@SECLEVEL=0"

#: `protocol_compat._V5_REFUSAL`: CONNACK, reason 0x84, property length 0.
V5_REFUSAL = b"\x20\x03\x00\x84\x00"


def step(msg: str) -> None:
    print(f"ok  {msg}", flush=True)


def fail(msg: str) -> None:
    print(f"FAIL {msg}", flush=True)
    sys.exit(1)


def http(method: str, url: str, *, body: bytes | None = None,
         headers: dict[str, str] | None = None, timeout: float = 5) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def http_json(method: str, url: str, **kwargs: Any) -> Any:
    status, raw = http(method, url, **kwargs)
    if status != 200:
        fail(f"{method} {url} answered {status}: {raw[:200]!r}")
    try:
        return json.loads(raw)
    except ValueError:
        fail(f"{method} {url} answered non-JSON: {raw[:200]!r}")


def wait_for(what: str, probe, timeout: float) -> Any:
    """Poll `probe` until it returns something truthy, or fail naming `what`."""
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = probe()
        except (OSError, urllib.error.URLError) as e:
            last = e
        else:
            if last:
                return last
        time.sleep(0.25)
    fail(f"timed out after {timeout:.0f}s waiting for {what} (last: {last!r})")


def device_tls_context() -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # The device does not verify this broker's self-signed certificate the way
    # a browser would; the handshake's ciphers are what is under test.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    ctx.set_ciphers(DEVICE_CIPHERS)
    return ctx


def tls_socket() -> ssl.SSLSocket:
    raw = socket.create_connection((HOST, TLS_PORT), timeout=5)
    return device_tls_context().wrap_socket(raw)


def check_services_answer() -> None:
    # The panel starts last (after the broker it reports on), so it is the one
    # to wait for; a container that is crash-looping never gets here.
    wait_for("the panel on :8099", lambda: http("GET", f"{PANEL}/")[0] == 200, 45)
    status, body = http("GET", f"{PANEL}/")
    if b"<html" not in body.lower():
        fail(f"panel / answered {status} without a page")
    info = http_json("GET", f"{PANEL}/api/info")
    step(f"panel answers (version {info.get('version')})")

    status, _ = http("GET", f"{API}/")
    if status != 200:
        fail(f"device API / answered {status}")
    # "Never 404 a device": an endpoint nobody handles still gets a 200.
    status, _ = http("POST", f"{API}/6/t5/dev_smoke_unknown_endpoint",
                     headers={"X-Device": X_DEVICE})
    if status != 200:
        fail(f"device API catch-all answered {status}, not 200")
    step("device API answers, unknown endpoints included")


def check_tls_and_v5_refusal() -> None:
    with tls_socket() as s:
        cipher, version, _ = s.cipher()
        if "DHE" in cipher:
            fail(f"negotiated {cipher}; the RSA-only offer was not what got used")
        step(f"TLS handshake with a device's RSA-only suites: {version} {cipher}")

        level = 5
        variable = b"\x00\x04MQTT" + bytes([level]) + b"\x02\x00\x3c" + b"\x00"
        body = variable + b"\x00\x05smoke"
        s.sendall(b"\x10" + bytes([len(body)]) + body)
        s.settimeout(5)
        got = b""
        while len(got) < len(V5_REFUSAL):
            chunk = s.recv(len(V5_REFUSAL) - len(got))
            if not chunk:
                break
            got += chunk
        # Let the broker close first. Hanging up on it mid-close resets the
        # socket under amqtt's `wait_closed`, which logs a traceback that
        # `run.sh` would then fail on -- a race in this probe, not a defect in
        # the image.
        try:
            while s.recv(64):
                pass
        except (OSError, ssl.SSLError):
            pass
    if got != V5_REFUSAL:
        fail(f"MQTT 5.0 CONNECT answered {got.hex()}, expected {V5_REFUSAL.hex()}")
    step("MQTT 5.0 CONNECT refused with a v5 CONNACK (0x84)")


def register_fake_t5() -> dict:
    form = {"Content-Type": "application/x-www-form-urlencoded", "X-Device": X_DEVICE}
    signup = http_json("POST", f"{API}/6/t5/dev_signup", headers=form,
                       body=b"mac=aa:bb:cc:00:00:01&firmware=1.0&timezone=0.0&locale=en-US")
    if signup.get("result", signup).get("id") != DEVICE_ID:
        fail(f"dev_signup did not hand back id {DEVICE_ID}: {signup}")
    creds = http_json("POST", f"{API}/6/t5/dev_iot_device_info", headers=form)
    ali = (creds.get("result") or {}).get("ali") or {}
    missing = [k for k in ("productKey", "deviceName", "deviceSecret", "mqttHost") if not ali.get(k)]
    if missing:
        fail(f"dev_iot_device_info lacks {missing}: {creds}")
    if ali["mqttHost"] != HOST:
        fail(f"device was handed broker {ali['mqttHost']!r}, not the api-url host {HOST!r}")
    step(f"fake T5 signed up and got credentials (pk={ali['productKey']} "
         f"dn={ali['deviceName']})")
    return ali


class FakeDevice:
    """A paho client that behaves like the T5's: connects, never subscribes."""

    def __init__(self, ali: dict) -> None:
        self.pk, self.dn = ali["productKey"], ali["deviceName"]
        ts = str(int(time.time() * 1000))
        client_id = (f"{self.pk}.{self.dn}|timestamp={ts},_ss=1,_v=sdk-c-4.0.0,"
                     f"securemode=2,signmethod=hmacsha256,ext=3,|")
        content = (f"clientId{self.pk}.{self.dn}deviceName{self.dn}"
                   f"productKey{self.pk}timestamp{ts}")
        # UPPERCASE, as the real firmware sends it (AGENTS.md).
        password = hmac.new(ali["deviceSecret"].encode(), content.encode(),
                            hashlib.sha256).hexdigest().upper()

        self.connected = threading.Event()
        self.connack: Any = None
        self.messages: list[mqtt.MQTTMessage] = []
        self.got_message = threading.Condition()

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                                  protocol=mqtt.MQTTv311)
        self.client.username_pw_set(f"{self.dn}&{self.pk}", password)
        self.client.tls_set_context(device_tls_context())
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, _c, _u, _flags, reason_code, _props) -> None:
        self.connack = reason_code
        self.connected.set()

    def _on_message(self, _c, _u, msg: mqtt.MQTTMessage) -> None:
        with self.got_message:
            self.messages.append(msg)
            self.got_message.notify_all()

    def connect(self) -> None:
        # The T5's own keep-alive.
        self.client.connect(HOST, TLS_PORT, keepalive=62)
        self.client.loop_start()
        if not self.connected.wait(10):
            fail("no CONNACK within 10s")
        if self.connack.is_failure:
            fail(f"CONNECT refused: {self.connack}")

    def wait_message(self, topic: str, timeout: float = 10) -> mqtt.MQTTMessage:
        deadline = time.monotonic() + timeout
        with self.got_message:
            while True:
                for msg in self.messages:
                    if msg.topic == topic:
                        return msg
                left = deadline - time.monotonic()
                if left <= 0:
                    seen = sorted({m.topic for m in self.messages})
                    fail(f"nothing arrived on {topic} within {timeout:.0f}s (got: {seen})")
                self.got_message.wait(left)

    def publish(self, topic: str, payload: dict) -> None:
        self.client.publish(topic, json.dumps(payload), qos=0).wait_for_publish(5)

    def close(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()


def device_detail() -> dict:
    return http_json("GET", f"{PANEL}/api/devices/{DEVICE_ID}")


def check_fake_device(ali: dict) -> None:
    dev = FakeDevice(ali)
    dev.connect()
    step("fake T5 CONNECTed over TLS with its signed credentials (uppercase hex)")
    try:
        prefix = f"/sys/{dev.pk}/{dev.dn}"
        expected = {f"{prefix}/thing/service/#", f"{prefix}/thing/event/+/post_reply"}

        # Subscribed on its behalf: what the add-on asked for, and what the
        # broker's own table will deliver to (read off amqtt's `_subscriptions`).
        def subscribed() -> dict | None:
            d = device_detail()
            filters = set((d.get("mqtt_delivery") or {}).get("filters") or [])
            if d.get("mqtt_connected") and expected <= filters \
                    and expected <= set(d.get("mqtt_subscriptions") or []):
                return d
            return None

        wait_for("the broker to subscribe the device on its behalf", subscribed, 10)
        step("broker subscribed the device server-side, with no SUBSCRIBE sent")

        # The add-on's own bridge client joins the broker 2s after it starts
        # (`mqtt/bridge.py::STARTUP_DELAY_SECONDS`), and until then the panel
        # rightly queues a command for the heartbeat instead. Retried until it
        # goes out over MQTT; the queued copies are harmless to a fake device.
        def send_light() -> dict | None:
            r = http_json("POST", f"{PANEL}/api/devices/{DEVICE_ID}/command",
                          headers={"Content-Type": "application/json"},
                          body=json.dumps({"action": "light"}).encode())
            return r if r.get("delivered") == "mqtt" else None

        result = wait_for("a panel command to go out over MQTT", send_light, 15)
        msg = dev.wait_message(f"{prefix}/thing/service/{result['suffix']}")
        step(f"panel command reached the device on {msg.topic}")

        marker = f"smoke-{time.time_ns()}"
        dev.publish(f"{prefix}/thing/event/property/post", {
            "id": "smoke-1", "version": "1.0", "method": "thing.event.property.post",
            "params": {"smokeMarker": marker,
                       "XDevice": X_DEVICE, "timestamp": int(time.time())},
        })
        reply = json.loads(dev.wait_message(f"{prefix}/thing/event/property/post_reply").payload)
        if reply.get("id") != "smoke-1" or reply.get("code") != 200:
            fail(f"unexpected post_reply: {reply}")
        step("property/post acknowledged on post_reply")

        state = wait_for("the property post in the panel's device state",
                         lambda: (s := device_detail().get("state") or {})
                         .get("smokeMarker") == marker and s, 10)
        if "XDevice" in state:
            fail("the XDevice credential was merged into device.state")
        step("property/post telemetry is in the panel's device state, XDevice stripped")
    finally:
        dev.close()


def main() -> None:
    started = time.monotonic()
    check_services_answer()
    check_tls_and_v5_refusal()
    check_fake_device(register_fake_t5())
    print(f"smoke test passed in {time.monotonic() - started:.1f}s", flush=True)


if __name__ == "__main__":
    main()
