"""A phone's own push endpoint (`app_api/push.py`, `app_api/notify/push.py`).

What is held here, in the order issue 86 asks:

* what may be registered — `https://`, plain `http://` only to a private target,
  no credentials, no fragment, a query kept verbatim, never this machine;
* the registration belongs to its session: logout, revocation from the panel, a
  password change and the 90-day expiry each end it, and a phone revoked while
  a notification was queued receives nothing;
* the message a fake UnifiedPush server receives: six stable keys, compact,
  no credential, TTL and Urgency set;
* the server's kind switches and the phone's own `kinds` both filter, and the
  dedupe and the rate limit are the ntfy topic's, shared;
* a slow, failing or vanished endpoint delays nobody else, and one answering
  404/410 three times in a row is unregistered;
* the endpoint — a capability URL — appears in no log line, no API answer and
  no panel response; only its host does;
* the OpenAPI file lists the routes and the message exactly.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import os
import socket
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, unused_port

from petkit_local.app_api import push
from petkit_local.app_api.auth import FILENAME, SESSION_IDLE_LIFETIME, AppAuth
from petkit_local.app_api.handlers.meta import OPENAPI_PATH
from petkit_local.app_api.notify import push as sink_module
from petkit_local.app_api.notify import rules
from petkit_local.app_api.notify.notifier import RATE_LIMITS, Notifier
from petkit_local.app_api.notify.push import (
    GONE_LIMIT, MAX_MESSAGE_BYTES, PUSH_TTL, GuardedResolver, encode, message,
)
from petkit_local.app_api.notify.rules import Notification
from petkit_local.app_api.notify.settings import (
    FILENAME as NTFY_FILENAME, KIND_ERROR, KIND_FEED_SERVED, KIND_OFFLINE, KIND_TOILET, KINDS,
    NtfyConfig,
)
from petkit_local.app_api.push import PushRegistration, PushRejected, check_endpoint, check_kinds
from petkit_local.app_api.server import PREFIX
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app
from tests.test_app_api import FAST, PASSWORD, Clock, Env, _Schemas
from tests.test_app_notify import FakeNtfy, _row

SECRET = "upS3cretTopicNobodyMaySee"
QUERY_SECRET = "t0kenInTheQuery"
TS = 1_763_150_000


# --- a fake UnifiedPush server --------------------------------------------------------

class FakePush:
    """Answers like a push server, per path; can hold a request open."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.status: dict[str, int] = {}
        self.hold: dict[str, asyncio.Event] = {}
        self.received = asyncio.Event()
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", self._handle)
        self.server = TestServer(app)

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        self.requests.append({"method": request.method, "path": request.path,
                              "raw": request.raw_path, "headers": dict(request.headers),
                              "body": await request.read()})
        self.received.set()
        gate = self.hold.get(request.path)
        if gate is not None:
            await gate.wait()
        status = self.status.get(request.path, 201)
        if 300 <= status < 400:
            return web.Response(status=status, headers={"Location": "/elsewhere"})
        return web.Response(status=status)

    async def start(self) -> None:
        await self.server.start_server()

    def url(self, path: str) -> str:
        return str(self.server.make_url(path))

    def to(self, path: str) -> list[dict[str, Any]]:
        return [r for r in self.requests if r["path"] == path]

    async def wait(self, count: int, path: str | None = None, timeout: float = 5.0) -> None:
        async def _until():
            while len(self.to(path) if path else self.requests) < count:
                self.received.clear()
                await self.received.wait()
        await asyncio.wait_for(_until(), timeout)


@pytest.fixture
async def fake():
    f = FakePush()
    await f.start()
    yield f
    for gate in f.hold.values():
        gate.set()
    await f.server.close()


@pytest.fixture
def allow_loopback(monkeypatch):
    """Let pushes reach the fake server on 127.0.0.1; every other rule stands."""
    real = push.refused_address

    def refused(value: Any) -> bool:
        try:
            if ipaddress.ip_address(str(value)).is_loopback:
                return False
        except ValueError:
            pass
        return real(value)

    monkeypatch.setattr(push, "refused_address", refused)


class PushEnv:
    """An account with a password, a notifier with both sinks, a worker draining it."""

    def __init__(self, tmp: Path) -> None:
        self.clock = Clock()
        self.auth = AppAuth(tmp / FILENAME, clock=self.clock, scrypt_params=FAST)
        self.config = NtfyConfig(tmp / NTFY_FILENAME)  # the topic sink: off
        self.notifier_clock = Clock(1000.0)
        self.notifier = Notifier(self.config, DeviceRegistry(), clock=self.notifier_clock,
                                 app_auth=self.auth)
        self.worker: asyncio.Task | None = None

    async def session(self, name: str, endpoint: str | None = None,
                      kinds: tuple[str, ...] | None = None) -> str:
        _, session = await self.auth.create_session(name)
        if endpoint is not None:
            await self.auth.set_push(session.id, PushRegistration(endpoint, kinds, self.clock()))
        return session.id

    async def event(self, row: dict[str, Any]) -> None:
        await self.notifier.handle({"kind": "event", "device_id": row["device_id"],
                                    "detail": {**row, "is_new": True}})

    def start(self) -> None:
        self.worker = asyncio.create_task(self.notifier.publish_forever())

    async def settle(self) -> None:
        """Until the queue is empty and every POST it started has finished."""
        for _ in range(100):
            await asyncio.sleep(0)
            if self.notifier._queue.empty():
                break
        await asyncio.sleep(0)
        await self.notifier.push.drain()

    async def close(self) -> None:
        if self.worker is not None:
            self.worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await self.worker
        await self.notifier.close()


@pytest.fixture
async def penv(tmp_path: Path):
    e = PushEnv(tmp_path)
    await e.auth.set_password(PASSWORD)
    yield e
    await e.close()


# --- what may be registered -----------------------------------------------------------

def test_an_endpoint_must_be_https_or_http_to_a_private_target():
    for good in ("https://ntfy.example.org/upAbC123?up=1",
                 "https://push.example.org/wpush/v2/gAAAA-_x%2Fy?a=1&b=2",
                 "https://192.168.1.5/upX",           # a LAN ntfy over https
                 "http://192.168.1.5:8080/upX?up=1",   # ... or plain http
                 "http://10.0.0.2/upX",
                 "http://100.101.102.103/upX",        # Tailscale / CGNAT
                 "http://[fd00::5]/upX",              # ULA
                 "http://ntfy/upX", "http://ntfy.lan/upX", "http://nas.local/upX",
                 "http://ntfy.home.arpa/upX", "https://ntfy.home.lan/upX"):
        assert check_endpoint(f"  {good} ") == good, good
    for bad in ("http://push.example.org/upX",       # public name over http
                "http://8.8.8.8/upX",                # public address over http
                "https://user:pw@push.example.org/upX", "https://tok@push.example.org/upX",
                "https://push.example.org/upX#frag", "https://push.example.org/upX#",
                "ftp://push.example.org/upX", "push.example.org/upX", "https:///upX",
                "https://push.example.org/up X", "https://push.example.org/upé",
                "https://push.example.org:99999/upX",
                "https://127.0.0.1/upX", "http://127.0.0.1:8097/upX", "https://[::1]/upX",
                "https://localhost/upX", "http://app.localhost/upX",
                "http://169.254.169.254/latest/meta-data", "https://[fe80::1]/upX",
                "https://0.0.0.0/upX", "https://224.0.0.1/upX", "https://240.0.0.1/upX",
                "https://[::ffff:127.0.0.1]/upX",
                "https://push.example.org/" + "u" * push.MAX_ENDPOINT_LENGTH,
                "", None, 5, ["https://push.example.org"]):
        with pytest.raises(PushRejected):
            check_endpoint(bad)


def test_an_address_of_this_machine_is_never_a_target():
    assert push.refused_address("10.255.255.254") is False  # nobody's, so not ours
    assert push.refused_address("192.168.77.1") is False
    assert push.refused_address("not an address") is True
    try:
        own = socket.gethostbyname(socket.gethostname())
    except OSError:
        pytest.skip("no resolvable host name here")
    if ipaddress.ip_address(own).is_loopback:
        pytest.skip("the host name resolves to loopback here")
    # The container's own address: where the panel answers unauthenticated.
    assert push.refused_address(own) is True
    with pytest.raises(PushRejected):
        check_endpoint(f"https://{own}/upX")


def test_kinds_are_known_ones_in_server_order_or_null():
    assert check_kinds(None) is None
    assert check_kinds([]) == ()
    assert check_kinds([KIND_OFFLINE, "future_kind", KIND_TOILET, KIND_TOILET]) == (
        KIND_TOILET, KIND_OFFLINE)
    for bad in ("toilet", [1], {"toilet": True}, ["x"] * (push.MAX_KINDS + 1)):
        with pytest.raises(PushRejected):
            check_kinds(bad)


# --- registering over the API ---------------------------------------------------------

@pytest.fixture
async def api(tmp_path: Path, event_store, pet_registry):
    e = Env(tmp_path, event_store, pet_registry)
    await e.start()
    await e.auth.set_password(PASSWORD)
    yield e
    await e.client.close()


async def _put(env: Env, token: str, body: Any, **kw: Any):
    return await env.client.put(f"{PREFIX}/push", json=body,
                                headers={"Authorization": f"Bearer {token}"}, **kw)


async def test_a_phone_registers_replaces_and_unregisters_its_endpoint(api: Env, tmp_path):
    schemas = _Schemas()
    token = await api.login()
    endpoint = f"https://push.example.org/{SECRET}?up=1&k={QUERY_SECRET}"
    r = await _put(api, token, {"endpoint": endpoint})
    assert r.status == 200
    text = await r.text()
    body = json.loads(text)
    schemas.check(body["push"], "PushRegistration")
    assert body["push"]["host"] == "push.example.org" and body["push"]["kinds"] is None
    assert SECRET not in text and QUERY_SECRET not in text

    info = await (await api.get("/info", token)).json()
    schemas.check(info, "Info")
    assert info["session"]["push"]["host"] == "push.example.org"

    # Stored with the session, verbatim, in the 0600 file — and read back.
    stored = json.loads((tmp_path / FILENAME).read_text())
    assert stored["sessions"][0]["push"]["endpoint"] == endpoint
    if os.name == "posix":
        assert stat.S_IMODE(os.stat(tmp_path / FILENAME).st_mode) == 0o600
    again = AppAuth(tmp_path / FILENAME, clock=api.clock, scrypt_params=FAST)
    assert [s.push.endpoint for s in again.push_targets()] == [endpoint]

    # Registering again replaces: one endpoint per session.
    r = await _put(api, token, {"endpoint": "https://other.example.net/upY",
                                "kinds": [KIND_ERROR, "future_kind"]})
    assert (await r.json())["push"] == {"host": "other.example.net", "kinds": [KIND_ERROR],
                                        "registered": api.clock()}
    assert [s.push.host for s in api.auth.push_targets()] == ["other.example.net"]

    r = await api.client.delete(f"{PREFIX}/push", headers={"Authorization": f"Bearer {token}"})
    assert r.status == 200 and await r.json() == {"ok": True}
    assert api.auth.push_targets() == []
    assert (await (await api.get("/info", token)).json())["session"]["push"] is None
    r = await api.client.delete(f"{PREFIX}/push", headers={"Authorization": f"Bearer {token}"})
    assert r.status == 200  # idempotent


async def test_a_bad_registration_is_refused_without_repeating_the_url(api: Env):
    token = await api.login()
    for body, code in (({"endpoint": f"http://push.example.org/{SECRET}"}, "invalid_push_endpoint"),
                       ({"endpoint": f"https://u:p@push.example.org/{SECRET}"},
                        "invalid_push_endpoint"),
                       ({"endpoint": f"https://127.0.0.1/{SECRET}"}, "invalid_push_endpoint"),
                       ({"endpoint": f"https://push.example.org/{SECRET}#x"},
                        "invalid_push_endpoint"),
                       ({}, "invalid_push_endpoint"),
                       ({"endpoint": "https://push.example.org/x", "kinds": "toilet"},
                        "bad_parameter"),
                       ([], "bad_json")):
        r = await _put(api, token, body)
        assert r.status == 400, body
        text = await r.text()
        assert json.loads(text)["error"] == code and SECRET not in text
    r = await api.client.put(f"{PREFIX}/push", data="endpoint=x",
                             headers={"Authorization": f"Bearer {token}"})
    assert r.status == 415
    r = await api.client.put(f"{PREFIX}/push", json={"endpoint": "https://push.example.org/x"})
    assert r.status == 401
    assert api.auth.push_targets() == []


async def test_whatever_ends_the_session_ends_its_registration(api: Env):
    tokens = {}
    for name in ("a", "b", "c", "d"):
        tokens[name] = await api.login(name=name)
        r = await _put(api, tokens[name], {"endpoint": f"https://push.example.org/up{name}"})
        assert r.status == 200

    def registered() -> set[str]:
        return {s.name for s in api.auth.push_targets()}

    assert registered() == {"a", "b", "c", "d"}
    # Logout from the app.
    r = await api.client.post(f"{PREFIX}/auth/logout",
                              headers={"Authorization": f"Bearer {tokens['a']}"})
    assert r.status == 200 and registered() == {"b", "c", "d"}
    # Revocation from the panel.
    b = next(s for s in api.auth.sessions() if s.name == "b")
    assert await api.auth.revoke(b.id)
    assert registered() == {"c", "d"}
    # A password change from c: every OTHER session goes, c keeps its own.
    r = await api.client.post(f"{PREFIX}/auth/password",
                              json={"current_password": PASSWORD,
                                    "new_password": "another long password"},
                              headers={"Authorization": f"Bearer {tokens['c']}"})
    assert r.status == 200 and registered() == {"c"}
    # 90 days without use.
    api.clock.advance(SESSION_IDLE_LIFETIME)
    assert registered() == set()
    # And a removed password closes everything.
    api.clock.advance(-SESSION_IDLE_LIFETIME)
    assert registered() == {"c"}
    await api.auth.clear_password()
    assert registered() == set()


# --- the message --------------------------------------------------------------------

async def test_the_push_server_receives_six_stable_keys(penv: PushEnv, fake, allow_loopback):
    endpoint = fake.url(f"/{SECRET}") + f"?up=1&k={QUERY_SECRET}%2Fz"
    await penv.session("Pixel", endpoint)
    penv.start()
    await penv.event(_row("10", "t5", content={"is_shit": 1, "shit_weight": 32},
                          pet_id=2, ts=TS + 0.7))
    await fake.wait(1)
    req = fake.requests[0]
    assert req["method"] == "POST" and req["path"] == f"/{SECRET}"
    assert req["raw"] == f"/{SECRET}?up=1&k={QUERY_SECRET}%2Fz"  # sent exactly as registered
    assert req["body"] == (b'{"v":1,"kind":"toilet","device_id":100,"pet_id":2,'
                           b'"code":"10","ts":1763150000}')
    headers = req["headers"]
    assert headers["Content-Type"] == "application/json"
    assert headers["TTL"] == str(PUSH_TTL) and headers["Urgency"] == "normal"
    assert "Authorization" not in headers and "Cookie" not in headers

    # The OpenAPI schema lists exactly these keys, and every kind.
    payload = json.loads(req["body"])
    _Schemas().check(payload, "PushMessage")
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    assert spec["components"]["schemas"]["PushMessage"]["properties"]["kind"]["enum"] == \
        list(KINDS)

    # A fault is urgent; the offline watchdog's notification has no code.
    await penv.notifier.handle({"kind": "connect", "device_id": 100,
                                "detail": {"online": False}})
    await fake.wait(2)
    second = json.loads(fake.requests[1]["body"])
    assert second["kind"] == KIND_OFFLINE and second["code"] is None
    assert second["pet_id"] is None and isinstance(second["ts"], int)
    assert fake.requests[1]["headers"]["Urgency"] == "high"


def test_a_state_condition_sends_its_key_and_never_the_fault_text():
    state = {"boxFull": 1, "errorMsg": "Motor stalled near the hidden sensor"}
    conditions = rules.active_conditions(7, "t5", state)
    bodies = {key: encode(message(c.notification, TS)) for key, c in conditions.items()}
    assert json.loads(bodies["boxFull"])["code"] == "boxFull"
    fault = next(b for k, b in bodies.items() if k.startswith("errorMsg"))
    assert json.loads(fault) == {"v": 1, "kind": KIND_ERROR, "device_id": 7, "pet_id": None,
                                 "code": "errorMsg", "ts": TS}
    assert b"Motor" not in fault
    # Even an absurd code stays small.
    huge = Notification(kind=KIND_ERROR, device_id="12", title="t", message="m",
                        code="x" * 10_000, pet_id=True, ts=TS)
    body = encode(message(huge, TS))
    assert len(body) < 200 < MAX_MESSAGE_BYTES
    assert json.loads(body)["device_id"] == 12


# --- which phones receive what --------------------------------------------------------

async def test_server_switches_and_each_phones_kinds_both_filter(penv: PushEnv, fake,
                                                                 allow_loopback, tmp_path):
    await penv.config.update({"kinds": {KIND_FEED_SERVED: False}})
    await penv.session("errors only", fake.url("/a"), (KIND_ERROR, KIND_FEED_SERVED))
    await penv.session("everything", fake.url("/b"))
    await penv.session("not registered")
    penv.start()
    await penv.event(_row("10", "t5", event_id="v"))       # toilet
    await penv.event(_row("1", "t5", event_id="e"))        # error
    await penv.event(_row("2", "d4h", event_id="f"))       # feed_served: off on the server
    await penv.settle()
    kinds = {path: [json.loads(r["body"])["kind"] for r in fake.to(path)] for path in ("/a", "/b")}
    assert kinds == {"/a": [KIND_ERROR], "/b": [KIND_TOILET, KIND_ERROR]}


async def test_with_no_sink_at_all_nothing_is_classified_or_counted(penv: PushEnv):
    assert not penv.config.settings.ready
    await penv.session("not registered")
    assert penv.notifier.submit(Notification(kind=KIND_TOILET, device_id=1, title="t",
                                             message="m", dedupe_key="k")) is False
    assert "k" not in penv.notifier._seen  # a later registration still gets it


async def test_dedupe_and_rate_limit_are_shared_with_the_ntfy_topic(penv: PushEnv, fake,
                                                                    allow_loopback):
    ntfy = FakeNtfy()
    server = await ntfy.start()
    try:
        await penv.config.update({"server": server, "enabled": True})
        await penv.session("Pixel", fake.url("/a"))
        penv.start()
        await penv.event(_row("10", "t5", event_id="same"))
        await penv.event(_row("10", "t5", event_id="same"))  # a retried report
        count, _ = RATE_LIMITS[KIND_TOILET]
        for i in range(count + 3):
            await penv.event(_row("10", "t5", event_id=f"v{i}"))
        await penv.settle()
        await ntfy.wait(count)
        await asyncio.sleep(0.05)
        assert len(ntfy.requests) == count
        assert len(fake.to("/a")) == count
    finally:
        await ntfy.server.close()


async def test_a_phone_revoked_while_a_notification_waited_receives_nothing(
        penv: PushEnv, fake, allow_loopback):
    revoked = await penv.session("revoked", fake.url("/gone"))
    await penv.session("kept", fake.url("/kept"))
    await penv.event(_row("10", "t5"))  # queued; the worker is not running yet
    assert await penv.auth.revoke(revoked)
    penv.start()
    await fake.wait(1, "/kept")
    await penv.settle()
    assert fake.to("/gone") == []


async def test_a_registration_dropped_between_fan_out_and_post_is_skipped(
        penv: PushEnv, fake, allow_loopback):
    sid = await penv.session("Pixel", fake.url("/a"))
    note = Notification(kind=KIND_TOILET, device_id=1, title="t", message="m")
    assert penv.notifier.push.dispatch(note) == 1
    await penv.auth.clear_push(sid)  # before the task got to run
    await penv.notifier.push.drain()
    assert fake.requests == []


# --- failures -----------------------------------------------------------------------

async def test_a_slow_or_failing_endpoint_holds_up_nobody(penv: PushEnv, fake, allow_loopback,
                                                         caplog, monkeypatch):
    monkeypatch.setattr(sink_module, "LANE_SIZE", 1)  # so /slow overflows at once
    ntfy = FakeNtfy()
    server = await ntfy.start()
    try:
        await penv.config.update({"server": server, "enabled": True})
        fake.hold["/slow"] = asyncio.Event()
        fake.status["/broken"] = 500
        await penv.session("slow", fake.url("/slow"))
        await penv.session("broken", fake.url("/broken"))
        await penv.session("fine", fake.url("/fine"))
        dead = f"http://127.0.0.1:{unused_port()}/{SECRET}"
        await penv.session("dead", dead)
        penv.start()
        with caplog.at_level(logging.WARNING):
            for i in range(3):
                await penv.event(_row("1", "t5", event_id=f"e{i}", device_id=100 + i))
            # Every other phone, and the ntfy topic, got all three while /slow
            # still holds its first request open; its lane kept one more.
            await fake.wait(3, "/fine")
            await fake.wait(3, "/broken")
            await ntfy.wait(3)
            assert len(fake.to("/slow")) == 1
            assert not penv.worker.done()
            fake.hold["/slow"].set()
            await penv.settle()
            assert len(fake.to("/slow")) == 2  # in order, the third dropped
        ours = " ".join(r.getMessage() for r in caplog.records
                        if r.name.startswith("petkit_local"))
        assert "is not keeping up" in ours and "HTTP 500" in ours
        assert "could not reach 127.0.0.1" in ours
        assert SECRET not in ours
        # All four are still registered: only "gone" unregisters.
        assert len(penv.auth.push_targets()) == 4
    finally:
        await ntfy.server.close()


async def test_an_endpoint_gone_three_times_in_a_row_is_unregistered(penv: PushEnv, fake,
                                                                     allow_loopback, caplog):
    sink = penv.notifier.push
    note = Notification(kind=KIND_TOILET, device_id=1, title="t", message="m")
    sid = await penv.session("Pixel", fake.url(f"/{SECRET}") + f"?k={QUERY_SECRET}")
    path = f"/{SECRET}"

    async def send(status: int) -> None:
        fake.status[path] = status
        assert sink.dispatch(note) == 1
        await sink.drain()

    # A success in between starts the count again.
    for status in (410, 404, 201, 410, 404):
        await send(status)
    assert [s.id for s in penv.auth.push_targets()] == [sid]
    with caplog.at_level(logging.WARNING):
        await send(410)
    assert penv.auth.push_targets() == []
    ours = " ".join(r.getMessage() for r in caplog.records if r.name.startswith("petkit_local"))
    assert "unregistered the endpoint on 127.0.0.1" in ours and "3 times in a row" in ours
    assert SECRET not in ours and QUERY_SECRET not in ours
    assert sink.dispatch(note) == 0  # nothing left to send to
    assert GONE_LIMIT == 3


async def test_a_new_registration_is_not_dropped_for_the_old_ones_failures(penv: PushEnv, fake,
                                                                          allow_loopback):
    sink = penv.notifier.push
    note = Notification(kind=KIND_TOILET, device_id=1, title="t", message="m")
    fake.status["/old"] = 410
    sid = await penv.session("Pixel", fake.url("/old"))
    for _ in range(GONE_LIMIT - 1):
        sink.dispatch(note)
        await sink.drain()
    await penv.auth.set_push(sid, PushRegistration(fake.url("/new"), None, 0.0))
    sink.dispatch(note)
    await sink.drain()
    # The old endpoint's last strike cannot clear the new one.
    assert not await penv.auth.clear_push(sid, fake.url("/old"))
    assert [s.push.endpoint for s in penv.auth.push_targets()] == [fake.url("/new")]


async def test_redirects_are_not_followed(penv: PushEnv, fake, allow_loopback):
    fake.status["/moved"] = 302
    status, reason = await penv.notifier.push.post(fake.url("/moved"), b"{}")
    assert status == 302 and "not followed" in reason
    assert [r["path"] for r in fake.requests] == ["/moved"]


async def test_this_machine_is_refused_at_connect_time_too(penv: PushEnv, fake):
    """No `allow_loopback` here: the real fence."""
    sink = penv.notifier.push
    status, reason = await sink.post(fake.url("/x"), b"{}")
    assert status is None and "never sent to" in reason
    # A NAME resolving to loopback passes registration's literal check, and is
    # stopped when it is dialled.
    port = fake.server.port
    status, reason = await sink.post(f"http://localhost:{port}/x", b"{}")
    assert status is None and "could not reach localhost" in reason
    resolver = GuardedResolver()
    try:
        with pytest.raises(OSError):
            await resolver.resolve("localhost", port)
    finally:
        await resolver.close()
    assert fake.requests == []


# --- secrecy in the panel -----------------------------------------------------------

async def test_the_panel_shows_each_phones_push_host_and_never_the_endpoint(tmp_path):
    auth = AppAuth(tmp_path / FILENAME, scrypt_params=FAST)
    await auth.set_password(PASSWORD)
    _, with_push = await auth.create_session("Pixel")
    await auth.create_session("Tablet")
    endpoint = f"https://ntfy.example.org/{SECRET}?up=1&k={QUERY_SECRET}"
    await auth.set_push(with_push.id, PushRegistration(check_endpoint(endpoint), None, 1.0))
    app = create_panel_app(DeviceRegistry(), None, EventHub(), {}, app_auth=auth)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        r = await client.get("/api/app-access")
        text = await r.text()
        pushes = {s["name"]: s["push"] for s in json.loads(text)["sessions"]}
        assert pushes == {"Pixel": {"host": "ntfy.example.org", "kinds": None, "registered": 1.0},
                          "Tablet": None}
        assert SECRET not in text and QUERY_SECRET not in text
        # Revoking from the panel stops that phone's pushes at once.
        r = await client.delete(f"/api/app-access/sessions/{with_push.id}")
        assert r.status == 200 and auth.push_targets() == []
    finally:
        await client.close()


# --- OpenAPI ------------------------------------------------------------------------

def test_the_openapi_file_documents_the_push_routes_and_message():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    route = spec["paths"]["/push"]
    assert set(route) == {"put", "delete"}
    callback = route["put"]["callbacks"]["pushMessage"]["{$request.body#/endpoint}"]["post"]
    schema = callback["requestBody"]["content"]["application/json"]["schema"]
    assert schema == {"$ref": "#/components/schemas/PushMessage"}
    documented = spec["components"]["schemas"]["PushMessage"]
    assert set(documented["properties"]) == set(documented["required"]) == set(
        message(Notification(kind=KIND_TOILET, device_id=1, title="", message=""), TS))
    request = spec["components"]["schemas"]["PushRegistrationRequest"]
    assert request["properties"]["endpoint"]["maxLength"] == push.MAX_ENDPOINT_LENGTH
