"""The phone app API (`app_api/`): its access policy, its sessions, its isolation.

What is being held here, in the order the issue that introduced it asks:

* no password set -> 401 on every route, public ones and unknown paths included;
* a wrong password is refused AND slowed (the address is locked out, doubling),
  the right one yields a token that works until logout, revocation, a password
  change, or 90 idle days — measured on a fake clock, not slept through;
* the app may change the password only with the current one, which revokes
  every other session; a reset from the panel revokes them all;
* `X-Forwarded-For` / `CF-Connecting-IP` count only from a trusted proxy;
* nothing of the panel or the device API answers on the app listener, and
  nothing of the app API answers on theirs;
* media cannot be read outside the media tree;
* the OpenAPI file describes exactly the registered routes, and every
  response here has exactly the keys its schema lists.
"""
from __future__ import annotations

import json
import os
import re
import socket
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest
import yaml
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, make_mocked_request, unused_port

from petkit_local.app_api import serializers
from petkit_local.app_api.auth import (
    FILENAME, LAST_USED_WRITE_INTERVAL, MAX_SESSIONS, SESSION_IDLE_LIFETIME, AppAuth,
    ScryptParams, clean_device_name, hash_token,
)
from petkit_local.app_api.handlers import timeline as timeline_handlers
from petkit_local.app_api.handlers.meta import OPENAPI_PATH
from petkit_local.app_api.middleware import CREDENTIALS, PUBLIC, access_of
from petkit_local.app_api.server import PREFIX, ROUTES, create_app_api
from petkit_local.app_api.throttle import LoginThrottle, client_address, parse_networks
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.http.appkeys import APP_API_RUNNER
from petkit_local.http.server import create_app
from petkit_local.main.lifecycle import _start_app_api
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app

#: Cheap enough for a test suite; the stored record carries its own parameters.
FAST = ScryptParams(n=2 ** 10, r=8, p=1)
PASSWORD = "correct horse battery"
JPEG = b"\xff\xd8\xff\xe0" + b"J" * 64
START = 1_800_000_000.0


class Clock:
    """One fake clock for the session store and the throttle."""

    def __init__(self, now: float = START) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Env:
    """An app API over a real store, registry, pet registry and media tree."""

    def __init__(self, tmp: Path, store: Any, pet_registry: Any) -> None:
        self.tmp = tmp
        self.clock = Clock()
        self.auth = AppAuth(tmp / FILENAME, clock=self.clock, scrypt_params=FAST)
        self.throttle = LoginThrottle(clock=self.clock)
        self.registry = DeviceRegistry()
        self.device = self.registry.get_or_create(petkit_id=100, device_type="t5",
                                                  serial_number="SN100")
        self.store = store
        self.pets = pet_registry
        self.media_root = tmp / "media"
        self.media_root.mkdir()
        self.app = create_app_api(self.auth, self.registry, event_store=store,
                                  pet_registry=pet_registry, media_root=str(self.media_root),
                                  data_dir=str(tmp), throttle=self.throttle)
        self.client: TestClient | None = None

    async def start(self) -> TestClient:
        self.client = TestClient(TestServer(self.app))
        await self.client.start_server()
        return self.client

    async def login(self, password: str = PASSWORD, name: str = "Pixel", **kw: Any) -> str:
        r = await self.client.post(f"{PREFIX}/auth/login",
                                   json={"password": password, "device_name": name}, **kw)
        assert r.status == 200, await r.text()
        return (await r.json())["token"]

    async def get(self, path: str, token: str | None = None, **kw: Any):
        headers = kw.pop("headers", {})
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return await self.client.get(PREFIX + path, headers=headers, **kw)


@pytest.fixture
async def env(tmp_path: Path, event_store, pet_registry):
    e = Env(tmp_path, event_store, pet_registry)
    await e.start()
    yield e
    await e.client.close()


@pytest.fixture
async def ready(env: Env) -> Env:
    """An env with the password set."""
    await env.auth.set_password(PASSWORD)
    return env


def _concrete(path: str) -> str:
    """A route template with every variable filled with something plausible."""
    return re.sub(r"\{[^}]+\}", "1", path)


# --- no password: closed --------------------------------------------------------

async def test_with_no_password_every_route_and_every_unknown_path_is_401(env: Env):
    assert not env.auth.password_set
    probes = [(m, PREFIX + _concrete(p)) for m, p, _ in ROUTES]
    probes += [("GET", "/app/v1/nope"), ("GET", "/"), ("POST", "/app/v1/devices"),
               ("GET", "/api/devices")]
    for method, path in probes:
        r = await env.client.request(method, path, json={"password": PASSWORD})
        assert r.status == 401, (method, path)
        body = await r.json()
        assert body["error"] == "password_not_set", (method, path)
        assert "panel" in body["message"]


async def test_a_stored_password_record_that_cannot_be_read_keeps_the_api_closed(tmp_path):
    (tmp_path / FILENAME).write_text(json.dumps({"password": {"algorithm": "md5"}}))
    assert not AppAuth(tmp_path / FILENAME, scrypt_params=FAST).password_set


# --- login, throttling ----------------------------------------------------------

async def test_a_wrong_password_is_refused_and_the_address_is_locked_out(ready: Env):
    r = await ready.client.post(f"{PREFIX}/auth/login", json={"password": "wrong-wrong"})
    assert r.status == 401
    assert (await r.json())["error"] == "invalid_password"
    assert r.headers["Retry-After"] == "1"

    # Locked: even the RIGHT password is not checked until the lockout passes.
    r = await ready.client.post(f"{PREFIX}/auth/login", json={"password": PASSWORD})
    assert r.status == 429
    assert (await r.json())["error"] == "too_many_attempts"
    assert int(r.headers["Retry-After"]) >= 1

    ready.clock.advance(1.5)
    r = await ready.client.post(f"{PREFIX}/auth/login", json={"password": "wrong-again"})
    assert r.status == 401
    assert r.headers["Retry-After"] == "2"  # doubled
    ready.clock.advance(2.5)
    assert await ready.login()


async def test_lockouts_double_up_to_the_cap_and_a_success_clears_them():
    clock = Clock()
    throttle = LoginThrottle(clock=clock)
    delays = []
    for _ in range(14):
        delays.append(throttle.failure("198.51.100.7"))
    assert delays[:5] == [1, 2, 4, 8, 16]
    assert delays[-1] == 15 * 60
    assert throttle.retry_after("198.51.100.7") == 15 * 60
    assert throttle.retry_after("198.51.100.8") == 0
    throttle.success("198.51.100.7")
    assert throttle.retry_after("198.51.100.7") == 0


def test_the_throttle_is_bounded_and_has_a_global_ceiling():
    clock = Clock()
    throttle = LoginThrottle(clock=clock, max_tracked=10, global_max_failures=50)
    for i in range(49):
        throttle.failure(f"10.0.{i // 250}.{i % 250}")
    assert len(throttle) == 10
    assert throttle.retry_after("192.0.2.1") == 0
    throttle.failure("10.9.9.9")  # the 50th failure anywhere
    assert throttle.retry_after("192.0.2.1") > 0  # now everyone waits
    clock.advance(11 * 60)
    assert throttle.retry_after("192.0.2.1") == 0


async def test_login_needs_a_json_object(ready: Env):
    r = await ready.client.post(f"{PREFIX}/auth/login", data="password=x")
    assert r.status == 415
    r = await ready.client.post(f"{PREFIX}/auth/login", data="[1]",
                                headers={"Content-Type": "application/json"})
    assert r.status == 400
    assert (await r.json())["error"] == "bad_json"


# --- tokens -------------------------------------------------------------------

async def test_the_token_works_until_logout(ready: Env):
    token = await ready.login()
    r = await ready.get("/devices", token)
    assert r.status == 200
    assert [d["id"] for d in (await r.json())["devices"]] == [100]

    r = await ready.client.post(f"{PREFIX}/auth/logout",
                                headers={"Authorization": f"Bearer {token}"})
    assert r.status == 200
    r = await ready.get("/devices", token)
    assert r.status == 401
    assert (await r.json())["error"] == "invalid_token"
    assert r.headers["WWW-Authenticate"].startswith("Bearer")


async def test_a_missing_malformed_or_unknown_token_is_401(ready: Env):
    await ready.login()
    for headers in ({}, {"Authorization": "Basic Zm9vOmJhcg=="},
                    {"Authorization": "Bearer"}, {"Authorization": "Bearer not-a-token"}):
        r = await ready.client.get(f"{PREFIX}/devices", headers=headers)
        assert r.status == 401, headers


async def test_a_token_slides_while_used_and_expires_after_90_idle_days(ready: Env):
    token = await ready.login()
    for _ in range(3):  # 3 x 89 days, used each time: well past 90 days in total
        ready.clock.advance(89 * 86400)
        assert (await ready.get("/devices", token)).status == 200
    ready.clock.advance(SESSION_IDLE_LIFETIME + 1)
    assert (await ready.get("/devices", token)).status == 401
    assert ready.auth.sessions() == []


async def test_last_used_is_written_at_most_every_few_minutes(ready: Env):
    token = await ready.login()
    path = ready.tmp / FILENAME
    stored = json.loads(path.read_text())["sessions"][0]["last_used"]
    ready.clock.advance(LAST_USED_WRITE_INTERVAL - 10)
    assert (await ready.get("/devices", token)).status == 200
    assert json.loads(path.read_text())["sessions"][0]["last_used"] == stored
    ready.clock.advance(20)
    assert (await ready.get("/devices", token)).status == 200
    assert json.loads(path.read_text())["sessions"][0]["last_used"] == ready.clock.now


async def test_sessions_survive_a_restart_and_the_file_holds_no_secret(ready: Env):
    token = await ready.login()
    raw = (ready.tmp / FILENAME).read_text()
    assert token not in raw and PASSWORD not in raw
    assert hash_token(token) in raw
    if os.name == "posix":
        assert (ready.tmp / FILENAME).stat().st_mode & 0o077 == 0

    reborn = AppAuth(ready.tmp / FILENAME, clock=ready.clock, scrypt_params=FAST)
    assert reborn.password_set
    assert (await reborn.authenticate(token)) is not None
    assert await reborn.verify_password(PASSWORD)
    assert not await reborn.verify_password(PASSWORD + "x")
    assert not await reborn.verify_password(None)


async def test_sessions_are_capped_dropping_the_least_recently_used(ready: Env):
    first, _ = await ready.auth.create_session("oldest")
    for i in range(MAX_SESSIONS):
        ready.clock.advance(1)
        await ready.auth.create_session(f"phone {i}")
    assert len(ready.auth.sessions()) == MAX_SESSIONS
    assert await ready.auth.authenticate(first) is None


def test_a_device_name_is_cleaned_before_it_is_stored():
    assert clean_device_name("  Pixel\n8\x00 Pro‮ ") == "Pixel 8 Pro"
    assert clean_device_name(None) == "Unnamed device"
    assert len(clean_device_name("x" * 500)) == 64


# --- password changes -----------------------------------------------------------

async def test_the_app_changes_the_password_only_with_the_current_one(ready: Env):
    mine = await ready.login(name="mine")
    ready.clock.advance(5)
    other = await ready.login(name="other")
    auth_header = {"Authorization": f"Bearer {mine}"}

    r = await ready.client.post(f"{PREFIX}/auth/password", headers=auth_header,
                                json={"current_password": "not it at all",
                                      "new_password": "a brand new one"})
    assert r.status == 401
    assert (await r.json())["error"] == "invalid_password"
    assert (await ready.get("/devices", other)).status == 200  # nothing changed

    ready.clock.advance(5)
    r = await ready.client.post(f"{PREFIX}/auth/password", headers=auth_header,
                                json={"current_password": PASSWORD, "new_password": "short"})
    assert r.status == 400
    assert (await r.json())["error"] == "password_rejected"

    r = await ready.client.post(f"{PREFIX}/auth/password", headers=auth_header,
                                json={"current_password": PASSWORD,
                                      "new_password": "a brand new one"})
    assert r.status == 200
    assert (await r.json())["revoked_sessions"] == 1
    assert (await ready.get("/devices", mine)).status == 200   # the caller stays
    assert (await ready.get("/devices", other)).status == 401  # everyone else goes

    r = await ready.client.post(f"{PREFIX}/auth/login", json={"password": PASSWORD})
    assert r.status == 401
    ready.clock.advance(5)
    assert await ready.login("a brand new one")


async def test_a_stolen_token_alone_cannot_change_the_password(ready: Env):
    token = await ready.login()
    r = await ready.client.post(f"{PREFIX}/auth/password",
                                headers={"Authorization": f"Bearer {token}"},
                                json={"new_password": "attacker's choice"})
    assert r.status == 401
    assert await ready.auth.verify_password(PASSWORD)


# --- the panel side -------------------------------------------------------------

@pytest.fixture
async def panel(env: Env):
    app = create_panel_app(env.registry, None, EventHub(), {"app_port": 8097},
                           app_auth=env.auth)
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client
    await client.close()


async def test_the_panel_sets_the_password_and_never_shows_it(env: Env, panel: TestClient):
    r = await panel.get("/api/app-access")
    body = await r.json()
    assert body["password_set"] is False and body["app_port"] == 8097

    r = await panel.post("/api/app-access/password", json={"password": "short"})
    assert r.status == 400

    r = await panel.post("/api/app-access/password", json={"password": PASSWORD})
    assert r.status == 200
    text = await r.text()
    assert PASSWORD not in text and "hash" not in text and "salt" not in text
    assert (await r.json())["password_set"] is True
    assert await env.login()


async def test_a_panel_reset_revokes_every_session(env: Env, panel: TestClient):
    await panel.post("/api/app-access/password", json={"password": PASSWORD})
    a = await env.login(name="a")
    env.clock.advance(1)
    b = await env.login(name="b")
    names = [s["name"] for s in (await (await panel.get("/api/app-access")).json())["sessions"]]
    assert names == ["b", "a"]

    r = await panel.post("/api/app-access/password", json={"password": "another password"})
    assert (await r.json())["revoked"] == 2
    for token in (a, b):
        assert (await env.get("/devices", token)).status == 401


async def test_the_panel_revokes_one_session_or_all(env: Env, panel: TestClient):
    await panel.post("/api/app-access/password", json={"password": PASSWORD})
    a = await env.login(name="a")
    env.clock.advance(1)
    b = await env.login(name="b")
    sid = next(s.id for s in env.auth.sessions() if s.name == "a")

    assert (await panel.delete(f"/api/app-access/sessions/{sid}")).status == 200
    assert (await env.get("/devices", a)).status == 401
    assert (await env.get("/devices", b)).status == 200
    assert (await panel.delete("/api/app-access/sessions/nope")).status == 404

    assert (await (await panel.delete("/api/app-access/sessions")).json())["revoked"] == 1
    assert (await env.get("/devices", b)).status == 401


async def test_removing_the_password_closes_the_api(env: Env, panel: TestClient):
    await panel.post("/api/app-access/password", json={"password": PASSWORD})
    token = await env.login()
    assert (await panel.delete("/api/app-access/password")).status == 200
    r = await env.get("/devices", token)
    assert r.status == 401
    assert (await r.json())["error"] == "password_not_set"


async def test_the_panel_validates_trusted_proxies(env: Env, panel: TestClient):
    r = await panel.post("/api/app-access/trusted-proxies",
                         json={"trusted_proxies": ["10.0.0.0/8", "not-an-ip"]})
    assert r.status == 400
    assert "not-an-ip" in (await r.json())["error"]
    assert env.auth.trusted_proxies == []
    r = await panel.post("/api/app-access/trusted-proxies",
                         json={"trusted_proxies": ["192.168.1.10", "fd00::/8"]})
    assert (await r.json())["trusted_proxies"] == ["192.168.1.10/32", "fd00::/8"]


async def test_the_panel_without_an_app_api_says_so():
    app = create_panel_app(DeviceRegistry(), None, EventHub(), {})
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        assert (await client.get("/api/app-access")).status == 503
    finally:
        await client.close()


# --- forwarded headers ----------------------------------------------------------

def _request_from(peer: str, headers: dict[str, str]) -> web.Request:
    class Transport:
        def get_extra_info(self, name, default=None):
            return (peer, 4321) if name == "peername" else default

    return make_mocked_request("POST", "/app/v1/auth/login", headers=headers,
                               transport=Transport())


def test_forwarded_headers_are_ignored_from_a_peer_that_is_not_trusted():
    forged = {"X-Forwarded-For": "1.2.3.4", "CF-Connecting-IP": "5.6.7.8"}
    assert client_address(_request_from("203.0.113.9", forged), []) == "203.0.113.9"
    trusted = parse_networks(["10.0.0.0/8"])
    assert client_address(_request_from("203.0.113.9", forged), trusted) == "203.0.113.9"


def test_a_trusted_proxy_names_the_client():
    trusted = parse_networks(["10.0.0.0/8", "172.16.0.5"])
    # CF-Connecting-IP first: Cloudflare overwrites it.
    assert client_address(_request_from("10.1.1.1", {"CF-Connecting-IP": "5.6.7.8",
                                                     "X-Forwarded-For": "1.2.3.4"}),
                          trusted) == "5.6.7.8"
    # Otherwise the right-most hop no trusted proxy accounts for; whatever the
    # client wrote to its left is not believed.
    xff = {"X-Forwarded-For": "6.6.6.6, 198.51.100.4, 172.16.0.5"}
    assert client_address(_request_from("10.1.1.1", xff), trusted) == "198.51.100.4"
    # Garbage stops the walk; the proxy itself is charged.
    assert client_address(_request_from("10.1.1.1", {"X-Forwarded-For": "1.2.3.4, junk"}),
                          trusted) == "10.1.1.1"
    # IPv4-mapped IPv6 peers are matched as IPv4.
    assert client_address(_request_from("::ffff:10.2.2.2", {"CF-Connecting-IP": "5.6.7.8"}),
                          trusted) == "5.6.7.8"


async def test_forged_forwarding_does_not_dodge_the_lockout(ready: Env):
    """Untrusted: every attempt is charged to the real peer, whatever it claims."""
    r = await ready.client.post(f"{PREFIX}/auth/login", json={"password": "wrong-wrong"},
                                headers={"X-Forwarded-For": "1.1.1.1"})
    assert r.status == 401
    r = await ready.client.post(f"{PREFIX}/auth/login", json={"password": PASSWORD},
                                headers={"X-Forwarded-For": "2.2.2.2",
                                         "CF-Connecting-IP": "3.3.3.3"})
    assert r.status == 429


async def test_behind_a_trusted_proxy_each_client_has_its_own_lockout(ready: Env):
    await ready.auth.set_trusted_proxies(["127.0.0.1", "::1"])
    r = await ready.client.post(f"{PREFIX}/auth/login", json={"password": "wrong-wrong"},
                                headers={"X-Forwarded-For": "1.1.1.1"})
    assert r.status == 401
    r = await ready.client.post(f"{PREFIX}/auth/login", json={"password": PASSWORD},
                                headers={"X-Forwarded-For": "1.1.1.1"})
    assert r.status == 429
    assert await ready.login(headers={"X-Forwarded-For": "2.2.2.2"})


# --- isolation ------------------------------------------------------------------

async def test_no_panel_or_device_route_is_reachable_on_the_app_listener(ready: Env):
    token = await ready.login()
    assert all(r.resource.canonical.startswith(PREFIX + "/")
               for r in ready.app.router.routes())
    for path in ("/", "/api/devices", "/api/info", "/api/settings", "/static/js/main.js",
                 "/api/devices/100/patcher", "/6/t5/dev_signup", "/6/t5/dev_device_info",
                 "/faces/x.jpg", "/app/v1/../api/devices", "/app/v2/devices"):
        r = await ready.client.get(path, headers={"Authorization": f"Bearer {token}"})
        assert r.status == 404, path
        assert (await r.json())["error"] == "not_found"


async def test_no_app_route_is_reachable_on_the_panel_or_the_device_api(tmp_path):
    registry = DeviceRegistry()
    panel = create_panel_app(registry, None, EventHub(), {})
    device_api = create_app(registry, {"api_url": "http://server/6/", "mqtt_port": 1883,
                                       "proxy_mode": False, "proxy_upstream": "",
                                       "proxy_block_run_cmd": True})
    for app in (panel, device_api):
        assert not any(r.resource.canonical.startswith("/app/") for r in app.router.routes())

    client = TestClient(TestServer(panel))
    await client.start_server()
    try:
        for method, path, _ in ROUTES:
            r = await client.request(method, PREFIX + _concrete(path))
            assert r.status in (403, 404, 405), path
    finally:
        await client.close()

    # The device API never 404s a device, so it answers — but with its own
    # catch-all, never anything of the app API's.
    client = TestClient(TestServer(device_api))
    await client.start_server()
    try:
        r = await client.get(f"{PREFIX}/devices")
        body = await r.text()
        assert "devices" not in body and "invalid_token" not in body
    finally:
        await client.close()


# --- media ------------------------------------------------------------------------

async def test_media_needs_a_token_and_never_escapes_the_media_tree(ready: Env):
    (ready.media_root / "T5" / "Snapshots").mkdir(parents=True)
    (ready.media_root / "T5" / "Snapshots" / "a.jpg").write_bytes(JPEG)
    outside = ready.tmp / "media-evil"
    outside.mkdir()
    (outside / "x.jpg").write_bytes(JPEG)
    (ready.tmp / "secret.txt").write_text("nope")

    assert (await ready.get("/media/T5/Snapshots/a.jpg")).status == 401
    token = await ready.login()
    r = await ready.get("/media/T5/Snapshots/a.jpg", token)
    assert r.status == 200 and await r.read() == JPEG
    r = await ready.get("/media/thumb/T5/Snapshots/a.jpg", token)
    assert r.status == 200 and await r.read() == JPEG

    for evil in ("../secret.txt", "%2e%2e/secret.txt", "..%2fsecret.txt",
                 "../media-evil/x.jpg", "%2Fetc%2Fhosts", "T5/Snapshots",
                 "T5/../../secret.txt", "thumb/../../secret.txt"):
        r = await ready.client.get(f"{PREFIX}/media/{evil}",
                                   headers={"Authorization": f"Bearer {token}"})
        assert r.status == 404, evil
        assert b"nope" not in await r.read()
        r = await ready.client.get(f"{PREFIX}/media/thumb/{evil}",
                                   headers={"Authorization": f"Bearer {token}"})
        assert r.status == 404, evil


# --- OpenAPI ----------------------------------------------------------------------

def _spec() -> dict[str, Any]:
    return yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))


def _openapi_path(aiohttp_path: str) -> str:
    return re.sub(r"\{(\w+):[^}]*\}", r"{\1}", aiohttp_path)


def test_the_openapi_file_describes_exactly_the_registered_routes():
    spec = _spec()
    assert spec["openapi"].startswith("3.1")
    assert spec["servers"] == [{"url": PREFIX}]
    documented = {(method.upper(), path) for path, ops in spec["paths"].items()
                  for method in ops if method in ("get", "post", "put", "patch", "delete")}
    app = create_app_api(AppAuth("/nonexistent/x.json"), DeviceRegistry())
    registered = {(r.method, _openapi_path(r.resource.canonical).removeprefix(PREFIX))
                  for r in app.router.routes()}
    assert documented == registered


def test_the_openapi_file_marks_exactly_the_routes_that_need_no_session():
    spec = _spec()
    open_routes = {(m, p) for m, p, h in ROUTES if access_of(h) in (PUBLIC, CREDENTIALS)}
    documented = {(method.upper(), path) for path, ops in spec["paths"].items()
                  for method, op in ops.items() if {} in (op.get("security") or [])}
    assert {(m, _openapi_path(p)) for m, p in open_routes} == documented
    assert spec["security"] == [{"bearer": []}]


async def test_the_openapi_file_is_served_publicly_once_a_password_exists(ready: Env):
    r = await ready.get("/openapi.yaml")
    assert r.status == 200
    assert r.headers["Content-Type"].startswith("application/yaml")
    assert yaml.safe_load(await r.text())["info"]["title"]


class _Schemas:
    """Checks a response against the schema of the same name: keys, exactly."""

    def __init__(self) -> None:
        self.schemas = _spec()["components"]["schemas"]

    def _resolve(self, schema: dict[str, Any]) -> dict[str, Any]:
        if "$ref" in schema:
            return self._resolve(self.schemas[schema["$ref"].rsplit("/", 1)[1]])
        if "allOf" in schema:
            merged: dict[str, Any] = {"required": [], "properties": {}}
            for part in schema["allOf"]:
                part = self._resolve(part)
                merged["required"] += part.get("required", [])
                merged["properties"].update(part.get("properties", {}))
            return merged
        return schema

    def check(self, value: Any, name: str) -> None:
        self._check(value, {"$ref": f"#/{name}"}, name)

    def _check(self, value: Any, schema: dict[str, Any], where: str) -> None:
        if "oneOf" in schema:
            if value is None:
                return
            schema = next(s for s in schema["oneOf"] if s.get("type") != "null")
        schema = self._resolve(schema)
        if value is None:
            return
        if "properties" in schema:
            assert isinstance(value, dict), where
            assert set(value) == set(schema["properties"]), (where, set(value) ^ set(schema["properties"]))
            assert set(schema.get("required", [])) <= set(value), where
            for key, sub in schema["properties"].items():
                self._check(value[key], sub, f"{where}.{key}")
        elif schema.get("type") == "array" or "items" in schema:
            for i, item in enumerate(value or []):
                self._check(item, schema.get("items", {}), f"{where}[{i}]")


async def _seed_history(env: Env, pet_id: int | None = None) -> None:
    base = START - 10 * 86400
    for i in range(12):
        await env.store.upsert_event({
            "device_id": 100, "device_type": "t5", "event_type": "error_start",
            "event_kind": "error", "ts": base + i * 7200, "related_event": f"err{i}"})
    for i in range(4):
        ts = base + 30 * 3600 + i * 7200 + 1800
        await env.store.upsert_event({
            "device_id": 100, "device_type": "t5", "event_type": "pet_out",
            "event_kind": "toilet_visit", "ts": ts, "related_event": f"v{i}",
            "pet_id": pet_id, "pet_ref": pet_id,
            "content_json": json.dumps({"pet_weight": 4200 + i})})


async def test_every_response_has_exactly_its_schemas_keys(ready: Env):
    schemas = _Schemas()
    pet = await ready.pets.create("Appka", device_ids=[100], weight=4.2)
    await ready.pets.add_face(pet["id"], JPEG)
    await _seed_history(ready, pet["id"])
    token = await ready.login()

    info = await (await ready.get("/info", token)).json()
    schemas.check(info, "Info")
    assert info["authenticated"] and info["server_version"]

    devices = (await (await ready.get("/devices", token)).json())["devices"]
    for d in devices:
        schemas.check(d, "DeviceSummary")
    detail = (await (await ready.get("/devices/100", token)).json())["device"]
    schemas.check(detail, "DeviceDetail")
    assert detail["entities"] and all(e["component"] in serializers.VALUE_COMPONENTS
                                      for e in detail["entities"])
    for e in detail["entities"]:
        schemas.check(e, "Entity")

    page = await (await ready.get("/timeline?limit=100", token)).json()
    schemas.check(page, "TimelinePage")
    assert len(page["items"]) == 16
    event = (await (await ready.get(f"/timeline/{page['items'][0]['id']}", token)).json())["event"]
    schemas.check(event, "EventDetail")

    pets = (await (await ready.get("/pets", token)).json())["pets"]
    for p in pets:
        schemas.check(p, "Pet")
    assert pets[0]["photo"] == f"{PREFIX}/pets/{pet['id']}/photo"
    pet_detail = (await (await ready.get(f"/pets/{pet['id']}", token)).json())["pet"]
    schemas.check(pet_detail, "PetDetail")
    r = await ready.get(f"/pets/{pet['id']}/photo", token)
    assert r.status == 200 and await r.read() == JPEG


async def test_info_says_little_without_a_token(ready: Env):
    r = await ready.get("/info")
    assert r.status == 200
    body = await r.json()
    assert body["authenticated"] is False
    assert body["server_version"] is None and body["session"] is None


# --- devices, timeline, pets ------------------------------------------------------

async def test_unknown_or_malformed_ids_are_404(ready: Env):
    token = await ready.login()
    for path in ("/devices/999", "/devices/abc", "/devices/1e3", "/timeline/999",
                 "/timeline/x", "/pets/999", "/pets/999/photo"):
        r = await ready.get(path, token)
        assert r.status == 404, path


async def test_the_timeline_pages_by_cursor_without_gaps_or_repeats(ready: Env):
    await _seed_history(ready)
    token = await ready.login()
    everything = [i["id"] for i in (await (await ready.get("/timeline?limit=100", token)).json())["items"]]
    assert len(everything) == 16

    seen: list[int] = []
    cursor = None
    while True:
        query = "/timeline?limit=5" + (f"&cursor={cursor}" if cursor else "")
        page = await (await ready.get(query, token)).json()
        seen += [i["id"] for i in page["items"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == everything


async def test_a_dense_window_holds_back_cards_it_cannot_see_whole(ready: Env, monkeypatch):
    """With the row budget exhausted, a card is only served once its rows are."""
    await _seed_history(ready)
    token = await ready.login()
    everything = [i["id"] for i in (await (await ready.get("/timeline?limit=100", token)).json())["items"]]
    monkeypatch.setattr(timeline_handlers, "ROW_BUDGET", 5)

    seen: list[int] = []
    cursor = None
    for _ in range(50):
        query = "/timeline?limit=3" + (f"&cursor={cursor}" if cursor else "")
        page = await (await ready.get(query, token)).json()
        seen += [i["id"] for i in page["items"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert len(seen) == len(set(seen))
    assert set(seen) <= set(everything)


async def test_a_session_straddling_a_page_boundary_is_never_split(ready: Env):
    """A visit's cleaning arrives minutes after it; paging one card at a time
    must not hand that cleaning out again as a card of its own."""
    t = START - 3600
    await ready.store.upsert_event({"device_id": 100, "device_type": "t5", "event_type": "pet_out",
                                    "event_kind": "toilet_visit", "ts": t, "related_event": "v"})
    await ready.store.upsert_event({"device_id": 100, "device_type": "t5",
                                    "event_type": "clean_over", "event_kind": "cleaning",
                                    "ts": t + 240})
    await ready.store.upsert_event({"device_id": 100, "device_type": "t5",
                                    "event_type": "error_start", "event_kind": "error",
                                    "ts": t + 120, "related_event": "e"})
    token = await ready.login()
    whole = (await (await ready.get("/timeline", token)).json())["items"]

    paged: list[dict] = []
    cursor = None
    while True:
        page = await (await ready.get("/timeline?limit=1" + (f"&cursor={cursor}" if cursor else ""),
                                      token)).json()
        paged += page["items"]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert [(i["id"], len(i["steps"])) for i in paged] == [(i["id"], len(i["steps"])) for i in whole]


async def test_timeline_filters_and_rejects_bad_parameters(ready: Env):
    await _seed_history(ready)
    token = await ready.login()
    faults = (await (await ready.get("/timeline?kind=fault&limit=100", token)).json())["items"]
    assert len(faults) == 12 and all("fault" in i["filters"] for i in faults)
    assert (await (await ready.get("/timeline?device=999", token)).json())["items"] == []
    for bad in ("kind=nope", "day=2026-13-40", "cursor=!!!", "cursor=WzFd", "limit=x"):
        r = await ready.get(f"/timeline?{bad}", token)
        assert r.status == 400, bad


async def test_a_pet_shows_its_weight_and_recent_visits(ready: Env):
    pet = await ready.pets.create("Appka", device_ids=[100], weight=4.2)
    now = time.time()
    await ready.store.upsert_event({
        "device_id": 100, "device_type": "t5", "event_type": "pet_out",
        "event_kind": "toilet_visit", "ts": now - 600, "related_event": "recent",
        "pet_id": pet["id"], "pet_ref": pet["id"],
        "content_json": json.dumps({"pet_weight": 4300})})
    token = await ready.login()
    detail = (await (await ready.get(f"/pets/{pet['id']}", token)).json())["pet"]
    assert detail["entered_weight_g"] == pytest.approx(4200)
    assert detail["photo"] is None
    assert [v["pet"] for v in detail["recent_visits"]] == [{"id": pet["id"], "name": "Appka"}]
    assert (await ready.get(f"/pets/{pet['id']}/photo", token)).status == 404


# --- the listener -------------------------------------------------------------------

def _services(env: Env, port: int) -> Any:
    return SimpleNamespace(
        config=SimpleNamespace(app_port=port, data_dir=str(env.tmp)),
        app_auth=env.auth, registry=env.registry, event_store=env.store,
        pet_registry=env.pets, media_root=str(env.media_root),
        mqtt_bridge=None, ha_publisher=None, hub=EventHub(), go2rtc=None, notifier=None,
        ble_registry=None)


async def test_the_listener_serves_the_app_api_and_shuts_down(env: Env):
    host = web.Application()
    port = unused_port()
    await _start_app_api(_services(env, port), host)
    runner = host[APP_API_RUNNER]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}{PREFIX}/info") as r:
                assert r.status == 401
                assert (await r.json())["error"] == "password_not_set"
    finally:
        await runner.cleanup()


async def test_a_port_already_taken_is_logged_and_nothing_else_stops(env: Env, caplog):
    with socket.socket() as taken:
        taken.bind(("0.0.0.0", 0))
        taken.listen()
        host = web.Application()
        await _start_app_api(_services(env, taken.getsockname()[1]), host)
    assert APP_API_RUNNER not in host
    assert "App API could not listen" in caplog.text


def test_the_app_api_imports_nothing_from_the_panel_or_the_device_api():
    """It reads the shared stores and helpers; the panel and `http/` stay out."""
    package = Path(serializers.__file__).parent
    for source in package.rglob("*.py"):
        text = source.read_text(encoding="utf-8")
        assert not re.search(r"petkit_local\.(web|http)\b", text), source.name
