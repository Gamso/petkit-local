"""The panel refuses a write another web site made the browser send.

The panel has no login (SECURITY.md), so the only thing between a hostile page
and `api/settings` is whether the user's browser will carry the request. These
pin the three ways it used to: a blind `text/plain` POST, a cross-origin
WebSocket, and a JSON body sent under a content type that needs no preflight.
They also pin what must keep working — Ingress, a direct LAN port, and curl.
"""
import json

import pytest
from aiohttp import WSServerHandshakeError
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app

JSON = {"Content-Type": "application/json"}


async def _client():
    live = {"proxy_mode": False}
    app = create_panel_app(DeviceRegistry(), BLERegistry(), EventHub(),
                           {"api_url": "http://x/6/", "capture_dir": "/nope"},
                           live_config=live)
    c = TestClient(TestServer(app))
    await c.start_server()
    return c, live


def _own_origin(c) -> str:
    return f"http://{c.server.host}:{c.server.port}"


async def test_a_blind_cross_site_post_cannot_turn_proxy_mode_on():
    """The attack itself: a form-style POST no CORS rule stops from being SENT."""
    c, live = await _client()
    try:
        r = await c.post("/api/settings", data=json.dumps({"proxy_mode": True}),
                         headers={"Origin": "https://evil.example",
                                  "Content-Type": "text/plain"})
        assert r.status == 403
        assert (await r.json())["error"]
        assert live["proxy_mode"] is False
    finally:
        await c.close()


@pytest.mark.parametrize("origin", [
    "https://evil.example",
    "null",                       # sandboxed iframe, data: page
    "http://127.0.0.1.evil.example",
])
async def test_a_cross_site_write_is_refused_whatever_its_content_type(origin):
    c, live = await _client()
    try:
        r = await c.post("/api/settings", json={"proxy_mode": True},
                         headers={"Origin": origin})
        assert r.status == 403
        assert live["proxy_mode"] is False
        r = await c.delete("/api/devices/1", headers={"Origin": origin})
        assert r.status == 403
    finally:
        await c.close()


async def test_the_panel_on_its_own_port_still_writes():
    """`http://<lan-ip>:8099`: the browser's Origin is the Host it asked."""
    c, live = await _client()
    try:
        r = await c.post("/api/settings", json={"proxy_mode": True},
                         headers={"Origin": _own_origin(c)})
        assert r.status == 200
        assert live["proxy_mode"] is True
    finally:
        await c.close()


async def test_the_panel_behind_ingress_still_writes():
    """Under Ingress the Origin is Home Assistant's, and the request reaches us
    from the Supervisor's proxy with the browser's host in X-Forwarded-Host."""
    c, live = await _client()
    try:
        r = await c.post("/api/settings", json={"proxy_mode": True},
                         headers={"Origin": "http://homeassistant.local:8123",
                                  "Host": "172.30.33.4:8099",
                                  "X-Forwarded-Host": "homeassistant.local:8123",
                                  "X-Ingress-Path": "/api/hassio_ingress/tok"})
        assert r.status == 200
        assert live["proxy_mode"] is True
    finally:
        await c.close()


async def test_a_default_port_on_one_side_only_is_the_same_host():
    c, _ = await _client()
    try:
        r = await c.post("/api/settings", json={"proxy_mode": True},
                         headers={"Origin": "https://ha.example",
                                  "X-Forwarded-Host": "HA.example:443"})
        assert r.status == 200
    finally:
        await c.close()


async def test_a_tls_proxy_that_rewrites_host_is_vouched_for_by_the_browser():
    """No forwarded host at all, but the browser itself says same-origin — a
    header page script cannot set."""
    c, _ = await _client()
    try:
        r = await c.post("/api/settings", json={"proxy_mode": True},
                         headers={"Origin": "https://panel.example",
                                  "Sec-Fetch-Site": "same-origin"})
        assert r.status == 200
        r = await c.post("/api/settings", json={"proxy_mode": True},
                         headers={"Sec-Fetch-Site": "cross-site"})
        assert r.status == 403
    finally:
        await c.close()


async def test_a_script_with_no_origin_still_writes():
    c, live = await _client()
    try:
        r = await c.post("/api/settings", json={"proxy_mode": True})
        assert r.status == 200
        assert live["proxy_mode"] is True
    finally:
        await c.close()


async def test_a_cross_site_read_is_not_this_checks_business():
    """The same-origin policy already hides a GET's answer from another site;
    refusing it here would only break a link opened from elsewhere."""
    c, _ = await _client()
    try:
        r = await c.get("/api/settings", headers={"Origin": "https://evil.example"})
        assert r.status == 200
    finally:
        await c.close()


async def test_a_cross_site_websocket_cannot_read_the_live_log():
    c, _ = await _client()
    try:
        with pytest.raises(WSServerHandshakeError) as refused:
            await c.ws_connect("/api/ws", headers={"Origin": "https://evil.example"})
        assert refused.value.status == 403
        ws = await c.ws_connect("/api/ws", headers={"Origin": _own_origin(c)})
        await ws.close()
        ws = await c.ws_connect("/api/ws")
        await ws.close()
    finally:
        await c.close()


@pytest.mark.parametrize("content_type", [
    "text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x",
])
async def test_a_json_body_must_be_declared_as_json(content_type):
    """The three types a cross-site page may send without a preflight."""
    c, live = await _client()
    try:
        r = await c.post("/api/settings", data=json.dumps({"proxy_mode": True}),
                         headers={"Content-Type": content_type})
        assert r.status == 415
        assert (await r.json())["error"]
        assert live["proxy_mode"] is False
        r = await c.post("/api/settings", data=json.dumps({"proxy_mode": True}),
                         headers={"Content-Type": "application/json; charset=utf-8"})
        assert r.status == 200
    finally:
        await c.close()
