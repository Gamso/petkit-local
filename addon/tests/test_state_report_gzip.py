"""`dev_state_report` with a gzip-framed body: read it, but never without a limit.

The endpoint takes no credential, so the body is whatever a LAN host cares to
send, and gzip turns a megabyte of request into a gigabyte of work on the one
event loop every device shares.
"""
import gzip
import json

import pytest
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.devices.registry import DeviceRegistry
from petkit_local.http.handlers import state_report
from petkit_local.http.handlers.state_report import MAX_INFLATED_STATE, _inflate
from petkit_local.http.server import create_app

CONFIG = {"api_url": "http://server/6/", "mqtt_port": 1883, "proxy_mode": False}
HDR = {"X-Device": "id=100&sn=SN100"}
REPORT = {"workState": 1, "sandPercent": 55, "usedTimes": 9}


async def _client(registry):
    client = TestClient(TestServer(create_app(registry, dict(CONFIG))))
    await client.start_server()
    await client.post("/6/t5/dev_signup", headers=HDR)
    return client


def _answered_normally(payload):
    return payload["result"]["interval"] == 30


async def test_a_gzip_body_is_still_read():
    reg = DeviceRegistry()
    client = await _client(reg)
    try:
        body = gzip.compress(("state=" + json.dumps(REPORT)).encode())
        r = await client.post("/6/t5/dev_state_report", headers=HDR, data=body)
        assert r.status == 200 and _answered_normally(await r.json())
        assert reg.get(100).state["sandPercent"] == 55
    finally:
        await client.close()


async def test_a_body_aiohttp_already_inflated_is_still_read():
    """With `Content-Encoding: gzip` the server hands the handler plain bytes,
    so the handler's own attempt fails and must fall back to them."""
    reg = DeviceRegistry()
    client = await _client(reg)
    try:
        body = gzip.compress(json.dumps(REPORT).encode())
        r = await client.post("/6/t5/dev_state_report", data=body,
                              headers={**HDR, "Content-Encoding": "gzip"})
        assert r.status == 200 and _answered_normally(await r.json())
        assert reg.get(100).state["sandPercent"] == 55
    finally:
        await client.close()


async def test_a_report_that_inflates_past_the_cap_is_ignored_not_expanded():
    """A valid report hidden behind megabytes of padding: small on the wire,
    and before the cap it was inflated whole and parsed."""
    reg = DeviceRegistry()
    client = await _client(reg)
    try:
        padded = dict(REPORT, pad="0" * (4 * MAX_INFLATED_STATE))
        body = gzip.compress(("state=" + json.dumps(padded)).encode())
        assert len(body) < 64 * 1024

        r = await client.post("/6/t5/dev_state_report", headers=HDR, data=body)
        # Never a 4xx: firmware reads that as a server fault and retries forever.
        assert r.status == 200 and _answered_normally(await r.json())
        assert "sandPercent" not in reg.get(100).state
    finally:
        await client.close()


def test_inflating_stops_at_the_cap(monkeypatch):
    monkeypatch.setattr(state_report, "MAX_INFLATED_STATE", 4096)
    assert _inflate(gzip.compress(b"\0" * 4096)) == b"\0" * 4096
    assert _inflate(gzip.compress(b"\0" * 4097)) == b""
    # The classic bomb: ~100 KB that would become 100 MB.
    assert _inflate(gzip.compress(b"\0" * (100 * 1024 * 1024))) == b""


@pytest.mark.parametrize("raw", [
    b"\x1f\x8b",
    b"\x1f\x8b\x08\x00garbage that is not a deflate stream",
    gzip.compress(b"state={}" * 500)[:-12],     # cut off mid-stream
    b"",
], ids=["magic-only", "corrupt", "truncated", "empty"])
def test_a_stream_that_is_not_whole_comes_back_as_it_was(raw):
    assert _inflate(raw) == raw


@pytest.mark.parametrize("raw", [
    b"\x1f\x8b\x08\x00garbage that is not a deflate stream",
    gzip.compress(b"state={}" * 500)[:-12],
], ids=["corrupt", "truncated"])
async def test_a_broken_gzip_body_gets_the_normal_answer(raw):
    reg = DeviceRegistry()
    client = await _client(reg)
    try:
        r = await client.post("/6/t5/dev_state_report", headers=HDR, data=raw)
        assert r.status == 200 and _answered_normally(await r.json())
    finally:
        await client.close()
