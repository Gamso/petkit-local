"""The address a patcher fetches from is an unauthenticated claim — check it.

A patcher run downloads a binary from `device.state["ip"]`, patches it, and has
the device install the result. Any LAN host can post a state report as device N
naming its own address, so these tests are about one thing: that such a report
cannot silently become the place the next run downloads from.
"""
import asyncio
import json

import pytest
from aiohttp.test_utils import TestClient, TestServer

import petkit_local.web.api.patchers as patchers_mod
from petkit_local.devices import addresses
from petkit_local.devices.addresses import CLAIMED, PEER, SETTLE_SECONDS
from petkit_local.devices.base import Device
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.devices.state_parsers import reported_ip
from petkit_local.events.normalize import apply_state_snapshot
from petkit_local.http.server import create_app
from tests.test_panel import _mk_client, _panel

REAL = "192.0.2.10"
ATTACKER = "192.0.2.66"
HDR = {"X-Device": "id=100&sn=SN100"}
T0 = 1_000_000.0


def _device(**state):
    d = Device(device_type="t5", petkit_id=100, serial_number="SN100")
    d.state.update(state)
    return d


# --- what a report claims ----------------------------------------------------

@pytest.mark.parametrize("body,ip", [
    ({"Ip": REAL}, REAL),
    ({"ip": REAL}, REAL),
    ({"other": f'Ver:1,Ip:"{REAL}",x'}, REAL),
    ({"workState": 1}, ""),
    ({"ip": "attacker.example"}, ""),
    ({"ip": 7}, ""),
    ("not a dict", ""),
])
def test_reported_ip_is_what_the_report_itself_carries(body, ip):
    assert reported_ip(body) == ip


@pytest.mark.parametrize("value,ip", [
    (REAL, REAL), (f" {REAL} ", REAL), (f"::ffff:{REAL}", REAL),
    ("", ""), (None, ""), (7, ""), ("2001:db8::1", ""), ("attacker.example", ""),
    (f"{REAL}:8080/x", ""), ("999.1.1.1", ""),
])
def test_only_one_ipv4_address_counts_as_an_address(value, ip):
    assert addresses.as_ipv4(value) == ip


# --- when the recorded address is believed ------------------------------------

def test_a_device_that_reports_its_own_address_is_believed():
    d = _device(ip=REAL)
    addresses.note(d, CLAIMED, REAL, now=T0)
    addresses.note(d, PEER, REAL, now=T0)
    assert addresses.distrust(d, now=T0 + 30) == ""


def test_behind_nat_the_gateway_as_peer_is_not_a_disagreement():
    """A bridged container sees every device as its gateway. That is one peer
    and one claim, for good — never grounds to refuse."""
    d = _device(ip=REAL)
    for tick in range(10):
        addresses.note(d, CLAIMED, REAL, now=T0 + tick * 30)
        addresses.note(d, PEER, "172.30.32.1", now=T0 + tick * 30)
    assert addresses.distrust(d, now=T0 + 300) == ""


def test_a_device_known_only_by_where_its_requests_come_from_is_believed():
    """No `Ip` in its reports: `state["ip"]` is the `request.remote` fallback."""
    d = _device(ip=REAL)
    addresses.note(d, PEER, REAL, now=T0)
    assert addresses.distrust(d, now=T0 + 30) == ""


def test_a_device_on_mqtt_alone_is_believed():
    """It stops polling HTTP, so there is no peer at all — only its own posts."""
    d = _device(ip=REAL)
    addresses.note(d, CLAIMED, REAL, now=T0)
    assert addresses.distrust(d, now=T0 + 30) == ""


# --- when it is not ------------------------------------------------------------

def test_a_second_claimed_address_is_a_disagreement():
    """The impostor has to keep posting to stay on record, and the real device
    does not stop — so both addresses are current at once."""
    d = _device(ip=ATTACKER)
    addresses.note(d, CLAIMED, REAL, now=T0)
    addresses.note(d, CLAIMED, ATTACKER, now=T0 + 1)
    problem = addresses.distrust(d, now=T0 + 2)
    assert REAL in problem and ATTACKER in problem


def test_a_second_peer_is_a_disagreement_even_with_one_claim():
    d = _device(ip=REAL)
    addresses.note(d, CLAIMED, REAL, now=T0)
    addresses.note(d, PEER, REAL, now=T0)
    addresses.note(d, PEER, ATTACKER, now=T0 + 1)
    assert ATTACKER in addresses.distrust(d, now=T0 + 2)


def test_a_forged_address_left_behind_is_not_believed_once_it_goes_quiet():
    """One forged report on a device that never reports an address of its own:
    `state["ip"]` keeps the forgery forever, and nothing contradicts it."""
    d = _device(ip=ATTACKER)
    addresses.note(d, CLAIMED, ATTACKER, now=T0)
    addresses.note(d, PEER, REAL, now=T0 + SETTLE_SECONDS + 60)
    problem = addresses.distrust(d, now=T0 + SETTLE_SECONDS + 61)
    assert "nothing from this device has confirmed" in problem


def test_a_real_change_of_lease_settles_by_itself():
    d = _device(ip="192.0.2.11")
    addresses.note(d, CLAIMED, REAL, now=T0)
    addresses.note(d, CLAIMED, "192.0.2.11", now=T0 + 30)
    assert addresses.distrust(d, now=T0 + 60) != ""
    addresses.note(d, CLAIMED, "192.0.2.11", now=T0 + SETTLE_SECONDS + 31)
    assert addresses.distrust(d, now=T0 + SETTLE_SECONDS + 32) == ""


def test_an_address_that_is_not_one_is_never_fetched_from():
    """The raw merge of a state snapshot can put any string under `ip`."""
    d = _device(ip="attacker.example:8080/x?")
    assert "not an IPv4 address" in addresses.distrust(d, now=T0)


def test_the_table_of_addresses_is_bounded():
    d = _device()
    for n in range(200):
        addresses.note(d, CLAIMED, f"192.0.2.{n}", now=T0 + n)
    assert len(d.addresses_seen[CLAIMED]) == addresses.MAX_TRACKED
    assert "192.0.2.199" in d.addresses_seen[CLAIMED]


def test_addresses_are_live_state_and_not_persisted():
    d = _device(ip=REAL)
    addresses.note(d, CLAIMED, REAL)
    assert "addresses_seen" not in d.to_dict()


# --- recorded from real traffic -------------------------------------------------

async def test_a_forged_state_report_is_recorded_as_a_second_claim():
    reg = DeviceRegistry()
    client = TestClient(TestServer(create_app(reg, {"api_url": "http://server/6/",
                                                    "mqtt_port": 1883})))
    await client.start_server()
    try:
        await client.post("/6/t5/dev_signup", headers=HDR)
        d = reg.get(100)

        await client.post("/6/t5/dev_state_report", headers=HDR,
                          data="state=" + json.dumps({"workState": 1, "Ip": REAL}))
        assert d.state["ip"] == REAL
        assert addresses.recent(d, CLAIMED) == [REAL]
        # Where the request really came from, kept apart from what it said.
        assert addresses.recent(d, PEER) == ["127.0.0.1"]
        assert addresses.distrust(d) == ""

        await client.post("/6/t5/dev_state_report", headers=HDR,
                          data="state=" + json.dumps({"workState": 1, "Ip": ATTACKER}))
        # The forgery is on record, exactly as before...
        assert d.state["ip"] == ATTACKER
        # ...and is now visibly one of two.
        assert addresses.recent(d, CLAIMED) == [REAL, ATTACKER]
        assert addresses.distrust(d) != ""
    finally:
        await client.close()


def test_a_snapshot_inside_an_event_counts_as_a_claim_too():
    d = _device()
    assert apply_state_snapshot(d, {"workState": 1, "Ip": ATTACKER})
    assert addresses.recent(d, CLAIMED) == [ATTACKER]


# --- the patcher API ------------------------------------------------------------

async def _patcher_panel(monkeypatch, device):
    reg = DeviceRegistry()
    reg._devices[device.petkit_id] = device
    app, reg, hub = _panel(reg=reg)
    used = []

    async def _apply(d, patcher_id, device_ip, *args, **kwargs):
        used.append(device_ip)

    monkeypatch.setattr(patchers_mod, "_patcher_apply", _apply)
    monkeypatch.setattr(patchers_mod, "_patcher_remove", _apply)
    return await _mk_client(app), used


async def _post(client, **body):
    r = await client.post("/api/devices/100/patcher", json=body)
    return r.status, await r.json()


async def test_a_patcher_run_is_refused_while_the_address_is_disputed(monkeypatch):
    """The attack: device 100 really is at REAL, and a LAN host keeps posting
    state reports in its name that say ATTACKER. Apply used to fetch `ctrl` from
    ATTACKER, patch it, and have the real device install it."""
    d = _device(ip=ATTACKER)
    addresses.note(d, CLAIMED, REAL)
    addresses.note(d, CLAIMED, ATTACKER)
    client, used = await _patcher_panel(monkeypatch, d)
    try:
        for action in ("apply", "remove"):
            status, reply = await _post(client, patcher="mqtt", action=action)
            assert status == 400
            assert ATTACKER in reply["error"] and REAL in reply["error"]
        await asyncio.sleep(0)
        assert used == []

        shown = await (await client.get("/api/devices/100/patcher")).json()
        assert shown["device_ip"] == ATTACKER
        assert REAL in shown["device_ip_problem"]
    finally:
        await client.close()


async def test_the_operator_can_name_the_address_and_that_one_is_used(monkeypatch):
    d = _device(ip=ATTACKER)
    addresses.note(d, CLAIMED, REAL)
    addresses.note(d, CLAIMED, ATTACKER)
    client, used = await _patcher_panel(monkeypatch, d)
    try:
        status, reply = await _post(client, patcher="mqtt", device_ip=REAL)
        assert status == 200 and reply["ok"] is True
        for _ in range(20):
            if used:
                break
            await asyncio.sleep(0.01)
        assert used == [REAL]

        status, reply = await _post(client, patcher="mqtt", device_ip="attacker.example")
        assert status == 400 and "IPv4" in reply["error"]
    finally:
        await client.close()


async def test_an_undisputed_address_is_used_without_being_asked_for(monkeypatch):
    d = _device(ip=REAL)
    addresses.note(d, CLAIMED, REAL)
    addresses.note(d, PEER, "172.30.32.1")
    client, used = await _patcher_panel(monkeypatch, d)
    try:
        shown = await (await client.get("/api/devices/100/patcher")).json()
        assert shown["device_ip"] == REAL and shown["device_ip_problem"] == ""

        status, reply = await _post(client, patcher="mqtt")
        assert status == 200 and reply["ok"] is True
        for _ in range(20):
            if used:
                break
            await asyncio.sleep(0.01)
        assert used == [REAL]
    finally:
        await client.close()


async def test_a_device_with_no_address_is_still_told_to_wait(monkeypatch):
    client, used = await _patcher_panel(monkeypatch, _device())
    try:
        status, reply = await _post(client, patcher="mqtt")
        assert status == 400 and "not known" in reply["error"]
        shown = await (await client.get("/api/devices/100/patcher")).json()
        assert shown["device_ip"] == "" and shown["device_ip_problem"] == ""
    finally:
        await client.close()
