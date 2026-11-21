"""BLE relay plumbing stays out of the Timeline, on both transports.

A T4 (fw 1.652) on HTTP relaying a CTW3 put 30 rows in a week into its owner's
Timeline -- `dev_event_report` codes 51, 52 and 53, the HTTP twins of the
`ble_relay_*` topics the MQTT bridge has never stored -- and, over MQTT, three
`connect_start`. One rule now covers both (`codes.MQTT_TRANSPORT_TOPICS`):
a `KIND_SYSTEM` report is handled for its side effects and never stored, and
the rows already stored are hidden from every Timeline reader.
"""
from __future__ import annotations

import json
import time
import urllib.parse
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.app_api.notify import rules
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events import codes, migrations
from petkit_local.events.store import EventStore
from petkit_local.http import appkeys as device_keys
from petkit_local.http.server import create_app
from petkit_local.mqtt.bridge import MQTTBridge
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app
from tests.test_app_api import PASSWORD, Env

RELAY_CODES = ("51", "52", "53")
DEVICE = {"api_url": "http://server/6/", "mqtt_port": 1883, "proxy_mode": False,
          "proxy_upstream": "", "proxy_block_run_cmd": True, "capture": False}
T4 = {"X-Device": "id=100&sn=SN100"}

#: What a T4 posts for a CTW3 session, in the shapes of the T5 capture the
#: 51/53 rows cite (the T4's own payloads are not captured).
CONTENT = {
    "51": {"start_time": 1794949771, "start_reason": 2, "action": 1,
           "device": {"mac": "a4c138aabbcc", "type": 24}},
    "52": {"start_time": 1794949774, "start_reason": 2, "result": 0, "action": 1,
           "err": None, "device": {"mac": "a4c138aabbcc", "type": 24}},
    "53": {"payload": [{"cmd": 252, "data": "AAE="}],
           "device": {"type": 24, "mac": "a4c138aabbcc"}},
}


def _form(code: str, event_id: str, content: dict, state: dict | None = None) -> str:
    fields = {"eventType": code, "eventId": event_id, "content": json.dumps(content)}
    if state is not None:
        fields["state"] = json.dumps(state)
    return urllib.parse.urlencode(fields)


def _row(code: str, kind: str, device_type: str = "t4", ts: float | None = None,
         event_id: str = "100_1794949771") -> dict:
    """A row as an install from before this fix stored it."""
    return {"event_uid": f"{event_id}:{code}", "related_event": event_id,
            "device_id": 100, "device_type": device_type, "event_type": code,
            "event_kind": kind, "ts": ts or time.time(), "source": "http",
            "content_json": json.dumps(CONTENT.get(code, {}))}


async def _seed_old_rows(store: EventStore) -> None:
    """The owner's mix: relay rows as they were classified, beside a real visit."""
    now = time.time()
    await store.upsert_event(_row("51", codes.KIND_SYSTEM, ts=now - 30))
    await store.upsert_event(_row("52", codes.KIND_OTHER, ts=now - 20))   # was unknown
    await store.upsert_event(_row("53", codes.KIND_SYSTEM, ts=now - 10))
    await store.upsert_event({**_row("connect_start", codes.KIND_OTHER, ts=now - 5,
                                     event_id="100_1794949700"), "source": "mqtt"})
    await store.upsert_event(_row("10", codes.KIND_TOILET, ts=now - 60, event_id="100_visit"))


# --- the table ---------------------------------------------------------------

@pytest.mark.parametrize("device_type", ["t4", "t5"])
def test_relay_codes_are_transport_on_a_relaying_litter_box(device_type):
    for code in RELAY_CODES:
        ec = codes.lookup(code, device_type)
        assert ec is not None, code
        assert ec.kind == codes.KIND_SYSTEM and ec.detail
        assert device_type in ec.families, code
        assert codes.is_transport(code, device_type)
        assert code in codes.codes_for(device_type)


def test_code_52_is_relay_finished_graded_on_what_backs_it():
    """No payload and no firmware name: inferred, and the note says from what."""
    ec = codes.lookup("52", "t4")
    assert ec.label == codes.MQTT_EVENT_TOPICS["ble_relay_over"].label
    assert ec.grade == codes.INFERRED
    assert "ble_relay_over" in ec.note and "T4" in ec.note
    # 51 and 53 stay confirmed (T5 capture) and now say the T4 sends them.
    for code in ("51", "53"):
        assert codes.lookup(code, "t4").grade == codes.CONFIRMED
        assert "1.652" in codes.lookup(code, "t4").note


def test_connect_start_is_a_known_transport_topic():
    ec = codes.lookup("connect_start", "t4")
    assert ec is not None and ec.kind == codes.KIND_SYSTEM and ec.detail
    assert ec.grade == codes.INFERRED and ec.families == frozenset({"t4"})
    assert "connect_start" in codes.MQTT_TRANSPORT_TOPICS
    assert codes.is_transport("connect_start", "t4")


def test_one_rule_names_every_transport_topic():
    """The bridge's set is the system kind, not a list kept by hand; the six
    topics it always skipped are still in it."""
    assert codes.MQTT_TRANSPORT_TOPICS == {
        name for name, ec in codes.MQTT_EVENT_TOPICS.items() if ec.kind == codes.KIND_SYSTEM}
    assert {"property", "property_post", "data_get", "ble_response", "ble_relay_start",
            "ble_relay_over"} < codes.MQTT_TRANSPORT_TOPICS


def test_the_rule_is_per_device_category():
    """52 is relay plumbing on a litter box; a feeder or a fountain sending it
    is an unknown code and must stay visible rather than vanish."""
    assert not codes.is_transport("52", "d4sh")
    assert not codes.is_transport("52", "w7h")
    assert not codes.is_transport("10", "t4")
    assert not codes.is_transport("", "t4") and not codes.is_transport(None)


# --- HTTP ingest -------------------------------------------------------------

@pytest.fixture
async def http_t4(tmp_path: Path, event_store: EventStore):
    reg = DeviceRegistry()
    device = reg.get_or_create(petkit_id=100, device_type="t4", serial_number="SN100")
    hub = EventHub()
    app = create_app(reg, dict(DEVICE, data_dir=str(tmp_path)))
    app[device_keys.EVENT_STORE] = event_store
    app[device_keys.EVENT_HUB] = hub
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client, device, event_store, hub
    await client.close()


async def test_http_relay_reports_from_a_t4_store_no_row(http_t4):
    client, device, store, hub = http_t4
    for code in RELAY_CODES:
        r = await client.post("/6/t4/dev_event_report", headers=T4,
                              data=_form(code, "100_1794949771", CONTENT[code]))
        assert r.status == 200 and (await r.json()) == {"result": "success"}

    assert await store.all_events() == []
    assert await store.query_timeline(device_id=100) == []
    # No "event" entry: it would re-read the Timeline and feed the notifier a
    # row that does not exist. The request itself is still in the live log.
    assert [e for e in hub.recent() if e["kind"] == "event"] == []
    logged = [e for e in hub.recent() if e["kind"] == "http"]
    assert len(logged) == 3 and all("dev_event_report" in e["summary"] for e in logged)


async def test_an_http_relay_report_still_refreshes_the_state_it_carries(http_t4):
    client, device, store, hub = http_t4
    await client.post("/6/t4/dev_event_report", headers=T4,
                      data=_form("52", "100_1794949774", CONTENT["52"],
                                 state={"sandPercent": 41}))
    assert device.state.get("sandPercent") == 41
    assert await store.all_events() == []


async def test_a_real_t4_event_beside_the_relay_is_still_stored(http_t4):
    client, device, store, hub = http_t4
    await client.post("/6/t4/dev_event_report", headers=T4,
                      data=_form("53", "100_1794949771", CONTENT["53"]))
    await client.post("/6/t4/dev_event_report", headers=T4,
                      data=_form("10", "100_visit", {"pet_weight": 4200}))
    rows = await store.query_timeline(device_id=100)
    assert [r["event_type"] for r in rows] == ["10"]
    assert [e["detail"]["event_type"] for e in hub.recent() if e["kind"] == "event"] == ["10"]


# --- MQTT --------------------------------------------------------------------

class _Publisher:
    def __init__(self):
        self.events = []

    async def publish_event(self, device, suffix, event_type, attrs=None):
        self.events.append(event_type)

    async def publish_state(self, device):
        pass

    async def publish_availability(self, device):
        pass


async def test_mqtt_connect_start_stores_no_row_and_the_rest_is_unchanged(event_store):
    reg = DeviceRegistry()
    t4 = reg.get_or_create(petkit_id=100, device_type="t4", serial_number="SN100")
    hub = EventHub()
    pub = _Publisher()
    bridge = MQTTBridge(reg, pub, event_store=event_store, hub=hub)

    await bridge._handle_event(t4, "connect_start", {"params": {
        "event_id": "100_1794949700", "content": json.dumps({"device": {"type": 24}}),
        "state": json.dumps({"sandPercent": 38})}})
    assert await event_store.all_events() == []
    assert t4.state.get("sandPercent") == 38
    assert [e for e in hub.recent() if e["kind"] == "event"] == []

    # A real event over the same bridge is stored and fires, as before.
    await bridge._handle_event(t4, "pet_out", {"params": {
        "event_id": "100_1794949800", "content": json.dumps({"pet_weight": 4200})}})
    assert [r["event_type"] for r in await event_store.query_timeline(device_id=100)] \
        == ["pet_out"]
    assert pub.events == ["pet_out"]


# --- rows already stored ------------------------------------------------------

async def test_stored_relay_rows_are_hidden_once_regraded(event_store):
    await _seed_old_rows(event_store)

    # 51/53 were already `system` and are hidden at once; 52 and connect_start
    # were `other` until the start-up backfill regrades them.
    shown = {r["event_type"] for r in await event_store.query_timeline(device_id=100)}
    assert shown == {"52", "connect_start", "10"}

    assert await migrations.backfill_event_rows(event_store, force=True) == 2
    rows = await event_store.query_timeline(device_id=100)
    assert [r["event_type"] for r in rows] == ["10"]

    # Hidden, not deleted: the relayed frames are protocol evidence.
    assert len(await event_store.all_events()) == 5
    # Asked for by kind, they are still there.
    system = await event_store.query_timeline(device_id=100, event_kind=codes.KIND_SYSTEM)
    assert {r["event_type"] for r in system} == {"51", "52", "53", "connect_start"}


async def test_the_panel_timeline_and_its_counts_leave_them_out(event_store, tmp_path):
    await _seed_old_rows(event_store)
    await migrations.backfill_event_rows(event_store, force=True)
    reg = DeviceRegistry()
    reg.get_or_create(petkit_id=100, device_type="t4", serial_number="SN100")
    app = create_panel_app(reg, None, EventHub(), {"data_dir": str(tmp_path),
                                                    "media_root": str(tmp_path / "m")},
                           event_store=event_store)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        body = await (await client.get("/api/timeline?device=100")).json()
    finally:
        await client.close()
    assert [s["event_type"] for s in body["sessions"]] == ["10"]
    assert body["counts"]["all"] == 1


async def test_the_app_api_timeline_leaves_them_out(tmp_path, event_store, pet_registry):
    await _seed_old_rows(event_store)
    await migrations.backfill_event_rows(event_store, force=True)
    env = Env(tmp_path, event_store, pet_registry)
    await env.start()
    try:
        await env.auth.set_password(PASSWORD)
        token = await env.login()
        page = await (await env.get("/timeline?limit=100", token)).json()
    finally:
        await env.client.close()
    rows = await event_store.query_timeline(device_id=100)
    assert len(page["items"]) == 1
    assert page["items"][0]["id"] == rows[0]["id"]


@pytest.mark.parametrize("code", [*RELAY_CODES, "connect_start"])
def test_no_notification_rule_fires_on_them(code):
    row = {**_row(code, codes.KIND_SYSTEM), "id": 1}
    assert rules.from_event(row) is None
