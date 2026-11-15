"""The phone app API's writes: actions, settings, the feeding plan, one-off feeds.

What is held here:

* an action runs only if Home Assistant shows that button for the model, and
  a destructive one only with `{"confirm": true}`; nothing reaches the device
  otherwise;
* a write is published when the device holds an MQTT session and queued for
  its heartbeat when it does not (or when publishing fails), and says which;
* a setting takes a TYPED value and is refused, unchanged, for the wrong type
  or out of bounds — never "ok" for a value thrown away;
* a feeding plan the panel would refuse is refused here too, and pausing keeps
  the meals while the feeder is told it has none;
* every answer has exactly its schema's keys.
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import pytest

from petkit_local.app_api.server import PREFIX, create_app_api
from petkit_local.devices.consumables import CONSUMABLE_RECORD_KEY
from petkit_local.web.hub import EventHub
from tests.test_app_api import PASSWORD, Env, _Schemas


class FakeSink:
    """The MQTT bridge: records what it publishes, or fails like a lost broker."""

    def __init__(self) -> None:
        self.published: list[tuple[int, str, Any]] = []
        self.fail = False

    async def publish_to_device(self, device, suffix, envelope) -> None:
        if self.fail:
            raise ConnectionError("MQTT bridge not connected")
        self.published.append((device.petkit_id, suffix, envelope))


class FakePublisher:
    def __init__(self) -> None:
        self.states: list[int] = []

    async def publish_state(self, device) -> None:
        self.states.append(device.petkit_id)


class WriteEnv(Env):
    def __init__(self, tmp: Path, store: Any, pet_registry: Any) -> None:
        super().__init__(tmp, store, pet_registry)
        self.single = self.registry.get_or_create(petkit_id=200, device_type="d4h",
                                                  serial_number="SN200")
        self.dual = self.registry.get_or_create(petkit_id=201, device_type="d4sh",
                                                serial_number="SN201")
        self.sink = FakeSink()
        self.publisher = FakePublisher()
        self.hub = EventHub()
        self.app = create_app_api(self.auth, self.registry, event_store=store,
                                  pet_registry=pet_registry, media_root=str(self.media_root),
                                  data_dir=str(tmp), throttle=self.throttle,
                                  command_sink=self.sink, ha_publisher=self.publisher,
                                  hub=self.hub)
        self.token = ""

    async def call(self, method: str, path: str, body: Any = None, **kw: Any):
        headers = {"Authorization": f"Bearer {self.token}"}
        if body is not None:
            kw["json"] = body
        return await self.client.request(method, PREFIX + path, headers=headers, **kw)


@pytest.fixture
async def w(tmp_path: Path, event_store, pet_registry):
    e = WriteEnv(tmp_path, event_store, pet_registry)
    await e.start()
    await e.auth.set_password(PASSWORD)
    e.token = await e.login()
    yield e
    await e.client.close()


async def _refused(r, status: int, code: str) -> None:
    assert r.status == status, await r.text()
    assert (await r.json())["error"] == code


# --- actions ------------------------------------------------------------------------

async def test_an_action_on_a_device_off_mqtt_is_queued_for_its_heartbeat(w: WriteEnv):
    r = await w.call("POST", "/devices/100/actions/cleaning_start")
    assert r.status == 200
    assert await r.json() == {"ok": True, "action": "cleaning_start", "delivered": "queued"}
    assert len(w.device.command_queue) == 1 and not w.sink.published
    assert w.publisher.states == [100]
    assert any("app: cleaning_start" in e["summary"] for e in w.hub.recent(kinds={"cmd"}))


async def test_an_action_on_a_device_on_mqtt_is_published(w: WriteEnv):
    w.device.mqtt_connected = True
    r = await w.call("POST", "/devices/100/actions/deodorize")
    assert (await r.json())["delivered"] == "mqtt"
    assert [device for device, _, _ in w.sink.published] == [100]
    assert not w.device.command_queue


async def test_a_failed_publish_falls_back_to_the_queue(w: WriteEnv):
    w.device.mqtt_connected = True
    w.sink.fail = True
    r = await w.call("POST", "/devices/100/actions/deodorize")
    assert (await r.json())["delivered"] == "queued"
    assert len(w.device.command_queue) == 1


@pytest.mark.parametrize("device, action", [
    (100, "feed"),             # a feeder's button on a litter box
    (200, "cleaning_start"),   # and the other way round
    (200, "feed_hopper_2"),    # a Dual-Hopper's button on a single hopper
    (100, "no_such_action"),
    (100, "child_lock"),       # an entity, not a button
])
async def test_an_action_the_model_does_not_offer_is_404_and_sends_nothing(
        w: WriteEnv, device: int, action: str):
    await _refused(await w.call("POST", f"/devices/{device}/actions/{action}"),
                   404, "action_not_found")
    assert not w.registry.get(device).command_queue and not w.sink.published


async def test_an_unknown_device_is_404(w: WriteEnv):
    await _refused(await w.call("POST", "/devices/999/actions/feed"), 404, "device_not_found")
    await _refused(await w.call("PUT", "/devices/abc/entities/volume", {"value": 1}),
                   404, "device_not_found")


@pytest.mark.parametrize("body", [None, {}, {"confirm": False}, {"confirm": "yes"},
                                  {"confirm": 1}])
async def test_a_destructive_action_needs_an_explicit_confirmation(w: WriteEnv, body):
    await _refused(await w.call("POST", "/devices/100/actions/dump_litter", body),
                   422, "confirmation_required")
    assert not w.device.command_queue


async def test_a_confirmed_destructive_action_runs_with_its_side_effects(w: WriteEnv):
    r = await w.call("POST", "/devices/100/actions/reset_n50", {"confirm": True})
    assert r.status == 200 and (await r.json())["delivered"] == "queued"
    # The N50 date lives nowhere else; the press must have recorded it.
    assert w.device.config[CONSUMABLE_RECORD_KEY]["n50"] > 0


async def test_an_action_body_must_be_json(w: WriteEnv):
    r = await w.client.post(f"{PREFIX}/devices/100/actions/deodorize", data="confirm",
                            headers={"Authorization": f"Bearer {w.token}",
                                     "Content-Type": "text/plain"})
    await _refused(r, 415, "unsupported_media_type")


async def test_the_device_detail_lists_its_actions_destructive_last(w: WriteEnv):
    detail = (await (await w.call("GET", "/devices/100")).json())["device"]
    keys = [a["key"] for a in detail["actions"]]
    assert "cleaning_start" in keys and "feed" not in keys
    flags = [a["destructive"] for a in detail["actions"]]
    assert flags == sorted(flags)
    assert {a["key"] for a in detail["actions"] if a["destructive"]} >= {"dump_litter"}


# --- settings -----------------------------------------------------------------------

async def test_a_switch_takes_a_boolean_and_answers_the_stored_value(w: WriteEnv):
    r = await w.call("PUT", "/devices/100/entities/child_lock", {"value": True})
    body = await r.json()
    assert r.status == 200, body
    assert body["delivered"] == "queued" and body["entity"]["value"] is True
    assert w.device.config["settings"]["manualLock"] == 1
    envelope = w.device.command_queue[-1]
    assert "manualLock" in json.dumps(envelope)


async def test_a_number_is_bounded_and_typed(w: WriteEnv):
    ok = await w.call("PUT", "/devices/100/entities/volume", {"value": 5})
    assert (await ok.json())["entity"]["value"] == 5
    for bad in (99, -1, "5", True, None, [5]):
        await _refused(await w.call("PUT", "/devices/100/entities/volume", {"value": bad}),
                       422, "refused")
    assert w.device.config["settings"]["volume"] == 5
    assert len(w.device.command_queue) == 1


async def test_a_select_takes_one_of_its_labels(w: WriteEnv):
    r = await w.call("PUT", "/devices/100/entities/sand_type", {"value": "tofu"})
    assert (await r.json())["entity"]["display"] == "tofu"
    for bad in ("Tofu", 1, "nope"):
        await _refused(await w.call("PUT", "/devices/100/entities/sand_type", {"value": bad}),
                       422, "refused")


async def test_a_local_only_setting_says_so(w: WriteEnv):
    r = await w.call("PUT", "/devices/100/entities/capability_full_video", {"value": False})
    assert (await r.json())["delivered"] == "local"
    assert not w.device.command_queue


async def test_read_only_and_unknown_entities_are_refused(w: WriteEnv):
    await _refused(await w.call("PUT", "/devices/100/entities/litter_weight", {"value": 1}),
                   422, "not_settable")
    await _refused(await w.call("PUT", "/devices/100/entities/nope", {"value": 1}),
                   404, "entity_not_found")
    # A button is an action, not a setting.
    await _refused(await w.call("PUT", "/devices/100/entities/cleaning_start", {"value": 1}),
                   404, "entity_not_found")
    await _refused(await w.call("PUT", "/devices/100/entities/volume", {}), 400, "bad_request")


async def test_a_raw_schedule_text_gets_the_editors_validation(w: WriteEnv):
    await _refused(await w.call("PUT", "/devices/100/entities/cleaning_schedule",
                                {"value": "not json"}), 422, "refused")
    await _refused(await w.call("PUT", "/devices/100/entities/cleaning_schedule",
                                {"value": '[{"time": 9999, "type": 0, "id": 1, "repeats": "1"}]'}),
                   422, "refused")
    r = await w.call("PUT", "/devices/100/entities/cleaning_schedule",
                     {"value": '[{"time": 600, "type": 0, "id": 1, "repeats": "1,2"}]'})
    assert r.status == 200


# --- the feeding plan ---------------------------------------------------------------

def _find_params(obj: Any) -> dict[str, Any]:
    """The `params` of a queued `property.set{feed}`, wherever the queue nests it."""
    if isinstance(obj, dict):
        if "feed" in obj and isinstance(obj["feed"], str):
            return obj
        obj = list(obj.values())
    if isinstance(obj, list):
        for v in reversed(obj):
            found = _find_params(v)
            if found:
                return found
    return {}


async def test_only_a_feeder_has_a_plan(w: WriteEnv):
    await _refused(await w.call("GET", "/devices/100/schedule"), 404, "not_a_feeder")
    await _refused(await w.call("PUT", "/devices/100/schedule", {"active": False}),
                   404, "not_a_feeder")
    await _refused(await w.call("GET", "/devices/100/deferred-feeds"), 404, "not_a_feeder")


async def test_a_feeder_with_no_plan_reads_as_an_empty_active_one(w: WriteEnv):
    plan = (await (await w.call("GET", "/devices/200/schedule")).json())["schedule"]
    assert plan == {"device_id": 200, "active": True, "dual_hopper": False, "groups": []}
    assert "feed_schedule" not in w.single.config


async def test_a_single_hopper_plan_round_trips_in_the_models_own_key(w: WriteEnv):
    groups = [{"days": [2, 1], "meals": [{"time": 8 * 3600, "amount": 20},
                                         {"time": 18 * 3600, "amount": 15, "id": "n_x"}]}]
    r = await w.call("PUT", "/devices/200/schedule", {"groups": groups})
    body = await r.json()
    assert r.status == 200, body
    assert body["delivered"] == "queued"
    stored = w.single.config["feed_schedule"]["schedule"][0]
    assert stored["re"] == "1,2"
    assert stored["it"] == [{"id": "n_28800", "t": 28800, "a": 20},
                            {"id": "n_x", "t": 64800, "a": 15}]
    assert body["schedule"]["groups"] == [{"days": [1, 2], "meals": [
        {"id": "n_28800", "time": 28800, "amount": 20, "amount1": None, "amount2": None},
        {"id": "n_x", "time": 64800, "amount": 15, "amount1": None, "amount2": None}]}]
    # Both halves of the push: feed_get for HTTP, property.set{feed} for MQTT.
    assert {"feed_get": "1"} in [c.get("payload") for c in w.single.command_queue]
    assert _find_params(w.single.command_queue)["feed"]


async def test_a_dual_hopper_plan_uses_both_amounts(w: WriteEnv):
    groups = [{"days": [1, 2, 3, 4, 5, 6, 7], "meals": [{"time": 3600, "amount1": 2}]}]
    body = await (await w.call("PUT", "/devices/201/schedule", {"groups": groups})).json()
    assert body["schedule"]["dual_hopper"] is True
    assert body["schedule"]["groups"][0]["meals"][0] == {
        "id": "n_3600", "time": 3600, "amount": None, "amount1": 2, "amount2": 0}
    assert w.dual.config["feed_schedule"]["schedule"][0]["it"][0] == {
        "id": "n_3600", "t": 3600, "a1": 2, "a2": 0}


async def test_pausing_keeps_the_meals_and_serves_none(w: WriteEnv):
    groups = [{"days": [1], "meals": [{"time": 3600, "amount": 10}]}]
    await w.call("PUT", "/devices/200/schedule", {"groups": groups})
    r = await w.call("PUT", "/devices/200/schedule", {"active": False})
    body = await r.json()
    assert body["schedule"]["active"] is False
    assert body["schedule"]["groups"][0]["meals"][0]["amount"] == 10
    assert json.loads(_find_params(w.single.command_queue[-1])["feed"])["schedule"] == []
    r = await w.call("PUT", "/devices/200/schedule", {"active": True})
    assert json.loads(_find_params(w.single.command_queue[-1])["feed"])["schedule"]


@pytest.mark.parametrize("body", [
    {"groups": [{"days": [8], "meals": []}]},
    {"groups": [{"days": [], "meals": []}]},
    {"groups": [{"days": ["1"], "meals": []}]},
    {"groups": [{"days": [1], "meals": [{"time": 86400, "amount": 1}]}]},
    {"groups": [{"days": [1], "meals": [{"time": -1, "amount": 1}]}]},
    {"groups": [{"days": [1], "meals": [{"time": "08:00", "amount": 1}]}]},
    {"groups": [{"days": [1], "meals": [{"time": 60, "amount": 256}]}]},
    {"groups": [{"days": [1], "meals": [{"time": 60, "amount": "5"}]}]},
    {"groups": [{"days": [1], "meals": [{"time": 60, "amount1": 5}]}]},   # dual key, single box
    {"groups": [{"days": [1], "meals": [{"time": 60}]}]},
    {"groups": [{"days": [1], "meals": [{"time": 60, "amount": 1}] * 25}]},
    {"groups": [{"days": [1], "meals": []}] * 8},
    {"groups": {"days": [1]}},
    {"active": "no"},
    {"groups": [{"days": [9], "meals": []}], "active": False},
])
async def test_an_invalid_plan_is_refused_and_nothing_changes(w: WriteEnv, body):
    before = json.dumps(w.single.config, sort_keys=True)
    await _refused(await w.call("PUT", "/devices/200/schedule", body), 422, "invalid_schedule")
    assert json.dumps(w.single.config, sort_keys=True) == before
    assert not w.single.command_queue


async def test_a_plan_write_must_say_something(w: WriteEnv):
    await _refused(await w.call("PUT", "/devices/200/schedule", {}), 400, "bad_request")


async def test_replacing_the_meals_keeps_the_pending_one_off_feeds(w: WriteEnv):
    await w.call("POST", "/devices/200/deferred-feeds", _soon(w.single, amount=3))
    await w.call("PUT", "/devices/200/schedule", {"groups": []})
    assert len(w.single.config["feed_schedule"]["deferred"]) == 1


# --- one-off feeds ------------------------------------------------------------------

def _soon(device, days: int = 2, **amounts: Any) -> dict[str, Any]:
    local = datetime.datetime.now(datetime.timezone(
        datetime.timedelta(hours=device.timezone_offset))) + datetime.timedelta(days=days)
    return {"date": local.strftime("%Y-%m-%d"), "time": "07:30", **amounts}


async def test_a_one_off_feed_is_added_listed_and_removed(w: WriteEnv):
    r = await w.call("POST", "/devices/200/deferred-feeds", _soon(w.single, amount=12))
    body = await r.json()
    assert r.status == 201, body
    feed = body["feed"]
    assert feed["amount"] == 12 and feed["amount1"] is None and feed["fire_at"] > 0
    assert feed["id"].startswith("d_") and feed["id"].endswith("_27000")
    later = await w.call("POST", "/devices/200/deferred-feeds", _soon(w.single, 3, amount=1))
    assert later.status == 201
    listed = (await (await w.call("GET", "/devices/200/deferred-feeds")).json())["deferred_feeds"]
    assert [f["id"] for f in listed] == [feed["id"], (await later.json())["feed"]["id"]]
    # The device is told on both transports, the deferred feed in `latest`.
    assert _find_params(w.single.command_queue)["feed"]

    r = await w.call("DELETE", f"/devices/200/deferred-feeds/{feed['id']}")
    assert (await r.json()) == {"ok": True, "delivered": "queued"}
    await _refused(await w.call("DELETE", f"/devices/200/deferred-feeds/{feed['id']}"),
                   404, "feed_not_found")
    assert len((await (await w.call("GET", "/devices/200/deferred-feeds")).json())
               ["deferred_feeds"]) == 1


async def test_a_dual_hopper_one_off_feed_takes_either_hopper(w: WriteEnv):
    r = await w.call("POST", "/devices/201/deferred-feeds", _soon(w.dual, amount2=4))
    feed = (await r.json())["feed"]
    assert (feed["amount"], feed["amount1"], feed["amount2"]) == (None, 0, 4)


@pytest.mark.parametrize("change", [
    {"amount": None},                      # no amount at all
    {"amount": 256}, {"amount": -1}, {"amount": "3"}, {"amount": True},
    {"date": "2020-01-01"},                # past
    {"date": "tomorrow"}, {"time": "25:00"}, {"time": 730},
])
async def test_an_invalid_one_off_feed_is_refused(w: WriteEnv, change):
    body = {**_soon(w.single, amount=3), **change}
    body = {k: v for k, v in body.items() if v is not None}
    await _refused(await w.call("POST", "/devices/200/deferred-feeds", body), 422, "invalid_feed")
    assert not (w.single.config.get("feed_schedule") or {}).get("deferred")
    assert not w.single.command_queue


async def test_listing_one_off_feeds_creates_nothing(w: WriteEnv):
    assert (await (await w.call("GET", "/devices/200/deferred-feeds")).json()) == \
        {"deferred_feeds": []}
    assert "feed_schedule" not in w.single.config


# --- shapes -------------------------------------------------------------------------

async def test_every_write_answer_has_exactly_its_schemas_keys(w: WriteEnv):
    schemas = _Schemas()
    detail = (await (await w.call("GET", "/devices/100")).json())["device"]
    schemas.check(detail, "DeviceDetail")
    for action in detail["actions"]:
        schemas.check(action, "Action")
    schemas.check(await (await w.call("POST", "/devices/100/actions/deodorize")).json(),
                  "ActionResult")
    schemas.check(await (await w.call("PUT", "/devices/100/entities/volume",
                                      {"value": 3})).json(), "EntityWriteResult")
    schemas.check((await (await w.call("GET", "/devices/201/schedule")).json())["schedule"],
                  "FeedingPlan")
    put = await w.call("PUT", "/devices/201/schedule",
                       {"groups": [{"days": [1], "meals": [{"time": 60, "amount1": 1}]}]})
    schemas.check(await put.json(), "FeedingPlanResult")
    added = await (await w.call("POST", "/devices/201/deferred-feeds",
                                _soon(w.dual, amount1=1))).json()
    schemas.check(added, "DeferredFeedResult")
    for feed in (await (await w.call("GET", "/devices/201/deferred-feeds")).json())["deferred_feeds"]:
        schemas.check(feed, "DeferredFeed")
    schemas.check(await (await w.call("DELETE", f"/devices/201/deferred-feeds/{added['feed']['id']}")).json(),
                  "WriteResult")
    error = await w.call("POST", "/devices/100/actions/dump_litter")
    schemas.check(await error.json(), "Error")


async def test_writes_need_a_session(w: WriteEnv):
    for method, path in (("POST", "/devices/100/actions/deodorize"),
                         ("PUT", "/devices/100/entities/volume"),
                         ("PUT", "/devices/200/schedule"),
                         ("POST", "/devices/200/deferred-feeds"),
                         ("DELETE", "/devices/200/deferred-feeds/x")):
        r = await w.client.request(method, PREFIX + path, json={"value": 1})
        await _refused(r, 401, "invalid_token")
    assert not w.device.command_queue
