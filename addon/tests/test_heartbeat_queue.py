"""How long a command may wait in the heartbeat queue.

The queue holds whatever was asked while a device was not polling, for as long
as that lasted, and the heartbeat stamps each entry with the time it is handed
over. So a feeder unplugged for a day while an automation pressed Feed five
times dispensed five meals on its first poll back.
"""
import json
import logging

from aiohttp.test_utils import TestClient, TestServer

from petkit_local.devices.base import MAX_QUEUED_COMMANDS, QUEUED_AT_KEY, Device
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.ha.commands import ALL_ACTIONS, make_mqtt_property_set
from petkit_local.ha.publisher import HAPublisher
from petkit_local.http.handlers.heartbeat import ACTION_TTL_SECONDS, still_wanted
from petkit_local.http.server import create_app
from petkit_local.patchers.esp32_ota import ota_trigger
from tests._fakes import FakeMessage, FakeMqttClient

CONFIG = {"api_url": "http://server/6/", "mqtt_port": 1883, "proxy_mode": False,
          "proxy_upstream": "", "proxy_block_run_cmd": True}
HDR = {"X-Device": "id=100&sn=SN100"}


def _feeder() -> Device:
    return Device(device_type="d4", petkit_id=100, serial_number="SN100")


def _queue_feed(device: Device) -> dict:
    suffix, envelope = ALL_ACTIONS["feed"](device)
    device.queue_command(envelope, suffix)
    return envelope


def _queue_setting(device: Device, **params) -> dict:
    envelope = make_mqtt_property_set(params)
    device.queue_command(envelope, "property/set")
    return envelope


def _age(cmd: dict, seconds: float) -> None:
    cmd[QUEUED_AT_KEY] -= seconds


def _payloads(cmds) -> list:
    return [c["params"] for c in cmds]


# --- the policy ---

def test_the_limit_is_the_firmwares_own_and_leaves_room_for_several_polls():
    """`net_http_heartbeat` discards an entry more than 60s old; a device polls
    every ~10-15s, so nothing asked of a live device can expire."""
    assert ACTION_TTL_SECONDS == 60


def test_an_action_asked_long_ago_is_not_run_when_the_device_returns(caplog):
    device = _feeder()
    feeds = [_queue_feed(device) for _ in range(5)]
    for feed in feeds:
        _age(feed, 24 * 3600)

    with caplog.at_level(logging.WARNING):
        assert still_wanted(device, device.pop_commands()) == []
    # Never silent: one line per command that was thrown away.
    dropped = [r for r in caplog.records if "feed_realtime" in r.getMessage()]
    assert len(dropped) == 5


def test_an_action_the_device_is_about_to_collect_is_delivered():
    device = _feeder()
    feed = _queue_feed(device)
    _age(feed, ACTION_TTL_SECONDS - 5)
    assert still_wanted(device, device.pop_commands()) == [feed]


def test_a_settings_write_is_still_wanted_however_late():
    device = _feeder()
    write = _queue_setting(device, lightMode=0)
    _age(write, 24 * 3600)
    assert still_wanted(device, device.pop_commands()) == [write]


def test_a_button_that_is_a_settings_write_is_kept_too():
    """`reset_desiccant` and friends are `property/set`: setting a value late
    is setting the same value."""
    device = _feeder()
    suffix, envelope = next(build(device) for build in ALL_ACTIONS.values()
                            if build(device)[0] == "property/set")
    device.queue_command(envelope, suffix)
    _age(envelope, 24 * 3600)
    assert still_wanted(device, device.pop_commands()) == [envelope]


def test_repeated_writes_of_a_field_collapse_into_the_latest():
    device = _feeder()
    _queue_setting(device, lightMode=0)
    _queue_setting(device, lightMode=1, volume=3)
    feed = _queue_feed(device)
    _queue_setting(device, lightMode=0)
    _queue_setting(device, volume=5, foodWarn=1)

    wanted = still_wanted(device, device.pop_commands())
    assert _payloads(wanted) == [feed["params"], {"lightMode": 0},
                                 {"volume": 5, "foodWarn": 1}]


def test_a_nested_object_is_never_collapsed():
    """It may be a partial write of its block; two of them are not one."""
    device = _feeder()
    _queue_setting(device, k3Config={"lightness": 1})
    _queue_setting(device, k3Config={"standard": 2})
    assert _payloads(still_wanted(device, device.pop_commands())) == [
        {"k3Config": {"lightness": 1}}, {"k3Config": {"standard": 2}}]


def test_what_was_not_queued_through_queue_command_is_left_alone():
    """A patcher's `run_cmd` string and a firmware job's trigger carry no
    stamp: they are delivered whenever the device comes back, as before."""
    device = _feeder()
    run_cmd = json.dumps({"user_cmd": {"run_cmd": "true"}})
    trigger = ota_trigger(1031)
    unstamped_set = {"msgType": 1, "payload": {"feed_get": "1"}}
    device.command_queue.extend([run_cmd, trigger, unstamped_set, dict(unstamped_set)])
    assert still_wanted(device, device.pop_commands()) == [
        run_cmd, trigger, unstamped_set, unstamped_set]


def test_an_expired_action_is_shown_in_the_panel_log():
    from petkit_local.web.hub import EventHub

    device = _feeder()
    _age(_queue_feed(device), 3600)
    hub = EventHub()
    still_wanted(device, device.pop_commands(), hub)
    assert "expired, not sent: feed_realtime" in hub.recent()[-1]["summary"]


# --- the queue itself ---

def test_the_queue_is_capped_and_sheds_its_oldest_command(caplog):
    device = _feeder()
    trigger = ota_trigger(1031)
    device.command_queue.append(trigger)
    with caplog.at_level(logging.WARNING):
        for n in range(MAX_QUEUED_COMMANDS + 10):
            _queue_setting(device, volume=n)
    assert len(device.command_queue) == MAX_QUEUED_COMMANDS
    # The unstamped entry is not ours to drop; the newest write survives.
    assert device.command_queue[0] == trigger
    assert device.command_queue[-1]["params"] == {"volume": MAX_QUEUED_COMMANDS + 9}
    assert sum("queue is full" in r.getMessage() for r in caplog.records) == 11


def test_the_stamp_never_reaches_the_device():
    from petkit_local.http.handlers.heartbeat import _to_heartbeat_content

    device = _feeder()
    feed, write = _queue_feed(device), _queue_setting(device, lightMode=1)
    device.queue_command({"msgType": 2, "payload": {"start_action": 0}, "type": "start"})
    for cmd in device.pop_commands():
        content = _to_heartbeat_content(cmd)
        assert "_queued_at" not in content and "_service_suffix" not in content
    assert feed and write


# --- end to end ---

async def test_ha_presses_against_an_unplugged_feeder_do_not_all_fire_on_its_return():
    reg = DeviceRegistry()
    app = create_app(reg, dict(CONFIG))
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        await client.post("/6/d4/dev_signup", headers=HDR)
        dev = reg.get(100)
        pub = HAPublisher(reg, {})
        pub._client = FakeMqttClient()
        pub._connected = True
        await pub.publish_discovery(dev)

        async def press(suffix, payload):
            await pub._commands.handle_command(
                FakeMessage(f"petkit-local/100/cmd/{suffix}", payload))

        # Yesterday: five Feed presses and a setting, while it was unplugged.
        for _ in range(5):
            await press("feed", b"feed")
        assert len(dev.command_queue) == 5
        for cmd in dev.command_queue:
            _age(cmd, 24 * 3600)
        _queue_setting(dev, lightMode=0)
        _age(dev.command_queue[-1], 24 * 3600)
        # Just now: one more press, which IS wanted.
        await press("feed", b"feed")

        r = await client.get("/6/poll/d4/heartbeat", headers=HDR)
        contents = [json.loads(e["content"]) for e in (await r.json())["result"]]
        assert [c.get("type", "property/set") for c in contents] == [
            "property/set", "feed_realtime"]
        assert contents[0]["payload"] == {"lightMode": 0}
        assert dev.command_queue == []

        # Nothing but stale actions: the device gets the idle answer.
        await press("feed", b"feed")
        _age(dev.command_queue[-1], 3600)
        r = await client.get("/6/poll/d4/heartbeat", headers=HDR)
        result = (await r.json())["result"]
        assert len(result) == 1 and "content" not in result[0]
    finally:
        await client.close()
