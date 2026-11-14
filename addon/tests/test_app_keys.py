"""Handlers read the `app[...]` key their OWN app was wired with.

There are two aiohttp apps and they do not share a vocabulary: the device API
keeps its config under `config` and the hub under `event_hub`, the panel says
`cfg` and `hub` (`web/appkeys.py`). A handler that reads the other app's
spelling does not fail loudly — `app.get` answers None, which every one of
these handlers treats as "not wired", and the one `app[...]` among them raised
into `never_fail_middleware`, which turns it into an empty success.

So each test here builds both apps the way `main/wiring.py` and
`main/lifecycle.py` do, from a real `Config`, and drives the real handler.
"""
import io
import logging
import wave

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.config import Config
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.http import appkeys as device_keys
from petkit_local.http.server import create_app
from petkit_local.media.transcode import have_ffmpeg
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app

HDR = {"X-Device": "id=100&sn=SN100"}
BUCKET = "https://192.168.1.5:9000"


# An upload is transcoded to ADTS before it is stored (`media/sounds.py`), so
# the sound tests need real audio and ffmpeg.
needs_ffmpeg = pytest.mark.skipif(not have_ffmpeg(), reason="ffmpeg is not installed")


def _wav(seconds=1.0, rate=16000):
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(bytes(int(seconds * rate) * 2))
    return buf.getvalue()


class FakeBridge:
    """A bridge with no live client: every delivery lands on the heartbeat queue."""
    _client = None


class FakePublisher:
    def __init__(self):
        self.unpublished = []

    async def unpublish_discovery(self, device):
        self.unpublished.append(device.petkit_id)


@pytest.fixture
async def apps(tmp_path):
    """Both apps over one registry and one hub, wired as production wires them."""
    config = Config()
    config.data_dir = str(tmp_path)
    config.bucket_endpoint = BUCKET
    app_config = config.to_app_config()

    registry = DeviceRegistry()
    feeder = registry.get_or_create(petkit_id=100, device_type="d4sh", serial_number="SN100")
    hub = EventHub()
    publisher = FakePublisher()

    # main/wiring.py
    device_app = create_app(registry, app_config)
    device_app[device_keys.EVENT_HUB] = hub
    # main/lifecycle.py
    panel_app = create_panel_app(
        registry, BLERegistry(), hub,
        {"api_url": config.api_url, "data_dir": config.data_dir,
         "bucket_endpoint": config.bucket_endpoint},
        FakeBridge(), live_config=app_config, ha_publisher=publisher)

    device = TestClient(TestServer(device_app))
    panel = TestClient(TestServer(panel_app))
    await device.start_server()
    await panel.start_server()
    try:
        yield device, panel, feeder, hub, publisher
    finally:
        await device.close()
        await panel.close()


async def _upload(panel, name="meow.wav", data=None):
    form = aiohttp.FormData()
    form.add_field("file", data if data is not None else _wav(),
                   filename=name, content_type="audio/wav")
    r = await panel.post("/api/devices/100/sounds", data=form)
    assert r.status == 200, await r.text()
    return (await r.json())["sound"]


@needs_ffmpeg
async def test_a_device_polling_dev_sound_get_is_told_about_an_uploaded_sound(apps, caplog):
    """The device app has no `cfg`. Reading it raised KeyError on every poll
    once a bucket endpoint was set — an ERROR traceback each time, and an
    answer of `{"result": {}}` whatever had been uploaded."""
    device, panel, feeder, hub, publisher = apps
    sound = await _upload(panel)

    with caplog.at_level(logging.ERROR):
        body = await (await device.post("/6/d4sh/dev_sound_get", headers=HDR)).json()
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    # Served from the API port over plain HTTP, not the bucket
    # (`handlers/stubs.py::handle_sound_get`).
    assert body["result"] == [{
        "id": sound["id"], "name": "meow", "duration": sound["duration"],
        "url": str(device.make_url(f"/sounds/100/{sound['filename']}")),
        "digest": sound["digest"], "size": sound["size"],
        "gmtCreate": str(sound["uploaded_at"] * 1000),
    }]


async def test_dev_sound_get_with_nothing_uploaded_is_an_empty_list_not_an_error(apps, caplog):
    device, panel, feeder, hub, publisher = apps
    with caplog.at_level(logging.ERROR):
        body = await (await device.post("/6/d4sh/dev_sound_get", headers=HDR)).json()
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    assert body == {"result": []}


@needs_ffmpeg
async def test_an_upload_pushes_no_sound_list_to_the_device(apps):
    """The push was gated on `app.get("event_hub")`, which the panel never has.
    It is gone altogether now: the device fetches the list itself, and the
    firmware's `soundList` handler reads entries as uncheckedly as
    `dev_sound_get` does (`web/api/sounds.py::api_sounds_upload`)."""
    device, panel, feeder, hub, publisher = apps
    await _upload(panel)
    assert [c for c in feeder.command_queue if c.get("_service_suffix") == "property/set"] == []


@needs_ffmpeg
async def test_play_and_select_reach_the_device(apps):
    """Both answered 400 "no event hub" on every press."""
    device, panel, feeder, hub, publisher = apps
    sound = await _upload(panel)
    feeder.command_queue.clear()

    r = await panel.post(f"/api/devices/100/sounds/{sound['id']}/play")
    assert r.status == 200, await r.text()
    assert (await r.json())["ok"] is True
    r = await panel.post(f"/api/devices/100/sounds/{sound['id']}/select")
    assert r.status == 200, await r.text()

    assert [c["_service_suffix"] for c in feeder.command_queue] == ["play_sound", "property/set"]
    assert feeder.command_queue[0]["params"] == {"soundId": sound["id"]}
    assert feeder.command_queue[1]["params"] == {"selectedSound": sound["id"]}
    assert feeder.config["settings"]["selectedSound"] == sound["id"]


async def test_deleting_a_device_removes_its_discovery_and_its_diagnostics(apps):
    """`publisher` and `event_hub` are not panel keys, so the entities stayed in
    Home Assistant as orphans and the hub kept the device's diagnostics."""
    device, panel, feeder, hub, publisher = apps
    hub.set_state_report(100, {"pim": 1})
    assert hub.diag(100)

    r = await panel.delete("/api/devices/100")
    assert r.status == 200, await r.text()
    assert publisher.unpublished == [100]
    assert hub.diag(100) == {}
