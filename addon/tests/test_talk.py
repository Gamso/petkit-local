"""Two-way talk's shared path (`media/talk.py`) and the panel's socket on it
(`web/api/talk.py`).

What is held here:

* availability is computed from what is recorded, and only an address that is
  one is ever dialled;
* ffmpeg only transcodes (stdin to stdout) — the sink connection is ours;
* what the browser sends reaches the sink as ffmpeg's output, and a stop
  flushes it before the device is let go;
* the panel is told why a talk could not start or stopped — ffmpeg's own
  reason included, where it used to drop the audio in silence;
* one talk per device: a second is `busy`, the first goes on;
* with ffmpeg installed, the panel's webm/opus really becomes 16 kHz mono
  ADTS-AAC.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, unused_port

from petkit_local.media import talk
from petkit_local.media.talk import PCM_INPUT, PROBED_INPUT, Talks, availability, transcoder_args
from petkit_local.web.api.talk import api_talk
from petkit_local.web.appkeys import REGISTRY
from tests._talk import (
    FAILING, HAVE_FFMPEG, PASSTHROUGH, FakeSink, assert_adts_16k_mono, closed_code, finished,
    next_json, registry, script, webm_opus,
)

WEBM = b"webm" * 64


class Panel:
    """The panel's talk route over `registry()`, on a fake sink and ffmpeg."""

    def __init__(self, tmp: Path, sink: FakeSink, monkeypatch) -> None:
        self.sink = sink
        self.registry = registry()
        self.talks = Talks(ffmpeg=script(tmp, "ffmpeg_pass.py", PASSTHROUGH), port=sink.port,
                           connect_timeout=2.0, write_timeout=2.0)
        monkeypatch.setattr(talk, "TALKS", self.talks)
        app = web.Application()
        app[REGISTRY] = self.registry
        app.router.add_get("/api/devices/{id}/talk", api_talk)
        self.client = TestClient(TestServer(app))

    async def talk(self, device: int = 100):
        ws = await self.client.ws_connect(f"/api/devices/{device}/talk")
        await ws.send_str(json.dumps({"type": "talk_start"}))
        return ws


@pytest.fixture
async def panel(tmp_path: Path, monkeypatch):
    sink = FakeSink()
    await sink.start()
    p = Panel(tmp_path, sink, monkeypatch)
    await p.client.start_server()
    yield p
    await p.client.close()
    await sink.close()


# --- availability and the transcoder ----------------------------------------------

def test_availability_is_computed_from_what_is_recorded():
    reg = registry()
    assert [availability(reg.get(i)) for i in (100, 101, 102, 200)] == [
        "available", "needs_patcher", "device_unreachable", "unsupported"]
    cam = reg.get(100)
    # Only an address that is one is ever dialled: a forged report is not.
    cam.state["ip"] = "127.0.0.1:1/?listen=1"
    assert availability(cam) == "device_unreachable"
    cam.state["ip"] = "::ffff:192.0.2.5"
    assert talk.sink_ip(cam) == "192.0.2.5"


def test_ffmpeg_only_transcodes():
    joined = " ".join(transcoder_args(PCM_INPUT))
    assert "-f s16le -ar 16000 -ch_layout mono -i pipe:0" in joined
    assert joined.endswith("-ar 16000 -ac 1 -c:a aac -b:a 48k -flush_packets 1 -f adts pipe:1")
    assert "tcp://" not in joined   # the sink connection is ours, never ffmpeg's
    # ffmpeg 7.1 encodes nothing from raw PCM under `-fflags nobuffer`.
    assert "nobuffer" not in joined
    assert ("-fflags nobuffer -flags low_delay -probesize 32 -analyzeduration 0 -i pipe:0"
            in " ".join(transcoder_args(PROBED_INPUT)))


# --- the panel's socket ---------------------------------------------------------------

async def test_the_panel_talks_through_the_shared_path(panel: Panel):
    ws = await panel.talk()
    assert await next_json(ws) == {"type": "talking"}
    await ws.send_bytes(WEBM)
    await ws.send_str(json.dumps({"type": "talk_stop"}))
    assert await next_json(ws) == {"type": "stopped"}
    await ws.close()
    await finished(panel.talks, panel.sink)
    assert panel.sink.payloads() == [WEBM]


async def test_audio_without_talk_start_still_starts_a_talk(panel: Panel):
    ws = await panel.client.ws_connect("/api/devices/100/talk")
    await ws.send_bytes(WEBM)
    await ws.close()   # what was sent still plays
    await finished(panel.talks, panel.sink)
    assert panel.sink.payloads() == [WEBM]


@pytest.mark.parametrize("device, code", [(999, "device_not_found"), (200, "not_a_camera"),
                                          (101, "needs_patcher"), (102, "device_unreachable")])
async def test_the_panel_is_told_why_a_talk_cannot_start(panel: Panel, device, code):
    ws = await panel.talk(device)
    msg = await next_json(ws)
    assert msg["type"] == "error" and msg["code"] == code and msg["msg"]
    await closed_code(ws)
    assert panel.sink.received == [] and panel.talks.active(device) is None


async def test_a_sink_that_does_not_listen_is_reported(panel: Panel):
    panel.talks.port = unused_port()
    ws = await panel.talk()
    msg = await next_json(ws)
    assert msg["code"] == "device_unreachable"
    await closed_code(ws)
    assert panel.talks.active(100) is None


async def test_a_missing_ffmpeg_is_reported_and_the_device_never_dialled(panel: Panel):
    panel.talks.ffmpeg = ("petkit-local-no-such-ffmpeg",)
    ws = await panel.talk()
    msg = await next_json(ws)
    assert (msg["code"], msg["msg"]) == ("transcoder_failed",
                                         "ffmpeg is not installed on the server")
    await closed_code(ws)
    assert panel.sink.received == [] and panel.talks.active(100) is None


async def test_the_panel_is_told_why_ffmpeg_stopped(panel: Panel, tmp_path: Path):
    """It used to drop the audio in silence."""
    panel.talks.ffmpeg = script(tmp_path, "ffmpeg_fail.py", FAILING)
    ws = await panel.talk()
    assert await next_json(ws) == {"type": "talking"}
    await ws.send_bytes(b"webm")
    msg = await next_json(ws)
    assert msg["type"] == "error" and msg["code"] == "transcoder_failed"
    assert "Invalid data found when processing input" in msg["msg"]
    await closed_code(ws)
    await finished(panel.talks, panel.sink)


async def test_the_device_hanging_up_is_reported(tmp_path: Path, monkeypatch):
    sink = FakeSink(hang_up=True)
    await sink.start()
    p = Panel(tmp_path, sink, monkeypatch)
    await p.client.start_server()
    try:
        ws = await p.talk()
        msg = await next_json(ws)
        if msg["type"] == "talking":
            await ws.send_bytes(WEBM)
            msg = await next_json(ws)
        assert (msg["type"], msg["code"]) == ("error", "device_unreachable")
        await closed_code(ws)
        await finished(p.talks, sink)
    finally:
        await p.client.close()
        await sink.close()


async def test_one_talk_per_device_and_the_first_goes_on(panel: Panel):
    first = await panel.talk()
    assert await next_json(first) == {"type": "talking"}
    await first.send_bytes(WEBM)
    second = await panel.talk()
    msg = await next_json(second)
    assert (msg["type"], msg["code"]) == ("error", "busy")
    await closed_code(second)
    await first.send_bytes(WEBM)
    await first.send_str(json.dumps({"type": "talk_stop"}))
    assert await next_json(first) == {"type": "stopped"}
    await first.close()
    await finished(panel.talks, panel.sink)
    assert panel.sink.payloads() == [WEBM * 2]


@pytest.mark.skipif(not HAVE_FFMPEG, reason="needs ffmpeg")
async def test_real_ffmpeg_turns_the_panels_webm_into_adts_aac(panel: Panel):
    webm = await webm_opus()
    if webm is None:
        pytest.skip("this ffmpeg cannot make webm/opus")
    panel.talks.ffmpeg = ("ffmpeg",)
    ws = await panel.talk()
    assert await next_json(ws) == {"type": "talking"}
    for i in range(0, len(webm), 4096):
        await ws.send_bytes(webm[i:i + 4096])
    await ws.send_str(json.dumps({"type": "talk_stop"}))
    assert await next_json(ws) == {"type": "stopped"}
    await ws.close()
    await finished(panel.talks, panel.sink)
    assert_adts_16k_mono(panel.sink.payloads()[0])
