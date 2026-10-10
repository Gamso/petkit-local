"""Custom feeding sounds reach the device, and in the one format it plays.

Observed on a YumShare Dual-Hopper 2 (by the author of the fork this came
from): an uploaded .m4a never played. Three things, two of them ours:
`dev_sound_get` raised (the sounds helper read the panel's config key on the
device-facing server) so the device was told there were no sounds; Play/Select
answered 400 "no event hub" (wrong key again, the other way round); and the
file was stored as received, while the firmware keeps a download as
`user_feed_over_<id>.aac` and plays it as ADTS.
"""
import io
import json
import os
import wave

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.devices.base import Device
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.http.server import create_app
from petkit_local.media import sounds as snd
from petkit_local.media.transcode import have_ffmpeg, run_ffmpeg
from petkit_local.web.api import sounds as api
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app

HDR = {"X-Device": "id=100&sn=SN100"}

needs_ffmpeg = pytest.mark.skipif(not have_ffmpeg(), reason="ffmpeg is not installed")


def _adts(frames=25, rate_index=8, payload=100, channels=1):
    """Synthetic ADTS: `frames` frames of `payload` bytes at 16 kHz (index 8)."""
    out = b""
    for _ in range(frames):
        length = 7 + payload
        hdr = bytes([0xFF, 0xF1, (1 << 6) | (rate_index << 2) | (channels >> 2),
                     ((channels & 3) << 6) | (length >> 11),
                     (length >> 3) & 0xFF, ((length & 7) << 5) | 0x1F, 0xFC])
        out += hdr + bytes(payload)
    return out


def _wav(seconds=1.0, rate=44100, channels=2, noise=False):
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(channels)
        w.setsampwidth(2)
        w.setframerate(rate)
        size = int(seconds * rate) * channels * 2
        # Silence encodes to almost nothing; noise is what fills a file.
        w.writeframes(os.urandom(size) if noise else bytes(size))
    return buf.getvalue()


def _seed(tmp_path, device_id=100, filename="sound_1.aac", data=None):
    d = tmp_path / "sounds" / str(device_id)
    d.mkdir(parents=True)
    (d / filename).write_bytes(data if data is not None else _adts())
    (d / "sounds.json").write_text(json.dumps([{
        "id": 1, "name": "Futti", "filename": filename, "size": 1, "digest": "x",
        "duration": 0, "uploaded_at": 1700000000}]))
    return d


def _device_app(tmp_path, **extra):
    return create_app(DeviceRegistry(), {
        "api_url": "http://server/6/", "mqtt_port": 1883, "proxy_mode": False,
        "proxy_upstream": "", "proxy_block_run_cmd": True,
        "bucket_endpoint": "https://192.0.2.1:9000", "data_dir": str(tmp_path), **extra})


def _panel(tmp_path):
    reg = DeviceRegistry()
    reg._devices[100] = Device(device_type="d4sh", petkit_id=100, serial_number="SN100")
    cfg = {"api_url": "http://x/6/", "mqtt_tls": True, "mqtt_tls_port": 443,
           "capture": False, "capture_dir": "/nope", "data_dir": str(tmp_path)}
    return create_panel_app(reg, BLERegistry(), EventHub(), cfg, None), reg


async def _sound_get(tmp_path, **extra):
    client = TestClient(TestServer(_device_app(tmp_path, **extra)))
    await client.start_server()
    try:
        await client.post("/6/d4sh/dev_signup", headers=HDR)
        r = await client.get("/6/d4sh/dev_sound_get", headers=HDR)
        return r.status, await r.json()
    finally:
        await client.close()


async def _upload(client, data, filename="Futtifutti.m4a"):
    form = aiohttp.FormData()
    form.add_field("file", data, filename=filename, content_type="audio/mp4")
    r = await client.post("/api/devices/100/sounds", data=form)
    return r.status, await r.json()


# --- the ADTS reader --------------------------------------------------------

def test_adts_is_recognised_and_timed():
    data = _adts(frames=25)  # 25 x 1024 samples at 16 kHz = 1.6 s
    assert snd.is_adts(data)
    assert abs(snd.adts_duration_seconds(data) - 1.6) < 0.01
    assert not snd.is_adts(b"\x00\x00\x00\x18ftypM4A ")
    assert snd.describe(data)["duration"] == 2


def test_an_mp3_is_not_mistaken_for_adts():
    """MPEG-1 Layer III starts `FF FB`: the same twelve set bits as ADTS. Only
    the layer bits tell them apart, and the startup conversion skips whatever
    this calls ADTS."""
    mp3 = bytes([0xFF, 0xFB, 0x90, 0x64]) + bytes(400)
    assert not snd.is_adts(mp3)
    assert snd.adts_duration_seconds(mp3) == 0.0


def test_only_16k_mono_counts_as_playable():
    assert snd.is_playable(_adts())
    assert not snd.is_playable(_adts(rate_index=4)), "44.1 kHz"
    assert not snd.is_playable(_adts(channels=2)), "stereo"


def test_a_garbage_buffer_is_timed_without_raising():
    assert snd.adts_duration_seconds(b"\xff\xf1" * 500) >= 0.0
    assert snd.adts_duration_seconds(b"") == 0.0


# --- the transcode, against the real ffmpeg ---------------------------------

@needs_ffmpeg
async def test_a_stereo_wav_becomes_16k_mono_adts():
    adts = await snd.transcode_to_adts(_wav(seconds=1.0))
    assert snd.is_playable(adts)
    assert 0.9 < snd.adts_duration_seconds(adts) < 1.3


@needs_ffmpeg
async def test_a_phone_m4a_is_converted(tmp_path):
    """`moov` at the end, which is why the input is a file and not a pipe."""
    wav, m4a = tmp_path / "in.wav", tmp_path / "in.m4a"
    wav.write_bytes(_wav(seconds=1.0))
    rc, _, err = await run_ffmpeg(
        ["ffmpeg", "-loglevel", "error", "-i", str(wav), "-c:a", "aac", str(m4a)],
        timeout=60, what="fixture")
    assert rc == 0, err
    assert snd.is_playable(await snd.transcode_to_adts(m4a.read_bytes()))


@needs_ffmpeg
@pytest.mark.parametrize("data", [
    b"this is not audio at all\n" * 40,
    bytes(4096),
    b"\x89PNG\r\n\x1a\n" + bytes(2000),
])
async def test_what_ffmpeg_cannot_decode_is_refused(data):
    with pytest.raises(snd.TranscodeError):
        await snd.transcode_to_adts(data)


@needs_ffmpeg
async def test_a_script_cannot_make_ffmpeg_open_another_file(tmp_path, monkeypatch):
    """ffmpeg picks its demuxer from the content, so an "audio file" that is
    a concat script has it read the files the text names. ffmpeg's own guard
    only refuses absolute and climbing names; a bare name beside the staged
    upload goes through, and only the demuxer whitelist stops it."""
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    (tmp_path / "secret.wav").write_bytes(_wav(seconds=1.0))
    concat = b"ffconcat version 1.0\nfile secret.wav\n"
    hls = (f"#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1.0,\nfile://{tmp_path}/secret.wav\n"
           "#EXT-X-ENDLIST\n").encode()
    for upload in (concat, hls):
        with pytest.raises(snd.TranscodeError):
            await snd.transcode_to_adts(upload)

    # The control: the same script IS followed once the whitelist is gone, so
    # the refusal above is the whitelist's doing and not an accident of ffmpeg.
    monkeypatch.setattr(snd, "INPUT_FORMATS", snd.INPUT_FORMATS + ",concat")
    assert snd.is_playable(await snd.transcode_to_adts(concat))


@needs_ffmpeg
async def test_a_sound_longer_than_the_cap_is_refused(monkeypatch):
    monkeypatch.setattr(snd, "MAX_ADTS_SIZE", 8 * 1024)
    with pytest.raises(snd.TranscodeError, match="too long"):
        await snd.transcode_to_adts(_wav(seconds=20.0, rate=8000, channels=1, noise=True))


async def test_no_ffmpeg_is_an_error_not_a_crash(monkeypatch):
    monkeypatch.setattr(snd, "have_ffmpeg", lambda: False)
    with pytest.raises(snd.TranscodeError, match="not installed"):
        await snd.transcode_to_adts(b"x")


async def test_the_staged_upload_is_removed_even_when_ffmpeg_fails(monkeypatch, tmp_path):
    staged = []

    async def fake_run(args, **kw):
        staged.append(args[args.index("-i") + 1])
        assert os.path.isfile(staged[0])
        return 1, b"", b"boom"

    monkeypatch.setattr(snd, "have_ffmpeg", lambda: True)
    monkeypatch.setattr(snd, "run_ffmpeg", fake_run)
    with pytest.raises(snd.TranscodeError):
        await snd.transcode_to_adts(b"x")
    assert staged and not os.path.exists(staged[0])


# --- the device-facing list -------------------------------------------------

async def test_the_device_is_told_about_its_sounds(tmp_path):
    """`dev_sound_get` runs on the device-facing server, whose config key is
    `config`: reading the panel's `cfg` there raised on every poll."""
    _seed(tmp_path)
    status, body = await _sound_get(tmp_path)
    assert status == 200
    assert body["result"][0]["url"] == "https://192.0.2.1:9000/sounds/100/sound_1.aac"
    assert set(body["result"][0]) >= {"id", "name", "duration", "url", "digest", "size"}


async def test_a_damaged_list_is_an_empty_answer_never_an_error(tmp_path):
    d = _seed(tmp_path)
    for damaged in ("{not json", '{"id": 1}', '[1, "x", {"name": "no id"}, {"id": "7", "filename": "a"}]'):
        (d / "sounds.json").write_text(damaged)
        assert await _sound_get(tmp_path) == (200, {"result": []})


async def test_the_list_names_only_files_that_exist_inside_the_folder(tmp_path):
    """The device fetches every URL it is handed, so an entry whose file is
    gone, or whose name climbs out of the device's folder, is not handed out."""
    d = _seed(tmp_path)
    (tmp_path / "sounds" / "outside.aac").write_bytes(_adts())
    (d / "sounds.json").write_text(json.dumps([
        {"id": 1, "name": "ok", "filename": "sound_1.aac"},
        {"id": 2, "name": "gone", "filename": "sound_2.aac"},
        {"id": 3, "name": "climbs", "filename": "../outside.aac"},
    ]))
    _, body = await _sound_get(tmp_path)
    assert [s["id"] for s in body["result"]] == [1]


# --- the panel --------------------------------------------------------------

async def test_play_and_select_find_the_panels_hub(tmp_path):
    app, reg = _panel(tmp_path)
    d = reg._devices[100]
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        r = await client.post("/api/devices/100/sounds/1/play")
        assert r.status == 200, await r.text()
        r = await client.post("/api/devices/100/sounds/1/select")
        assert r.status == 200, await r.text()
    finally:
        await client.close()
    queued = [json.loads(c) if isinstance(c, str) else c for c in d.command_queue]
    assert any("play_sound" in json.dumps(c) for c in queued), "play reached the heartbeat queue"
    assert d.config["settings"]["selectedSound"] == 1


async def test_an_upload_is_stored_as_adts_with_its_duration(tmp_path, monkeypatch):
    async def fake_transcode(data):
        assert data == b"not really an m4a"
        return _adts(frames=50)  # 3.2 s

    monkeypatch.setattr(api, "transcode_to_adts", fake_transcode)
    app, _ = _panel(tmp_path)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        status, body = await _upload(client, b"not really an m4a")
    finally:
        await client.close()
    assert status == 200, body
    assert body["sound"]["filename"] == "sound_1.aac"
    assert body["sound"]["duration"] == 3
    assert body["sound"]["name"] == "Futtifutti"
    stored = (tmp_path / "sounds" / "100" / "sound_1.aac").read_bytes()
    assert snd.is_adts(stored) and body["sound"]["size"] == len(stored)


@needs_ffmpeg
async def test_an_upload_that_is_not_audio_is_refused_and_nothing_is_stored(tmp_path):
    app, _ = _panel(tmp_path)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        status, body = await _upload(client, b"<html>not a sound</html>" * 20, "page.mp3")
        listed = await (await client.get("/api/devices/100/sounds")).json()
    finally:
        await client.close()
    assert status == 400 and "could not convert page" in body["error"]
    assert listed == {"sounds": []}
    assert not os.path.exists(tmp_path / "sounds" / "100" / "sound_1.aac")


async def test_an_oversized_upload_is_refused_before_it_is_converted(tmp_path, monkeypatch):
    async def never(data):
        raise AssertionError("an oversized upload reached ffmpeg")

    monkeypatch.setattr(api, "transcode_to_adts", never)
    monkeypatch.setattr(api, "MAX_SOUND_SIZE", 1000)
    app, _ = _panel(tmp_path)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        status, body = await _upload(client, bytes(200_000))
    finally:
        await client.close()
    assert status == 400 and "too large" in body["error"]


async def test_the_uploads_filename_never_becomes_a_path(tmp_path, monkeypatch):
    async def fake_transcode(data):
        return _adts()

    monkeypatch.setattr(api, "transcode_to_adts", fake_transcode)
    app, _ = _panel(tmp_path)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        status, body = await _upload(client, b"x", "../../../etc/cron.d/evil.sh")
    finally:
        await client.close()
    assert status == 200, body
    assert body["sound"]["filename"] == "sound_1.aac"
    assert "/" not in body["sound"]["name"] and ".." not in body["sound"]["name"]
    assert sorted(os.listdir(tmp_path / "sounds" / "100")) == ["sound_1.aac", "sounds.json"]


async def test_deleting_an_entry_cannot_remove_a_file_outside_the_folder(tmp_path):
    d = _seed(tmp_path)
    outside = tmp_path / "devices.json"
    outside.write_text("{}")
    (d / "sounds.json").write_text(json.dumps(
        [{"id": 1, "name": "x", "filename": "../../devices.json"}]))
    app, _ = _panel(tmp_path)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        r = await client.delete("/api/devices/100/sounds/1")
    finally:
        await client.close()
    assert r.status == 200 and outside.exists()


# --- sounds stored before uploads were converted ----------------------------

async def test_an_unconverted_upload_is_converted_at_startup(tmp_path, monkeypatch):
    d = _seed(tmp_path, filename="sound_1.m4a", data=b"\x00\x00\x00\x18ftypM4A ")

    async def fake_transcode(data):
        return _adts(frames=25)

    monkeypatch.setattr(snd, "transcode_to_adts", fake_transcode)
    assert await snd.migrate_sounds(str(tmp_path)) == 1
    meta = json.loads((d / "sounds.json").read_text())
    assert meta[0]["filename"] == "sound_1.aac" and meta[0]["duration"] == 2
    assert not os.path.exists(d / "sound_1.m4a") and snd.is_adts((d / "sound_1.aac").read_bytes())
    assert await snd.migrate_sounds(str(tmp_path)) == 0, "already playable: nothing to do"


async def test_an_aac_the_device_cannot_play_is_converted_in_place(tmp_path, monkeypatch):
    """Already named `.aac`, but 44.1 kHz stereo: the new file replaces it
    under the same name and must not be deleted as "the old one"."""
    d = _seed(tmp_path, data=_adts(rate_index=4, channels=2))

    async def fake_transcode(data):
        return _adts(frames=25)

    monkeypatch.setattr(snd, "transcode_to_adts", fake_transcode)
    assert await snd.migrate_sounds(str(tmp_path)) == 1
    assert snd.is_playable((d / "sound_1.aac").read_bytes())


async def test_a_file_that_cannot_be_converted_is_left_as_it_was(tmp_path, monkeypatch):
    d = _seed(tmp_path, filename="sound_1.mp3", data=b"junk")
    before = (d / "sounds.json").read_text()

    async def refuse(data):
        raise snd.TranscodeError("nope")

    monkeypatch.setattr(snd, "transcode_to_adts", refuse)
    assert await snd.migrate_sounds(str(tmp_path)) == 0
    assert (d / "sounds.json").read_text() == before and (d / "sound_1.mp3").read_bytes() == b"junk"


async def test_the_conversion_survives_a_damaged_tree(tmp_path):
    assert await snd.migrate_sounds(str(tmp_path)) == 0, "no sounds directory at all"
    root = tmp_path / "sounds"
    (root / "100").mkdir(parents=True)
    (root / "100" / "sounds.json").write_text("{broken")
    (root / "200").mkdir()
    (root / "200" / "sounds.json").write_text(json.dumps(
        [7, {"id": 1}, {"id": 2, "filename": "../../x"}, {"id": 3, "filename": "missing.mp3"}]))
    (root / "stray-file").write_text("x")
    assert await snd.migrate_sounds(str(tmp_path)) == 0
