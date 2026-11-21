"""Proxy mode's `dev_discern_pic` rewrite: the cloud's face photos, normalised.

Issue #129: passed through raw, one 4096x3072 photo in the cloud's list ran a
D4SH's camera process out of memory on every start, restarting it every ~82 s.
Now every `discern[].url` points at our `/faces/` route, which serves that
photo at 224x224 (`ai/cloud_faces.py`), and a photo that cannot be fetched or
normalised is dropped from the list instead of being handed over.

Most tests stand a normaliser in for ffmpeg (absent on some dev machines); the
one marked for ffmpeg drives the real one on a real 4096x3072 JPEG.
"""
import asyncio
import json
import os
import re
import subprocess

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.ai import cloud_faces
from petkit_local.ai.cloud_faces import (
    CLOUD_FACES_DIR, INDEX_FILE, CloudFaces, cloud_face_filename, drop_faces,
)
from petkit_local.ai.pets import _NON_URL_SAFE, PetRegistry
from petkit_local.devices.base import Device
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.http import appkeys as device_keys
from petkit_local.http.proxy import close_proxy_session
from petkit_local.http.redact import RULE_FACES, RedactionPolicy, redact_body
from petkit_local.http.server import create_app
from petkit_local.media.transcode import FACE_PHOTO_SIZE, have_ffmpeg, jpeg_dimensions
from petkit_local.web.hub import EventHub

HDR = {"X-Device": "id=100&sn=SN100&type=T5"}
FACES_BASE = "http://192.0.2.199:8080"
#: Everything the device's shell command may see in a filename.
SHELL_SAFE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _jpeg(width: int, height: int) -> bytes:
    """A JPEG header `jpeg_dimensions` reads as width x height."""
    return (b"\xff\xd8\xff\xe0" + (16).to_bytes(2, "big") + b"\x00" * 14
            + b"\xff\xc0" + (17).to_bytes(2, "big") + b"\x08"
            + height.to_bytes(2, "big") + width.to_bytes(2, "big") + b"\x00" * 9
            + b"\xff\xd9")


BIG = _jpeg(4096, 3072)
SMALL = _jpeg(FACE_PHOTO_SIZE, FACE_PHOTO_SIZE)


@pytest.fixture
def normalise(monkeypatch):
    """Stand in for ffmpeg: anything not already 224x224 comes out 224x224."""
    calls = []

    async def fake(data, size=FACE_PHOTO_SIZE):
        calls.append(jpeg_dimensions(data))
        return data if jpeg_dimensions(data) == (size, size) else _jpeg(size, size)

    monkeypatch.setattr(cloud_faces, "normalize_face_photo", fake)
    return calls


def _config(data_dir, **kw):
    base = {
        "api_url": f"{FACES_BASE}/6/",
        "mqtt_port": 1883,
        "bucket_endpoint": "https://192.0.2.199:9000",
        "data_dir": str(data_dir),
        "capture": False,
        "capture_dir": str(data_dir / "capture"),
        "proxy_mode": True,
        "proxy_upstream": "",
        "proxy_block_run_cmd": True,
        "proxy_block_ota": True,
        "proxy_media_real_oss": False,
    }
    base.update(kw)
    return base


class Cloud:
    """A fake PetKit: `dev_discern_pic` plus the photo store it points at."""

    def __init__(self, photos: dict[str, bytes | int | float]):
        #: path -> bytes to serve, an HTTP status, or seconds to stall first.
        self.photos = photos
        self.hits: list[str] = []
        self.entries: list[dict] = []
        self.client: TestClient | None = None
        self.base = ""

    async def start(self):
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", self.handle)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        self.base = str(self.client.make_url("")).rstrip("/")
        return self

    def url(self, name: str) -> str:
        return f"{self.base}/pavatar/2026/11/14/{name}"

    async def handle(self, request):
        self.hits.append(request.path)
        if request.path.endswith("/dev_discern_pic"):
            return web.json_response({"result": {"list": self.entries}})
        name = request.path.rsplit("/", 1)[-1]
        photo = self.photos.get(name, 404)
        if isinstance(photo, float):
            await asyncio.sleep(photo)
            photo = BIG
        if isinstance(photo, int):
            return web.Response(status=photo)
        return web.Response(body=photo, content_type="image/jpeg")

    async def close(self):
        await self.client.close()


async def _device_app(config, hub=None):
    app = create_app(DeviceRegistry(), config)
    app[device_keys.EVENT_HUB] = hub if hub is not None else EventHub()
    client = TestClient(TestServer(app))
    await client.start_server()
    r = await client.post("/6/t5/dev_signup", headers=HDR)
    assert r.status == 200
    return client


async def _close(client):
    await close_proxy_session(client.app)
    await client.close()


async def _discern(client):
    r = await client.post("/6/t5/dev_discern_pic", headers=HDR)
    assert r.status == 200
    return await r.json()


def _faces_path(url: str) -> str:
    assert url.startswith(f"{FACES_BASE}/faces/"), url
    return url[len(FACES_BASE):]


# --- the rewrite ----------------------------------------------------------------

async def test_a_4096x3072_cloud_photo_is_served_at_224x224(tmp_path, normalise):
    cloud = await (Cloud({"big": BIG, "crop": SMALL}).start())
    cloud.entries = [
        {"id": 101392625, "color": 3, "discern": [
            {"id": 1794662888, "url": cloud.url("big"), "extra": "kept"},
            {"id": 17, "url": cloud.url("crop")},
        ]},
        {"id": 7, "discern": [{"id": 18, "url": cloud.url("crop"), "color": "#fff"}],
         "petExtra": [1, 2]},
    ]
    hub = EventHub()
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base), hub)
    try:
        body = await _discern(client)
        got = body["result"]["list"]

        # Ids, colours, extra keys and order are the cloud's; only urls changed.
        expected = json.loads(json.dumps(cloud.entries))
        for pet in expected:
            for face in pet["discern"]:
                face["url"] = f"{FACES_BASE}/faces/cloud_{face['id']}.jpg"
        assert got == expected
        assert [list(f) for f in got[0]["discern"]] == [["id", "url", "extra"], ["id", "url"]]

        # The device's GET is served the normalised copy, from the cache.
        before = len(cloud.hits)
        r = await client.get(_faces_path(got[0]["discern"][0]["url"]))
        assert r.status == 200
        assert jpeg_dimensions(await r.read()) == (FACE_PHOTO_SIZE, FACE_PHOTO_SIZE)
        assert cloud.hits[before:] == []
        assert (4096, 3072) in normalise

        # Recorded like every other redaction, so the panel shows it.
        assert hub.redaction_counts()[RULE_FACES] == 3
        assert os.path.isfile(tmp_path / CLOUD_FACES_DIR / "cloud_1794662888.jpg")
    finally:
        await _close(client)
        await cloud.close()


@pytest.mark.skipif(not have_ffmpeg(), reason="needs ffmpeg")
async def test_the_real_normaliser_shrinks_a_real_4096x3072_photo(tmp_path):
    big = subprocess.run(
        ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=c=gray:s=4096x3072",
         "-frames:v", "1", "-pix_fmt", "yuvj420p", "-f", "mjpeg", "pipe:1"],
        capture_output=True, check=True, timeout=60).stdout
    assert jpeg_dimensions(big) == (4096, 3072)
    cloud = await (Cloud({"big": big}).start())
    cloud.entries = [{"id": 1, "discern": [{"id": 5, "url": cloud.url("big")}]}]
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base))
    try:
        body = await _discern(client)
        url = body["result"]["list"][0]["discern"][0]["url"]
        r = await client.get(_faces_path(url))
        assert r.status == 200
        assert jpeg_dimensions(await r.read()) == (FACE_PHOTO_SIZE, FACE_PHOTO_SIZE)
    finally:
        await _close(client)
        await cloud.close()


@pytest.mark.parametrize("face_id", [
    "1;rm -rf /", "$(reboot)", "`id`", "../../etc/passwd", "1 2", None, "", -5, 0,
    True, 1.5, "9" * 400, {"x": 1}, "abc",
])
def test_the_filename_passes_the_device_shell_whitelist(face_id):
    """It lands in `wget -O /tmp/pet_face_pic.jpg -c <url>` on the device."""
    name = cloud_face_filename(face_id, "https://img.example/a b;c.jpg")
    assert SHELL_SAFE.match(name), name
    assert not _NON_URL_SAFE.search(name)
    assert cloud_faces.FILENAME_RE.match(name)
    assert name.startswith("cloud_") and name.endswith(".jpg")


def test_an_integer_id_names_the_file_and_anything_else_hashes_the_url():
    assert cloud_face_filename(1794662888, "https://x/a") == "cloud_1794662888.jpg"
    assert cloud_face_filename("42", "https://x/a") == "cloud_42.jpg"
    hostile = cloud_face_filename("1;rm -rf", "https://x/a")
    assert hostile != "cloud_1.jpg" and hostile.startswith("cloud_u")
    assert cloud_face_filename(None, "https://x/a") != cloud_face_filename(None, "https://x/b")


def test_the_rule_is_off_without_somewhere_to_serve_from():
    """`faces_base` unset — MQTT, the presence heartbeat — leaves urls alone."""
    payload = {"result": {"list": [{"id": 1, "discern": [{"id": 2, "url": "https://x/a"}]}]}}
    device = Device(device_type="t5", petkit_id=100, serial_number="SN100", mac="aa")
    off = redact_body(json.dumps(payload).encode(), endpoint="/6/t5/dev_discern_pic",
                      policy=RedactionPolicy(device=device, api_url=f"{FACES_BASE}/6/"))
    assert json.loads(off.body) == payload and off.records == []

    on = redact_body(json.dumps(payload).encode(), endpoint="/6/t5/dev_discern_pic",
                     policy=RedactionPolicy(device=device, faces_base=FACES_BASE))
    assert json.loads(on.body)["result"]["list"][0]["discern"][0]["url"] == \
        f"{FACES_BASE}/faces/cloud_2.jpg"
    assert on.captured["cloud_faces"] == {"cloud_2.jpg": "https://x/a"}
    assert [r.rule for r in on.records] == [RULE_FACES]
    assert on.records[0].path == "result.list[0].discern[0].url"


# --- failures --------------------------------------------------------------------

async def test_a_photo_that_cannot_be_fetched_is_dropped(tmp_path, normalise):
    cloud = await (Cloud({"good": BIG, "gone": 404}).start())
    cloud.entries = [
        {"id": 1, "discern": [{"id": 10, "url": cloud.url("gone")},
                              {"id": 11, "url": cloud.url("good")}]},
        {"id": 2, "discern": [{"id": 20, "url": cloud.url("gone")}]},
        {"id": 3, "discern": [{"id": 30, "url": cloud.url("good")}]},
    ]
    hub = EventHub()
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base), hub)
    try:
        got = (await _discern(client))["result"]["list"]
        # Face 10 goes, pet 2 (left with no photo) goes, order is kept.
        assert [(p["id"], [f["id"] for f in p["discern"]]) for p in got] == [
            (1, [11]), (3, [30])]
        assert hub.redaction_counts()[RULE_FACES] == 4 + 2  # 4 rewrites, 2 drops
        r = await client.get("/faces/cloud_10.jpg")
        assert r.status == 404
    finally:
        await _close(client)
        await cloud.close()


async def test_a_photo_that_cannot_be_normalised_is_never_passed_raw(tmp_path, monkeypatch):
    """`normalize_face_photo` returns its input when ffmpeg fails or is missing."""
    async def broken(data, size=FACE_PHOTO_SIZE):
        return data

    monkeypatch.setattr(cloud_faces, "normalize_face_photo", broken)
    cloud = await (Cloud({"big": BIG, "notjpeg": b"<html>", "crop": SMALL}).start())
    cloud.entries = [{"id": 1, "discern": [
        {"id": 10, "url": cloud.url("big")},
        {"id": 11, "url": cloud.url("notjpeg")},
        {"id": 12, "url": cloud.url("crop")},
    ]}]
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base))
    try:
        got = (await _discern(client))["result"]["list"]
        # A 224x224 crop needs no ffmpeg; the other two are dropped.
        assert [f["id"] for f in got[0]["discern"]] == [12]
        for name in ("cloud_10.jpg", "cloud_11.jpg"):
            assert (await client.get(f"/faces/{name}")).status == 404
        r = await client.get("/faces/cloud_12.jpg")
        assert await r.read() == SMALL
    finally:
        await _close(client)
        await cloud.close()


async def test_a_slow_photo_keeps_its_url_and_is_served_when_it_arrives(
        tmp_path, normalise, monkeypatch):
    monkeypatch.setattr(cloud_faces, "PREFETCH_TIMEOUT", 0.1)
    cloud = await (Cloud({"slow": 0.6}).start())
    cloud.entries = [{"id": 1, "discern": [{"id": 10, "url": cloud.url("slow")}]}]
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base))
    try:
        got = (await _discern(client))["result"]["list"]
        assert got[0]["discern"][0]["url"] == f"{FACES_BASE}/faces/cloud_10.jpg"
        # The device's GET joins the fetch already running, not a second one.
        r = await client.get("/faces/cloud_10.jpg")
        assert r.status == 200
        assert jpeg_dimensions(await r.read()) == (FACE_PHOTO_SIZE, FACE_PHOTO_SIZE)
        assert cloud.hits.count("/pavatar/2026/11/14/slow") == 1
    finally:
        await _close(client)
        await cloud.close()


async def test_an_unreadable_reply_body_is_left_alone():
    body = b"not json"
    assert drop_faces(body, {"cloud_1.jpg"}) == (body, [])


# --- restart, proxy off, and our own pets -----------------------------------------

async def test_a_restart_keeps_serving_a_cached_face(tmp_path, normalise):
    cloud = await (Cloud({"big": BIG}).start())
    cloud.entries = [{"id": 1, "discern": [{"id": 10, "url": cloud.url("big")}]}]
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base))
    try:
        await _discern(client)
    finally:
        await _close(client)
    index = json.loads((tmp_path / CLOUD_FACES_DIR / INDEX_FILE).read_text())
    assert index == {"cloud_10.jpg": cloud.url("big")}

    # A new process, with PetKit unreachable: the cached copy still answers.
    await cloud.close()
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base))
    try:
        r = await client.get("/faces/cloud_10.jpg")
        assert r.status == 200
        assert jpeg_dimensions(await r.read()) == (FACE_PHOTO_SIZE, FACE_PHOTO_SIZE)
    finally:
        await _close(client)


async def test_a_restart_that_lost_the_photo_fetches_it_again_from_the_index(
        tmp_path, normalise):
    cloud = await (Cloud({"big": BIG}).start())
    cloud.entries = [{"id": 1, "discern": [{"id": 10, "url": cloud.url("big")}]}]
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base))
    try:
        await _discern(client)
    finally:
        await _close(client)
    os.remove(tmp_path / CLOUD_FACES_DIR / "cloud_10.jpg")

    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base))
    try:
        r = await client.get("/faces/cloud_10.jpg")
        assert r.status == 200
        assert jpeg_dimensions(await r.read()) == (FACE_PHOTO_SIZE, FACE_PHOTO_SIZE)
    finally:
        await _close(client)
        await cloud.close()


async def test_with_proxy_off_nothing_is_fetched_for_an_old_cloud_url(tmp_path, normalise):
    cloud = await (Cloud({"big": BIG}).start())
    faces = CloudFaces(str(tmp_path / CLOUD_FACES_DIR))
    await faces.remember({"cloud_10.jpg": cloud.url("big")})
    client = await _device_app(_config(tmp_path, proxy_upstream=cloud.base, proxy_mode=False))
    try:
        r = await client.get("/faces/cloud_10.jpg")
        assert r.status == 404
        assert cloud.hits == []
        # Nor for a name the rewrite never handed out, proxy on or off.
        client.app[device_keys.CONFIG]["proxy_mode"] = True
        assert (await client.get("/faces/cloud_99.jpg")).status == 404
        assert (await client.get("/faces/cloud_1;id.jpg")).status == 404
        assert cloud.hits == []
    finally:
        await _close(client)
        await cloud.close()


async def test_a_replaced_photo_is_fetched_again(tmp_path, normalise):
    faces = CloudFaces(str(tmp_path / CLOUD_FACES_DIR))
    os.makedirs(faces.root)
    (tmp_path / CLOUD_FACES_DIR / "cloud_10.jpg").write_bytes(SMALL)
    await faces.remember({"cloud_10.jpg": "https://img.example/a.jpg?sig=1"})
    # A re-signed URL for the same photo keeps the cached copy...
    await faces.remember({"cloud_10.jpg": "https://img.example/a.jpg?sig=2"})
    assert faces.cached("cloud_10.jpg")
    # ...a different photo under the same face id does not.
    await faces.remember({"cloud_10.jpg": "https://img.example/b.jpg"})
    assert faces.cached("cloud_10.jpg") is None


async def test_our_own_pets_are_served_as_before(tmp_path, pet_registry: PetRegistry):
    """Proxy off, the local path: our filenames, our faces/ directory, no cache."""
    config = _config(tmp_path, proxy_mode=False)
    app = create_app(DeviceRegistry(), config)
    app[device_keys.PET_REGISTRY] = pet_registry
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        device = app[device_keys.REGISTRY].get_or_create(
            petkit_id=100, device_type="t5", serial_number="SN100")
        pet = await pet_registry.create("Mruczek", device_ids=[device.petkit_id])
        face = await pet_registry.add_face(pet["id"], SMALL)
        body = await _discern(client)
        name = os.path.basename(face["photo_path"])
        assert body == {"result": {"list": [{"id": pet["id"], "discern": [
            {"id": face["id"], "url": f"{FACES_BASE}/faces/{name}"}]}]}}
        assert name.startswith("pet_")
        r = await client.get(f"/faces/{name}")
        assert r.status == 200 and await r.read() == SMALL
        assert not (tmp_path / CLOUD_FACES_DIR).exists()
    finally:
        await _close(client)
