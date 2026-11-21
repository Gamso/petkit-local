"""Face photo quality on upload, and which devices took each face (issue #132).

What is held here:

* a photo under 224 px on its short side is refused, by the registry, the
  panel (400) and the app API (422 `photo_too_small`), judged from the header;
* blur, darkness and overexposure are warnings from the grey 224x224 crop,
  never refusals; `not_square` comes from the header, decoder or not;
* with no decoder the pixel checks are reported as not done and the upload
  still succeeds;
* `accepted_by` / `waiting_for` come from each assigned device's `discernPic`;
* both upload responses carry `quality`, and the app API's match the schema.

The pixel rules are tested on synthetic grey buffers, which need nothing; the
end-to-end decode needs ffmpeg and is skipped without it, as elsewhere.
"""
from __future__ import annotations

import asyncio
import random
import shutil
import subprocess
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.ai import face_quality as fq
from petkit_local.ai.pets import FACE_SMALL, PetRegistry, face_acceptance, faces_held
from petkit_local.config import Config
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.media import transcode
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app
from tests.test_app_api import _Schemas
from tests.test_app_api_pets import JPEG, PetEnv, _refused, p  # noqa: F401 (fixture)

N = fq.ANALYSIS_SIZE


def header_jpeg(width: int, height: int) -> bytes:
    """A JPEG header that declares `width`x`height`, and no image to decode."""
    return (b"\xff\xd8\xff\xc0\x00\x11\x08" + height.to_bytes(2, "big")
            + width.to_bytes(2, "big") + b"\x03\x01\x22\x00\x02\x11\x01\x03\x11\x01"
            + b"\xff\xd9")


def _no_ffmpeg(monkeypatch) -> None:
    monkeypatch.setattr(transcode, "have_ffmpeg", lambda: False)


# --- the pixel rules, on synthetic grey -------------------------------------------

def _grey(fn) -> bytes:
    return bytes(max(0, min(255, int(fn(x, y)))) for y in range(N) for x in range(N))


def _textured(base: int = 128, spread: int = 60) -> bytes:
    rng = random.Random(132)
    return _grey(lambda x, y: base + rng.randint(-spread, spread))


def test_a_sharp_well_exposed_crop_has_no_warnings():
    metrics = fq.grey_metrics(_textured(), N, N)
    assert metrics["laplacian_var"] > fq.BLUR_MIN_VARIANCE
    assert fq.pixel_warnings(metrics) == []


def test_a_smooth_crop_is_blurry():
    # A gentle gradient: mid-grey, nothing clipped, and no fine detail at all.
    metrics = fq.grey_metrics(_grey(lambda x, y: 60 + (x + y) * 120 / (2 * N)), N, N)
    assert metrics["laplacian_var"] < fq.BLUR_MIN_VARIANCE
    assert fq.pixel_warnings(metrics) == [fq.WARN_BLURRY]


def test_a_dark_crop_is_too_dark_and_a_bright_one_overexposed():
    dark = fq.grey_metrics(_textured(base=20, spread=15), N, N)
    assert fq.WARN_TOO_DARK in fq.pixel_warnings(dark)
    assert fq.WARN_OVEREXPOSED not in fq.pixel_warnings(dark)
    bright = fq.grey_metrics(_textured(base=240, spread=15), N, N)
    assert fq.WARN_OVEREXPOSED in fq.pixel_warnings(bright)
    assert fq.WARN_TOO_DARK not in fq.pixel_warnings(bright)


def test_a_black_cat_well_exposed_is_not_too_dark():
    # Dark fur that is not crushed: a mean above DARK_MEAN, little at 0.
    metrics = fq.grey_metrics(_textured(base=70, spread=40), N, N)
    assert fq.WARN_TOO_DARK not in fq.pixel_warnings(metrics)


# --- the header: size and shape ---------------------------------------------------

@pytest.mark.parametrize("width, height, refused", [
    (223, 400, True), (400, 223, True), (224, 224, False), (4032, 3024, False)])
def test_the_short_side_must_be_224(width: int, height: int, refused: bool):
    assert (fq.size_refusal(header_jpeg(width, height)) == fq.FACE_TOO_SMALL) is refused


def test_a_header_that_cannot_be_read_refuses_nothing():
    assert fq.size_refusal(JPEG) is None


async def test_without_ffmpeg_the_pixels_are_not_checked_and_nothing_fails(monkeypatch):
    _no_ffmpeg(monkeypatch)
    q = await fq.assess_face_photo(header_jpeg(1000, 400))
    assert q.as_json() == {"checked": False, "warnings": [fq.WARN_NOT_SQUARE]}
    q = await fq.assess_face_photo(header_jpeg(400, 300))
    assert q.as_json() == {"checked": False, "warnings": []}
    assert (await fq.assess_face_photo(JPEG)).as_json() == {"checked": False, "warnings": []}


async def test_a_refused_photo_is_not_decoded(monkeypatch):
    async def must_not_run(*a, **kw):
        raise AssertionError("decoded a photo that is refused anyway")
    monkeypatch.setattr(fq, "_grey_crop", must_not_run)
    q = await fq.assess_face_photo(header_jpeg(100, 100))
    assert q.refusal == fq.FACE_TOO_SMALL and not q.checked


async def test_the_registry_refuses_a_small_photo(pet_registry: PetRegistry):
    pet = await pet_registry.create("Kinder")
    small = header_jpeg(200, 200)
    assert await pet_registry.face_refusal(pet["id"], small) == FACE_SMALL
    assert await pet_registry.add_face(pet["id"], small) is None
    assert await pet_registry.faces(pet["id"]) == []


# --- end to end, through ffmpeg -----------------------------------------------------

def _photo(source: str, width: int, height: int, vf: str | None = None) -> bytes:
    return subprocess.run(
        [shutil.which("ffmpeg"), "-v", "error", "-f", "lavfi",
         "-i", f"{source}=size={width}x{height}:rate=1",
         *(["-vf", vf] if vf else []), "-frames:v", "1", "-q:v", "2",
         "-f", "mjpeg", "pipe:1"], capture_output=True, check=True).stdout


needs_ffmpeg = pytest.mark.skipif(not transcode.have_ffmpeg(), reason="ffmpeg is not installed")


@needs_ffmpeg
@pytest.mark.parametrize("source, size, vf, expected", [
    ("testsrc", (640, 480), None, []),
    ("testsrc", (1200, 480), None, [fq.WARN_NOT_SQUARE]),
    ("testsrc", (640, 480), "gblur=sigma=12", [fq.WARN_BLURRY]),
    ("color=c=black", (480, 480), None, [fq.WARN_BLURRY, fq.WARN_TOO_DARK]),
    ("color=c=white", (480, 480), None, [fq.WARN_BLURRY, fq.WARN_OVEREXPOSED]),
])
async def test_a_real_photo_is_judged_on_its_crop(source, size, vf, expected):
    photo = await asyncio.to_thread(_photo, source, *size, vf)
    q = await fq.assess_face_photo(photo)
    assert q.checked, q
    assert q.warnings == expected, q.metrics


# --- which devices took a face ------------------------------------------------------

def _registry() -> DeviceRegistry:
    reg = DeviceRegistry()
    reg.get_or_create(petkit_id=100, device_type="t5", serial_number="SN100")   # AI
    reg.get_or_create(petkit_id=101, device_type="t5", serial_number="SN101")   # AI
    reg.get_or_create(petkit_id=102, device_type="t4", serial_number="SN102")   # no AI
    return reg


def test_accepted_by_is_read_from_discern_pic():
    reg = _registry()
    reg.get(100).state["discernPic"] = [7, "8", True, "junk"]
    assert faces_held(reg.get(100)) == {7, 8}
    assert faces_held(reg.get(101)) == set()
    pet = {"device_ids_json": "[101, 100, 102, 999]"}
    assert face_acceptance(7, pet, reg) == {"accepted_by": [100], "waiting_for": [101]}
    reg.get(101).state["discernPic"] = [7]
    assert face_acceptance(7, pet, reg) == {"accepted_by": [101, 100], "waiting_for": []}
    # A device that cannot recognise waits for nothing; nor does a pet on no device.
    assert face_acceptance(9, pet, reg) == {"accepted_by": [], "waiting_for": [101, 100]}
    assert face_acceptance(7, {"device_ids_json": None}, reg) == {
        "accepted_by": [], "waiting_for": []}


# --- the app API ---------------------------------------------------------------------

async def test_the_app_refuses_a_small_photo(p: PetEnv):  # noqa: F811
    pet = await p.pets.create("Kinder")
    await _refused(await p.upload(pet["id"], header_jpeg(200, 300)), 422, "photo_too_small")
    assert await p.pets.faces(pet["id"]) == []


async def test_the_app_upload_says_quality_and_who_took_it(p: PetEnv, monkeypatch):  # noqa: F811
    _no_ffmpeg(monkeypatch)
    schemas = _Schemas()
    pet = await p.pets.create("Kinder", device_ids=[100])
    r = await p.upload(pet["id"], header_jpeg(1000, 400))
    assert r.status == 201, await r.text()
    body = await r.json()
    assert body["quality"] == {"checked": False, "warnings": ["not_square"]}
    schemas.check(body["quality"], "PhotoQuality")
    face = body["face"]
    schemas.check(face, "PetFace")
    assert (face["accepted_by"], face["waiting_for"]) == ([], [100])

    # The device reports holding it: the faces list and the pet both say so.
    p.device.state["discernPic"] = [face["id"]]
    listed = (await (await p.call("GET", f"/pets/{pet['id']}/faces")).json())["faces"]
    assert listed[0]["accepted_by"] == [100] and listed[0]["waiting_for"] == []
    summary = (await (await p.call("GET", f"/pets/{pet['id']}")).json())["pet"]
    schemas.check(summary, "PetDetail")
    assert summary["faces"] == listed


# --- the panel -------------------------------------------------------------------------

@pytest.fixture
async def panel(event_store, tmp_path: Path):
    live = Config().to_app_config()
    pets = PetRegistry(event_store, str(tmp_path / "faces"), settings=live)
    devices = _registry()
    cfg = {"api_url": "http://x/6/", "capture": False, "capture_dir": "/nope",
           "data_dir": str(tmp_path), "settings_path": str(tmp_path / "overrides.json")}
    app = create_panel_app(devices, BLERegistry(), EventHub(), cfg, None,
                           live_config=live, event_store=event_store, pet_registry=pets)
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client, pets, devices
    await client.close()


async def test_the_panel_upload_says_quality_and_who_took_it(panel, monkeypatch):
    _no_ffmpeg(monkeypatch)
    client, pets, devices = panel
    pet = await pets.create("Lilith", device_ids=[100, 101])
    r = await client.post(f"/api/pets/{pet['id']}/faces", data=header_jpeg(150, 300))
    assert r.status == 400
    assert (await r.json())["error"] == "too small: at least 224 px on the short side"

    r = await client.post(f"/api/pets/{pet['id']}/faces", data=header_jpeg(224, 224))
    assert r.status == 200, await r.text()
    body = await r.json()
    assert body["quality"] == {"checked": False, "warnings": []}
    face_id = body["face"]["id"]
    assert body["face"] == {"id": face_id, "url": f"api/pets/{pet['id']}/faces/{face_id}/photo",
                            "accepted_by": [], "waiting_for": [100, 101]}

    devices.get(101).state["discernPic"] = [face_id]
    pets_listed = (await (await client.get("/api/pets")).json())["pets"]
    assert pets_listed[0]["faces"][0]["accepted_by"] == [101]
    assert pets_listed[0]["faces"][0]["waiting_for"] == [100]
    faces = (await (await client.get(f"/api/pets/{pet['id']}/faces")).json())["faces"]
    assert faces == pets_listed[0]["faces"]
