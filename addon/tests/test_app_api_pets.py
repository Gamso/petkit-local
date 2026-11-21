"""The phone app API's pet writes (issue #131): pets, and their reference face photos.

What is held here:

* a pet is created, edited and deleted with the panel's rules — a changed
  weight forgets the learned one, Home Assistant is told — but a field of the
  wrong type is REFUSED rather than read as "no value", and weights are grams;
* a face photo is the raw JPEG body, bounded in size, refused when it is not a
  JPEG or the pet is full, normalised to 224x224, and served back only under
  the pet that owns it;
* every route needs a session;
* each change reaches every `/ws` socket as a `pet` or `pet_removed` message;
* `supports_ai` says which devices recognise pets;
* the panel's pet routes answer as they did before their rules moved into
  `PetRegistry`;
* every answer has exactly its schema's keys.
"""
from __future__ import annotations

import asyncio
import io
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.ai.pets import PetRegistry
from petkit_local.app_api.appkeys import UPDATE_FEED
from petkit_local.app_api.handlers import pets as pet_handlers
from petkit_local.app_api.server import PREFIX, create_app_api
from petkit_local.app_api.updates import UpdateFeed
from petkit_local.config import Config
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events.store import MAX_FACES_PER_PET
from petkit_local.media.transcode import FACE_PHOTO_SIZE, have_ffmpeg, jpeg_dimensions
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app
from tests.test_app_api import PASSWORD, Env, _Schemas
from tests.test_app_api_updates import _next, _quiet

#: Valid JPEG magic, not a decodable image: stored as uploaded when it cannot
#: be converted (`test_ai_pets.py::test_an_unconvertible_photo_is_still_stored`).
JPEG = b"\xff\xd8\xff\xe0" + b"J" * 64

COALESCE = 0.2
SWEEP = 0.6


class FakePublisher:
    """Home Assistant's publisher: records which pets it was told about."""

    def __init__(self) -> None:
        self.pets: list[dict[str, Any]] = []

    async def publish_pet_discovery(self, pet: dict[str, Any]) -> None:
        self.pets.append(pet)

    async def publish_state(self, device) -> None:
        pass


class PetEnv(Env):
    def __init__(self, tmp: Path, store: Any, pet_registry: Any) -> None:
        super().__init__(tmp, store, pet_registry)
        self.publisher = FakePublisher()
        self.app = create_app_api(self.auth, self.registry, event_store=store,
                                  pet_registry=pet_registry, media_root=str(self.media_root),
                                  data_dir=str(tmp), throttle=self.throttle,
                                  ha_publisher=self.publisher)
        self.feed = UpdateFeed(self.auth, self.registry, EventHub(), event_store=store,
                               pet_registry=pet_registry, media_root=str(self.media_root),
                               coalesce=COALESCE, sweep=SWEEP)
        self.app[UPDATE_FEED] = self.feed
        self.token = ""

    async def call(self, method: str, path: str, body: Any = None, *,
                   token: str | None = None, **kw: Any):
        headers = kw.pop("headers", {})
        headers.setdefault("Authorization", f"Bearer {self.token if token is None else token}")
        if body is not None:
            kw["json"] = body
        return await self.client.request(method, PREFIX + path, headers=headers, **kw)

    async def upload(self, pet_id: int, data: Any, **kw: Any):
        headers = {"Content-Type": "image/jpeg", **kw.pop("headers", {})}
        return await self.call("POST", f"/pets/{pet_id}/faces", data=data, headers=headers, **kw)

    async def connect(self):
        return await self.client.ws_connect(
            f"{PREFIX}/ws", headers={"Authorization": f"Bearer {self.token}"})


@pytest.fixture
async def p(tmp_path: Path, event_store, pet_registry):
    e = PetEnv(tmp_path, event_store, pet_registry)
    await e.start()
    await e.auth.set_password(PASSWORD)
    e.token = await e.login()
    yield e
    await e.client.close()


async def _refused(r, status: int, code: str) -> None:
    assert r.status == status, await r.text()
    body = await r.json()
    assert body["error"] == code and body["message"], body


# --- create ---------------------------------------------------------------------

async def test_a_pet_is_created_with_its_weight_in_grams_and_ha_is_told(p: PetEnv):
    r = await p.call("POST", "/pets", {"name": "  Kinder ", "weight_g": 5100,
                                       "device_ids": [100, 100, 7]})
    assert r.status == 201, await r.text()
    pet = (await r.json())["pet"]
    _Schemas().check(pet, "Pet")
    assert pet["name"] == "Kinder"
    assert pet["device_ids"] == [100, 7]
    assert pet["entered_weight_g"] == pytest.approx(5100)
    assert pet["photo"] is None and pet["faces"] == []
    stored = await p.pets.get(pet["id"])
    assert stored["weight"] == pytest.approx(5.1), "kilograms, as the panel stores it"
    assert [x["id"] for x in p.publisher.pets] == [pet["id"]]

    listed = (await (await p.call("GET", "/pets")).json())["pets"]
    assert [x["id"] for x in listed] == [pet["id"]]


async def test_a_pet_with_only_a_name_has_no_weight_and_no_devices(p: PetEnv):
    pet = (await (await p.call("POST", "/pets", {"name": "Lilith"})).json())["pet"]
    assert (pet["entered_weight_g"], pet["weight_g"], pet["device_ids"]) == (None, None, [])


@pytest.mark.parametrize("body", [
    {},
    {"name": ""},
    {"name": "   "},
    {"name": 7},
    {"name": "A", "weight_g": "4200"},
    {"name": "A", "weight_g": True},
    {"name": "A", "weight_g": 0},
    {"name": "A", "weight_g": -5},
    {"name": "A", "weight_g": 100_001},
    {"name": "A", "device_ids": 100},
    {"name": "A", "device_ids": ["100"]},
    {"name": "A", "device_ids": [True]},
])
async def test_a_bad_create_is_refused_and_creates_nothing(p: PetEnv, body: dict):
    await _refused(await p.call("POST", "/pets", body), 400, "invalid_pet")
    assert await p.pets.all() == []
    assert p.publisher.pets == []


async def test_a_create_needs_a_json_object(p: PetEnv):
    await _refused(await p.call("POST", "/pets", data="name=A"), 415, "unsupported_media_type")
    await _refused(await p.call("POST", "/pets", data="{", headers={
        "Content-Type": "application/json"}), 400, "bad_json")
    await _refused(await p.call("POST", "/pets", ["A"]), 400, "bad_json")


# --- edit -----------------------------------------------------------------------

async def test_an_edit_changes_only_what_it_names(p: PetEnv):
    pet = await p.pets.create("Kinder", device_ids=[100], weight=5.1)
    r = await p.call("PATCH", f"/pets/{pet['id']}", {"name": "Kinder II"})
    assert r.status == 200, await r.text()
    body = (await r.json())["pet"]
    _Schemas().check(body, "Pet")
    assert (body["name"], body["device_ids"]) == ("Kinder II", [100])
    assert body["entered_weight_g"] == pytest.approx(5100)

    body = (await (await p.call("PATCH", f"/pets/{pet['id']}", {"device_ids": []})).json())["pet"]
    assert body["device_ids"] == [] and body["name"] == "Kinder II"
    body = (await (await p.call("PATCH", f"/pets/{pet['id']}",
                                {"weight_g": None})).json())["pet"]
    assert body["entered_weight_g"] is None
    assert len(p.publisher.pets) == 3


async def _learned(pets: PetRegistry, pet_id: int) -> None:
    await pets.update(pet_id, learned_weight_g=5400, learned_samples=5)


async def test_a_new_weight_forgets_the_learned_one_as_in_the_panel(p: PetEnv):
    pet = await p.pets.create("Kinder", weight=5.1)
    await _learned(p.pets, pet["id"])
    body = (await (await p.call("PATCH", f"/pets/{pet['id']}", {"name": "K"})).json())["pet"]
    assert body["weight_learned"] is True and body["weight_g"] == 5400

    # The same weight again is not a correction.
    body = (await (await p.call("PATCH", f"/pets/{pet['id']}",
                                {"weight_g": 5100})).json())["pet"]
    assert body["weight_learned"] is True

    body = (await (await p.call("PATCH", f"/pets/{pet['id']}",
                                {"weight_g": 4900})).json())["pet"]
    assert body["weight_learned"] is False and body["weight_g"] == pytest.approx(4900)
    stored = await p.pets.get(pet["id"])
    assert (stored["learned_weight_g"], stored["learned_samples"]) == (None, 0)


async def test_the_learned_weight_can_be_reset_on_its_own(p: PetEnv):
    pet = await p.pets.create("Kinder", weight=5.1)
    await _learned(p.pets, pet["id"])
    body = (await (await p.call("PATCH", f"/pets/{pet['id']}",
                                {"reset_learned_weight": True})).json())["pet"]
    assert body["weight_learned"] is False and body["weight_g"] == pytest.approx(5100)


@pytest.mark.parametrize("body, code", [
    ({}, "bad_request"),
    ({"unknown": 1}, "bad_request"),
    ({"reset_learned_weight": False}, "bad_request"),
    ({"reset_learned_weight": "yes"}, "invalid_pet"),
    ({"name": ""}, "invalid_pet"),
    ({"weight_g": "heavy"}, "invalid_pet"),
    ({"device_ids": [1.5]}, "invalid_pet"),
])
async def test_a_bad_edit_is_refused_and_changes_nothing(p: PetEnv, body: dict, code: str):
    pet = await p.pets.create("Kinder", device_ids=[100], weight=5.1)
    await _refused(await p.call("PATCH", f"/pets/{pet['id']}", body), 400, code)
    assert await p.pets.get(pet["id"]) == pet
    assert p.publisher.pets == []


async def test_an_unknown_pet_is_404_on_every_route(p: PetEnv):
    for method, path, body in (("PATCH", "/pets/999", {"name": "X"}),
                               ("DELETE", "/pets/999", None),
                               ("PATCH", "/pets/abc", {"name": "X"}),
                               ("GET", "/pets/999/faces", None),
                               ("DELETE", "/pets/999/faces/1", None),
                               ("GET", "/pets/999/faces/1/photo", None)):
        await _refused(await p.call(method, path, body), 404, "pet_not_found")
    await _refused(await p.upload(999, JPEG), 404, "pet_not_found")


# --- delete ---------------------------------------------------------------------

async def test_deleting_a_pet_takes_its_photos_with_it(p: PetEnv):
    pet = await p.pets.create("Kinder")
    face = await p.pets.add_face(pet["id"], JPEG)
    r = await p.call("DELETE", f"/pets/{pet['id']}")
    assert r.status == 200 and await r.json() == {"ok": True}
    assert await p.pets.get(pet["id"]) is None
    assert not Path(face["photo_path"]).exists()
    await _refused(await p.call("DELETE", f"/pets/{pet['id']}"), 404, "pet_not_found")


# --- faces ----------------------------------------------------------------------

async def test_a_face_is_uploaded_listed_served_and_deleted(p: PetEnv):
    schemas = _Schemas()
    pet = await p.pets.create("Kinder")
    listed = await (await p.call("GET", f"/pets/{pet['id']}/faces")).json()
    schemas.check(listed, "PetFaceList")
    assert listed == {"faces": [], "max_faces": MAX_FACES_PER_PET}

    r = await p.upload(pet["id"], JPEG)
    assert r.status == 201, await r.text()
    face = (await r.json())["face"]
    schemas.check(face, "PetFace")
    assert face["photo"] == f"{PREFIX}/pets/{pet['id']}/faces/{face['id']}/photo"

    listed = await (await p.call("GET", f"/pets/{pet['id']}/faces")).json()
    assert listed["faces"] == [face]
    summary = (await (await p.call("GET", f"/pets/{pet['id']}")).json())["pet"]
    schemas.check(summary, "PetDetail")
    assert summary["faces"] == [face]
    assert summary["photo"] == f"{PREFIX}/pets/{pet['id']}/photo"

    r = await p.call("GET", f"/pets/{pet['id']}/faces/{face['id']}/photo")
    assert r.status == 200 and await r.read() == JPEG

    r = await p.call("DELETE", f"/pets/{pet['id']}/faces/{face['id']}")
    assert r.status == 200 and await r.json() == {"ok": True}
    assert await p.pets.faces(pet["id"]) == []
    await _refused(await p.call("DELETE", f"/pets/{pet['id']}/faces/{face['id']}"),
                   404, "face_not_found")


async def test_a_real_phone_photo_is_over_the_json_body_cap_and_still_accepted(p: PetEnv):
    """The app API's `client_max_size` is 64 KiB, sized for JSON; a photo is MBs."""
    pet = await p.pets.create("Kinder")
    big = JPEG + b"\x00" * (3 * 1024 * 1024)
    r = await p.upload(pet["id"], io.BytesIO(big))
    assert r.status == 201, await r.text()
    assert len(await p.pets.faces(pet["id"])) == 1


async def test_a_face_of_another_pet_is_not_found(p: PetEnv):
    owner = await p.pets.create("Owner")
    other = await p.pets.create("Other")
    face = await p.pets.add_face(owner["id"], JPEG)
    for method, path in (("GET", f"/pets/{other['id']}/faces/{face['id']}/photo"),
                         ("DELETE", f"/pets/{other['id']}/faces/{face['id']}"),
                         ("DELETE", f"/pets/{owner['id']}/faces/999"),
                         ("GET", f"/pets/{owner['id']}/faces/x/photo")):
        await _refused(await p.call(method, path), 404, "face_not_found")
    assert len(await p.pets.faces(owner["id"])) == 1


async def test_a_face_whose_file_is_gone_is_404(p: PetEnv):
    pet = await p.pets.create("Kinder")
    face = await p.pets.add_face(pet["id"], JPEG)
    Path(face["photo_path"]).unlink()
    await _refused(await p.call("GET", f"/pets/{pet['id']}/faces/{face['id']}/photo"),
                   404, "photo_not_found")


@pytest.mark.parametrize("data, headers", [
    (b"\x89PNG\r\n\x1a\n" + b"P" * 64, {"Content-Type": "image/png"}),
    (b"GIF89a" + b"G" * 64, {}),
])
async def test_a_photo_that_is_not_a_jpeg_is_refused(p: PetEnv, data: bytes, headers: dict):
    pet = await p.pets.create("Kinder")
    await _refused(await p.upload(pet["id"], data, headers=headers), 415, "not_jpeg")
    assert await p.pets.faces(pet["id"]) == []


async def test_a_multipart_form_is_refused_as_not_a_jpeg(p: PetEnv):
    """The panel takes the raw bytes and no form, and so does the app."""
    import aiohttp

    pet = await p.pets.create("Kinder")
    form = aiohttp.FormData()
    form.add_field("file", JPEG, filename="face.jpg", content_type="image/jpeg")
    r = await p.call("POST", f"/pets/{pet['id']}/faces", data=form)
    await _refused(r, 415, "not_jpeg")
    assert await p.pets.faces(pet["id"]) == []


async def test_an_empty_upload_is_refused(p: PetEnv):
    pet = await p.pets.create("Kinder")
    await _refused(await p.upload(pet["id"], b""), 400, "bad_request")


async def test_a_photo_over_the_cap_is_refused_whether_or_not_it_says_its_length(
        p: PetEnv, monkeypatch):
    monkeypatch.setattr(pet_handlers, "MAX_FACE_BYTES", 1024)
    pet = await p.pets.create("Kinder")
    too_big = JPEG + b"\x00" * 2048
    # With a Content-Length, refused before reading...
    await _refused(await p.upload(pet["id"], too_big), 413, "photo_too_large")

    # ...and chunked, without one, refused once the cap is passed.
    async def stream():
        for i in range(0, len(too_big), 256):
            yield too_big[i:i + 256]
    await _refused(await p.upload(pet["id"], stream()), 413, "photo_too_large")
    assert await p.pets.faces(pet["id"]) == []
    # At the cap exactly is fine.
    assert (await p.upload(pet["id"], JPEG + b"\x00" * (1024 - len(JPEG)))).status == 201


async def test_a_pet_holds_at_most_max_faces(p: PetEnv):
    pet = await p.pets.create("Kinder")
    for _ in range(MAX_FACES_PER_PET):
        assert (await p.upload(pet["id"], JPEG)).status == 201
    r = await p.upload(pet["id"], JPEG)
    await _refused(r, 409, "too_many_faces")
    assert str(MAX_FACES_PER_PET) in (await r.json())["message"]
    assert len(await p.pets.faces(pet["id"])) == MAX_FACES_PER_PET


async def test_a_photo_that_cannot_be_written_is_a_500(p: PetEnv, monkeypatch):
    pet = await p.pets.create("Kinder")

    async def refuse(pet_id, data):
        return None
    monkeypatch.setattr(p.pets, "add_face", refuse)
    await _refused(await p.upload(pet["id"], JPEG), 500, "photo_not_stored")


async def test_an_upload_goes_through_the_registrys_normalisation(p: PetEnv, monkeypatch):
    """What is stored is what `normalize_face_photo` made of the upload."""
    normalised = b"\xff\xd8\xff\xdb" + b"N" * 32
    seen: list[bytes] = []

    async def fake_normalize(data: bytes) -> bytes:
        seen.append(data)
        return normalised
    monkeypatch.setattr("petkit_local.ai.pets.normalize_face_photo", fake_normalize)
    pet = await p.pets.create("Kinder")
    face = (await (await p.upload(pet["id"], JPEG)).json())["face"]
    assert seen == [JPEG]
    r = await p.call("GET", face["photo"].removeprefix(PREFIX))
    assert await r.read() == normalised


def _photo(width: int, height: int) -> bytes:
    """A real JPEG of this size, drawn by ffmpeg."""
    return subprocess.run(
        [shutil.which("ffmpeg"), "-v", "error", "-f", "lavfi",
         "-i", f"testsrc=size={width}x{height}:rate=1", "-frames:v", "1",
         "-f", "mjpeg", "pipe:1"], capture_output=True, check=True).stdout


@pytest.mark.skipif(not have_ffmpeg(), reason="ffmpeg is not installed")
async def test_an_uploaded_face_is_stored_at_224_by_224(p: PetEnv):
    pet = await p.pets.create("Kinder")
    photo = await asyncio.to_thread(_photo, 640, 480)
    assert jpeg_dimensions(photo) == (640, 480)
    face = (await (await p.upload(pet["id"], photo)).json())["face"]
    served = await (await p.call("GET", face["photo"].removeprefix(PREFIX))).read()
    assert jpeg_dimensions(served) == (FACE_PHOTO_SIZE, FACE_PHOTO_SIZE) == (224, 224)


# --- access ---------------------------------------------------------------------

NEW_ROUTES = [("POST", "/pets"), ("PATCH", "/pets/1"), ("DELETE", "/pets/1"),
              ("GET", "/pets/1/faces"), ("POST", "/pets/1/faces"),
              ("DELETE", "/pets/1/faces/1"), ("GET", "/pets/1/faces/1/photo")]


@pytest.mark.parametrize("method, path", NEW_ROUTES)
async def test_every_pet_write_needs_a_session(p: PetEnv, method: str, path: str):
    pet = await p.pets.create("Kinder")
    face = await p.pets.add_face(pet["id"], JPEG)
    path = path.replace("/pets/1", f"/pets/{pet['id']}").replace("/faces/1", f"/faces/{face['id']}")
    for token in ("", "not-a-token"):
        r = await p.client.request(method, PREFIX + path, json={"name": "X"},
                                   headers={"Authorization": f"Bearer {token}"} if token else {})
        await _refused(r, 401, "invalid_token")
    assert await p.pets.get(pet["id"]) == pet
    assert len(await p.pets.faces(pet["id"])) == 1


# --- which devices recognise pets ------------------------------------------------

async def test_a_device_says_whether_it_recognises_pets(p: PetEnv):
    p.registry.get_or_create(petkit_id=101, device_type="t4", serial_number="SN101")
    devices = {d["id"]: d for d in (await (await p.call("GET", "/devices")).json())["devices"]}
    for d in devices.values():
        _Schemas().check(d, "DeviceSummary")
    assert devices[100]["supports_ai"] is True   # a T5: a camera and an NPU
    assert devices[101]["supports_ai"] is False  # a T4: weight only
    detail = (await (await p.call("GET", "/devices/101")).json())["device"]
    assert detail["supports_ai"] is False
    # Learned from the device itself, which asks for its recognition config.
    p.registry.get(101).config["ai_observed"] = True
    detail = (await (await p.call("GET", "/devices/101")).json())["device"]
    assert detail["supports_ai"] is True


# --- live updates ---------------------------------------------------------------

async def test_every_pet_change_reaches_the_other_phones(p: PetEnv):
    schemas = _Schemas()
    ws = await p.connect()
    await _next(ws, "hello")

    pet = (await (await p.call("POST", "/pets", {"name": "Kinder"})).json())["pet"]
    message = await _next(ws, "pet")
    schemas.check(message, "UpdatePet")
    assert message["pet"] == pet

    await p.call("PATCH", f"/pets/{pet['id']}", {"name": "Kinder II"})
    assert (await _next(ws, "pet"))["pet"]["name"] == "Kinder II"

    face = (await (await p.upload(pet["id"], JPEG)).json())["face"]
    assert (await _next(ws, "pet"))["pet"]["faces"] == [face]
    await p.call("DELETE", f"/pets/{pet['id']}/faces/{face['id']}")
    assert (await _next(ws, "pet"))["pet"]["faces"] == []

    await p.call("DELETE", f"/pets/{pet['id']}")
    message = await _next(ws, "pet_removed")
    schemas.check(message, "UpdatePetRemoved")
    assert message == {"type": "pet_removed", "pet_id": pet["id"]}

    # A refused write changes nothing, so nothing is sent.
    await p.call("POST", "/pets", {"name": ""})
    assert not [m for m in await _quiet(ws, COALESCE * 3) if m["type"].startswith("pet")]
    await ws.close()


async def test_a_pet_already_there_is_not_announced_and_a_panel_edit_is(p: PetEnv):
    """Pets known at connect come from `GET /pets`; one changed elsewhere (the
    panel, which does not call the feed) arrives on the sweep."""
    pet = await p.pets.create("Kinder")
    ws = await p.connect()
    await _next(ws, "hello")
    assert not [m for m in await _quiet(ws, SWEEP * 1.5) if m["type"].startswith("pet")]
    await p.pets.update(pet["id"], name="Renamed in the panel")
    message = await _next(ws, "pet", timeout=SWEEP * 4)
    assert message["pet"]["name"] == "Renamed in the panel"
    await ws.close()


# --- the panel, unchanged -----------------------------------------------------------

@pytest.fixture
async def panel(event_store, tmp_path):
    live = Config().to_app_config()
    pets = PetRegistry(event_store, str(tmp_path / "faces"), settings=live)
    cfg = {"api_url": "http://x/6/", "capture": False, "capture_dir": "/nope",
           "data_dir": str(tmp_path), "settings_path": str(tmp_path / "overrides.json")}
    app = create_panel_app(DeviceRegistry(), BLERegistry(), EventHub(), cfg, None,
                           live_config=live, event_store=event_store, pet_registry=pets)
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client, pets
    await client.close()


async def test_the_panel_edits_pets_as_it_did(panel):
    client, pets = panel
    pet = (await (await client.post("/api/pets", json={
        "name": " Lilith ", "device_ids": ["7", "x"], "weight": "3.7"})).json())["pet"]
    # Garbage still degrades rather than failing, as the panel always did.
    assert (pet["name"], pet["weight"], pet["device_ids_json"]) == ("Lilith", 3.7, "[]")
    await pets.update(pet["id"], learned_weight_g=3950, learned_samples=4)

    r = await client.post(f"/api/pets/{pet['id']}", json={"weight": "abc"})
    after = (await r.json())["pet"]
    assert after["weight"] is None and after["learned_samples"] == 0
    r = await client.post(f"/api/pets/{pet['id']}", json={"device_ids": [7, 8]})
    assert (await r.json())["pet"]["device_ids_json"] == "[7, 8]"
    assert (await client.post("/api/pets/999", json={"name": "X"})).status == 404


async def test_the_panel_refuses_photos_with_its_own_words(panel):
    client, pets = panel
    pet = await pets.create("Lilith")
    owner = await pets.create("Owner")
    for data, words in ((b"", "empty body"), (b"not a jpeg", "not a valid JPEG")):
        r = await client.post(f"/api/pets/{pet['id']}/faces", data=data)
        assert r.status == 400 and (await r.json())["error"] == words
    for _ in range(MAX_FACES_PER_PET):
        assert (await client.post(f"/api/pets/{pet['id']}/faces", data=JPEG)).status == 200
    r = await client.post(f"/api/pets/{pet['id']}/faces", data=JPEG)
    assert r.status == 400
    assert (await r.json())["error"] == f"at most {MAX_FACES_PER_PET} photos per pet"

    face = await pets.add_face(owner["id"], JPEG)
    assert (await client.delete(f"/api/pets/{pet['id']}/faces/{face['id']}")).status == 404
    assert (await client.get(f"/api/pets/{pet['id']}/faces/{face['id']}/photo")).status == 404
    assert (await client.delete(f"/api/pets/{owner['id']}/faces/{face['id']}")).status == 200
