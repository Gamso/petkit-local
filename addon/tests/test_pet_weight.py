"""A pet's weight: the HA sensor, the learned reference, and the two thresholds.

Three things built on one query (`EventStore.pet_recent_weights`): the median
the `Weight` sensor shows, the reference `match_by_weight` compares a visit
with once enough visits stand behind it, and the panel's controls over both.
"""
import json
import re
import sqlite3
import tempfile
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.ai.pets import (
    LEARNED_WEIGHT_MIN_SAMPLES, SETTING_MARGIN_G, SETTING_TOLERANCE_PCT,
    WEIGHT_MATCH_MARGIN_G, WEIGHT_MATCH_SETTINGS, WEIGHT_MATCH_TOLERANCE,
    PetRegistry, clamp_weight_setting, reference_weight_grams,
)
from petkit_local.config import Config
from petkit_local.devices.base import Device
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.events.store import (
    PLAUSIBLE_WEIGHT_G, WEIGHT_MEDIAN_SAMPLES, EventStore, visit_weight_grams,
)
from petkit_local.ha.entities.pet import PET_SENSORS
from petkit_local.ha.publisher import HAPublisher
from petkit_local.mqtt.bridge import MQTTBridge
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app
from tests._fakes import FakeMqttClient

BOX = 7
JS = Path(__file__).resolve().parents[1] / "petkit_local" / "web" / "static" / "js"


class Household:
    """One camera-less box and the two code paths a visit takes through it.

    `visit` does what both transports do, in their order: attribute, store,
    then let the pet learn from what was stored.
    """

    def __init__(self, pets: PetRegistry, store: EventStore) -> None:
        self.pets = pets
        self.store = store
        self.box = Device(device_type="t4", petkit_id=BOX)
        self.ts = 1_000_000.0

    async def report(self, event_type: str, content: dict, pet_ref: int | None = None) -> int | None:
        self.ts += 600
        row = {"device_id": BOX, "device_type": "t4", "event_type": event_type,
               "event_kind": "toilet_visit", "event_uid": f"{self.ts}_{event_type}",
               "ts": self.ts, "pet_ref": pet_ref, "content_json": json.dumps(content)}
        pet = await self.pets.attribute_event(self.box, row, content)
        await self.store.record_event(row)
        if pet:
            await self.pets.refresh_learned_weight(pet["id"])
        return row["pet_id"]

    async def visit(self, grams: float, pet_ref: int | None = None) -> int | None:
        """A visit SUMMARY (code 10) weighing `grams`; the pet it was credited to."""
        return await self.report("10", {"pet_weight": grams}, pet_ref)


@pytest.fixture
def home(pet_registry: PetRegistry, event_store: EventStore) -> Household:
    return Household(pet_registry, event_store)


# --- the samples -------------------------------------------------------------

async def test_only_a_visit_summary_is_a_weight_sample(home: Household):
    """A visit is several reports sharing one session; the others carry a
    partial weight or none, and a mid-visit reading must not be learned from."""
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    # Code 9 is a weight check DURING the visit: credited to the cat by the
    # device, but not a summary.
    await home.report("9", {"pet_weight": 3100}, pet_ref=cat["id"])
    await home.visit(3663)
    assert await home.store.pet_recent_weights(cat["id"]) == [3663]


async def test_a_weight_of_zero_or_an_absurd_one_is_not_a_sample(home: Household):
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    for grams in (0, 120, 45_000, "n/a"):
        # Named by the device, so the visit IS credited -- only its weight is refused.
        assert await home.visit(grams, pet_ref=cat["id"]) == cat["id"]
    await home.visit(3663, pet_ref=cat["id"])
    assert await home.store.pet_recent_weights(cat["id"]) == [3663]
    low, high = PLAUSIBLE_WEIGHT_G
    assert visit_weight_grams({"pet_weight": low - 1}) is None
    assert visit_weight_grams({"petWeight": high}) == high
    assert visit_weight_grams("not a dict") is None


async def test_recent_weights_are_newest_first_bounded_and_cut_at_since(home: Household):
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    for grams in range(3600, 3700, 10):
        await home.visit(grams)
    weights = await home.store.pet_recent_weights(cat["id"])
    assert weights == [3690, 3680, 3670, 3660, 3650, 3640, 3630]
    assert len(weights) == WEIGHT_MEDIAN_SAMPLES
    assert await home.store.pet_recent_weights(cat["id"], since=home.ts - 700) == [3690, 3680]
    assert await home.store.pet_recent_weights(cat["id"], since=home.ts) == []


# --- part 1: the Home Assistant sensor ----------------------------------------

def test_the_weight_sensor_is_a_measurement_in_grams():
    sensor = next(e for e in PET_SENSORS if e.key == "weight")
    assert (sensor.component, sensor.device_class, sensor.unit, sensor.state_class) == (
        "sensor", "weight", "g", "measurement")
    assert sensor.value_path == "state.weight"
    # Entity keys are user state: one key per entity, none reused.
    assert len({e.key for e in PET_SENSORS}) == len(PET_SENSORS)


async def test_the_sensor_is_the_median_of_the_newest_weighed_visits(home: Household):
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    assert (await home.store.pet_visit_stats(cat["id"]))["weight"] is None
    # One wild reading among seven -- two cats in the box -- moves nothing.
    for grams in (3650, 3660, 3640, 3670, 4190, 3655, 3662):
        await home.visit(grams, pet_ref=cat["id"])
    stats = await home.store.pet_visit_stats(cat["id"])
    assert stats["weight"] == 3660
    assert stats["last_visit_weight"] == 3662


def _publisher() -> HAPublisher:
    pub = HAPublisher(DeviceRegistry(), {})
    pub._client = FakeMqttClient()
    pub._connected = True
    return pub


def _pet_state(pub: HAPublisher, pet_id: int) -> dict:
    states = [p for t, p, _ in pub._client.published if t == f"petkit-local/pet/{pet_id}/state"]
    return json.loads(states[-1])["state"]


async def test_the_weight_is_published_and_declared_to_home_assistant(home: Household):
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    for grams in (3650, 3660, 3640):
        await home.visit(grams)
    pub = _publisher()
    await pub.publish_pet_discovery(cat)
    await pub.publish_pet_state(cat, home.store)

    assert _pet_state(pub, cat["id"])["weight"] == 3650
    configs = [json.loads(p) for t, p, _ in pub._client.published if t.endswith("/config")]
    weight = next(c for c in configs if c["unique_id"].endswith(f"pet_{cat['id']}_weight"))
    assert weight["state_class"] == "measurement"
    assert weight["device_class"] == "weight"
    assert weight["unit_of_measurement"] == "g"
    # HA drops a sensor whose template raises on a key not published yet.
    assert "default(" in weight["value_template"]


async def test_every_pet_is_republished_when_home_assistant_connects(home: Household):
    """A pet's state was only ever published when one of its events landed, so
    after a restart its sensors read `unknown` until the cat next used the box."""
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    await home.visit(3663)
    pub = _publisher()
    await pub._publish_all_discovery()
    assert pub._client.published == [], "no pet source wired: nothing to replay"

    pub.set_pet_source(home.pets, home.store)
    await pub._publish_all_discovery()
    assert _pet_state(pub, cat["id"])["weight"] == 3663
    assert any(t.endswith("/config") for t, _, _ in pub._client.published)


async def test_a_failing_store_does_not_break_the_connect(home: Household):
    class Broken:
        async def all(self):
            raise RuntimeError("database is locked")

    pub = _publisher()
    pub.set_pet_source(Broken(), home.store)
    await pub._publish_all_discovery()  # must not raise into start()'s backoff


async def test_a_new_visit_republishes_the_weight_over_mqtt(event_store, tmp_path):
    """Through the real bridge: attribution, the stored row, the learned
    weight and the publish, in that order."""
    reg = DeviceRegistry()
    box = reg.get_or_create(petkit_id=BOX, device_type="t4", serial_number="SN")
    pets = PetRegistry(event_store, str(tmp_path / "faces"))
    cat = await pets.create("Lilith", device_ids=[BOX], weight=3.7)
    pub = _publisher()
    bridge = MQTTBridge(reg, pub, event_store=event_store, hub=EventHub(), pet_registry=pets)
    for n, grams in enumerate((3650, 3660, 3640)):
        await bridge._handle_event(box, "pet_out", {
            "params": {"event_id": f"{BOX}_{n}", "content": json.dumps({"pet_weight": grams})}})

    assert _pet_state(pub, cat["id"])["weight"] == 3650
    stored = await pets.get(cat["id"])
    assert (stored["learned_weight_g"], stored["learned_samples"]) == (3650, 3)
    assert stored["weight"] == 3.7


async def test_a_visit_reported_over_http_is_learned_from(event_store, tmp_path):
    """The other transport: `dev_event_report` on a T4, which is HTTP-only."""
    from petkit_local.http.server import create_app

    reg = DeviceRegistry()
    reg.get_or_create(petkit_id=100, device_type="t4", serial_number="SN100")
    pets = PetRegistry(event_store, str(tmp_path / "faces"))
    cat = await pets.create("Lilith", device_ids=[100], weight=3.7)
    app = create_app(reg, {"api_url": "http://server/6/", "mqtt_port": 1883,
                           "proxy_mode": False, "capture": False})
    app["event_store"] = event_store
    app["pet_registry"] = pets
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        for n, grams in enumerate((3650, 3660, 3640)):
            r = await client.post(
                "/6/t4/dev_event_report", headers={"X-Device": "id=100&sn=SN100"},
                data=f"eventType=10&eventId=100_{n}&content=%7B%22pet_weight%22%3A{grams}%7D")
            assert r.status == 200
    finally:
        await client.close()
    pet = await pets.get(cat["id"])
    assert (pet["learned_weight_g"], pet["learned_samples"]) == (3650, 3)
    assert reference_weight_grams(pet) == 3650


# --- part 2: the learned reference --------------------------------------------

async def test_the_entered_weight_is_used_until_enough_visits_stand_behind_it(home: Household):
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    for n in range(1, LEARNED_WEIGHT_MIN_SAMPLES):
        await home.visit(3900)
        pet = await home.pets.get(cat["id"])
        assert pet["learned_samples"] == n
        assert reference_weight_grams(pet) == 3700, "too few visits to move the reference"
    await home.visit(3900)
    pet = await home.pets.get(cat["id"])
    assert reference_weight_grams(pet) == 3900
    # What the owner typed is never overwritten.
    assert pet["weight"] == 3.7


async def test_the_reference_follows_a_cat_that_gains_weight(home: Household):
    """3.7 kg entered, the cat grows to 4.6 kg: every visit is a small step from
    the last reference, so it keeps being matched -- which the entered weight
    alone stops doing once the cat is 555 g (15%) away from it."""
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    for grams in range(3700, 4601, 50):
        assert await home.visit(grams) == cat["id"], grams
    assert reference_weight_grams(await home.pets.get(cat["id"])) == 4450
    assert abs(4600 - 3700) > 3700 * WEIGHT_MATCH_TOLERANCE


async def test_a_visit_the_device_named_always_counts(home: Household):
    """A face-confirmed visit is evidence whatever it weighs against the
    reference -- and seeds a pet nobody entered a weight for."""
    cat = await home.pets.create("Kinder", device_ids=[BOX])
    assert await home.visit(5100) is None, "no weight, no face: nobody"
    for grams in (5100, 5120, 5080):
        assert await home.visit(grams, pet_ref=cat["id"]) == cat["id"]
    pet = await home.pets.get(cat["id"])
    assert pet["weight"] is None and reference_weight_grams(pet) == 5100
    assert await home.visit(5090) == cat["id"], "now matched by the learned weight"


async def test_an_unattributed_visit_teaches_nobody(home: Household):
    kinder = await home.pets.create("Kinder", device_ids=[BOX], weight=5.1)
    lilith = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    for _ in range(5):
        assert await home.visit(4400) is None  # between the two
    for pet_id in (kinder["id"], lilith["id"]):
        assert (await home.pets.get(pet_id))["learned_samples"] in (None, 0)


async def test_two_cats_converging_end_unattributed_never_swapped(home: Household):
    """A gains, B loses, until 100 g apart. The ambiguity margin has to stop
    crediting visits before either reference can drift onto the other cat."""
    a = await home.pets.create("A", device_ids=[BOX], weight=4.0)
    b = await home.pets.create("B", device_ids=[BOX], weight=5.0)
    wobble = (0, 25, -20, 10, -30, 15)  # scale noise, deterministic
    outcomes = []
    for step in range(60):
        drift = min(step * 10, 450)
        for owner, true_weight in ((a, 4000 + drift), (b, 5000 - drift)):
            grams = true_weight + wobble[(step + owner["id"]) % len(wobble)]
            credited = await home.visit(grams)
            assert credited in (None, owner["id"]), (
                f"step {step}: {owner['name']}'s {grams} g visit went to the other cat")
            outcomes.append(credited)
        ref_a = reference_weight_grams(await home.pets.get(a["id"]))
        ref_b = reference_weight_grams(await home.pets.get(b["id"]))
        assert ref_a < ref_b, f"step {step}: the references crossed ({ref_a} / {ref_b})"

    assert all(o is not None for o in outcomes[:20]), "told apart while 600 g+ apart"
    assert all(o is None for o in outcomes[-20:]), "100 g apart: nobody is credited"
    # And the references stopped where the crediting did, still a margin apart.
    assert ref_b - ref_a >= WEIGHT_MATCH_MARGIN_G


async def test_a_reset_goes_back_to_the_entered_weight_and_stays_there(home: Household):
    cat = await home.pets.create("Lilith", device_ids=[BOX], weight=3.7)
    for _ in range(4):
        await home.visit(3950)
    assert reference_weight_grams(await home.pets.get(cat["id"])) == 3950

    pet = await home.pets.reset_learned_weight(cat["id"], now=home.ts)
    assert (pet["learned_weight_g"], pet["learned_samples"]) == (None, 0)
    assert reference_weight_grams(pet) == 3700
    # The visits from before the reset are not evidence any more: one new
    # visit is one sample, not the old median back again.
    await home.visit(3720)
    pet = await home.pets.get(cat["id"])
    assert (pet["learned_weight_g"], pet["learned_samples"]) == (3720, 1)
    assert reference_weight_grams(pet) == 3700
    # The HA sensor is what the scale observed, and a reset does not rewrite that.
    assert (await home.store.pet_visit_stats(cat["id"]))["weight"] == 3950
    assert await home.pets.reset_learned_weight(999) is None


async def test_an_old_database_gains_the_learned_columns():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "petkit.db"
        old = sqlite3.connect(str(path))
        old.executescript("""
            CREATE TABLE pets (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                device_ids_json TEXT DEFAULT '[]', face_id TEXT, photo_path TEXT,
                weight REAL, created_at REAL NOT NULL);
            INSERT INTO pets (name, weight, created_at) VALUES ('Lilith', 3.7, 1.0);
        """)
        old.commit()
        old.close()

        store = EventStore(path)
        try:
            await store.connect()
            pet = (await store.all_pets())[0]
            assert pet["weight"] == 3.7
            assert pet["learned_weight_g"] is None and pet["learned_since"] is None
            assert reference_weight_grams(pet) == 3700
            await store.upsert_pet({"learned_weight_g": 3800, "learned_samples": 3},
                                   pet_id=pet["id"])
            assert reference_weight_grams(await store.get_pet(pet["id"])) == 3800
        finally:
            await store.close()


# --- part 3: the two thresholds ------------------------------------------------

def test_the_defaults_are_the_rule_as_it_was():
    assert WEIGHT_MATCH_SETTINGS[SETTING_TOLERANCE_PCT][2] == 15
    assert WEIGHT_MATCH_SETTINGS[SETTING_MARGIN_G][2] == 300
    config = Config()
    for key, (_low, _high, default) in WEIGHT_MATCH_SETTINGS.items():
        assert getattr(config, key) == default == config.to_app_config()[key]


@pytest.mark.parametrize("key,value,expected", [
    (SETTING_TOLERANCE_PCT, 20, 20), (SETTING_TOLERANCE_PCT, "25", 25),
    (SETTING_TOLERANCE_PCT, 0, 1), (SETTING_TOLERANCE_PCT, 900, 50),
    (SETTING_TOLERANCE_PCT, 12.6, 13), (SETTING_MARGIN_G, -5, 0),
    (SETTING_MARGIN_G, 99_999, 2000), (SETTING_MARGIN_G, "abc", None),
    (SETTING_MARGIN_G, None, None), (SETTING_MARGIN_G, True, None),
    (SETTING_MARGIN_G, float("inf"), None), (SETTING_MARGIN_G, [300], None),
])
def test_a_threshold_is_clamped_and_garbage_is_refused(key, value, expected):
    assert clamp_weight_setting(key, value) == expected


async def test_the_thresholds_are_read_live_from_the_panel_settings(event_store, tmp_path):
    live: dict = {}
    pets = PetRegistry(event_store, str(tmp_path / "faces"), settings=live)
    kinder = await pets.create("Kinder", device_ids=[BOX], weight=5.1)
    lilith = await pets.create("Lilith", device_ids=[BOX], weight=3.7)
    assert pets.weight_match_thresholds() == (0.15, 300.0)

    # 4300 g: 600 from Lilith (over her 555 g tolerance), 800 from Kinder.
    assert await pets.match_by_weight(BOX, 4300) is None
    live[SETTING_TOLERANCE_PCT] = 20
    live[SETTING_MARGIN_G] = 100
    assert await pets.match_by_weight(BOX, 4300) == lilith["id"]
    # A wider margin refuses a visit the default one credits.
    assert await pets.match_by_weight(BOX, 4600) == kinder["id"]
    live[SETTING_MARGIN_G] = 500
    assert await pets.match_by_weight(BOX, 4600) is None
    # A hand-edited overrides file cannot switch the rule off or break it.
    live[SETTING_TOLERANCE_PCT] = "nonsense"
    live[SETTING_MARGIN_G] = -40
    assert pets.weight_match_thresholds() == (0.15, 0.0)


# --- the panel -----------------------------------------------------------------

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
    yield client, pets, live, tmp_path
    await client.close()


async def test_the_panel_saves_clamps_and_persists_the_thresholds(panel):
    client, pets, live, tmp_path = panel
    info = await (await client.get("/api/info")).json()
    assert info["settings"][SETTING_TOLERANCE_PCT] == 15
    assert info["settings"][SETTING_MARGIN_G] == 300
    assert info["weight_match_limits"][SETTING_MARGIN_G] == {"min": 0, "max": 2000, "default": 300}
    assert info["weight_learning"] == {"min_samples": LEARNED_WEIGHT_MIN_SAMPLES,
                                       "median_of": WEIGHT_MEDIAN_SAMPLES}

    r = await client.post("/api/settings", json={SETTING_TOLERANCE_PCT: 22,
                                                 SETTING_MARGIN_G: 123456})
    body = await r.json()
    assert body["changed"] == {SETTING_TOLERANCE_PCT: 22, SETTING_MARGIN_G: 2000}
    assert pets.weight_match_thresholds() == (0.22, 2000.0)
    saved = json.loads((tmp_path / "overrides.json").read_text())
    assert saved == {SETTING_TOLERANCE_PCT: 22, SETTING_MARGIN_G: 2000}

    # ...and they come back at the next start.
    restarted = Config(data_dir=str(tmp_path))
    (tmp_path / "settings_overrides.json").write_text(json.dumps(saved))
    restarted.apply_panel_overrides()
    assert restarted.to_app_config()[SETTING_TOLERANCE_PCT] == 22

    r = await client.post("/api/settings", json={SETTING_MARGIN_G: "a lot"})
    assert r.status == 400
    assert live[SETTING_MARGIN_G] == 2000, "a refused value changes nothing"
    # A margin of 0 is a real value, not an unset one.
    await client.post("/api/settings", json={SETTING_MARGIN_G: 0})
    assert (await (await client.get("/api/settings")).json())["settings"][SETTING_MARGIN_G] == 0


async def test_the_panel_shows_both_weights_and_resets_the_learned_one(panel, event_store):
    client, pets, _live, _tmp = panel
    home = Household(pets, event_store)
    pet = (await (await client.post("/api/pets", json={
        "name": "Lilith", "device_ids": [BOX], "weight": "3.7"})).json())["pet"]
    assert pet["weight"] == 3.7
    for _ in range(4):
        await home.visit(3950)

    listed = (await (await client.get("/api/pets")).json())["pets"][0]
    assert listed["entered_weight_g"] == 3700
    assert (listed["learned_weight_g"], listed["learned_samples"]) == (3950, 4)
    assert listed["learned_weight_in_use"] is True
    assert listed["reference_weight_g"] == 3950

    r = await client.post(f"/api/pets/{pet['id']}", json={"reset_learned_weight": True})
    after = (await r.json())["pet"]
    assert (after["weight"], after["learned_weight_g"], after["learned_samples"]) == (3.7, None, 0)
    listed = (await (await client.get("/api/pets")).json())["pets"][0]
    assert listed["learned_weight_in_use"] is False and listed["reference_weight_g"] == 3700


async def test_typing_a_new_weight_restarts_the_learning(panel, event_store):
    """A correction the learned value went on overriding would be one the
    owner could not make."""
    client, pets, _live, _tmp = panel
    home = Household(pets, event_store)
    cat = await pets.create("Lilith", device_ids=[BOX], weight=3.7)
    for _ in range(4):
        await home.visit(3950)

    # Renaming, or re-sending the same weight, resets nothing.
    await client.post(f"/api/pets/{cat['id']}", json={"name": "Lili", "weight": 3.7})
    assert (await pets.get(cat["id"]))["learned_samples"] == 4

    await client.post(f"/api/pets/{cat['id']}", json={"weight": 4.2})
    pet = await pets.get(cat["id"])
    assert (pet["weight"], pet["learned_samples"]) == (4.2, 0)
    assert reference_weight_grams(pet) == 4200

    # Untrusted input never reaches the REAL column as text.
    for junk in ("heavy", -3, True, [4]):
        await client.post(f"/api/pets/{cat['id']}", json={"weight": junk})
        assert (await pets.get(cat["id"]))["weight"] is None


async def test_a_camera_less_litter_box_is_enough_to_manage_pets(event_store, tmp_path):
    """Weight matching exists for the box that cannot recognise anything, and
    the Pets tab used to show its pets only beside a recognising device."""
    reg = DeviceRegistry()
    reg.get_or_create(petkit_id=1, device_type="t4", serial_number="A")
    reg.get_or_create(petkit_id=2, device_type="t5", serial_number="B")
    reg.get_or_create(petkit_id=3, device_type="d4", serial_number="C")
    app = create_panel_app(reg, BLERegistry(), EventHub(), {"api_url": "http://x/6/"}, None,
                           event_store=event_store)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        devices = {d["id"]: d for d in await (await client.get("/api/devices")).json()}
    finally:
        await client.close()
    assert (devices[1]["is_litter"], devices[1]["supports_ai"]) == (True, False)
    assert (devices[2]["is_litter"], devices[2]["supports_ai"]) == (True, True)
    assert devices[3]["is_litter"] is False

    js = (JS / "pets.js").read_text(encoding="utf-8")
    assert "ds.filter(d => d.is_litter && !d.supports_ai)" in js
    assert "recognising || scaleOnly.length > 0" in js


def test_the_pets_tab_carries_the_weight_controls():
    js = (JS / "pets.js").read_text(encoding="utf-8")
    for key in WEIGHT_MATCH_SETTINGS:
        assert f"'{key}'" in js, key
    for action in ("edit-pet-weight", "reset-learned-weight"):
        assert f"onAction('{action}'" in js and f'data-action="{action}"' in js
    assert "onChange('save-weight-match'" in js and 'data-change="save-weight-match"' in js
    assert "reset_learned_weight: true" in js
    # Both weights are shown, with how many visits stand behind the learned one.
    for field in ("entered_weight_g", "learned_weight_g", "learned_samples",
                  "learned_weight_in_use"):
        assert f"p.{field}" in js, field
    # Each threshold explains what raising and lowering it does.
    card = js[js.index("function weightMatchCard"):]
    card = card[:card.index("\nfunction ")]
    assert len(re.findall(r"Raise it", card)) == 2 and len(re.findall(r"lower it", card)) == 2
    # The floor the tolerance text quotes is the one the matcher applies.
    from petkit_local.ai.pets import WEIGHT_MATCH_MIN_TOLERANCE_G
    assert f"{WEIGHT_MATCH_MIN_TOLERANCE_G} g" in card
