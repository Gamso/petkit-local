"""A hopper pair's feeds, filed per hopper under how each was asked for.

No feeder reports these: PetKit's account sums them, so here they are summed
from the feed events and from the stored schedule. What these tests pin is the
part that can go quietly wrong -- which bucket a feed lands in, and that the
answer is the same whether the feed arrives live or is replayed after a
restart.
"""
import json
import time

import pytest

from petkit_local.devices.base import Device
from petkit_local.events import ingest, normalize
from petkit_local.ha.categories import get_entities_for_device
from petkit_local.ha.entities.sensors import FEEDER_DUAL_FEED_SENSORS
from petkit_local.utils.const import DEVICE_TYPES_FEEDER, DEVICE_TYPES_FEEDER_DUAL

#: A D4S's `feed_over` content for a feed it was told to run, verbatim (fw
#: 1.198, 2026-08-26, captured on PineappleEmperor's fork): quote-wrapped, the
#: id a bare token, `manual: 0`.
D4S_REALTIME_FEED = (
    '"{"start_time":1787736067,"id":r_20260826_37258_37258-1,'
    '"day":20260826,"manual":0,"time":33675,"real_amount1":0,'
    '"real_amount2":2,"online_state":1,"completed_at":1787736076,'
    '"result":0,"err_code":0}"'
)

BUCKETS = ("addAmountTotal1", "addAmountTotal2",
           "planRealAmountTotal1", "planRealAmountTotal2")


def _feeder(device_type="d4s"):
    return Device(device_type=device_type, petkit_id=1, serial_number="SN")


def _feed(dev, **content):
    normalize.apply_derived_state(dev, "feed_over", {"day": 20260826, **content})


def _buckets(dev):
    return tuple(dev.state["feedState"][name] for name in BUCKETS)


# --- which bucket ------------------------------------------------------------

def test_a_feed_the_device_was_told_to_run_is_manual_whatever_its_manual_flag_says():
    """The real report: `manual: 0` on a `feed_realtime`. Read from that flag,
    every feed pressed in Home Assistant would be filed as scheduled."""
    content = ingest._as_dict(D4S_REALTIME_FEED)
    assert content["id"] == "r_20260826_37258_37258-1" and content["manual"] == 0

    dev = _feeder()
    normalize.apply_derived_state(dev, "feed_over", content)
    assert _buckets(dev) == (0, 2, 0, 0)
    # The family totals are unchanged by the split.
    assert dev.state["feedState"]["times"] == 1
    assert dev.state["feedState"]["realAmountTotal"] == 2


def test_an_instance_of_a_recurring_meal_is_on_schedule():
    dev = _feeder()
    _feed(dev, id="s_20260826_64800", manual=0, real_amount1=1, real_amount2=3)
    assert _buckets(dev) == (0, 0, 1, 3)


def test_a_one_off_feed_is_manual_and_not_part_of_the_days_plan():
    dev = _feeder()
    _feed(dev, id="d_20260826_61500", real_amount1=2, real_amount2=0)
    assert _buckets(dev) == (2, 0, 0, 0)


def test_a_feed_the_device_itself_calls_manual_is_manual():
    dev = _feeder()
    _feed(dev, manual=1, real_amount1=1, real_amount2=0)
    assert _buckets(dev) == (1, 0, 0, 0)
    # ...but its own id still wins: a scheduled meal is a scheduled meal.
    _feed(dev, id="s_20260826_64800", manual=1, real_amount1=1, real_amount2=0)
    assert _buckets(dev) == (1, 0, 1, 0)


def test_a_feed_that_says_neither_is_counted_and_filed_nowhere():
    """Guessing it into a bucket would make one of the two sensors lie; the
    totals still have it."""
    dev = _feeder()
    _feed(dev, real_amount1=4, real_amount2=1)
    _feed(dev, id="x_unknown", real_amount1=1, real_amount2=0)
    assert _buckets(dev) == (0, 0, 0, 0)
    assert dev.state["feedState"]["realAmountTotal"] == 6


def test_the_buckets_add_up_over_a_day_and_start_over_with_it():
    dev = _feeder("d4sh")
    _feed(dev, id="s_20260826_25200", real_amount1=1, real_amount2=1)
    _feed(dev, id="r_20260826_37258_37258-1", real_amount1=0, real_amount2=2)
    _feed(dev, id="s_20260826_64800", real_amount1=1, real_amount2=0)
    assert _buckets(dev) == (0, 2, 2, 1)

    normalize.apply_derived_state(dev, "feed_over", {
        "day": 20260827, "id": "r_20260827_100_100-1", "real_amount1": 3, "real_amount2": 0})
    assert _buckets(dev) == (3, 0, 0, 0)


def test_a_restart_files_every_feed_the_way_it_was_filed_live():
    """The day is replayed from the event store. A rule that leaned on
    anything but the content -- "is this the feed we sent last" -- would file
    all but the latest manual feed as scheduled the second time round."""
    feeds = [
        {"day": 20260826, "id": "r_20260826_30000_30000-1", "real_amount1": 1, "real_amount2": 0},
        {"day": 20260826, "id": "s_20260826_43200", "real_amount1": 2, "real_amount2": 2},
        {"day": 20260826, "id": "r_20260826_50000_50000-1", "real_amount1": 0, "real_amount2": 1},
    ]
    live = _feeder()
    # As at runtime: each feed is one we just sent when its report arrives.
    for content in feeds:
        live.config.setdefault("local", {})["lastFeedId"] = content["id"]
        normalize.apply_derived_state(live, "feed_over", content)

    restarted = _feeder()
    restarted.config["local"] = {"lastFeedId": feeds[-1]["id"]}
    normalize.seed_feed_state(restarted, feeds, None)

    assert _buckets(live) == _buckets(restarted) == (1, 1, 2, 2)


def test_a_single_hopper_feeder_grows_no_per_hopper_split():
    dev = _feeder("d4")
    normalize.apply_derived_state(dev, "feed_over", {
        "day": 20261022, "id": "s_20261022_64800", "real_amount": 10})
    assert dev.state["feedState"] == {"day": 20261022, "times": 1, "realAmountTotal": 10}
    normalize.apply_feed_plan_state(dev)
    assert not set(dev.state["feedState"]) & {*BUCKETS, "planAmountTotal1", "planAmountTotal2"}


# --- what the schedule intends today -----------------------------------------

#: Wednesday 2026-08-26, local noon. PetKit numbers weekdays from SUNDAY = 1,
#: so a Wednesday is 4.
WEDNESDAY_NOON = time.mktime((2026, 8, 26, 12, 0, 0, 0, 0, -1))


def _planned(dev, now=WEDNESDAY_NOON):
    normalize.apply_feed_plan_state(dev, now=now)
    return (dev.state["feedState"]["planAmountTotal1"],
            dev.state["feedState"]["planAmountTotal2"])


def test_the_plan_is_todays_meals_per_hopper():
    dev = _feeder()
    dev.config["feed_schedule"] = {"schedule": [
        {"re": "1,2,3,4,5,6,7", "it": [{"id": "n_25200", "t": 25200, "a1": 1, "a2": 0},
                                       {"id": "n_64800", "t": 64800, "a1": 1, "a2": 2}]},
        {"re": "4", "it": [{"id": "n_43200", "t": 43200, "a1": 0, "a2": 3}]},
        # Sunday and Thursday only: not today.
        {"re": "1,5", "it": [{"id": "n_50000", "t": 50000, "a1": 9, "a2": 9}]},
    ]}
    assert _planned(dev) == (2, 5)
    # The next day the Wednesday meal drops out and Thursday's comes in.
    assert _planned(dev, WEDNESDAY_NOON + 86400) == (11, 11)


def test_a_feeder_with_no_schedule_plans_nothing_and_reads_zero_not_unknown():
    dev = _feeder()
    assert _planned(dev) == (0, 0)
    assert _buckets(dev) == (0, 0, 0, 0)
    # A schedule that is not the shape we store plans nothing either.
    for junk in ("{}", [], {"schedule": "x"}, {"schedule": [None, {"re": 4, "it": "x"}]}):
        dev.config["feed_schedule"] = junk
        assert _planned(dev) == (0, 0)


def test_one_off_feeds_are_not_in_the_plan():
    """A deferred feed leaves the store once it has fired, so counting it
    would shrink the day's plan at the moment it was carried out."""
    dev = _feeder()
    dev.config["feed_schedule"] = {
        "schedule": [{"re": "4", "it": [{"id": "n_43200", "t": 43200, "a1": 1, "a2": 1}]}],
        "deferred": [{"id": "d_20260826_61500", "a1": 5, "a2": 5,
                      "fire_at": WEDNESDAY_NOON + 3600}]}
    assert _planned(dev) == (1, 1)


def test_recomputing_the_plan_leaves_the_days_dispensed_totals_alone():
    dev = _feeder()
    _feed(dev, id="s_20260826_25200", real_amount1=1, real_amount2=1)
    before = dict(dev.state["feedState"])
    _planned(dev)
    after = dev.state["feedState"]
    assert {k: after[k] for k in before} == before


def test_the_state_document_carries_the_plan_without_a_feed_having_happened():
    """Both builders -- the one Home Assistant reads and the panel's -- or the
    two would show different numbers for the same sensor."""
    from petkit_local.ha.publisher import HAPublisher
    from petkit_local.web.api.devices import _state_doc

    weekday = str((time.localtime().tm_wday + 2) % 7 or 7)
    for build in (lambda d: HAPublisher._build_state(_Publisher(), d), _state_doc):
        dev = _feeder("d4sh")
        dev.config["feed_schedule"] = {"schedule": [
            {"re": weekday, "it": [{"id": "n_25200", "t": 25200, "a1": 2, "a2": 1}]}]}
        feed_state = build(dev)["state"]["feedState"]
        assert (feed_state["planAmountTotal1"], feed_state["planAmountTotal2"]) == (2, 1)
        assert feed_state["addAmountTotal1"] == 0
        json.dumps(feed_state)


class _Publisher:
    go2rtc = None


# --- who gets the sensors ----------------------------------------------------

@pytest.mark.parametrize("device_type", sorted(DEVICE_TYPES_FEEDER))
def test_only_a_hopper_pair_publishes_the_per_hopper_sensors(device_type):
    keys = {e.key for e in get_entities_for_device(_feeder(device_type))}
    ours = {e.key for e in FEEDER_DUAL_FEED_SENSORS}
    if device_type in DEVICE_TYPES_FEEDER_DUAL:
        assert ours <= keys
    else:
        assert not ours & keys


def test_every_per_hopper_sensor_reads_a_field_something_here_writes():
    dev = _feeder()
    _feed(dev, id="s_20260826_25200", real_amount1=1, real_amount2=1)
    normalize.apply_feed_plan_state(dev)
    for entity in FEEDER_DUAL_FEED_SENSORS:
        root, block, leaf = entity.value_path.split(".")
        assert (root, block) == ("state", "feedState")
        assert leaf in dev.state["feedState"], entity.key
        # Portions, not grams: `a1`/`a2` count portions on this hardware.
        assert entity.unit == "portions"
