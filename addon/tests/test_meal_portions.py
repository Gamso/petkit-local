"""What a meal put out, on its timeline card: `served_amounts` (issue #98).

The app showed "3 portions · Hopper 1 ×2 · Hopper 2 ×1" by reading every
meal's event once more and caching it, because only the event carried the
`real_amount*` fields. These pin what the card itself now says, per model and
per category, and that an empty meal is 0 while an unknown one is None.
"""
import json

from petkit_local.events import ingest
from petkit_local.events.sessions import served_amounts


def _card(event_type, device_type, content=None, event_kind="feeding"):
    """The one card a single stored report makes."""
    row = {"id": 1, "device_id": 1, "device_type": device_type, "ts": 1000.0,
           "event_type": event_type, "event_kind": event_kind,
           "related_event": "m1", "parent_event": None, "media": [], "pet_id": None,
           "content_json": None if content is None else json.dumps(content)}
    sessions = ingest.group_sessions([row])
    assert len(sessions) == 1
    return sessions[0]


def _served(event_type, device_type, content=None, event_kind="feeding"):
    return served_amounts(_card(event_type, device_type, content, event_kind), device_type)


# --- Dual-Hopper ----------------------------------------------------------------

def test_a_dual_hopper_meal_gives_each_hopper_and_their_sum():
    content = {"result": 0, "real_amount1": 2, "real_amount2": 1}
    expected = {"total": 3, "hopper1": 2, "hopper2": 1}
    assert _served("feed_over", "d4sh", content) == expected
    # HTTP code 4 is the same report on the other transport.
    assert _served("4", "d4sh", content) == expected
    assert _served("feed_over", "d4s", content) == expected


def test_a_dual_hopper_meal_that_served_nothing_is_zero_not_unknown():
    """Issue #2's shape: both hoppers 0 beside err_code 8."""
    content = {"result": 8, "err_code": 8, "real_amount1": 0, "real_amount2": 0}
    assert _served("feed_over", "d4sh", content) == {"total": 0, "hopper1": 0, "hopper2": 0}


def test_a_hopper_the_report_left_out_is_unknown_not_zero():
    assert _served("feed_over", "d4sh", {"real_amount2": 1}) == \
        {"total": 1, "hopper1": None, "hopper2": 1}


def test_a_dual_hopper_reads_only_its_own_fields():
    """The unsuffixed field is a single hopper's, in another unit: a Dual-Hopper
    report carrying only that is a report with no hopper amounts."""
    assert _served("feed_over", "d4sh", {"real_amount": 10}) is None


# --- single hopper -----------------------------------------------------------------

def test_a_single_hopper_meal_has_a_total_and_no_hoppers():
    assert _served("feed_over", "d4h", {"result": 0, "real_amount": 10}) == \
        {"total": 10, "hopper1": None, "hopper2": None}
    assert _served("feed_over", "d4", {"realAmount": 20}) == \
        {"total": 20, "hopper1": None, "hopper2": None}


def test_a_skipped_single_hopper_meal_is_zero():
    """A D4H holding back a meal reports `real_amount: 0, err_code: 10`; the
    firmware's own skip (FEED_RESULT 9) sets -1. Both mean nothing came out."""
    held = {"result": 8, "err_code": 10, "real_amount": 0, "surplus_standard": 2}
    assert _served("feed_over", "d4h", held)["total"] == 0
    skipped = {"result": 9, "real_amount": -1}
    assert _served("feed_over", "d4h", skipped) == {"total": 0, "hopper1": None, "hopper2": None}


# --- cards that have no amount -------------------------------------------------------

def test_a_report_without_an_amount_has_none():
    """Feeder code 2 (`{id, day, manual, time, online_state, eat_video, state}`)
    closes a meal without saying what it served."""
    assert _served("2", "d4h", {"id": "s_1", "day": 20261117, "manual": 0}) is None


def test_only_a_finished_feed_carries_an_amount():
    # A cat leaving the bowl: feeding kind, done role, but not a feed.
    assert _served("6", "d4sh", {"real_amount1": 1}) is None
    assert _served("eat_over", "d4sh", {"real_amount1": 1}) is None
    # A feed whose end never arrived.
    assert _served("feed_start", "d4sh", {"id": "r_1"}) is None
    # Not a feeder at all.
    assert _served("drink_over", "w7h", {"real_amount": 1}, event_kind="drinking") is None


def test_the_code_is_read_in_the_devices_own_category():
    """4 is a finished feed on a feeder and a cleaning step on a litter box."""
    assert _served("4", "t5", {"real_amount": 3}, event_kind="cleaning") is None


def test_junk_amounts_never_raise():
    assert _served("feed_over", "d4sh", {"real_amount1": "lots", "real_amount2": [1]}) is None
    assert _served("feed_over", "d4h", {"real_amount": {"a": 1}}) is None
    card = _card("feed_over", "d4h", {"real_amount": 1})
    assert served_amounts({**card, "content": "not a dict"}, "d4h") is None
    assert served_amounts({**card, "content": None}, "d4h") is None
    assert served_amounts(card, None) is not None, "an MQTT name needs no category"
