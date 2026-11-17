"""How long a pet drank or ate: `duration_sec` on a drinking or eating card.

Until issue #97 only a toilet visit had a duration; every other card was built
with `duration_sec=None`, so the app's "average drink" and its stem charts came
out empty against a real server. These pin each source the duration may come
from, in its order, and the cases where it must stay None rather than guess.
"""
import json

from petkit_local.events import ingest


def _row(id, ts, event_type, event_kind, device_type, related_event=None,
         content=None, parent_event=None, device_id=1, pet_id=None):
    return {
        "id": id, "device_id": device_id, "device_type": device_type, "ts": ts,
        "event_type": event_type, "event_kind": event_kind,
        "related_event": related_event, "parent_event": parent_event,
        "media": [], "pet_id": pet_id,
        "content_json": None if content is None else json.dumps(content),
    }


def _only_card(rows):
    sessions = ingest.group_sessions(rows)
    assert len(sessions) == 1, sessions
    return sessions[0]


# --- drinking (W7H) ------------------------------------------------------------

def test_a_drink_is_timed_from_its_start_report_to_its_end_report():
    """MQTT `drink_over` carries `event_start` but no `event_end`, so with the
    `drink_start` of the same event_id present the two arrivals are paired --
    the same latency on both ends, rather than a device start against our
    arrival."""
    rows = [
        _row(1, 1000.0, "drink_start", "drinking", "w7h", related_event="d1",
             content={"event_start": 997}),
        _row(2, 1030.0, "drink_over", "drinking", "w7h", related_event="d1",
             content={"event_start": 997, "mark": 997, "upload": 1, "media": 1},
             pet_id=7),
    ]
    card = _only_card(rows)
    assert card["event_type"] == "drink_over"
    assert card["duration_sec"] == 30.0
    assert card["pet_id"] == 7, "the recognised pet must stay the card's"
    assert card["weight"] is None


def test_a_drink_without_its_start_report_is_timed_from_its_own_start():
    """The start report lost: the closing report's own `event_start` against
    its arrival. Overstated by the report's latency, but measured."""
    rows = [_row(2, 1030.0, "drink_over", "drinking", "w7h", related_event="d1",
                 content={"event_start": 1004, "mark": 1001})]
    assert _only_card(rows)["duration_sec"] == 26.0


def test_a_drink_that_states_its_own_span_is_timed_by_it():
    """HTTP code 6 on a W7H carries `event_start` and `event_end`: both the
    device's clock, so they win over any pairing of arrival times."""
    rows = [
        _row(1, 1000.0, "5", "drinking", "w7h", related_event="d1",
             content={"event_start": 990}),
        _row(2, 1030.0, "6", "drinking", "w7h", related_event="d1",
             content={"event_start": 990, "event_end": 1012, "start_time": 990,
                      "mark": 990}),
    ]
    card = _only_card(rows)
    assert card["event_type"] == "6"
    assert card["duration_sec"] == 22.0


def test_a_drink_whose_end_never_arrived_has_no_duration():
    """A card headed by its start report: there is nothing to measure to, and
    the start's own `event_start` against its arrival would be latency
    presented as a drink."""
    rows = [_row(1, 1000.0, "drink_start", "drinking", "w7h", related_event="d1",
                 content={"event_start": 990})]
    card = _only_card(rows)
    assert card["event_type"] == "drink_start"
    assert card["duration_sec"] is None


def test_a_drink_with_no_start_anywhere_has_no_duration():
    rows = [_row(2, 1030.0, "drink_over", "drinking", "w7h", related_event="d1",
                 content={"img": "x", "upload": 1})]
    assert _only_card(rows)["duration_sec"] is None


def test_a_span_the_clocks_disagree_about_is_not_a_zero():
    rows = [_row(2, 1030.0, "drink_over", "drinking", "w7h", related_event="d1",
                 content={"event_start": 1100})]
    assert _only_card(rows)["duration_sec"] is None


def test_junk_in_the_time_fields_never_raises():
    rows = [_row(2, 1030.0, "drink_over", "drinking", "w7h", related_event="d1",
                 content={"event_start": "soon", "event_end": [1], "mark": None})]
    assert _only_card(rows)["duration_sec"] is None


def test_a_drink_with_no_episode_id_is_still_timed_from_its_content():
    """A standalone row has no group to find a start in, but its own content
    can still say when it began."""
    rows = [_row(2, 1030.0, "drink_over", "drinking", "w7h",
                 content={"event_start": 1010})]
    card = _only_card(rows)
    assert card["kind"] == "event"
    assert card["duration_sec"] == 20.0


# --- eating (camera feeder, codes 5/6 inferred) --------------------------------

def test_a_meal_eaten_is_timed_from_eat_start_not_from_the_feed():
    """Codes are per category: on a D4SH 5 opens the eating that 6 closes.
    A `feed_start` (3) in the same episode is a feeding start as well, and
    must not be taken for the cat starting to eat."""
    rows = [
        _row(1, 900.0, "3", "feeding", "d4sh", related_event="m1",
             content={"id": "s_20261117_32400", "time": 32400}),
        _row(2, 1000.0, "5", "feeding", "d4sh", related_event="m1",
             content={"mark": 996, "start_time": 996}),
        _row(3, 1045.0, "6", "feeding", "d4sh", related_event="m1"),
    ]
    card = _only_card(rows)
    assert card["event_type"] == "6"
    assert card["duration_sec"] == 45.0


def test_a_meal_eaten_finds_its_start_through_its_own_link():
    """As reported for a D4SH, code 6's `content.related_event` names the code
    5 that opened the episode. Where that is a different event_id the start is
    found through the link, which is the device's word, not a guess."""
    rows = [
        _row(1, 1000.0, "5", "feeding", "d4sh", related_event="e5",
             content={"mark": 996, "start_time": 996}),
        _row(2, 1050.0, "6", "feeding", "d4sh", related_event="e6",
             parent_event="e5", content={"related_event": "e5"}),
    ]
    by_type = {s["event_type"]: s for s in ingest.group_sessions(rows)}
    assert by_type["6"]["duration_sec"] == 50.0
    assert by_type["5"]["duration_sec"] is None


def test_a_meal_eaten_without_its_start_uses_start_time_or_mark():
    over = _row(3, 1045.0, "eat_over", "feeding", "d4sh", related_event="m1",
                content={"start_time": 1015, "mark": 1010})
    assert _only_card([over])["duration_sec"] == 30.0
    only_mark = _row(3, 1045.0, "eat_over", "feeding", "d4sh", related_event="m1",
                     content={"mark": 1010})
    assert _only_card([only_mark])["duration_sec"] == 35.0


def test_a_meal_eaten_with_nothing_to_go_on_has_no_duration():
    rows = [_row(3, 1045.0, "6", "feeding", "d4sh", related_event="m1")]
    assert _only_card(rows)["duration_sec"] is None


def test_a_start_from_another_device_is_never_paired():
    rows = [
        _row(1, 1000.0, "eat_start", "feeding", "d4sh", related_event="m1",
             device_id=2),
        _row(2, 1045.0, "eat_over", "feeding", "d4sh", related_event="m1"),
    ]
    over = next(s for s in ingest.group_sessions(rows) if s["event_type"] == "eat_over")
    assert over["duration_sec"] is None


# --- what stays as it was --------------------------------------------------------

def test_a_dispensed_feed_is_not_timed():
    """A feed is the motor's span, not a pet's: not what `duration_sec` means,
    even though its start and end are both there."""
    rows = [
        _row(1, 1000.0, "feed_start", "feeding", "d4sh", related_event="f1"),
        _row(2, 1010.0, "feed_over", "feeding", "d4sh", related_event="f1",
             content={"start_time": 999, "real_amount1": 1, "real_amount2": 0}),
    ]
    assert _only_card(rows)["duration_sec"] is None


def test_a_toilet_visit_is_timed_exactly_as_before():
    rows = [
        _row(1, 1000.0, "9", "toilet_visit", "t5", related_event="v1",
             content={"pet_weight": 2320, "mark": 990}),
        _row(2, 1060.0, "10", "toilet_visit", "t5", related_event="v1",
             content={"time_in": 995, "time_out": 1051, "mark": 990,
                      "pet_weight": 2320}),
    ]
    card = _only_card(rows)
    assert card["kind"] == "visit"
    assert card["duration_sec"] == 56.0


def test_other_cards_stay_untimed():
    """An "appeared" card carries `start_time`/`mark` too, and is no span."""
    rows = [_row(1, 1030.0, "20", "pet", "t5", related_event="p1",
                 content={"start_time": 1000, "mark": 1000})]
    assert _only_card(rows)["duration_sec"] is None

