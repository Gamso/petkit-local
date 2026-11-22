"""A CTW3's drink records, stored: the one path every source of them shares.

Records reach petkit-local two ways. The parent's relay forwards a history
stream when one comes (`mqtt/ble_relay.py`), which in practice it does not:
the relay passes reads only, and the 212 that starts the history needs a
DIRECT phone-to-fountain link (#140). So the phone app runs that sync itself
and uploads what it read (`app_api/handlers/accessories.py`, #141).

Both go through `store_drink_records`, so a record seen by both is stored
once: the row is built by `ingest.from_ble_drink_record` from the fountain's
own `workTime`, which is the record's only identity, and its uid follows from
it. The accessory's `history["cursor"]` moves the same way from either side.

Nothing here raises on what it is handed. An upload is app input and a stream
is device input: a value that is not a plausible record is counted, never
stored and never raised on.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from petkit_local.devices.ble.ctw3 import DrinkRecord, ctw3_record_time
from petkit_local.events import ingest
from petkit_local.events.codes import KIND_DRINKING
from petkit_local.utils.coerce import to_float

if TYPE_CHECKING:
    from petkit_local.devices.ble.registry import BLEDevice
    from petkit_local.events.store import EventStore

log = logging.getLogger(__name__)

#: The widest `workTime` a record can carry: a big-endian u32.
WORK_TIME_MAX = 0xFFFFFFFF
#: The range of `stayTime`, a signed Java short (`CTW3DataConvertor.parseWorkData`).
STAY_TIME_MIN = -0x8000
STAY_TIME_MAX = 0x7FFF


@dataclass
class StoredDrinks:
    """What one batch of records came to."""

    #: Rows written for the first time.
    stored: int = 0
    #: Records whose row was already held (the same `workTime` seen before).
    duplicates: int = 0
    #: Records at or before the `since` they were filtered against.
    skipped: int = 0
    #: Records past the `limit`, not looked at.
    dropped: int = 0
    #: The store failed and the batch was lost (logged).
    failed: bool = False
    #: The accessory's cursor once the batch is in; None while it has none.
    cursor: float | None = None
    #: The cursor moved, so the accessory registry needs saving.
    cursor_moved: bool = False

    @property
    def written(self) -> int:
        """Records that reached the store, new or not."""
        return self.stored + self.duplicates


def history_cursor(ble: BLEDevice) -> float:
    """The start of the newest record stored for this accessory, 0.0 for none."""
    history = ble.history if isinstance(ble.history, dict) else {}
    return to_float(history.get("cursor"), 0.0) or 0.0


def drink_record(work_time: Any, stay_time: Any, now: float | None = None) -> DrinkRecord | None:
    """One uploaded record as the relay would have decoded it, or None.

    `work_time` is the fountain's own value, sent on unchanged: seconds since
    2000-01-01 UTC as the app sets the clock, or a plain Unix count if a parent
    set it so -- `ctw3_record_time` reads both, exactly as for a relayed packet,
    and keeping the raw value is what gives the row the same uid either way.
    The bounds are the record's own widths (a u32 and a signed short), and a
    negative stay is refused as `decode_ctw3_records` refuses it.

    App input: anything else, a bool or a float included, is None.
    """
    if (type(work_time) is not int or type(stay_time) is not int
            or not 0 <= work_time <= WORK_TIME_MAX
            or not STAY_TIME_MIN <= stay_time <= STAY_TIME_MAX or stay_time < 0):
        return None
    start = ctw3_record_time(work_time, now)
    if start is None:
        return None
    return DrinkRecord(raw_time=work_time, start=start, seconds=stay_time)


async def store_drink_records(store: EventStore | None, ble: BLEDevice,
                              records: Iterable[DrinkRecord], *,
                              since: float | None = None, limit: int | None = None,
                              now: float | None = None) -> StoredDrinks:
    """Store drink records as rows on the ACCESSORY's id, and move its cursor.

    Each record becomes one drinking row (`ingest.from_ble_drink_record`,
    `codes.BLE_DRINK_EVENT`), deduplicated by its uid -- the accessory's id and
    the fountain's own `workTime` -- in one transaction. A record at or before
    `since` is skipped without a write (None skips nothing), and only the first
    `limit` records left are written. The cursor then moves to the newest
    record written, never past now (a clock set a day ahead must not hide
    tomorrow's real drinks), and never backwards.

    Never raises: a store that fails loses this batch, logged, and says so in
    `failed`; the cursor stays where it was.
    """
    result = StoredDrinks(cursor=history_cursor(ble) or None)
    records = list(records)
    fresh = records if since is None else [r for r in records if r.start > since]
    result.skipped = len(records) - len(fresh)
    if limit is not None and len(fresh) > max(limit, 0):
        result.dropped = len(fresh) - max(limit, 0)
        fresh = fresh[:max(limit, 0)]
    if not fresh or store is None:
        return result

    name = f"{ble.ble_type.upper()} (id={ble.petkit_id})"
    new = 0
    newest = 0.0
    try:
        async with store.transaction() as tx:
            for record in fresh:
                row = ingest.from_ble_drink_record(
                    ble.petkit_id, ble.ble_type, record.raw_time, record.start,
                    record.seconds)
                _, inserted = await tx.record_event(row)
                new += int(inserted)
                newest = max(newest, record.start)
    except Exception:  # noqa: BLE001 - a lost batch must not take its caller down
        log.exception("%s: storing %d drink records failed", name, len(fresh))
        result.failed = True
        return result
    result.stored = new
    result.duplicates = len(fresh) - new
    cursor = min(newest, time.time() if now is None else now)
    if cursor > history_cursor(ble):
        history = ble.history if isinstance(ble.history, dict) else {}
        ble.history = {**history, "cursor": cursor}
        result.cursor = cursor
        result.cursor_moved = True
    log.info("%s: %d drink records stored (%d already held)", name, new,
             len(fresh) - new)
    return result


def announce_drinks(hub: Any, ble: BLEDevice, result: StoredDrinks, *,
                    parent_id: int | None, source: str | None = None) -> None:
    """Tell the panel's log, and the app's live feed, that drinks were stored.

    One line per batch, not per record: a first sync can carry hundreds. It is
    also what tells the app's live feed to re-read this accessory and its
    timeline. `is_new` False keeps the phone notifier off it -- a drink hours
    old is history, not news. Nothing when nothing new was stored.
    """
    if not result.stored or hub is None:
        return
    detail: dict[str, Any] = {"event_kind": KIND_DRINKING, "device_id": ble.petkit_id,
                              "device_type": ble.ble_type, "is_new": False,
                              "parent_id": parent_id}
    if source is not None:
        detail["source"] = source
    hub.publish("event", ble.petkit_id,
                f"{result.written} drink records from history ({result.stored} new)",
                detail=detail)
