"""Repairs run over rows that are already stored.

Every derived field on an `events` row — `event_kind`, `parent_event`,
`related_event`, `pet_ref` — is computed once at ingest time and persisted, so a
row written before a field existed, or before its event code was recognised,
keeps a wrong value that no amount of later reading fixes. A pass here
recomputes those fields from the untouched device `content` and writes back
only what actually changed, which is what makes it safe to run on every start.

Kept out of `events/store.py` on purpose: this is not persistence, it is the
normalizers in `events/normalize.py` applied a second time — to stored rows
instead of to payloads. The store arrives as an argument, and is imported here
for typing only.
"""
from __future__ import annotations

import hashlib
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from petkit_local.events import codes, normalize
from petkit_local.events.normalize import (_as_dict, _extract_pet_ref, _parent_event_of,
                                           classify_event_kind)
from petkit_local.utils.const import VERSION

if TYPE_CHECKING:
    from petkit_local.events.store import EventStore

log = logging.getLogger(__name__)

#: Rows read per query. Small enough that a page of `content_json` is a few
#: hundred kilobytes, large enough that a year of history is a few dozen reads.
PAGE_SIZE = 2000


def _stamp() -> int | None:
    """What a finished pass leaves in the database: a fingerprint of the code
    whose output it persisted.

    A stored row only goes stale when the code that derives its fields changes,
    so that code IS the version. Hashing it, rather than keeping a number
    somebody must remember to bump, means a release that regrades one event
    code re-runs the pass by itself — and so does a development build deployed
    without a version bump. The running version is mixed in so that every
    release re-checks once regardless.

    Returns None when a source file cannot be read (never 0, which is what a
    database no pass has completed on holds): the pass then runs on every
    start, which is the old behaviour and always correct.
    """
    digest = hashlib.sha256(VERSION.encode())
    try:
        for module in (codes, normalize, sys.modules[__name__]):
            digest.update(Path(module.__file__).read_bytes())
    except (OSError, TypeError):
        return None
    # `user_version` is a signed 32-bit field; keep it positive and non-zero.
    return int.from_bytes(digest.digest()[:4], "big") >> 1 or 1


async def backfill_event_rows(store: EventStore, force: bool = False) -> int:
    """Recompute derived fields on already-stored events.

    Classification happens at ingest time and is persisted, so rows written
    before a field existed (`parent_event`) or before an event_type code was
    recognised keep their stale values forever — a cleaning report saved as
    `event_kind="other"` never attaches to its visit, and shows up as a bare
    standalone card. Recomputing from the untouched `content_json` repairs
    them in place. Safe to run on every start: it only writes rows whose
    derived values actually change.

    The rewrites go in one `store.transaction()`: every repaired row would
    otherwise be its own commit, and a start-up interrupted halfway would
    leave the timeline partly regrouped.

    It runs before the device-facing servers come up, so it must cost nothing
    when there is nothing to do. A completed pass leaves `_stamp()` in the
    database and a start that finds its own stamp there returns without
    reading a row; when the stamp differs the table is walked a page at a
    time, so memory stays flat and no row is out of reach however long the
    history is. `force` runs the pass regardless.
    """
    stamp = _stamp()
    if not force and stamp is not None and await store.backfill_stamp() == stamp:
        return 0

    fixed = 0
    after_id = 0

    async with store.transaction():
        while True:
            rows = await store.events_for_backfill(after_id, PAGE_SIZE)
            if not rows:
                break
            after_id = rows[-1]["id"]
            fixed += await _repair(store, rows)
        if stamp is not None:
            await store.set_backfill_stamp(stamp)

    if fixed:
        log.info("Backfilled %d event rows (event_kind/parent_event/related_event/pet_ref)", fixed)
    return fixed


async def _repair(store: EventStore, rows: list[dict]) -> int:
    """Rewrite the rows of one page whose derived values are stale."""
    fixed = 0
    for row in rows:
        content = _as_dict(row.get("content_json"))
        updates = {}

        kind = classify_event_kind(row.get("event_type"), content,
                                   row.get("device_type"))
        if kind != row.get("event_kind"):
            updates["event_kind"] = kind

        parent = _parent_event_of(content)
        if parent and parent != row.get("parent_event"):
            updates["parent_event"] = parent

        # Rows from before `event_id` was understood as the session key
        # stored it only as event_uid; recover it so they group instead of
        # floating.
        if not row.get("related_event") and row.get("event_uid"):
            uid = str(row["event_uid"])
            updates["related_event"] = uid.rsplit(":", 1)[0] if ":" in uid else uid

        # Every row stored before `score_info[].id` was understood carries
        # the identity in its content and nothing in `pet_ref`. Recovering
        # it is what makes an existing install's history bindable at all.
        ref = _extract_pet_ref(content)
        if ref is not None and ref != row.get("pet_ref"):
            updates["pet_ref"] = ref

        if updates:
            await store.update_event_fields(row["id"], **updates)
            fixed += 1
    return fixed
