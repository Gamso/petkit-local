"""Which kinds of notification the phones get: the panel's switches.

One switch per kind, for every phone and every way it receives them — its
notification socket (`channel.py`) and the push endpoint it registered
(`push.py`). A phone's own `kinds` then choose among what the switches let
through; they can narrow it, never widen it.

Persisted to `{data_dir}/notify_kinds.json`, atomically (`utils/jsonio.py`),
and set ONLY from the panel (Setup -> Phone app access) — never an add-on
option, never a CLI flag.

The switches used to live in `ntfy.json`, beside an ntfy server, its access
token and a topic, from when notifications also went to an ntfy server. That
file is read once, at start: its `kinds` are written here and the file is
deleted, because the token in it is a secret and nothing reads it any more.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from petkit_local.utils.jsonio import atomic_write_text, read_json

log = logging.getLogger(__name__)

FILENAME = "notify_kinds.json"
#: The file the switches were kept in before, with the ntfy settings. Read once
#: to migrate them, then deleted (see the module docstring).
LEGACY_FILENAME = "ntfy.json"

#: Every kind of notification, in the order the panel lists them. A kind is the
#: unit a switch turns off and the unit the rate limit counts.
KIND_TOILET = "toilet"
KIND_FEED_SERVED = "feed_served"
KIND_FEED_FAILED = "feed_failed"
KIND_ERROR = "error"
KIND_FOOD_LOW = "food_low"
KIND_CONSUMABLE = "consumable"
KIND_OFFLINE = "offline"

KINDS: tuple[str, ...] = (KIND_TOILET, KIND_FEED_SERVED, KIND_FEED_FAILED, KIND_ERROR,
                          KIND_FOOD_LOW, KIND_CONSUMABLE, KIND_OFFLINE)

KIND_LABELS: dict[str, str] = {
    KIND_TOILET: "Toilet visit",
    KIND_FEED_SERVED: "Meal served",
    KIND_FEED_FAILED: "Meal failed",
    KIND_ERROR: "Device error",
    KIND_FOOD_LOW: "Food low",
    KIND_CONSUMABLE: "Filter, litter or consumable to replace",
    KIND_OFFLINE: "Device offline",
}


class SettingsRejected(ValueError):
    """A value the panel sent that cannot be stored; the message says which."""


@dataclass(frozen=True)
class NotifySettings:
    """One snapshot of the switches. Immutable: an update builds a new one."""

    kinds: Mapping[str, bool] = field(default_factory=lambda: dict.fromkeys(KINDS, True))

    def wants(self, kind: str) -> bool:
        """Whether `kind` is switched on."""
        return bool(self.kinds.get(kind, False))

    def public(self) -> list[dict[str, Any]]:
        """The panel's view: every kind, its label and its switch, in order."""
        return [{"kind": k, "label": KIND_LABELS[k], "enabled": self.wants(k)} for k in KINDS]


def _parse(raw: Any) -> NotifySettings:
    """The switches out of a stored record. A missing kind is on, a bad record all on."""
    stored = raw.get("kinds") if isinstance(raw, dict) else None
    if not isinstance(stored, dict):
        stored = {}
    return NotifySettings(kinds={k: bool(stored.get(k, True)) for k in KINDS})


def _dump(settings: NotifySettings) -> str:
    return json.dumps({"version": 1, "kinds": dict(settings.kinds)}, indent=2)


class NotifyConfig:
    """The switches and their one file. Mutations persist before they return."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._settings = self._load()

    @property
    def settings(self) -> NotifySettings:
        return self._settings

    def _load(self) -> NotifySettings:
        legacy = self._path.with_name(LEGACY_FILENAME)
        if self._path.exists() or not legacy.exists():
            settings = _parse(read_json(self._path, {}))
            # Still there next to a migrated file: a migration cut short
            # between its two steps, or a downgrade that wrote one again.
            if legacy.exists():
                _discard(legacy)
            return settings
        settings = _parse(read_json(legacy, {}))
        try:
            atomic_write_text(self._path, _dump(settings))
        except OSError as e:
            # Keep the old file, so the switches are not lost: the migration
            # runs again on the next start.
            log.warning("notifications: could not write %s (%s); the kind switches are read "
                        "from %s until it can be", FILENAME, type(e).__name__, LEGACY_FILENAME)
            return settings
        _discard(legacy)
        log.info("notifications: the kind switches moved from %s to %s; its ntfy settings "
                 "were removed", LEGACY_FILENAME, FILENAME)
        return settings

    async def set_kinds(self, kinds: Any) -> NotifySettings:
        """Switch the kinds named in `kinds` (`{kind: bool}`); the others are left alone.

        Raises:
            SettingsRejected: `kinds` names an unknown kind or a non-boolean
                value. Nothing is changed then.
        """
        if not isinstance(kinds, dict) or not all(
                k in KINDS and isinstance(v, bool) for k, v in kinds.items()):
            raise SettingsRejected(f"kinds must map some of {', '.join(KINDS)} to true/false")
        async with self._lock:
            new = NotifySettings(kinds={**self._settings.kinds, **kinds})
            await asyncio.to_thread(atomic_write_text, self._path, _dump(new))
            self._settings = new
        return new


def _discard(legacy: Path) -> None:
    """Delete the old `ntfy.json`: it holds an ntfy access token nothing reads any more."""
    try:
        legacy.unlink(missing_ok=True)
    except OSError as e:
        log.warning("notifications: could not delete %s (%s); it may still hold the old ntfy "
                    "access token, so delete it by hand", legacy, type(e).__name__)
