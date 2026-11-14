"""Where notifications go: the ntfy server, its access token, the topic, the kinds.

Persisted to `{data_dir}/ntfy.json`, atomically and 0600 like `app_api.json`
(`utils/jsonio.py`), and configured ONLY from the panel — never an add-on
option, never a CLI flag. Options are readable through the Supervisor and land
in backups in the clear, and two of these values are secrets:

* the **access token** authorises publishing to the server. It is stored as
  given (it has to be sent), never written to the log, and never handed back to
  the panel in full — `public()` shows only its last four characters;
* the **topic** is, on ntfy, the subscription itself: anyone who knows its name
  and may read it receives every notification. It is generated with `secrets`
  on first enable and can be regenerated; the panel does show it, because the
  phone has to be told what to subscribe to.

A record that cannot be read back loads as "disabled" — notifications fail
closed, they never start publishing somewhere half-configured.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from petkit_local.utils.jsonio import atomic_write_text, read_json

log = logging.getLogger(__name__)

FILENAME = "ntfy.json"

#: Every kind of notification, in the order the panel lists them. A kind is the
#: unit a switch turns off and the unit the rate limit counts.
KIND_TOILET = "toilet"
KIND_FEED_SERVED = "feed_served"
KIND_FEED_FAILED = "feed_failed"
KIND_ERROR = "error"
KIND_FOOD_LOW = "food_low"
KIND_CONSUMABLE = "consumable"
KIND_OFFLINE = "offline"
KIND_TEST = "test"

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

#: What ntfy itself accepts as a topic name.
TOPIC_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
#: Bytes of randomness in a generated topic: 192 bits, so a topic cannot be guessed.
TOPIC_BYTES = 24
MAX_TOKEN_LENGTH = 256
MAX_SERVER_LENGTH = 256


class SettingsRejected(ValueError):
    """A value the panel sent that cannot be stored; the message says which."""


def generate_topic() -> str:
    """A fresh, unguessable topic name, within ntfy's alphabet and length."""
    return "petkit-" + secrets.token_urlsafe(TOPIC_BYTES)


def check_server(value: Any) -> str:
    """`value` as a server base URL, or `SettingsRejected`.

    `http(s)://host[:port][/path]`, normalised without a trailing slash. A URL
    carrying credentials, a query or a fragment is refused: the token travels
    in its own header, and a URL is the kind of value that ends up in a log.
    """
    if not isinstance(value, str):
        raise SettingsRejected("server must be a URL")
    text = value.strip()
    if not text:
        return ""
    if len(text) > MAX_SERVER_LENGTH:
        raise SettingsRejected(f"server URL is longer than {MAX_SERVER_LENGTH} characters")
    try:
        parts = urlsplit(text)
        _port = parts.port  # raises ValueError on a malformed port
    except ValueError:
        raise SettingsRejected("server is not a valid URL") from None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise SettingsRejected("server must start with https:// (or http://) and name a host")
    if parts.username is not None or parts.password is not None:
        raise SettingsRejected("server URL must not contain credentials; use the token field")
    if parts.query or parts.fragment:
        raise SettingsRejected("server URL must not contain a query or a fragment")
    if any(c.isspace() or ord(c) < 0x21 for c in text):
        raise SettingsRejected("server URL must not contain spaces or control characters")
    return text.rstrip("/")


def check_topic(value: Any) -> str:
    """`value` as a topic name, or `SettingsRejected`."""
    if not isinstance(value, str) or not TOPIC_RE.match(value.strip()):
        raise SettingsRejected("topic must be 1-64 letters, digits, '-' or '_'")
    return value.strip()


def check_token(value: Any) -> str:
    """`value` as an access token ("" removes it), or `SettingsRejected`."""
    if not isinstance(value, str):
        raise SettingsRejected("token must be a string")
    text = value.strip()
    if len(text) > MAX_TOKEN_LENGTH:
        raise SettingsRejected(f"token is longer than {MAX_TOKEN_LENGTH} characters")
    # It is sent as an HTTP header: anything outside visible ASCII would either
    # be refused by the client or smuggle a second header in.
    if any(not (0x21 <= ord(c) <= 0x7E) for c in text):
        raise SettingsRejected("token must be printable ASCII with no spaces")
    return text


def mask_token(token: str) -> str:
    """What the panel may see of a token: its last four characters, at most."""
    if not token:
        return ""
    return "••••" + (token[-4:] if len(token) >= 12 else "")


@dataclass(frozen=True)
class NtfySettings:
    """One snapshot of the settings. Immutable: an update builds a new one."""

    enabled: bool = False
    server: str = ""
    token: str = ""
    topic: str = ""
    kinds: Mapping[str, bool] = field(default_factory=lambda: dict.fromkeys(KINDS, True))

    @property
    def ready(self) -> bool:
        """Whether there is somewhere to publish: on, a server and a topic."""
        return self.enabled and bool(self.server) and bool(self.topic)

    def wants(self, kind: str) -> bool:
        """Whether `kind` is switched on. The test notification always is."""
        return kind == KIND_TEST or bool(self.kinds.get(kind, False))

    @property
    def publish_url(self) -> str:
        """`{server}/{topic}`. A secret in itself — never log it."""
        return f"{self.server}/{self.topic}"

    def public(self) -> dict[str, Any]:
        """The panel's view: everything but the token, which is masked."""
        return {
            "enabled": self.enabled,
            "server": self.server,
            "topic": self.topic,
            "token_set": bool(self.token),
            "token_masked": mask_token(self.token),
            "insecure": self.server.startswith("http://"),
            "kinds": [{"kind": k, "label": KIND_LABELS[k], "enabled": self.wants(k)}
                      for k in KINDS],
        }


class NtfyConfig:
    """The settings and their one file. Mutations persist before they return."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._settings = self._load()

    @property
    def settings(self) -> NtfySettings:
        return self._settings

    def _load(self) -> NtfySettings:
        raw = read_json(self._path, {})
        if not isinstance(raw, dict):
            raw = {}
        try:
            server = check_server(raw.get("server", ""))
            token = check_token(raw.get("token", ""))
            topic = check_topic(raw["topic"]) if raw.get("topic") else ""
        except SettingsRejected as e:
            # Never the value itself: it may be the token.
            log.warning("ntfy: the stored settings are unusable (%s); notifications stay "
                        "off until they are saved again in the panel", e)
            return NtfySettings()
        stored = raw.get("kinds") if isinstance(raw.get("kinds"), dict) else {}
        kinds = {k: bool(stored.get(k, True)) for k in KINDS}
        return NtfySettings(enabled=raw.get("enabled") is True, server=server, token=token,
                            topic=topic, kinds=kinds)

    async def _save(self, settings: NtfySettings) -> None:
        """Persist and adopt `settings`. The caller holds `_lock`."""
        text = json.dumps({
            "version": 1,
            "enabled": settings.enabled,
            "server": settings.server,
            "token": settings.token,
            "topic": settings.topic,
            "kinds": dict(settings.kinds),
        }, indent=2)
        await asyncio.to_thread(atomic_write_text, self._path, text)
        self._settings = settings

    async def update(self, body: Mapping[str, Any]) -> NtfySettings:
        """Apply what the panel sent; returns the new settings.

        Keys left out are left alone — in particular `token`, which the panel
        never has in full and so only sends when the owner typed a new one
        (`""` removes it). Enabling with no topic yet generates one.

        Raises:
            SettingsRejected: any value is invalid. Nothing is changed then.
        """
        current = self._settings
        changes: dict[str, Any] = {}
        if "server" in body:
            changes["server"] = check_server(body["server"])
        if "token" in body and body["token"] is not None:
            changes["token"] = check_token(body["token"])
        if "topic" in body and body["topic"] not in (None, ""):
            changes["topic"] = check_topic(body["topic"])
        if "enabled" in body:
            if not isinstance(body["enabled"], bool):
                raise SettingsRejected("enabled must be true or false")
            changes["enabled"] = body["enabled"]
        if "kinds" in body:
            kinds = body["kinds"]
            if not isinstance(kinds, dict) or not all(
                    k in KINDS and isinstance(v, bool) for k, v in kinds.items()):
                raise SettingsRejected(f"kinds must map some of {', '.join(KINDS)} to true/false")
            changes["kinds"] = {**current.kinds, **kinds}
        new = replace(current, **changes)
        if new.enabled and not new.server:
            raise SettingsRejected("set the ntfy server URL before enabling notifications")
        if new.enabled and not new.topic:
            new = replace(new, topic=generate_topic())
        async with self._lock:
            await self._save(new)
        return new

    async def regenerate_topic(self) -> NtfySettings:
        """Replace the topic with a fresh random one; the old one stops receiving."""
        async with self._lock:
            await self._save(replace(self._settings, topic=generate_topic()))
        return self._settings
