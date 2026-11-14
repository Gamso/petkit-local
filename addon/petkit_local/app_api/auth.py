"""The app API's one account: its password, the phones signed in, the proxies trusted.

Everything here persists to ONE file, `{data_dir}/app_api.json`, written
atomically like every other state file (`utils/jsonio.py`, created 0600 by
`mkstemp`). It is never an add-on option: options are readable through the
Supervisor and land in backups in the clear, and the password is set from the
panel, which is reachable only through Ingress or the LAN.

What is stored, and what deliberately is not:

* the password only as a `hashlib.scrypt` digest, with its random salt and the
  cost parameters it was made with — so raising the defaults later still
  verifies a password set under the old ones;
* each session only as the SHA-256 of its token. A token is 256 random bits, so
  a fast hash is enough: there is nothing to brute-force, and what the file
  must not hold is anything that could be replayed;
* never the token itself, never the password, never a client address.

A session expires after `SESSION_IDLE_LIFETIME` without use, and each use
slides that deadline — but `last_used` is written to disk at most once every
`LAST_USED_WRITE_INTERVAL`, because a phone polling the timeline would
otherwise rewrite and fsync this file on every request.

No password means no app API at all: `password_set` is False, and the
middleware answers 401 to everything. A record that cannot be read back counts
as no password — the API fails closed, it never falls open.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
import unicodedata
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from petkit_local.app_api.throttle import IPNetwork, parse_networks
from petkit_local.utils.jsonio import atomic_write_text, read_json

log = logging.getLogger(__name__)

FILENAME = "app_api.json"

#: A phone unused for this long has to sign in again.
SESSION_IDLE_LIFETIME = 90 * 86400
#: How stale the stored `last_used` may get before a request rewrites it.
LAST_USED_WRITE_INTERVAL = 5 * 60
#: Phones signed in at once. A login beyond it drops the least recently used.
MAX_SESSIONS = 50
MIN_PASSWORD_LENGTH = 8
#: Upper bound on what is hashed. scrypt takes any length; this is about not
#: being handed a megabyte to hash per login attempt.
MAX_PASSWORD_LENGTH = 1024
MAX_DEVICE_NAME = 64
#: Trusted proxy entries. A handful is all a real chain has.
MAX_TRUSTED_PROXIES = 32
#: scrypt runs in a worker thread and needs ~32 MiB each at the default cost,
#: so at most this many run at once whatever the request rate.
MAX_CONCURRENT_HASHES = 2


@dataclass(frozen=True)
class ScryptParams:
    """scrypt's cost parameters. Stored with each digest, so they can change."""

    n: int = 2 ** 15
    r: int = 8
    p: int = 1
    dklen: int = 32

    @property
    def maxmem(self) -> int:
        # scrypt needs 128 * r * n bytes, and hashlib's 32 MiB default is
        # exactly that at n=2**15 — so it refuses without some headroom.
        return 128 * self.r * self.n * 2 + 1024 * 1024


DEFAULT_SCRYPT = ScryptParams()


class PasswordRejected(ValueError):
    """A new password that does not meet the rules; the message says which."""


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(text: Any) -> bytes | None:
    try:
        return base64.b64decode(str(text), validate=True)
    except (ValueError, TypeError):
        return None


def hash_token(token: str) -> str:
    """The stored form of a session token: hex SHA-256 of its UTF-8 bytes."""
    return hashlib.sha256(token.encode("utf-8", "surrogatepass")).hexdigest()


def clean_device_name(value: Any) -> str:
    """A phone's self-chosen name, made safe to store and to render.

    Control and format characters become spaces (a name is shown in the panel
    and written to the log), whitespace is collapsed, length capped.
    """
    text = value if isinstance(value, str) else ""
    text = "".join(" " if unicodedata.category(c)[0] == "C" else c for c in text)
    text = " ".join(text.split())[:MAX_DEVICE_NAME]
    return text or "Unnamed device"


def check_new_password(password: Any) -> str:
    """`password` if it may be set; raises `PasswordRejected` otherwise."""
    if not isinstance(password, str):
        raise PasswordRejected("password must be a string")
    if len(password) < MIN_PASSWORD_LENGTH:
        raise PasswordRejected(f"password must be at least {MIN_PASSWORD_LENGTH} characters")
    if len(password) > MAX_PASSWORD_LENGTH:
        raise PasswordRejected(f"password must be at most {MAX_PASSWORD_LENGTH} characters")
    return password


@dataclass
class AppSession:
    """One signed-in phone. `id` is public (the panel revokes by it); the token is not."""

    id: str
    token_hash: str
    name: str
    created: float
    last_used: float

    @property
    def expires(self) -> float:
        return self.last_used + SESSION_IDLE_LIFETIME

    def public(self) -> dict[str, Any]:
        """What the panel and the app may see: everything but the token hash."""
        return {"id": self.id, "name": self.name, "created": self.created,
                "last_used": self.last_used, "expires": self.expires}

    @classmethod
    def from_json(cls, raw: Any) -> AppSession | None:
        """A stored session, or None for anything malformed (dropped, not fatal)."""
        if not isinstance(raw, dict):
            return None
        try:
            session = cls(id=str(raw["id"]), token_hash=str(raw["token_hash"]),
                          name=clean_device_name(raw.get("name")),
                          created=float(raw["created"]), last_used=float(raw["last_used"]))
        except (KeyError, TypeError, ValueError):
            return None
        if len(session.token_hash) != 64 or not session.id:
            return None
        return session


class AppAuth:
    """The password, the sessions and the trusted proxies, and their one file.

    Every mutation is async and persists before it returns, under one lock, so
    two writes can never interleave their snapshots. Reads are synchronous.
    """

    def __init__(self, path: str | os.PathLike[str], *,
                 clock: Callable[[], float] = time.time,
                 scrypt_params: ScryptParams = DEFAULT_SCRYPT) -> None:
        self._path = Path(path)
        self._clock = clock
        self._params = scrypt_params
        self._lock = asyncio.Lock()
        self._hash_slots = asyncio.Semaphore(MAX_CONCURRENT_HASHES)
        self._password: dict[str, Any] | None = None
        self._sessions: dict[str, AppSession] = {}  # keyed by token hash
        self._trusted: list[str] = []
        self._networks: list[IPNetwork] = []
        self._load()

    # --- persistence ----------------------------------------------------

    def _load(self) -> None:
        raw = read_json(self._path, {})
        if not isinstance(raw, dict):
            raw = {}
        record = raw.get("password")
        self._password = record if self._valid_record(record) else None
        if record is not None and self._password is None:
            log.warning("app API: the stored password record is unreadable; the app API "
                        "stays closed until a password is set again in the panel")
        for item in raw.get("sessions") or []:
            session = AppSession.from_json(item)
            if session is not None:
                self._sessions[session.token_hash] = session
        try:
            proxies = [str(p) for p in raw.get("trusted_proxies") or []]
            self._networks = parse_networks(proxies)
            self._trusted = proxies
        except (TypeError, ValueError) as e:
            log.warning("app API: ignoring the stored trusted proxies (%s)", e)

    @staticmethod
    def _valid_record(record: Any) -> bool:
        if not isinstance(record, dict) or record.get("algorithm") != "scrypt":
            return False
        try:
            n, r, p, dklen = (int(record[k]) for k in ("n", "r", "p", "dklen"))
        except (KeyError, TypeError, ValueError):
            return False
        return (n > 1 and n & (n - 1) == 0 and r > 0 and p > 0 and dklen >= 16
                and _unb64(record.get("salt")) is not None
                and _unb64(record.get("hash")) is not None)

    def _snapshot(self) -> str:
        return json.dumps({
            "version": 1,
            "password": self._password,
            "sessions": [asdict(s) for s in self._sessions.values()],
            "trusted_proxies": list(self._trusted),
        }, indent=2)

    async def _save(self) -> None:
        """Write the current state. The caller holds `_lock`."""
        await asyncio.to_thread(atomic_write_text, self._path, self._snapshot())

    # --- password -------------------------------------------------------

    @property
    def password_set(self) -> bool:
        return self._password is not None

    def _hash(self, password: str, salt: bytes, params: ScryptParams) -> bytes:
        return hashlib.scrypt(password.encode("utf-8", "surrogatepass"), salt=salt,
                              n=params.n, r=params.r, p=params.p,
                              dklen=params.dklen, maxmem=params.maxmem)

    async def verify_password(self, candidate: Any) -> bool:
        """Whether `candidate` is the password. False when none is set.

        The digest is compared with `hmac.compare_digest`. A non-string or an
        oversized candidate is still hashed — under the same cost — so the
        answer takes as long whatever was sent.
        """
        record = self._password
        if record is None:
            return False
        text = candidate if isinstance(candidate, str) else ""
        valid_input = isinstance(candidate, str) and 0 < len(candidate) <= MAX_PASSWORD_LENGTH
        params = ScryptParams(int(record["n"]), int(record["r"]), int(record["p"]),
                              int(record["dklen"]))
        salt = _unb64(record["salt"]) or b""
        expected = _unb64(record["hash"]) or b""
        async with self._hash_slots:
            digest = await asyncio.to_thread(
                self._hash, text[:MAX_PASSWORD_LENGTH], salt, params)
        return hmac.compare_digest(digest, expected) and valid_input

    async def set_password(self, password: Any, *, keep_session: str | None = None) -> int:
        """Set or replace the password; returns how many sessions it revoked.

        Every session is revoked except `keep_session` (a session id) — the
        app passes its own, so the phone that changed the password stays
        signed in; the panel passes nothing, so a reset signs everyone out.

        Raises:
            PasswordRejected: the password does not meet the rules.
        """
        password = check_new_password(password)
        salt = secrets.token_bytes(16)
        params = self._params
        async with self._hash_slots:
            digest = await asyncio.to_thread(self._hash, password, salt, params)
        async with self._lock:
            self._password = {
                "algorithm": "scrypt", "n": params.n, "r": params.r, "p": params.p,
                "dklen": params.dklen, "salt": _b64(salt), "hash": _b64(digest),
                "set_at": self._clock(),
            }
            revoked = self._drop_sessions(keep=keep_session)
            await self._save()
        return revoked

    async def clear_password(self) -> int:
        """Remove the password, which closes the app API; revokes every session."""
        async with self._lock:
            self._password = None
            revoked = self._drop_sessions(keep=None)
            await self._save()
        return revoked

    # --- sessions -------------------------------------------------------

    def _drop_sessions(self, keep: str | None) -> int:
        before = len(self._sessions)
        self._sessions = {h: s for h, s in self._sessions.items() if keep and s.id == keep}
        return before - len(self._sessions)

    def _prune_expired(self, now: float) -> bool:
        expired = [h for h, s in self._sessions.items() if s.expires <= now]
        for h in expired:
            del self._sessions[h]
        return bool(expired)

    def sessions(self) -> list[AppSession]:
        """Live sessions, most recently used first."""
        now = self._clock()
        return sorted((s for s in self._sessions.values() if s.expires > now),
                      key=lambda s: s.last_used, reverse=True)

    async def create_session(self, device_name: Any) -> tuple[str, AppSession]:
        """Sign a phone in: `(token, session)`. The token is shown exactly once."""
        token = secrets.token_urlsafe(32)
        now = self._clock()
        session = AppSession(id=secrets.token_hex(8), token_hash=hash_token(token),
                             name=clean_device_name(device_name), created=now, last_used=now)
        async with self._lock:
            self._prune_expired(now)
            while len(self._sessions) >= MAX_SESSIONS:
                oldest = min(self._sessions.values(), key=lambda s: s.last_used)
                del self._sessions[oldest.token_hash]
            self._sessions[session.token_hash] = session
            await self._save()
        return token, session

    async def authenticate(self, token: str | None) -> AppSession | None:
        """The live session `token` belongs to, sliding its expiry; else None.

        The lookup is by hash, so what timing could reveal is a prefix of a
        SHA-256 digest, not of the token. An expired session is deleted on the
        spot rather than left for a sweep.
        """
        if not token or not self.password_set:
            return None
        key = hash_token(token)
        session = self._sessions.get(key)
        if session is None or not hmac.compare_digest(session.token_hash, key):
            return None
        now = self._clock()
        if session.expires <= now:
            async with self._lock:
                self._sessions.pop(key, None)
                await self._save()
            return None
        if now - session.last_used >= LAST_USED_WRITE_INTERVAL:
            async with self._lock:
                session.last_used = now
                await self._save()
        return session

    def is_live(self, session: AppSession) -> bool:
        """Whether `session` still stands — without sliding its expiry.

        What an open WebSocket asks every second or so: it authenticated once,
        on the upgrade, and must stop the moment the session is signed out,
        revoked, dropped by a password change, or the password is removed.
        """
        current = self._sessions.get(session.token_hash) if self.password_set else None
        return current is session and current.expires > self._clock()

    async def revoke(self, session_id: str) -> bool:
        """Revoke one session by its public id; False if there is none."""
        async with self._lock:
            match = next((h for h, s in self._sessions.items() if s.id == session_id), None)
            if match is None:
                return False
            del self._sessions[match]
            await self._save()
        return True

    async def revoke_all(self) -> int:
        async with self._lock:
            revoked = self._drop_sessions(keep=None)
            await self._save()
        return revoked

    # --- trusted proxies ------------------------------------------------

    @property
    def trusted_proxies(self) -> list[str]:
        return list(self._trusted)

    @property
    def trusted_networks(self) -> list[IPNetwork]:
        return list(self._networks)

    async def set_trusted_proxies(self, entries: Any) -> list[str]:
        """Replace the trusted proxy list; returns it normalised.

        Raises:
            ValueError: not a list of strings, too long, or an entry that is
                neither an address nor a CIDR. Nothing is changed then.
        """
        if not isinstance(entries, list) or not all(isinstance(e, str) for e in entries):
            raise ValueError("expected a list of addresses or CIDRs")
        if len(entries) > MAX_TRUSTED_PROXIES:
            raise ValueError(f"at most {MAX_TRUSTED_PROXIES} entries")
        networks = parse_networks(entries)
        normalised = [str(n) for n in networks]
        async with self._lock:
            self._networks = networks
            self._trusted = normalised
            await self._save()
        return normalised
