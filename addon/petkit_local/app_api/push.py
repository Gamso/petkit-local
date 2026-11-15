"""A phone's own push endpoint: what may be registered, and where we never post.

The phone app registers a UnifiedPush endpoint (`PUT /app/v1/push`) and the
notifier POSTs a small JSON message to it for each notification
(`notify/push.py`). The registration belongs to the session that made it — it
is stored on the `AppSession` in `app_api.json` — so logout, revocation from the
panel, a password change and the 90-day expiry all remove it with the session,
and there is no second store to drift out of step with the first.

An endpoint is a CAPABILITY URL: whoever knows it can push to that phone. It is
therefore treated like a token everywhere but in `app_api.json` (0600): the
panel, the API's answers and the log only ever show its host.

What may be registered (`check_endpoint`):

* `https://`, or plain `http://` only for a target that is not on the public
  internet — a non-global address literal (RFC 1918, CGNAT/Tailscale, ULA) or a
  local-only name (`ntfy`, `*.lan`, `*.local`, `*.home.arpa`, `*.internal`).
  A self-hosted ntfy on the LAN is a real case, so private targets are allowed;
* no credentials, no fragment; a query is allowed (UnifiedPush endpoints use
  one); at most `MAX_ENDPOINT_LENGTH` printable ASCII characters;
* never an address of THIS machine (`refused_address`): loopback, link-local,
  unspecified, multicast, reserved, or any address bound to one of its own
  interfaces. go2rtc's API listens on loopback without authentication, and the
  panel answers unauthenticated on the container's own address — a push must
  never become a way to reach either. The same test runs again on every
  address a name resolves to when the message is sent (`notify/push.py`), so a
  name pointing at 127.0.0.1 is refused at connect time, not just at
  registration.
"""
from __future__ import annotations

import ipaddress
import logging
import socket
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from petkit_local.app_api.notify.settings import KINDS
from petkit_local.utils.coerce import to_float

log = logging.getLogger(__name__)

#: UnifiedPush endpoints are a few hundred characters; rewrite proxies add a
#: token to the query. Twice the longest seen is plenty and bounds the file.
MAX_ENDPOINT_LENGTH = 2048
#: Entries accepted in `kinds` (there are seven kinds today).
MAX_KINDS = 32

#: Names that only ever resolve inside a home network: plain `http://` is
#: allowed to them. `.localhost` is not among them — it is this machine.
LOCAL_SUFFIXES = (".lan", ".local", ".home.arpa", ".internal")

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


class PushRejected(ValueError):
    """An endpoint or a kinds list that cannot be registered; the message says why."""


def _own_address(ip: IPAddress) -> bool:
    """Whether `ip` is bound to one of this machine's interfaces.

    Binding to an address succeeds exactly when it is local, whatever the
    interface — no interface listing needed, and no packet is sent.
    """
    family = socket.AF_INET6 if ip.version == 6 else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_DGRAM) as sock:
            sock.bind((str(ip), 0))
    except OSError:
        return False
    return True


def refused_address(value: str | IPAddress) -> bool:
    """Whether a push may never be sent to this address. Never raises.

    Looked up through the module at every call (`notify/push.py` too), so the
    test suite can point pushes at its loopback fake server.
    """
    try:
        ip = value if not isinstance(value, str) else ipaddress.ip_address(value.split("%")[0])
    except ValueError:
        return True
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    if (ip.is_loopback or ip.is_link_local or ip.is_unspecified or ip.is_multicast
            or ip.is_reserved):
        return True
    return _own_address(ip)


def literal_address(host: str) -> IPAddress | None:
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        return None


def _local_name(host: str) -> bool:
    return "." not in host or host.endswith(LOCAL_SUFFIXES)


def endpoint_host(endpoint: str) -> str:
    """All of an endpoint that may be shown or logged: its host name."""
    try:
        return urlsplit(endpoint).hostname or "?"
    except ValueError:
        return "?"


def check_endpoint(value: Any) -> str:
    """`value` as an endpoint that may be registered, or `PushRejected`.

    The message never repeats the URL: it is a secret, and the message is
    returned to the app and may be logged.
    """
    if not isinstance(value, str) or not value.strip():
        raise PushRejected("endpoint must be a URL")
    text = value.strip()
    if len(text) > MAX_ENDPOINT_LENGTH:
        raise PushRejected(f"endpoint is longer than {MAX_ENDPOINT_LENGTH} characters")
    if any(not (0x21 <= ord(c) <= 0x7E) for c in text):
        raise PushRejected("endpoint must be printable ASCII without spaces")
    try:
        parts = urlsplit(text)
        _port = parts.port  # raises ValueError on a malformed port
    except ValueError:
        raise PushRejected("endpoint is not a valid URL") from None
    host = (parts.hostname or "").rstrip(".")
    if parts.scheme not in ("https", "http") or not host:
        raise PushRejected("endpoint must start with https:// and name a host")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise PushRejected("endpoint must not contain credentials")
    if "#" in text:
        raise PushRejected("endpoint must not contain a fragment")
    ip = literal_address(host)
    if ip is not None and refused_address(ip):
        raise PushRejected("endpoint must not point at this server, loopback or a "
                           "link-local, multicast or reserved address")
    if ip is None and (host == "localhost" or host.endswith(".localhost")):
        raise PushRejected("endpoint must not point at this server")
    if parts.scheme == "http" and not (ip is not None and not ip.is_global
                                       or ip is None and _local_name(host)):
        raise PushRejected("plain http:// is allowed only for a private address or a "
                           "local name; use https://")
    return text


def check_kinds(value: Any) -> tuple[str, ...] | None:
    """`kinds` as registered: None (every kind the server sends) or a tuple.

    Kinds this server does not know are dropped, not refused, so a newer app
    may name kinds an older server has not got yet. The result is in the
    server's own order, without repeats.

    Raises:
        PushRejected: not null and not a list of strings, or too long.
    """
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(k, str) for k in value):
        raise PushRejected("kinds must be a list of strings, or null for every kind")
    if len(value) > MAX_KINDS:
        raise PushRejected(f"kinds may list at most {MAX_KINDS} entries")
    wanted = set(value)
    return tuple(k for k in KINDS if k in wanted)


@dataclass(frozen=True)
class PushRegistration:
    """One session's endpoint. `kinds` None follows the server's own switches."""

    endpoint: str
    kinds: tuple[str, ...] | None
    registered: float

    @property
    def host(self) -> str:
        return endpoint_host(self.endpoint)

    def allows(self, kind: str) -> bool:
        return self.kinds is None or kind in self.kinds

    def public(self) -> dict[str, Any]:
        """What the app and the panel see: the host, never the endpoint."""
        return {"host": self.host,
                "kinds": list(self.kinds) if self.kinds is not None else None,
                "registered": self.registered}

    def to_json(self) -> dict[str, Any]:
        return {"endpoint": self.endpoint,
                "kinds": list(self.kinds) if self.kinds is not None else None,
                "registered": self.registered}

    @classmethod
    def from_json(cls, raw: Any) -> PushRegistration | None:
        """A stored registration, or None for anything no longer acceptable."""
        if not isinstance(raw, dict):
            return None
        try:
            endpoint = check_endpoint(raw.get("endpoint"))
            kinds = check_kinds(raw.get("kinds"))
        except PushRejected as e:
            # Never the endpoint itself: it is a capability URL.
            log.warning("app API: dropping a stored push registration (%s)", e)
            return None
        return cls(endpoint, kinds, to_float(raw.get("registered"), 0.0) or 0.0)
