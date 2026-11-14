"""Who is asking, and how long they must wait after getting the password wrong.

Two things a login endpoint on the open internet needs and nothing else here
does.

`client_address` decides which address a request is charged to. Behind a
reverse proxy every request arrives from the proxy, so the real address has to
come from a header — and a header is whatever the sender wrote. So
`X-Forwarded-For` and `CF-Connecting-IP` are read ONLY when the TCP peer is one
of the operator's trusted proxies (set in the panel; empty by default, which
trusts nobody). Anyone else who sends them is charged to their own address,
exactly as if they had not.

`LoginThrottle` makes a wrong password expensive. Each failure locks the
address out for twice as long as the one before (1 s, 2 s, 4 s ... capped at
15 min), and a success clears it. A second, global ceiling bounds failures
across ALL addresses, because a list of addresses is cheap to come by and each
attempt costs us a scrypt hash. Both are in memory and bounded: a restart
forgets them, which costs an attacker nothing they could not get by waiting.
"""
from __future__ import annotations

import ipaddress
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from aiohttp import web

IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network
IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

#: First lockout, in seconds. Doubles with every further failure.
BASE_DELAY = 1.0
#: The longest one address is ever made to wait.
MAX_DELAY = 15 * 60.0
#: A failure older than this no longer counts towards the next lockout.
FORGET_AFTER = 24 * 3600.0
#: Addresses remembered at once; the least recently failed is dropped first.
MAX_TRACKED = 4096
#: Failures from everywhere, within `GLOBAL_WINDOW`, before every login waits.
GLOBAL_MAX_FAILURES = 100
GLOBAL_WINDOW = 10 * 60.0


def parse_ip(value: str | None) -> IPAddress | None:
    """`value` as an address, IPv4-mapped IPv6 unwrapped; None if it is not one."""
    if not value:
        return None
    try:
        ip = ipaddress.ip_address(value.strip())
    except ValueError:
        return None
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    return ip


def parse_networks(entries: Iterable[str]) -> list[IPNetwork]:
    """Each entry as a network; a bare address is a /32 or /128.

    Raises:
        ValueError: an entry is neither an address nor a CIDR. The message
            names it, so the panel can say which one.
    """
    networks: list[IPNetwork] = []
    for entry in entries:
        text = str(entry).strip()
        if not text:
            continue
        try:
            networks.append(ipaddress.ip_network(text, strict=False))
        except ValueError:
            raise ValueError(f"not an address or CIDR: {text!r}") from None
    return networks


def _trusted(ip: IPAddress | None, networks: Iterable[IPNetwork]) -> bool:
    return ip is not None and any(ip in net for net in networks)


def client_address(request: web.Request, trusted: list[IPNetwork]) -> str:
    """The address a request is charged to, as a string.

    `request.remote` unless that peer is a trusted proxy. Then
    `CF-Connecting-IP` if it holds one valid address — Cloudflare sets it to
    the visitor and overwrites any value the visitor sent — and otherwise the
    right-most `X-Forwarded-For` hop that is not itself a trusted proxy, which
    is the first address a trusted hop vouched for. A malformed hop stops the
    walk there: what lies to the left of garbage was never vouched for.

    Never raises; an unparseable peer is returned verbatim (aiohttp gives None
    for a Unix socket, which becomes "").
    """
    remote = request.remote or ""
    if not trusted or not _trusted(parse_ip(remote), trusted):
        return remote

    cf = parse_ip(request.headers.get("CF-Connecting-IP"))
    if cf is not None:
        return str(cf)

    hops = [h for header in request.headers.getall("X-Forwarded-For", [])
            for h in header.split(",")]
    for hop in reversed(hops):
        ip = parse_ip(hop)
        if ip is None:
            break
        if not _trusted(ip, trusted):
            return str(ip)
    return remote


@dataclass
class _Record:
    failures: int = 0
    locked_until: float = 0.0
    last_failure: float = 0.0


class LoginThrottle:
    """Per-address exponential lockout plus a global failure ceiling.

    `retry_after` is asked BEFORE the password is even hashed, so a locked-out
    address costs us a dictionary lookup and nothing else.
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic,
                 base_delay: float = BASE_DELAY, max_delay: float = MAX_DELAY,
                 forget_after: float = FORGET_AFTER, max_tracked: int = MAX_TRACKED,
                 global_max_failures: int = GLOBAL_MAX_FAILURES,
                 global_window: float = GLOBAL_WINDOW) -> None:
        self._clock = clock
        self._base = base_delay
        self._max = max_delay
        self._forget = forget_after
        self._max_tracked = max_tracked
        self._global_window = global_window
        self._records: OrderedDict[str, _Record] = OrderedDict()
        # Bounded by construction: only the newest `global_max_failures` matter.
        self._recent: deque[float] = deque(maxlen=global_max_failures)

    def __len__(self) -> int:
        return len(self._records)

    def retry_after(self, address: str) -> float:
        """Seconds `address` must still wait before a login is even tried; 0 if none."""
        now = self._clock()
        waits = [0.0]
        if len(self._recent) == self._recent.maxlen and self._recent:
            # The ceiling is reached until its oldest failure leaves the window.
            waits.append(self._recent[0] + self._global_window - now)
        rec = self._records.get(address)
        if rec is not None:
            waits.append(rec.locked_until - now)
        return max(waits)

    def failure(self, address: str) -> float:
        """Record a wrong password from `address`; returns the lockout it earned."""
        now = self._clock()
        self._recent.append(now)
        rec = self._records.pop(address, None)
        if rec is None or now - rec.last_failure > self._forget:
            rec = _Record()
        rec.failures += 1
        rec.last_failure = now
        delay = min(self._max, self._base * 2 ** min(rec.failures - 1, 32))
        rec.locked_until = now + delay
        self._records[address] = rec  # re-inserted last: the newest failure
        while len(self._records) > self._max_tracked:
            self._records.popitem(last=False)
        return delay

    def success(self, address: str) -> None:
        """A right password clears that address's record (not the global one)."""
        self._records.pop(address, None)
