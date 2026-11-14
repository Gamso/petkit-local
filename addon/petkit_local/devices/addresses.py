"""How far a device's recorded LAN address can be believed.

`device.state["ip"]` is whatever the last report in the device's name said it
was, and the device API takes no credential: any host on the LAN can post a
state report as device N carrying `"Ip": "<its own address>"`. For a stream URL
that is a nuisance. For the patchers it is the whole game — they download a
binary from that address, patch it, and tell the real device to install it.

Nothing here can authenticate the device; there is no secret to do it with.
What an impostor cannot do, short of knocking the real device off the network,
is stop it talking. It goes on reporting its own address and connecting from
it, so a forged address shows up as a DISAGREEMENT, and that is what is kept
here: every address recently named for a device, by which kind of evidence.

Two kinds, never mixed, because behind NAT (a bridged container, Docker
Desktop) they legitimately differ for good:

* `CLAIMED` — an address a report carried in its body, over HTTP or MQTT.
* `PEER` — the address an HTTP request in the device's name arrived from. The
  real one on a host-networked or port-mapped add-on, the gateway behind NAT.

Live state only, like the rest of `Device`'s liveness fields.
"""
from __future__ import annotations

import ipaddress
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from petkit_local.devices.base import Device

CLAIMED = "claimed"
PEER = "peer"

#: How long an address counts as current. A device reports every ~30s on either
#: transport, so twenty missed reports is "gone"; and a real change of lease
#: settles by itself this long after the old address was last heard of.
SETTLE_SECONDS = 600.0

#: Addresses remembered per kind. The input is unauthenticated, so the table is
#: bounded; two is already a disagreement, and the rest is for the message.
MAX_TRACKED = 8


def as_ipv4(value: Any) -> str:
    """`value` as a dotted quad, or `""` if it is not one IPv4 address.

    An IPv4-mapped IPv6 peer (`::ffff:192.0.2.5`, what a dual-stack listener
    reports) is unwrapped, so it compares equal to the address the device
    claims. Never raises: both callers pass untrusted input.
    """
    if not isinstance(value, str) or not value:
        return ""
    try:
        address = ipaddress.ip_address(value.strip())
    except ValueError:
        return ""
    if isinstance(address, ipaddress.IPv6Address):
        address = address.ipv4_mapped
    return str(address) if address is not None else ""


def note(device: Device, kind: str, address: Any, now: float | None = None) -> None:
    """Record that `address` was just named for `device`. Ignores a non-address."""
    ip = as_ipv4(address)
    if not ip:
        return
    seen = device.addresses_seen.setdefault(kind, {})
    seen.pop(ip, None)  # re-inserted last, so the oldest is always first
    seen[ip] = time.time() if now is None else now
    while len(seen) > MAX_TRACKED:
        del seen[next(iter(seen))]


def recent(device: Device, kind: str, now: float | None = None) -> list[str]:
    """The addresses of one kind named within `SETTLE_SECONDS`, oldest first."""
    cutoff = (time.time() if now is None else now) - SETTLE_SECONDS
    return [ip for ip, at in device.addresses_seen.get(kind, {}).items() if at >= cutoff]


def distrust(device: Device, now: float | None = None) -> str:
    """Why `device.state["ip"]` should not be fetched from, or `""` if it may be.

    Three things are refused: an address that is not one; two addresses of the
    same kind inside the window (somebody else is speaking in this device's
    name, or its lease just changed — indistinguishable, and waiting settles
    the second); and an address nothing recent backs, which is what a single
    forged report leaves behind on a device that does not report its own.

    `""` is not proof. See the module docstring for what this cannot see.
    """
    recorded = device.state.get("ip", "")
    ip = as_ipv4(recorded)
    if not ip:
        return f"the address on record ({str(recorded)[:40]!r}) is not an IPv4 address"

    minutes = int(SETTLE_SECONDS // 60)
    claimed = recent(device, CLAIMED, now)
    peers = recent(device, PEER, now)
    if len(claimed) > 1:
        return (f"reports in this device's name have given more than one address in the "
                f"last {minutes} minutes ({', '.join(claimed)})")
    if len(peers) > 1:
        return (f"requests in this device's name have come from more than one address in "
                f"the last {minutes} minutes ({', '.join(peers)})")
    if ip not in claimed and ip not in peers:
        return f"nothing from this device has confirmed {ip} in the last {minutes} minutes"
    return ""
