"""Refuse a panel write that another web site made the browser send.

The panel has no login by design (SECURITY.md): Home Assistant Ingress
authenticates in front of it, and a mapped 8099 is an open debug port. That
makes "who can reach the port" the whole access rule — and a browser breaks it,
because a page from anywhere can make the browser of somebody who CAN reach the
port send a request there. A blind `text/plain` POST to `api/settings` needs no
CORS answer to take effect, and a WebSocket is not covered by the same-origin
policy at all, so `api/ws` would stream the live log to any page that asked.

A browser cannot be stopped from sending those, but it always says where the
page came from, and page script cannot change that header. So this is a check
of `Origin` against the host the request was addressed to — not authentication,
and nothing a script or `curl` has to care about: no `Origin`, no check.

PANEL ONLY. The device API must never answer a device 4xx (AGENTS.md), and no
firmware sends `Origin` anyway.
"""
from __future__ import annotations

import json
import logging
from urllib.parse import urlsplit

from aiohttp import web

log = logging.getLogger(__name__)

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_DEFAULT_PORTS = {"http": ":80", "https": ":443", "ws": ":80", "wss": ":443"}


def _authority(value: str, scheme: str) -> str:
    """`host[:port]` lower-cased, with the scheme's default port dropped.

    `Origin` never carries a default port and a proxy's `Host` sometimes does,
    so the two are only comparable once it is gone from both.
    """
    value = value.strip().lower()
    default = _DEFAULT_PORTS.get(scheme, "")
    if default and value.endswith(default):
        value = value[: -len(default)]
    return value


def _addressed_hosts(request: web.Request, scheme: str) -> set[str]:
    """Every name the browser may have addressed this request to.

    `Host` is the answer on a direct `http://<lan-ip>:8099`. Behind Ingress, or
    the operator's own TLS proxy, the request reaches us from a proxy that may
    have rewritten `Host` to its upstream; Home Assistant records the original
    in `X-Forwarded-Host`, which is what the browser's `Origin` matches there.

    Trusting a forwarded header is safe HERE because of who the check is
    against: a cross-site page cannot add a custom header without a CORS
    preflight, which this app never grants. Anything that can set it freely is
    not a browser and could simply omit `Origin`.
    """
    hosts = {_authority(request.headers.get("Host", ""), scheme)}
    for value in request.headers.getall("X-Forwarded-Host", ()):
        hosts.update(_authority(part, scheme) for part in value.split(","))
    hosts.discard("")
    return hosts


def is_cross_site(request: web.Request) -> bool:
    """True when a browser says this request was made by another site's page."""
    # Set by the browser itself and unforgeable from page script (`Sec-` names
    # are forbidden to it). Only sent in a secure context, so it settles the
    # HTTPS installs and says nothing on a plain `http://<lan-ip>:8099`. It is
    # also what keeps a TLS proxy working that rewrites `Host` and forwards no
    # `X-Forwarded-Host`: the browser has already vouched for the page.
    fetch_site = request.headers.get("Sec-Fetch-Site", "").lower()
    if fetch_site == "same-origin":
        return False
    if fetch_site == "cross-site":
        return True

    origin = request.headers.get("Origin")
    if origin is None:
        # curl, a script, Home Assistant's own REST calls. Every current
        # browser sends Origin on a cross-origin write and on every WebSocket.
        return False
    parts = urlsplit(origin.strip())
    if not parts.scheme or not parts.netloc:
        # "null" — a sandboxed frame, a `data:` page, a redirect chain. Never
        # the panel itself, which is always loaded from a real http(s) origin.
        return True
    scheme = parts.scheme.lower()
    return _authority(parts.netloc, scheme) not in _addressed_hosts(request, scheme)


@web.middleware
async def same_origin_writes(request: web.Request, handler):
    """Answer 403 to a cross-site write or WebSocket upgrade.

    Reads are left alone: the same-origin policy already keeps another site
    from seeing a GET's answer, and no GET here changes anything. A WebSocket
    upgrade IS a GET, and is the one read that policy does not protect.
    """
    upgrade = request.headers.get("Upgrade", "").lower() == "websocket"
    if (request.method not in SAFE_METHODS or upgrade) and is_cross_site(request):
        log.warning("panel: refused cross-site %s %s from origin %r (host %r)",
                    request.method, request.path,
                    request.headers.get("Origin"), request.headers.get("Host"))
        return web.Response(status=403, content_type="application/json",
                            text=json.dumps({"error": "cross-site request refused"}))
    return await handler(request)
