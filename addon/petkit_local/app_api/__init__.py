"""The phone app's API: an authenticated, versioned facade on its own port.

A standalone Android app talks to petkit-local through this and nothing else,
from outside the LAN, through the owner's reverse proxy or tunnel. So unlike
the panel (Ingress or LAN only) and the device API (the firmware cannot
authenticate), every request here is authenticated by petkit-local itself.

    server.py       the application, the route table (`ROUTES`), the port's only content
    middleware.py   the access policy: no password -> 401 everywhere; bearer sessions
    auth.py         the one account: scrypt password, hashed session tokens,
                    trusted proxies, persisted to `{data_dir}/app_api.json`
    throttle.py     failed-login lockouts, and which client address a request is charged to
    serializers.py  every JSON shape returned, explicit, so the panel can change freely
    handlers/       one module per area: meta, auth, devices, timeline, media, pets
    openapi.yaml    the contract the app's client is generated from (tested against ROUTES)

Layering: this package reads the same stores, registries and helpers the panel
reads (`events/`, `ai/pets.py`, `ha/categories.py`, `ha/state_doc.py`,
`media/serving.py`) and imports nothing from `web/` or `http/`. Only `main/`
builds it, and the panel reaches `AppAuth` only through the object
`main/lifecycle.py` hands it.
"""
