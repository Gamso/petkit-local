# Security

## Reporting a vulnerability

Open a [GitHub security advisory](https://github.com/alex-so-3/petkit-local/security/advisories/new),
or a normal issue if the problem is not sensitive. There is no formal SLA — this
is a hobby project — but security reports are read first.

## What this app is, in security terms

It impersonates a cloud service to hardware on your LAN, and it can modify that
hardware. That is the point of it, and it means the threat model is unusual
enough to be worth stating plainly.

### Things that are unauthenticated by design

| Surface | Why |
|---|---|
| Device API (host `80`) | The firmware has no credential to offer beyond an `X-Device` header it also computes itself. Anything on the LAN can talk to it. |
| Media bucket (host `9000`) | It stands in for Aliyun OSS, whose credentials this app issues to the device. Object keys are containment-checked, so an upload cannot escape the media root — but anyone on the LAN can upload. |
| Web panel (container `8099`) | Unmapped by default and reached through Home Assistant Ingress, which authenticates. **Map it to a host port and the whole API — device settings, commands, pet records, the patchers — is available with no login.** |

The panel does refuse one thing: a write, or a WebSocket, that a page from
another site made your browser send (`web/origin.py` compares `Origin` with the
host the request was addressed to, and JSON bodies must be sent as
`application/json`). That stops a web page you happen to visit from driving a
panel only you can reach. It is not a login: anything that can reach the port
directly — `curl`, a script, another machine on the LAN — is still unrestricted,
and it does not stop DNS rebinding.

Web Bluetooth provisioning needs a secure context. Serving the panel a second
time over HTTPS with a self-signed certificate would have given it one, and was
rejected: it means a second unauthenticated surface, on a port nobody expects,
presenting a certificate nobody can verify. Provisioning asks the operator for a
real certificate in front of the ordinary panel port instead. (Confirmed on
hardware: it works through Ingress when Home Assistant itself is served over a
valid certificate.)

### The one surface meant for the internet

| Surface | How it is protected |
|---|---|
| Phone app API (container `8097`, unmapped by default) | Its own password, set only from the panel, stored as a salted `scrypt` hash. Every route answers 401 until one is set; afterwards every route but `/app/v1/info` and `/app/v1/openapi.yaml` needs a per-phone bearer token, of which only a SHA-256 hash is stored. Tokens end on logout, revocation from the panel, any password change, or 90 days unused. Failed logins lock the client address out for exponentially longer (up to 15 min), with a global ceiling; `X-Forwarded-For` / `CF-Connecting-IP` count only from proxies the owner listed as trusted. Only `/app/v1/...` is routed there — no panel route, no device route, no admin feature (patchers, firmware, capture, proxy settings, BLE). |

What it does not do, so you can judge it:

- **It does not terminate TLS.** It speaks plain HTTP and expects your reverse
  proxy or tunnel to provide HTTPS. Reached over plain HTTP from outside, the
  password and the tokens travel in the clear.
- **One password, no accounts and no second factor.** Whoever has the password
  sees everything the app sees. Choose a long one; the lockout slows guessing,
  it does not make a weak password strong.
- **The lockout is in memory**, so a restart forgets it, and it is per address:
  an attacker with many addresses gets each its own allowance, until the global
  ceiling (100 failures in 10 minutes) makes every login wait.
- **The read-only API still reveals a lot**: device state, the visit history,
  photos and video from inside your home. Treat the password accordingly.
- **The panel remains the recovery path and is not authenticated.** Anything
  that can reach the panel can reset the app password. That is why the panel
  must never be exposed the way the app API is.

### What it calls out to

Nothing in petkit-local reaches the internet on a schedule of its own. Beyond
Home Assistant's broker, the outbound calls — all off until you configure or
press something — are:

| Call | When | What it carries |
|---|---|---|
| PetKit's cloud (`http/cloud_fetch.py`, the firmware fetch) | only when a panel button asks it to (an accessory secret, a pet's photos, an ESP32 firmware image) | a request signed as the device |
| Proxy mode's upstream (`http/proxy.py`, `mqtt/upstream.py`) | while proxy mode is on, relaying the device's own traffic | the device's traffic, redacted on the way back |
| Cloudflare's TURN API (`media/turn.py`) | when a live view is opened and a TURN key is configured | a request for short-lived relay credentials |
| Your ntfy server (`app_api/notify/`) | while phone notifications are on, once per notification (bounded per device and kind) | `POST {server}/{topic}` with `Authorization: Bearer <token>`, a title, a priority, tags and one sentence: a device's product name, a pet's name, what happened. No device id, no media, no link, no credential of anything else. TLS is verified and redirects are not followed, so the token goes to the configured host or nowhere. |

The ntfy access token and the topic are secrets — on ntfy the topic name is the
subscription. Both live in `/data/ntfy.json` (0600), never in the add-on
options; neither is written to the log, and the panel shows the token only by
its last four characters. Give the token a user that may only **write** that one
topic, and the phone a separate user that may only read it.

### Things that are deliberately permissive

- **`mqtt_strict_auth` defaults to `false`**, so the device-facing MQTT broker
  accepts any CONNECT. A signature nuance silently locking a device out is a
  worse failure than an open broker on a LAN that already exposes the device
  API. Turn it on if your network is shared.
- **Proxy mode does not verify the upstream MQTT broker's TLS certificate**
  (`mqtt/upstream.py`). The Aliyun IoT endpoint chains to a private root that is
  in no public store, is never sent, and has no AIA to fetch it from — the
  device itself is in the same position. So the connection is encrypted but not
  authenticated, and anything on the path could pose as the cloud. The guards
  that strip firmware pushes and shell commands out of a cloud reply still
  apply, and do not rest on server identity. The HTTP side is verified
  (`http/proxy.py`), with one exception: an `https://` upstream entered as an
  IP address has no name to check the certificate against, so it is reached
  unverified and a warning says so.
- **The patchers modify firmware on a rooted device.** They verify the binary is
  a MIPS executable and check free space first, but a patched device is outside
  its warranty and outside PetKit's update path.

### Captures contain secrets

Traffic capture writes verbatim payloads — nothing in a capture is filtered,
because it is only useful if it is exact. Any capture can contain your Wi-Fi
SSID, your LAN topology and the device's signing secret, and one taken in proxy
mode also carries the PetKit account credentials their cloud issues. **Read a
capture before attaching it to an issue**, and attach only what the question
needs; `requests.jsonl`, `state_report.jsonl` and `mqtt.jsonl` usually answer it
on their own.

### Supported versions

The latest release only. This is a single-maintainer project with no backport
branch.
