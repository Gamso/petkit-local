# PetKit Local

Local cloud replacement for PetKit devices, packaged as a Home Assistant app.
Your PetKit litter boxes, feeders, fountains and Bluetooth accessories talk to this app
instead of PetKit's servers; entities appear in Home Assistant via MQTT
discovery. Nothing leaves your network.

```
PetKit device ──HTTP───► petkit-local ──MQTT discovery──► Home Assistant
              ──MQTT──►  embedded broker ──bridge──►
              ──HTTPS─►  media bucket ──decrypt/remux──► /media/petkit
```

## Requirements

- A broker for the **MQTT** integration, so entities can be published. With the
  Mosquitto app the credentials come from the Supervisor and there is nothing
  to configure. With any other broker — one on your LAN, or in another
  container — **set `ha_mqtt_host` in Options**, plus `ha_mqtt_user` and
  `ha_mqtt_pass` if it needs them. Nothing warns you if you skip this: the
  device side works, the panel works, and Home Assistant simply never sees a
  single entity.
- A way to point the device at this app. BLE provisioning works on every
  model; ESP32 models can also be redirected by DNS. See the repository README.

## Installation

1. **Settings → Apps → Install app → ⋮ → Repositories**, add this repository's URL,
   then install **PetKit Local**. (Older Home Assistant releases call the same
   place *Settings → Add-ons → Add-on Store*.)
2. Configure the options (below) and start it.
3. Redirect your device to the app, then power-cycle it. It registers itself
   and its entities show up in Home Assistant.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `api_url` | *(empty = auto)* | URL the device is told to use for the HTTP API. **Leave empty to auto-detect the HA host's LAN IP** (recommended — embedded PetKit devices can't resolve mDNS `.local`). Override only for a specific address/port, e.g. `http://<ha-host-ip>/6/`. A `.local` value is ignored in favour of the detected IP. |
| `bucket_endpoint` | *(empty)* | Where devices upload photos and video. Empty derives it from the host IP and the port `9000/tcp` is published on. Set it when uploads must go through a different hostname, port or TLS-terminating proxy. |
| `ha_mqtt_host` | *(empty)* | **Your Home Assistant MQTT broker**, so entities can be published to HA. Auto-detected if you run the Mosquitto app; otherwise set it (an external broker's hostname or IP). Leave empty to skip HA publishing entirely — the device side still works. |
| `ha_mqtt_port` | `1883` | HA MQTT broker port. |
| `ha_mqtt_user` / `ha_mqtt_pass` | *(empty)* | HA MQTT broker credentials (leave empty for anonymous). |
| `log_level` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR`. |
| `offline_timeout` | `180` | Seconds without contact before a device is shown unavailable. |
| `mqtt_tls` | `true` | Add a TLS listener to the device-facing broker (self-signed, generated on first start). |
| `mqtt_tls_port` | `443` | Port for that listener. Change this and the matching port mapping together if your device dials a different one. |
| `mqtt_strict_auth` | `false` | Enforce the Aliyun HMAC signature, and refuse any client that is not a known device. Off by default so a signature nuance cannot lock a device out. |
| `esp32_http_only` | `true` | Keep the T3, T4 and D4 on the HTTP heartbeat. They cannot use this broker unless reflashed, and need one DNS record for this — see *Networking*. Turn it off only for a reflashed device whose own broker name is pointed here. |
| `go2rtc_api_password` | *(empty)* | Publishes the bundled go2rtc's HTTP API on `1984/tcp` (map that port too) behind basic auth, user `petkit`, for the **WebRTC Camera** integration (HACS). Empty keeps the API inside the container — see [Live view and talk over WebRTC](#live-view-and-talk-over-webrtc) for what it opens. |
| `go2rtc_webrtc_candidates` | `[]` | WebRTC addresses announced after the LAN one, for viewers outside your LAN — see [WebRTC from outside the LAN](#webrtc-from-outside-the-lan). |

Fresh Element Gemini D4S firmware 1.198 was tested with the default
`mqtt_tls: true`. It uses HTTP heartbeats for state and commands; no device MQTT
session was observed. A one-portion command to hopper 1 dispensed food, but
the completion event reported zero in both `real_amount` fields. Treat reported
dispense quantities as unverified on this model. Disabling `mqtt_tls` caused a
signup loop after one restart, so leave it enabled.

Payload capture and proxy mode are **not** options here. Both are things you turn on
while watching a device, so they live in the panel's **Setup → Settings**, take
effect immediately, and persist to `/data/settings_overrides.json`.

## Ports

| Container port | Default host port | Purpose |
|------|------|---------|
| `80` | `80` | Device HTTP API. ESP32 models want it here: a T4 provisioned with `:8080` reaches Wi-Fi and then fails to connect to the server, and one redirected by DNS dials 80 with nowhere to tell it otherwise. |
| `443` | `443` | Device MQTT over TLS. |
| `1883` | *(unmapped)* | Plain MQTT listener, internal to this app. Map it only if a device connects in plaintext. |
| `9000` | `9000` | Media upload bucket the device PUTs photos and videos to. |
| `8099` | *(unmapped)* | The web panel over plain HTTP, **no authentication**. Ingress proxies this internally. |
| `8097` | *(unmapped)* | The phone app API (`/app/v1` only). **Authenticated by its own password**, and closed until one is set. Map it for your reverse proxy or tunnel — see [The phone app](#the-phone-app). |
| `8554` | *(unmapped)* | Camera RTSP from the bundled go2rtc. Home Assistant reaches it over the internal network, so map it only to watch the stream from elsewhere on the LAN — and note 8554 clashes with the go2rtc app. |
| `8555` tcp + udp | *(unmapped)* | WebRTC media from the bundled go2rtc. Unmapped, WebRTC is off and go2rtc does not listen on it. Map both to any free host port to turn it on (8555 itself is taken on a host that runs Frigate). No authentication of its own: a viewer needs the signalling first, which only the panel and the go2rtc API hand out. |
| `1984` | *(unmapped)* | go2rtc's HTTP API. Served on this port only when it is mapped **and** `go2rtc_api_password` is set; otherwise it stays on loopback inside the container whatever is mapped. Basic auth over plain HTTP. |

Remapping a port is honoured where it can be: the host port `80/tcp` is
published on goes into the address devices are handed, and the same for the
bucket on `9000/tcp`. Two cannot follow. A device redirected by DNS dials port
80 with nothing to tell it otherwise, and the MQTT port comes from the
firmware's own build — so leave `80` and `443` alone unless you know which of
your devices is provisioned with what.

**MQTT port coexistence:** the device-facing broker is a *separate* broker from
Home Assistant's. Its plain listener is unmapped by default, so it cannot clash
with the Mosquitto app.

**The panel is unauthenticated on port 8099.** The sidebar entry goes through
Ingress and is authenticated by Home Assistant; port 8099 is the same
application without that check, which is why it is unmapped by default. Map it
only if you want to `curl` the JSON API while debugging.

Web Bluetooth provisioning needs a secure context. This app deliberately does
not manufacture one for you with a second, self-signed HTTPS port — that would
publish the whole API to the LAN again. Provisioning needs Home Assistant served
over HTTPS with a certificate your browser trusts. A localhost proxy on the
computer with Bluetooth also counts as a secure context. The Provision tab
explains this when Home Assistant is served over plain HTTP.

Port 9000 likewise accepts uploads without authentication — it stands in for
Aliyun OSS, whose credentials this app issues to the device itself.

## Networking

The device must resolve `api_url` to this app.

**BLE provisioning works on every model** and is the most reliable route — see
below.

**ESP32 models** (T3/T4/D3/D4/D4S, Feeder, Feeder Mini) can also be redirected
by DNS, because they talk plain HTTP. The BLE-only EverSweet fountains are not on that
list and never will be: they have no network of their own, so there is no name
to redirect (see the README's supported-devices table). The EverSweet Ultra AI
does have Wi-Fi, and is a Linux model — see below. There is no one name to point here: like the Linux
models, they are given their API server during Bluetooth setup, so it is
whichever of PetKit's regional servers the app handed that device. Find it in
your DNS server's query log, or redirect every PetKit domain. The device dials
**port 80** and cannot be told otherwise, so the API has to be reachable there.

Redirecting a name redirects it for this app too, so **proxy mode then
reaches itself instead of the cloud**. It notices and stops forwarding rather
than serving you its own reply labelled as PetKit's. To use proxy mode on such a
network, set *DNS for upstream lookups* in Setup to a resolver that still answers
truthfully — that setting is used for finding the upstream server and nothing
else.

**The T3, T4 and D4 stay on HTTP, and need one DNS record for it.** Their
firmware ignores the broker address it is given and builds one from its MQTT
product key, `<productKey>.iot-as-mqtt.eu-central-1.aliyuncs.com`, and it
trusts no certificate but the one compiled into it. So this app cannot be
their broker. With `esp32_http_only` on (the default) they are handed the key
`petkit-local-http`; point its name at an address that refuses the
connection:

```
petkit-local-http.iot-as-mqtt.eu-central-1.aliyuncs.com  →  127.0.0.1
```

With that refused, the device retries for about a minute, then polls the HTTP
heartbeat for good, and commands reach it there within one poll (~10 s).
**Without the record the name still resolves**, because Aliyun answers every
name under that domain, and the device tries to log in to PetKit's real
broker instead. This applies whether the device was redirected by DNS or
provisioned over Bluetooth.

**To put them on MQTT instead, patch the firmware** (Patchers tab — see
*Patching a T3, T4 or D4 firmware* above). A patched device trusts a
certificate this app holds, so it no longer needs the refuse trick: every
patched device is handed the key `petkit-local-mqtt`, and you point that one
name at the HA host rather than at `127.0.0.1`:

```
petkit-local-mqtt.iot-as-mqtt.eu-central-1.aliyuncs.com  →  <HA host>
```

The broker then answers to it, and the device switches to MQTT. The two
records are all the DNS any number of these devices need. HTTP stays the
bootstrap and the fallback either way, so the HTTP record above is what every
unpatched device needs and is the right default.

**Linux models** (T5/T6/T7/D4H/D4SH/W7H) enforce HTTPS, so a DNS override alone
will not do; use BLE provisioning, which sidesteps HTTPS entirely. They also
compile the cloud's CA into `ctrl` and so will not trust any other MQTT broker
until it is patched — the panel's **Patchers** tab does that.

Those models are not all the same CPU: W7H is ARM, the rest are Ingenic MIPS.
The patchers that only move files — CA Certificate and Local Camera Streaming —
work on either. MQTT TLS Bypass and Local Storage rewrite machine code, and
Persistent SSH installs a prebuilt binary, so those three are MIPS-only for now
and the tab says so per patch rather than hiding itself.

## Patching a T3, T4 or D4 firmware

These models trust exactly one certificate for MQTT, compiled into their
firmware, so no broker but PetKit's can serve them. The **Patchers** tab has a
card for each one that rebuilds the firmware with that certificate swapped
for one this app holds the key to — PetKit's own image, otherwise byte for
byte — and flashes it over the air, the way the device takes any update.

1. **Fetch from PetKit.** Asks PetKit, signed as the device, for its model's
   current firmware, and keeps both PetKit's image and the patched one. It
   needs the signing secret PetKit knows for this device: one it learned
   through proxy mode, or one imported with `dev_signup`. A device whose
   secret was issued here is refused (`704`). If your DNS points PetKit's
   names at this app, set *DNS for upstream lookups* in Setup first.
2. **Flash patched.** The device is told on its next heartbeat, or takes it
   the next time it boots. Keep it powered until the card's log says done.
3. **Roll back to stock** flashes PetKit's image back the same way. A patched
   device is on MQTT and no longer polls the heartbeat, so arming a job closes
   its MQTT session and the broker refuses it until the job ends, so it falls
   back to the heartbeat and is told there. If the card still says it is
   waiting, power-cycle the device.

A flash writes the device's spare firmware partition and switches to it. The
partition it was running from keeps its old firmware, and the device falls
back to that if the new one ever fails to start, so one flash is enough.

Once it is patched, the device is handed the broker name every patched device
shares — `petkit-local-mqtt.iot-as-mqtt.eu-central-1.aliyuncs.com`, shown on
the firmware card — and it then connects over MQTT instead of polling the HTTP
heartbeat, so a command reaches it in well under a second. For that to work the
name has to resolve to this add-on: add one DNS record pointing it at the HA
host, the same kind of record the HTTP-only mode needs, once for all your
patched devices. The card says whether it already resolves here. The broker
answers to that name with a certificate the patched firmware trusts; no device
option changes, and nothing happens for a device you have not flashed. Rolling
back to stock hands it the placeholder key again and it returns to HTTP.

A device patched with version 2.2.1 or earlier was handed a name of its own,
`<productKey>.iot-as-mqtt.eu-central-1.aliyuncs.com`, and keeps dialling it
until it next asks for its credentials, which it does when it boots. Keep that
record until the card shows it connected under the shared name, then remove it.

This has not yet been flashed onto a device by this app. It is done at your
own risk, like every patcher.

## Importing a device's configuration

A device moved here keeps its settings and schedules only if this app already
has them when it first asks. A litter box reads its whole settings block at boot
and adopts what it is served, so an unknown device gets this app's defaults; a
feeder that fetches an empty schedule stops feeding. **Import first, then
redirect the device.**

**Setup → Import configuration** takes a JSON file of PetKit's own replies to
each device, keyed by the endpoint that produced them, with or without their
`{"result": ...}` wrapper — what a capture of the device talking to PetKit
holds:

```json
{"devices": [
  {"type": "t4",
   "dev_signup":         {"result": {"id": 300000001, "sn": "...", "mac": "...", "secret": "..."}},
   "dev_device_info":    {"result": {"id": 300000001, "settings": {"...": "..."}}},
   "dev_schedule_get":   {"result": [{"id": 1, "time": 480, "type": 0, "repeats": "1,2,3,4,5,6,7"}]},
   "dev_multi_config":   {"result": {"lightMultiRange": "{\"lightMultiRange\":[[0,1440]]}"}},
   "dev_k3_device_info": {"result": {"k3Config": {"config": {}}, "k3Device": {"id": 2, "mac": "...", "secret": "..."}}}},
  {"type": "d4",
   "dev_signup":   {"result": {"id": 300000002, "settings": {"factor": 10}}},
   "dev_feed_get": {"result": {"schedule": [{"re": "1,2,3,4,5,6,7", "it": [{"id": "n_23400", "t": 23400, "a": 10}]}]}}}
]}
```

Any reply may be left out. The device is created if it has not registered yet,
and an existing one adopts the imported values. `dev_signup`'s `secret` is kept
too, so the device is handed back the signing secret it already has. A Pura
Air in `dev_k3_device_info` (or `dev_device_info`'s `k3Device`) is paired to
its parent. The same import is `POST /api/import` on the panel's port.

## The camera in Home Assistant

Apply **Local Camera Streaming** and the bundled go2rtc republishes the
feed as RTSP. The address appears on that patcher's card in the panel and as the
`Camera Stream URL` sensor — copy it from one of those rather than typing it,
because the hostname depends on how this was installed. Paste it into a
**Generic Camera** in Home Assistant.

Two things worth knowing:

- **Do not give Home Assistant the device's own `http://…/main.flv` address.**
  HA opens a stream URL with PyAV, and that one segfaults libav and restarts the
  whole of Home Assistant. The device addresses are listed for VLC and your own
  tooling; go2rtc is there to stand between HA and the device.
- **The device is only streamed from while somebody is watching.** go2rtc dials
  it on the first viewer and drops the connection after the last one leaves, so
  however many people watch, the device sees one connection.
- **It wants a few seconds between connections.** Reopening the stream the
  instant it was closed can fail once; waiting a moment and retrying works. That
  is the device, not go2rtc — it behaves the same when opened directly.

There is no snapshot URL: the device answers every path with the same video
stream, so the MQTT camera entity stays empty and the sensor carries the URL
instead.

### Live view and talk over WebRTC

RTSP is what Home Assistant's Generic Camera reads, and it is a couple of
seconds behind. The bundled go2rtc can also serve WebRTC — sub-second, with
sound — and carry the viewer's microphone back to the device's speaker.
**All of it is off until you turn it on**, and each step opens something:

1. **Map `8555/tcp` and `8555/udp`** (*Network*, any free host port) and
   restart. go2rtc then listens for WebRTC media and announces the Home
   Assistant host and that port to browsers. From then on it also asks a
   public STUN server (`stun.cloudflare.com`) for its public address — the one
   thing in this feature that leaves your network.
2. **Apply Two-Way Talk** on the *Patchers* tab for a device you want to talk
   through. Its stream then gets a microphone track: go2rtc hands the audio to
   an `ffmpeg` in this container, which sends it to the device.
3. **A player.** The panel's own **Talk** button on a camera's page needs
   neither of the steps above (only the patcher): it sends the browser's
   microphone through the panel, about a second behind, and needs a desktop
   browser on an HTTPS page. For WebRTC there is no player in the panel yet.
   Home Assistant's camera card has no microphone button either, so the
   consumer is the **WebRTC Camera** integration (HACS), and it needs go2rtc's
   API: set `go2rtc_api_password`, map `1984/tcp`, restart, add the
   integration with `http://petkit:<password>@<HA host>:1984`, and use a card
   with `url: "<the device's PetKit id>"` and `media: video,audio,microphone`.

**What the published API is.** Anyone who can reach port 1984 and knows the
password can list the streams (the list shows each camera's LAN address),
watch and listen to every camera, speak through every patched one, and add
streams of their own for go2rtc to connect to. It is basic auth over plain
HTTP, so the password crosses your LAN in the clear: use one you use nowhere
else. What it cannot do is reconfigure or restart go2rtc or run a command:
only the endpoints needed to list and play a stream are served on a published
API, and go2rtc refuses a command (`exec:`) source that arrives over it.
Without the option, or without the mapping, the API stays on loopback.

The panel also proxies the play-only part of that API under `/api/stream/`,
for a player on the panel's own origin. It is behind Ingress like the rest of
the panel — and, like the rest of the panel, open to your LAN if you map
port 8099.

Outside the add-on (docker compose, a bare process) there is no port mapping
to read, so WebRTC is turned on with `--webrtc-candidate <host>:<port>`, or
`--webrtc-candidate auto` when the container has a LAN address of its own.

### WebRTC from outside the LAN

On the LAN a browser reaches go2rtc's WebRTC port directly. From outside —
a phone on mobile data, Home Assistant behind your reverse proxy — it cannot:
go2rtc announces the LAN address, plus whatever public address and *random
port* your router mapped for its STUN lookup, and that mapping is good for the
STUN server only. WebRTC then fails, the card falls back to a mode without
WebRTC, and anything that needs it is gone: two-way talk and sub-second
latency. Video alone still works, which is what makes this look like "sound is
broken off Wi-Fi".

Two steps fix it. Forward the host port you mapped for `8555/udp` and
`8555/tcp` on your router to the Home Assistant host (say 8565 → 8565). Then
announce it:

```yaml
go2rtc_webrtc_candidates:
  - "stun:8565"             # dynamic public address: ask STUN, use this port
  # - "home.example.org:8565"  # or a name / static address
```

The LAN address stays first in the list, so viewers at home keep the direct
route. The option takes go2rtc's own candidate syntax verbatim. Forwarding
that port puts go2rtc's WebRTC listener on the internet: it serves nobody
without the signalling, which stays behind Home Assistant or the API password,
but it is a listener you did not have before.

A relay is the alternative to forwarding a port. If `turn.json` exists in the
add-on's data directory with a Cloudflare TURN key (`{"key_id": "...",
"api_token": "..."}`), the panel's `/api/turn` mints one-hour relay credentials
from it for a player to use. The token itself is never sent to a browser; the
credentials are, to anyone who can open the panel.

## Telling two cats apart by weight

A litter box that cannot say who visited — one with no camera, or a visit the
camera did not recognise — has its visits matched to a pet by weight. On the
**Pets** tab, click a pet's weight and enter it once, in kilograms.

- **The weight is then learned.** Each pet's reference weight follows the
  median of the last 7 visits credited to it, so a cat that gains or loses
  weight keeps being recognised without anyone re-entering anything. The
  learned value is used once 3 visits stand behind it. The weight you entered
  is kept beside it, and the card shows both and which one is in use. **Reset
  learned** goes back to the entered weight; so does typing a new one.
- **Only a visit that was credited teaches anything** — one the box itself
  named, or one that matched a single pet clearly. A visit that could be either
  cat's stays unnamed, and no pet's weight moves because of it. Two cats that
  end up weighing the same therefore stop being told apart; they do not start
  trading visits.
- **Two thresholds** on the same tab adjust the match. *Tolerance* (15% of the
  pet's weight, never less than 500 g) is how far a visit may be from a pet;
  raise it if a cat's visits often stay unnamed. *Ambiguity margin* (300 g) is
  how much nearer the best pet must be than the next; raise it if two cats of
  similar weight get each other's visits.

Each pet also has a **Weight** sensor in Home Assistant: the same median of its
last 7 weighed visits, in grams, suitable for a long-term graph.

## Provisioning over Bluetooth

The **Provision** tab hands a device its Wi-Fi credentials, a custom `apiServers`
URL and a timezone — which is the only way a device ever gets one, so a box
provisioned without it stamps UTC onto its video until it is re-provisioned.

Web Bluetooth only runs on a **secure page**, so this needs Home Assistant served
over HTTPS with a certificate your browser trusts (a reverse proxy, or Home
Assistant's own TLS). With one in place it works from the sidebar as usual,
Ingress included. A localhost proxy on the computer with Bluetooth is another
secure context. The previously linked hosted page is for camera-equipped Linux
models and does not support ESP32 feeders such as the D4S Gemini.

Chrome or Edge only, on desktop or Android. Firefox, Safari and iOS have no Web
Bluetooth at all.

## The phone app

A separate app can show your devices, the timeline, photos and clips, and your
pets from outside your home network, run their buttons, change their settings and
edit a feeder's plan. It talks to petkit-local through the **app API**, a
versioned JSON API on its own port, `8097`. Nothing of the
panel and nothing of the device API is reachable on that port, and the app API
is reachable on neither of theirs.

Unlike the panel, the app API authenticates every request itself — it is meant
to be reached from the internet, through your own reverse proxy or tunnel. With
no password set it refuses every request, so mapping the port changes nothing
until you choose one.

### Setting it up

1. **Set a password** in the panel, **Setup → Phone app access** (at least 8
   characters). The panel is the only place to set it, and the way back in if
   it is forgotten: setting it again there never asks for the old one.
2. **Map port 8097.** Under Home Assistant, in the app's **Network** settings,
   give `8097/tcp` a host port. Standalone, publish it in `docker-compose.yml`
   (there is a commented line; bind it to `127.0.0.1` if your reverse proxy runs
   on the same machine), or move it with `--app-port`.
3. **Point your reverse proxy or tunnel at that port, on a host of its own** —
   for example `petkit.example.org` → `http://<this host>:8097`. Serve it over
   HTTPS; the app sends its password and token in every login and request.
4. **Do not put forward authentication on that host** (Authelia, Authentik,
   Cloudflare Access and the like). They work by browser redirects and cookies,
   which a native app cannot follow, and petkit-local is already doing the
   authenticating.
   The proxy must also pass **WebSockets** through for live updates
   (`/app/v1/ws`): Cloudflare does by default; nginx needs the usual
   `Upgrade`/`Connection` headers on that location.
5. Sign in from the app with the password. Each phone gets its own session,
   listed under **Phone app access**, where you can sign one out or all of
   them.

**Never expose the panel (8099) or the device API (80) the same way.** The
panel has no login of its own and can flash firmware; the device API cannot
have one, because the firmware has no way to authenticate. Only `8097` is
built for the internet.

### Sessions and the password

- A session ends when the phone signs out, when you sign it out in the panel,
  when the password changes, or after **90 days without use** — every request
  extends it, so a phone in daily use never has to sign in again.
- Setting, resetting or removing the password in the panel signs out **every**
  phone. Removing it closes the app API again.
- The app can change the password too, but only by sending the current one as
  well — a stolen phone's session is not enough to lock you out. That signs out
  every other phone and keeps the one that made the change.
- Only a hash of the password and of each session token is stored, in
  `/data/app_api.json`; neither is ever an add-on option or written to the log.

### Wrong passwords and trusted proxies

Each wrong password locks the address it came from out for twice as long as
the last (1 s, 2 s, 4 s … up to 15 minutes). Behind a reverse proxy every
request comes from the proxy, so all phones would share one allowance — set
**Trusted reverse proxies** on the same card to the address (or CIDR) your
proxy connects from. Only then are `X-Forwarded-For` and `CF-Connecting-IP`
believed; from anyone else they are ignored, because anyone can send them.

- The address your proxy connects from is what petkit-local sees, not its
  public one: the Docker or LAN address of the reverse-proxy container or host.
  Under Home Assistant OS, a reverse-proxy add-on usually connects from
  `172.30.32.0/23`.
- `CF-Connecting-IP` is preferred when present. Trust it only if every request
  really comes through Cloudflare (a Cloudflare Tunnel, or a firewall that
  admits nothing else); otherwise use `X-Forwarded-For` alone, which your proxy
  appends to.
- Leave the list empty if nothing sits in front of port 8097.

### What the app can change

Exactly what Home Assistant can, through the same code, so a value is checked
and sent the same way whichever of the two changed it:

- **Buttons** — feed now, clean, deodorize, a W7H's deep clean and the rest:
  only the ones Home Assistant shows for that model. The ones the panel shows
  in red (dump the litter, start maintenance, reset a filter's date, drain a
  fountain, deep clean, flip the camera) cannot be undone, and the API refuses
  them unless the app confirms explicitly.
- **Settings** — the switches, numbers, lists and times Home Assistant can
  set, within the same bounds.
- **A feeder's plan** — its meals, and pausing it: paused, the meals are kept
  and the feeder is served none until it is resumed. And **one-off feeds** at a
  date and time.

A change reaches the device at once if it is connected over MQTT, otherwise on
its next poll; the app is told which. Each one is in the panel's log, marked
`app:`, and Home Assistant shows it straight away.

### Bluetooth accessories

A Bluetooth fountain (W5, W4, CTW2, CTW3) or Pura Air (K3) paired in the panel
is a device of its own in the app, as it is in Home Assistant, with the same
entities: a fountain's pump, filter and settings, a K3's liquid level and
battery. Each one names the device that relays it, and that device lists its
accessories — which is how a litter box's page knows it has a Pura Air whose
cartridge level to show.

An accessory is reached only through that device, and only while the device is
connected over MQTT. A button or setting on it is therefore sent at once or
refused: there is no poll to wait for. A K3's readings ride on its litter box's
reports, so it has no "last seen" of its own, and nothing to press.

### Live updates

While the app is open it holds one WebSocket, `/app/v1/ws`, authenticated by
the same token. It is sent every device once, then only what changes: the
values that moved (at most once a second per device), a device going online or
offline, a new or updated timeline card. Signing the phone out — from the app or
from the panel — closes its socket within a couple of seconds. A phone that
stops reading is disconnected rather than queued for, and simply reconnects.

### For app developers

The API is described by an OpenAPI 3.1 file served at
`/app/v1/openapi.yaml` (public once a password is set), which a Kotlin client
can be generated from. Media URLs in responses are paths; fetch them with the
same `Authorization: Bearer` header as everything else — there are no signed
URLs to leak into logs. Adding a field is a compatible change; anything else
moves to `/app/v2`.

Live updates come over one WebSocket, `/app/v1/ws` (not to be confused with
live video, below). Its messages are in the same file (`UpdateMessage` and the
`Update*` schemas): a `hello` with every device as `DeviceDetail`, then `state`
(only the entities that changed), `device` (a summary, when it went online or offline),
`device_removed` and `timeline` (a card to insert or replace by `id`, with the
ids it `replaces`). Open the socket before fetching the timeline and merge by
`id`. It closes with `4401` when the session ends (sign in again), `1013` when
the app read too slowly (reconnect) and `1001` on shutdown — the same codes as
the live-video socket, so one handler covers both. Writes answer
`delivered`: `mqtt`, `queued` or `local`.

Bluetooth accessories are listed by `/devices`, `/devices/{id}` and the socket
like any device, under their own id: `connection` is `ble`, `parent_id` names
the device relaying them, and that device's `accessories` lists each as
`{id, type, kind}` (a `purifier` there is a K3). Their actions and settings use
the same routes and answer `409 accessory_unreachable` when the parent is not on
MQTT; the feeding and live-video routes answer 404 for them.

### Live video

A camera's live view reaches the app through the same port and the same
tunnel, so nothing else needs mapping: go2rtc's own port (1984) stays closed.
It works for a camera whose stream the panel already shows (the camera patch is
applied and go2rtc is serving it). Two ways, both behind the same token, both
limited to the camera named in the request:

- **Fragmented MP4 over a WebSocket** — `/app/v1/devices/{id}/live`. go2rtc's
  "MSE" stream, relayed. Expect **a few hundred milliseconds to about a
  second** of delay, plus whatever the camera itself adds (not measured on
  these cameras). The first picture waits for the camera's next keyframe.
  WebRTC is not used from outside: its video travels over UDP, which a
  Cloudflare tunnel does not carry.
- **HLS** — `/app/v1/devices/{id}/live/hls/playlist.m3u8`, the fallback for a
  connection where the WebSocket keeps dropping. **Several seconds** of delay,
  the player buffering whole segments.

**At most 2 viewers per camera and 4 in all**, WebSocket and HLS together.
Each viewer is a separate copy of the video through your tunnel and upload,
and a camera has one connection to give (go2rtc shares it). One phone holds at
most one view of a camera: opening it again replaces the older view. A viewer
that arrives a few seconds after the previous one left may be told the stream
could not be opened — the camera needs a few seconds between connections — and
should simply retry.

**Check your tunnel provider's terms before watching through it.** Cloudflare's
terms for its CDN plans restrict serving video and other large files unless it
goes through a product sold for it, and a tunnel's traffic crosses that same
network. A live view to your own phone is small next to what that rule is
about, but it is video: read the current terms for your plan, keep the caps,
and watch on the LAN (the panel) when you are home. Other providers have terms
of their own.

For app developers (the full message flow and close codes are in the OpenAPI
file):

- **HLS with Media3/ExoPlayer** is the shortest path: an `HlsMediaSource` on
  `playlist.m3u8`, with the bearer header set once through
  `DefaultHttpDataSource.Factory().setDefaultRequestProperties(...)`, which
  every playlist and segment request then carries. Every URI in the playlists
  is relative to that route.
- **The WebSocket** needs the bearer header on its upgrade request, and a
  browser's `WebSocket` cannot send one — so go2rtc's `video-stream.js`, which
  opens its own socket, cannot be pointed at it as is. In a WebView (Android's
  supports Media Source Extensions), open the socket natively with OkHttp,
  which can send the header, and hand each message to the page — through a
  `WebMessagePort` (androidx.webkit, array-buffer messages) — where a short MSE
  script creates a `SourceBuffer` with the type from the first text message
  and appends every binary one in order (the MSE half of go2rtc's
  `video-rtc.js` is a model). Natively, a Media3 `DataSource` fed from the same
  OkHttp socket can play the stream as fragmented MP4, with no WebView, for
  more work.
- On close code `1013` (the phone did not keep up) switch to HLS; on `4401`
  sign in again; on `1011` or an `error` message retry after a few seconds.

### Talking through a camera

The app can talk through a camera's speaker the way the panel's **Talk** button
does, through the same port and the same token: apply **Two-Way Talk** on the
panel's *Patchers* tab first. The phone's microphone goes to petkit-local, which
transcodes it with `ffmpeg` and sends it to the device on your LAN. The
device's own sink (TCP 9010) is never exposed by this: the app reaches it only
through its signed-in session.

One talk per device at a time, from the app or the panel: a second is refused
and the first goes on. Half-duplex — mute the live view's sound while talking,
or the camera's microphone hears its speaker.

For app developers: `/app/v1/devices/{id}/talk` is a WebSocket (bearer header
on the upgrade). Wait for `{"type":"ready"}`, then send the microphone as
binary frames of raw PCM — signed 16-bit little-endian, 16 kHz, mono, exactly
what `AudioRecord` records with `ENCODING_PCM_16BIT` and `CHANNEL_IN_MONO` —
and end with `{"type":"stop"}`. Each device's `talk` field says whether to
offer the button at all (`available`, `needs_patcher`, `device_unreachable`,
`unsupported`), and anything that goes wrong is an `error` message with a
`code`; both are listed in the OpenAPI file.

### Notifications

The phone app receives notifications **over its own connection to petkit-local**,
with nothing else to install or run: see [Straight to the
app](#straight-to-the-app). It can also have them pushed to it, through a
UnifiedPush distributor of your choice: see [Pushed straight to the
app](#pushed-straight-to-the-app). Nothing goes through Google or PetKit either
way. Which kinds are sent is set in the panel, **Setup → Phone app access**,
under the signed-in phones, and applies to every phone and every way they
arrive.

What can be sent, each with its own switch:

| Kind | Sent when |
|---|---|
| Toilet visit | a litter box reports a finished visit (with the pet's name when it was recognised or matched by weight) |
| Meal served | a feeder finishes a meal, or skips one because the bowl was not empty |
| Meal failed | a meal ends in an error, or nothing came out (hoppers empty, blocked outlet) |
| Device error | a device reports a fault, as an event or in its state |
| Food low | a feeder's hopper reads empty and still does ten minutes later |
| Filter, litter or consumable to replace | waste bin or tray full, litter below 20 %, fountain filter or water, N50/N60/desiccant spent, deodorizer liquid low |
| Device offline | the offline watchdog decides a device is gone (after `offline_timeout`, not on a missed poll) |

A condition is announced when it **starts**, once — a full bin is not repeated
on every report, and whatever was already true when the add-on started is not
announced at all. Each event is announced once, however often the device
retries it. Each device may send only a few notifications of one kind per
period (one error per five minutes, one food-low per hour, …), so a device that
flaps cannot flood the phone. Device offline works the same with or without
Home Assistant (`--no-ha` included): the watchdog that decides it runs either
way, and only telling Home Assistant depends on HA publishing being on.

#### Pushed straight to the app

The phone app can register **its own** UnifiedPush endpoint with petkit-local
when you sign in — nothing to paste in the panel, one endpoint per phone, as
many phones as are signed in. The app picks a distributor on the phone (the
ntfy app pointed at your own ntfy server, or any other UnifiedPush
distributor), gets an endpoint from it, and hands it to petkit-local; from then
on each notification is POSTed there.

- **The kind switches above apply**, and so does the rest: each event is
  announced once, and the per-device rate limit counts it once whichever way it
  goes. The app may narrow the kinds further for its own phone.
- **The registration belongs to the phone's session.** Signing it out — from the
  app, from **Phone app access** (which shows, per phone, whether push is on and
  with which server), by a password change, or after 90 days unused — stops its
  pushes at once. An endpoint that answers "gone" (HTTP 404 or 410) three times
  in a row is dropped; the app registers again when it next opens.
- **What is sent is not a sentence but a few keys** — the kind, the device id,
  the pet id, an event code and the time, e.g.
  `{"v":1,"kind":"toilet","device_id":100,"pet_id":2,"code":"10","ts":1763150000}`
  — so the app writes the notification itself, in the phone's language. No
  name, no text, no token, no link.
- **With ntfy as the distributor**, the endpoint is on your ntfy server, and
  petkit-local publishes to it **without a token**: give anonymous users write
  access to UnifiedPush topics, which is what ntfy's UnifiedPush documentation
  recommends (`ntfy access '*' 'up*' write-only`). The topic name in the
  endpoint is long and random, and it is what keeps others out.
- **The endpoint must be `https://`.** Plain `http://` is accepted only for a
  private address or a name that only exists at home (`ntfy`, `*.lan`,
  `*.local`, `*.home.arpa`, `*.internal`) — a self-hosted ntfy reached over the
  LAN or a VPN. An endpoint on petkit-local's own machine is refused.

For app developers: `PUT /app/v1/push` `{endpoint, kinds?}` registers (or
replaces) the calling session's endpoint and `DELETE /app/v1/push` removes it;
`session.push` in `/app/v1/info` says whether one is registered. The message is
the `PushMessage` schema of the OpenAPI file — every key and every `code` is
listed there. `kinds` are the session's own, shared with the notification
socket below; left out of `PUT /app/v1/push`, they stay as they were.

#### Straight to the app

The app can also keep **one connection of its own** open to petkit-local in the
background, the way a push distributor keeps one to its server, and receive
every notification over it. Nothing else is needed — no push server, no second
app on the phone, no access rule — only the app API you already reach for
everything else, through the same reverse proxy or tunnel.

- **The same notifications, decided once.** The kind switches above, the
  once-per-event rule and the per-device rate limit decide whether a
  notification is sent, once, for the pushed endpoints and this connection
  alike; they can never disagree. Each phone then keeps only the
  kinds chosen in its app, whichever way it receives them.
- **Nothing is lost to a dead zone.** petkit-local keeps the last 500
  notifications of the last 3 days (in `/data/app_notifications.json`, so a
  restart keeps them too). A phone that was offline or switched off is sent what
  it missed when it reconnects, then the rest live. A phone never receives what
  happened before it was signed in. If more was missed than is kept, the app is
  told that some are missing.
- **It stays quiet.** The connection carries notifications only, never device
  updates, and petkit-local pings it every 45 seconds — well inside the 100
  seconds after which Cloudflare drops an idle connection — so it survives a
  tunnel and costs about what a push distributor's own connection costs.
- **It belongs to the phone's session**, like everything else: signing the phone
  out (from the app, from **Phone app access**, by a password change, or after
  90 days unused) closes it at once. **Phone app access** shows, per phone,
  whether it is connected right now, or when it last was.

For app developers: `GET /app/v1/notifications/ws?after=<id>` is the socket
(bearer token on the upgrade). It sends a `hello` (with `last_id`, how many
notifications are `replayed`, and whether there was a `gap`), then each
notification as `{"type":"notification","id":42,"replay":false,"message":{…}}`,
where `message` is the same `PushMessage` a pushed endpoint receives. Pass the
highest `id` received as `after` when reconnecting; leave it out the very first
time. One socket per session: opening a second closes the first with `4409`.
It closes with `4401` when the session ends (sign in again, do not retry),
`1013` if the app read too slowly (reconnect with `after`; nothing is lost) and
`1001` on shutdown. `GET`/`PUT /app/v1/notifications` `{kinds}` read and set the
kinds this phone receives, with no push endpoint needed, and list which kinds
the panel has switched on. Every message and parameter is in the OpenAPI file
(`NotificationSocketMessage`, `NotificationSettings`).

## T4 litter-level calibration

On the Pura Max (T4), leveling the litter and confirming that it is full are
separate operations. If the device still reports low litter after a refill:

1. Fill to the **MAX** line, with no cat inside.
2. Double-press **OK** on the device and wait for leveling to finish.
3. Press **Finish Litter Calibration** in the panel, or the matching Home
   Assistant button. Confirm the panel's reminder only after completing the
   first two steps.

The button is available only for T4 and refuses offline, busy, occupied or
unknown device state. It confirms the current fill as the new full baseline;
it is not a way to silence a low-litter warning without adding litter.
Queued delivery is not calibration success: the displayed percentage changes
only when the device reports it. The command and HTTP completion were verified
on two stock T4 devices running firmware 1.652. Other models keep their
existing command meanings.

## Troubleshooting

- **No entities appear:** confirm the device reached it (the log
  shows `Signup` / `State report`), and that the MQTT integration is configured.
- **Controls show "unknown":** they render from device settings; they populate
  from defaults immediately and update on the first change.
- **Device shows unavailable:** it hasn't reported within `offline_timeout`.
- **Device connects but then does nothing:** power-cycle it before investigating
  anything else. The firmware can abort its own start-up sequence part-way and
  carry on looking healthy — the process stays alive and the MQTT session
  authenticates normally, so from this side it is indistinguishable from a
  device that simply has nothing to say. Seen on a T5 and on a W7H; a reboot
  fixed both. If it comes back, the device's own `devRun.log` in **Device logs**
  names the file and line that handled each response, which is how the T5's
  cause was found.
- **Wrong sensor values:** the state parser is tuned from limited samples — turn
  on `capture` and open an issue with the collected `*.jsonl` — but read the
  warning under **Getting help** first.

## Getting help

- The **Log** tab shows every request a device makes, and can save it to a file
  to attach to an issue.
- The **Setup** tab has connection diagnostics and the exact values a device
  needs.
- Turning on **Traffic capture** (Setup → Settings) records raw payloads to
  `/data/capture/*.jsonl`. That is the single most useful thing to attach when
  something is wrong for a device model this project has not verified — see the
  supported-device table in the repository README for which those are.

  **⚠ Read a capture before you post it.** A capture is a verbatim recording of
  everything the device said and was told; nothing in it is filtered, because it
  is only useful if it is exact. Any of these files can contain your **Wi-Fi
  SSID**, LAN addresses, the device serial and its signing secret, and if proxy
  mode was on, `proxy_http.jsonl` and `proxy_mqtt.jsonl` also carry the full
  exchanges with PetKit **including your account credentials**. Attach only what
  the question needs.
