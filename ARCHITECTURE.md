# Architecture

This is a cloud, not a client. A PetKit device is pointed at this add-on instead of PetKit's
servers, and everything follows from that one fact: the device believes it is talking to the cloud,
so every answer has to be shaped like the cloud's, and a wrong answer is not an error message — it
is a device that reboots, retries forever, or silently stops working.

One process, one asyncio loop, one container, five servers inside it: an HTTP API and an MQTT
broker that the device talks to, a bucket it uploads media to, the web panel you look at, and
the app API a phone reaches from outside — the only one of them built to be exposed, and the only
one that authenticates its callers itself.
Plus one outbound client — `aiomqtt` connected to *Home Assistant's* broker, which is where
entities are published.

Do not confuse the two brokers. The one we run is for devices; the one we connect to is HA's.

(Which port each of those listens on, and what happens if you remap one, is an operator question:
see [`addon/DOCS.md`](addon/DOCS.md).)

## Packages

```
petkit_local/
├── main/          process lifecycle: cli -> wiring -> lifecycle
├── config.py      every tunable, from /data/options.json + Supervisor + CLI + panel
├── http/          the cloud a device believes it is talking to
│   ├── server.py      route table, and the catch-all that must stay last
│   ├── appkeys.py     the app[...] / request[...] contract (device API + bucket)
│   ├── handlers/      one module per endpoint
│   ├── middleware/    device identity, proxy forwarding, request logging
│   ├── redact/        what a cloud reply is allowed to contain
│   ├── proxy.py       upstream session + forward()
│   ├── bucket.py      unauthenticated OSS sink, listening on 0.0.0.0
│   ├── dns.py         resolver for upstream lookups only
│   └── cloud_fetch.py signs as the device to ask PetKit something directly
├── mqtt/          broker.py (amqtt) · auth.py (Aliyun HMAC) · topics.py
│                  bridge.py (device <-> us) · ble_relay.py (accessories through a parent)
│                  upstream.py (us <-> real Aliyun)
├── devices/       base.py Device identity and state · payloads.py the response bodies
│                  defaults.py per-category seed data · registry.py (atomic JSON)
│                  state_parsers.py + state_tables.py + consumables.py
│                  liveness.py the stale check + offline watchdog (HA or not)
│                  ble/ (framing, W5, CTW3, registry)
├── ha/            categories.py device category -> entities · discovery.py EntityDef -> payload
│                  publisher.py (the one HA-broker connection, outbound)
│                  command_router.py + commands.py (inbound: HA write -> device command)
│                  schedules.py (what a valid schedule is; the panel's and the app's writes)
│                  learn.py (proxy mode: settings the real cloud tells a device)
│                  timezone_sync.py (re-pushes a locale-derived UTC offset across DST)
│                  state_doc.py (the document every value_path is read from, for HA's readers)
│                  entities/ one module per component
├── events/        codes.py THE protocol tables · decode.py renders values for humans
│                  normalize.py transport -> row · sessions.py rows -> visits
│                  store.py SQLAlchemy async · models.py · migrations.py
├── media/         pipeline.py decrypt -> remux -> path -> row · stitch.py joins ~4s chunks
│                  serving.py what a media path resolves to, thumbnails, role -> slot
│                  crypto · transcode · layout · retention · go2rtc
├── web/           panel.py the application + the whole route table · api/ the JSON handlers
│                  appkeys.py the app[...] contract · hub.py event ring/WS · static/ · templates/
├── app_api/       the phone app's API, on its own port (8097), /app/v1 only
│                  server.py the application + ROUTES · middleware.py the access policy
│                  auth.py password (scrypt), hashed session tokens, trusted proxies
│                  throttle.py failed-login lockout, client address · serializers.py every shape
│                  live.py the WebSocket feed (one hub subscription, diffed, fanned out)
│                  handlers/ · openapi.yaml the contract, tested against ROUTES
│                  notify/ ntfy push: hub subscriber -> rules -> bounded publish queue
├── patchers/      on-device patches: cacert · mqtt · cloud · camera · ssh
│                  firmware (ESP32: a whole image rebuilt, not a file on a device)
│                  esp32_ota (that image stored, offered over OTA, rolled back)
├── ai/pets.py     pet CRUD + the face photos the device's NPU matches against
└── utils/         const · crypto · tls · capture · jsonio · paths · coerce · dicts · timeutil
```

**Layering.** `devices` depends only on `events` and `utils` — it does not know Home Assistant
exists. `ha` depends on `devices`; asking "which entities does this device publish" is an HA
question and is answered in `ha/categories.py`. Only `main/` imports `web` at runtime: the panel's
`EventHub` is handed to whatever wants to narrate into it, and everywhere else it is a
`TYPE_CHECKING`-only import, so nothing outside `main` depends on the panel existing.
`app_api` imports nothing from `web` or `http` (a test holds it): it reads the same stores and
helpers the panel reads — `events/`, `ai/pets.py`, `ha/categories.py`, `ha/state_doc.py`,
`media/serving.py` — and writes through the ones Home Assistant writes through
(`ha/commands.py`, `ha/command_router.py::deliver_command`, `ha/schedules.py`), so the two answer
alike without one depending on the other. The `EventHub` it is fed from is handed in by `main/`
and typed `Any` there. The panel's
Phone app access card is the one place `web` reaches into `app_api`, to administer its account.

## The app API

```
phone --HTTPS--> your tunnel / reverse proxy --HTTP--> app_api/server.py (port 8097)
                                                        └─ middleware.py
                                                             no password set   -> 401, everything
                                                             @public           -> /info, openapi.yaml
                                                             @credentials      -> /auth/login (throttled)
                                                             otherwise         -> Bearer session, else 401
                                                             └─> handlers/ -> serializers.py
writes:  handlers/controls.py, feeding.py -> ha/commands.py, ha/schedules.py
                                          -> ha/command_router.deliver_command (MQTT, else queue)
/ws:     EventHub --(device id marked dirty)--> live.LiveFeed: re-read, diff, fan out per socket
```

A separate application on a separate port, not a sub-app of the panel: a reverse proxy pointed at
it can reach `/app/v1` and nothing else, whatever a path is spelled like. The JSON it returns is
built field by field in `serializers.py`, never the panel's internal dicts, so the panel can change
without breaking a released app; `openapi.yaml` describes exactly those shapes and exactly the
routes in `server.ROUTES`.

The WebSocket never trusts the hub's events as state — they are log lines — only as a hint of
WHICH device to re-read; what a phone is sent is the diff of the serialized device against what was
last sent, at most once per second per device, plus a 30-second sweep for the changes nothing
narrates (the offline watchdog). One `LiveFeed` serves every socket; it runs only while one is open.

## The two transports

A device speaks **either** HTTP **or** MQTT — not both, and not one for requests and the other for
telemetry. Everything starts on HTTP, because that is where a device registers and asks where the
server is. If it can then reach the broker, it does, and **stops polling the HTTP heartbeat**
entirely (confirmed on a T5: quiet over HTTP ~40s after CONNECT, for as long as the session lives).
From that moment its state reports, its visit and cleaning events and its accessories' frames all
arrive over MQTT instead — and `events/normalize.py` turns either form into the same row, which is
why it is called transport-agnostic.

The same is true in the other direction. A command from Home Assistant is not a third path: it
goes to the device over whichever transport that device is on, and `Device.mqtt_connected` is what
decides. Only two things sit outside this: media, which the device uploads to `http/bucket.py`
under its own credentials, and our own connection to Home Assistant's broker.

What MQTT actually buys is latency. A command can be pushed the moment you flip a control; over
HTTP it waits in a queue until the device next polls.

## Over HTTP

Four middlewares wrap every request, in the order `http/server.py` registers them — outermost
first. `http/middleware/__init__.py` documents why that order is load-bearing.

```
device --HTTP--> http/server.py          route table; /{path:.*} is registered LAST, so
                   │                     an unknown endpoint is answered, never 404'd
                   ├─ never_fail         no failure of ours reaches the device as a 5xx
                   ├─ logging            panel live log, capture file, online/last_seen
                   ├─ device             X-Device + URL -> request["device_type"], ...
                   └─ proxy              off: calls the handler and returns its answer
                        │                on:  calls the handler FIRST and keeps that answer,
                        │                     then forwards upstream — the cloud's reply is
                        │                     returned only if it is usable and survives
                        │                     http/redact/. Every other path returns ours.
                        └─> handlers/<endpoint>.py
                              │  reads and mutates one Device; a state or event report
                              │  goes on to events/normalize.py the same as an MQTT one
                              └─> devices/payloads.py builds the body the firmware expects
```

That the local handler runs even in proxy mode is deliberate: it is what makes "the upstream is
unreachable, slow, or refuses this device" a non-event rather than an outage.

A command for a device on this transport waits in `Device.command_queue` until the device polls
`dev_heartbeat` and is handed it in the reply.

## Over MQTT

```
device --MQTT--> mqtt/broker.py (amqtt)
                   ├─ mqtt/auth.py    HMAC on CONNECT — and it subscribes the device on its
                   │                  behalf, because a T5 sends no SUBSCRIBE of its own
                   └─ mqtt/bridge.py  holds one `#` subscription and dispatches each frame:
                          ├─> devices/state_parsers.py -> device.state
                          ├─> events/normalize.py -> events/store.py
                          └─> ha/publisher.py -> Home Assistant
```

One `#` subscription covers every device, so anything that escapes per-message handling takes the
bridge down for all of them at once.

## From Home Assistant

```
HA's broker --MQTT--> ha/publisher.py     owns the one connection to it, subscribes
                        │                 petkit-local/+/cmd/+ and hands the stream on
                        └─> ha/command_router.py
                              └─> ha/commands.py builds the device command
                                    ├─ device.mqtt_connected?  mqtt/bridge.py publishes it
                                    └─ otherwise               Device.command_queue, for the
                                                               next HTTP heartbeat
```

`Device.mqtt_connected` picking that transport is load-bearing: publishing to a topic nobody
subscribes to raises nothing, so a stale `True` does not fail — it swallows the command.

## Media

A device uploads to `http/bucket.py` under a key it chose. `media/pipeline.py` decrypts it, remuxes
it, gives it a path a human can read, and records a row. Recordings arrive as many ~4-second chunks;
`media/stitch.py` joins each episode into one file and deletes the sources only after verifying the
result. `media/go2rtc.py` runs go2rtc in front of the device's own stream, because the device
refuses a second connection for several seconds and because Home Assistant's camera stack segfaults
reading its FLV directly.

## Where the knowledge lives

Two files carry almost all of the reverse-engineered protocol, and both are deliberately large:

- **`events/codes.py`** — every event code, in seven namespaces that must never be merged, each row
  graded by the evidence behind it and naming the firmware function it came from. It is one file
  because the namespaces collide, and the collisions are only visible when they sit together.
- **`devices/payloads.py`** — the response bodies. What a device is told about itself, its server,
  its storage credentials and its schedule. A value invented here is served to the device as its
  owner's setting, which is why several fields are deliberately left unset.

Read [`AGENTS.md`](AGENTS.md) before changing either. It collects the invariants
that are not obvious from the code — the ones where the natural-looking change is the wrong one.
