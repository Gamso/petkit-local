"""Embedded MQTT broker using amqtt with Aliyun IoT auth.

The device connects over TLS (Aliyun securemode=2). We keep a plain-TCP
listener for the internal bridge and, when enabled, add a TLS listener on the
device-facing port. A Linux device patched to trust it is served a self-signed
cert; a patched ESP32 asks by name for the leaf its firmware's CA signed
(`Esp32Certificate`).

This broker exists so the device has somewhere to connect that behaves like
Aliyun IoT; it is NOT Home Assistant's broker (that one is Mosquitto, spoken to
by `ha/publisher.py`). Both run in the same process, and `mqtt/bridge.py` is
the client that joins them.
"""
from __future__ import annotations

import datetime
import logging
import os
import socket
import ssl
import weakref
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from amqtt.broker import Broker, BrokerConfig
from amqtt.contexts import ListenerConfig
from amqtt.mqtt.protocol.broker_handler import BrokerProtocolHandler

from petkit_local.config import _supervisor_host_ip
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.mqtt import protocol_compat
from petkit_local.mqtt.auth import AliyunAuthPlugin, parse_client_id
from petkit_local.patchers.esp32_ota import (
    broker_leaf_paths, ensure_broker_leaf, wants_esp32_leaf,
)
from petkit_local.utils.tls import subject_alt_names

if TYPE_CHECKING:
    from petkit_local.web.hub import EventHub

log = logging.getLogger(__name__)


def _get_host_ip() -> str | None:
    """Best-effort host LAN IP, for the generated cert's SAN.

    Two sources, in order of trust: the Supervisor API (authoritative inside a
    HA OS install) and a UDP socket "connected" to a public address, which
    makes the kernel pick the outbound interface without sending a packet.
    Both are wrapped: a cert with no IP SAN still works for every client that
    isn't verifying the hostname, so failing here must not stop the broker.
    """
    try:
        ip = _supervisor_host_ip()
        if ip:
            return ip
    except Exception:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


def _san_of(cert_path: str) -> set[str]:
    """Every name and address the cert at `cert_path` claims, as strings.

    Empty when it cannot be read, which the caller treats as "cannot tell" and
    stays quiet about rather than warning on a guess.
    """
    try:
        from cryptography import x509  # noqa: PLC0415 - optional dependency
        with open(cert_path, "rb") as fh:
            cert = x509.load_pem_x509_certificate(fh.read())
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        return ({str(ip) for ip in san.get_values_for_type(x509.IPAddress)}
                | set(san.get_values_for_type(x509.DNSName)))
    except Exception:  # noqa: BLE001 - absent, unreadable or no SAN
        return set()


def _warn_if_uncovered(cert_path: str, hosts: list[str]) -> None:
    """Say so when an EXISTING cert does not name an address devices dial.

    Deliberately a warning and not a regeneration. Minting a new certificate
    invalidates the copy the `cacert` patcher put in every patched device's
    trust store, so a silent refresh would cut working installs off from media
    upload at the moment of an unrelated update. The operator deletes the file
    and re-runs the patcher when they are ready.
    """
    if not hosts:
        return
    named = _san_of(cert_path)
    if not named:
        return
    missing = [h for h in hosts if h not in named]
    if missing:
        log.warning(
            "The TLS certificate at %s does not name %s, which is where devices "
            "are told to upload. Clients that verify the address will refuse it. "
            "To re-issue: stop the add-on, delete %s and its key, start it, then "
            "re-apply the CA patcher to every device.",
            cert_path, ", ".join(missing), cert_path)


def ensure_self_signed(cert_path: str, key_path: str,
                       extra_hosts: list[str] | None = None) -> bool:
    """Generate a self-signed cert/key pair at the given paths if missing.

    Self-signed is sufficient because the device's mbedtls is patched to skip
    verification (see the repo's CLAUDE.md on cert pinning). The SAN carries
    the host IP anyway so that clients which DO check the hostname — curl with
    VERIFYHOST, for one — can still be pointed at the broker for debugging.

    `extra_hosts` are additional addresses to name, and the caller passes the
    one devices are actually told to dial. Autodetecting the host's own IP is
    not the same question: it agrees on a single-homed HA OS box by luck of
    topology, and disagrees behind a reverse proxy, on a multi-homed host
    (where the fallback picks the interface facing the internet, not the one
    facing the litter box), or whenever `api_url` names a host rather than an
    address — in which case no IP SAN can match it at all.

    An existing cert is never re-issued, only warned about; see
    `_warn_if_uncovered`.

    Returns:
        True if a usable cert exists afterwards, including the case where one
        was already there. False means `cryptography` is missing or generation
        failed; the caller then starts without a TLS listener rather than not
        starting at all.
    """
    wanted = [h for h in (extra_hosts or []) if h]
    if os.path.exists(cert_path) and os.path.exists(key_path):
        _warn_if_uncovered(cert_path, wanted)
        return True
    try:
        from cryptography import x509  # noqa: PLC0415 - optional dependency, probed at use
        from cryptography.hazmat.primitives import hashes, serialization  # noqa: PLC0415
        from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415
        from cryptography.x509.oid import NameOID  # noqa: PLC0415

        host_ip = _get_host_ip()
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "petkit-local")])
        san_names = subject_alt_names(["petkit-local", host_ip or "", *wanted])
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(name).issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=3650))
            .add_extension(x509.SubjectAlternativeName(san_names), critical=False)
            .sign(key, hashes.SHA256())
        )
        os.makedirs(os.path.dirname(cert_path) or ".", exist_ok=True)
        with open(key_path, "wb") as f:
            f.write(key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            ))
        with open(cert_path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        log.info("Generated self-signed MQTT cert at %s", cert_path)
        return True
    except Exception as e:
        log.error("Could not generate self-signed cert: %s", e)
        return False


def _device_friendly(ctx: ssl.SSLContext) -> ssl.SSLContext:
    """Accept the only suites a device's mbedTLS offers.

    It offers RSA key-exchange and PSK suites — no ECDHE, no DHE. Python 3.12's
    default context excludes plain RSA (no forward secrecy), so the handshake
    would fail with no shared cipher.
    """
    ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


class Esp32Certificate:
    """The certificate a patched ESP32 is shown, chosen by the name it dials.

    Every other device is served `broker.crt`, which the Linux models were
    patched to trust (`patchers/cacert.py`) and must keep being served. A
    patched ESP32 trusts only the CA in its firmware (`patchers/firmware.py`)
    and checks the certificate against `<productKey>.iot-as-mqtt.<region>.
    aliyuncs.com`, the name it built and sent as SNI — so that name, and only
    that name, gets the leaf the ESP32 CA signed.

    The leaf is read from disk when it changes rather than once at start-up: the
    CA is minted by the first firmware fetch, usually long after the broker
    started. A device that sends no SNI, or a name before any CA exists, gets
    `broker.crt`, fails verification and stays on the HTTP heartbeat.
    """

    def __init__(self, data_dir: str) -> None:
        self._data_dir = data_dir
        self._ctx: ssl.SSLContext | None = None
        self._stamp: tuple[int, int] | None = None

    def context(self) -> ssl.SSLContext | None:
        """The ESP32 server context, or None while there is no leaf."""
        cert_path, key_path = broker_leaf_paths(self._data_dir)
        try:
            stamp = (os.stat(cert_path).st_mtime_ns, os.stat(key_path).st_mtime_ns)
        except OSError:
            return None
        if stamp != self._stamp:
            ctx = _device_friendly(ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER))
            ctx.load_cert_chain(cert_path, key_path)
            self._ctx, self._stamp = ctx, stamp
        return self._ctx

    def sni_callback(self, ssl_obj, server_name: str | None, _ctx) -> None:
        """`SSLContext.sni_callback`: switch an ESP32 broker name to its leaf.

        Never raises: an exception here aborts the handshake for whoever was
        connecting, and the fallback — the default certificate — is the
        behaviour every device had before this existed.
        """
        if not server_name or not wants_esp32_leaf(server_name):
            return
        try:
            ctx = self.context()
            if ctx is not None:
                ssl_obj.context = ctx
        except Exception:  # noqa: BLE001 - see the docstring
            log.exception("Could not serve the ESP32 broker certificate for %s", server_name)


def configure_device_tls(ctx: ssl.SSLContext, data_dir: str) -> ssl.SSLContext:
    """Make a listener context serve every device: ciphers, and the ESP32 leaf."""
    _device_friendly(ctx)
    ctx.sni_callback = Esp32Certificate(data_dir).sni_callback
    return ctx


#: Set for the duration of `DeviceBroker._initialize_client_session`: each
#: connection is served by its own task, so this scopes the forced clean session
#: to the CONNECTs a DeviceBroker reads and leaves any other amqtt broker alone.
_force_device_clean: ContextVar[bool] = ContextVar("petkit_force_device_clean",
                                                   default=False)
_clean_patch_installed = False


#: The device SDK whose resumed session went deaf (#150): the T4's
#: `_v=sdk-c-3.2.0`. A D4SH on `sdk-c-4.1.0` given the same fresh session
#: declared itself offline (`iotStatus=0`) ten minutes into every connection
#: and reconnected every fifteen, so the forced clean session stays on the
#: SDK it was shown to fix.
NEVER_RESUMED_SDK_PREFIX = "sdk-c-3."


def never_resumes(client_id: str) -> bool:
    """True if this CONNECT is a device whose session must never be resumed."""
    parsed = parse_client_id(client_id)
    if not parsed:
        return False
    return parsed["params"].get("_v", "").startswith(NEVER_RESUMED_SDK_PREFIX)


def _install_device_clean_session() -> None:
    """Make a device's CONNECT read as clean_session=1, under a DeviceBroker.

    amqtt builds the `Session` from the CONNECT inside
    `BrokerProtocolHandler.init_from_connect` and branches on its
    `clean_session` straight after, with no hook in between, so the flag is
    set where the session is born. Idempotent.
    """
    global _clean_patch_installed
    if _clean_patch_installed:
        return
    original = BrokerProtocolHandler.init_from_connect.__func__

    async def init_from_connect(cls, *args: Any, **kwargs: Any) -> Any:
        handler, session = await original(cls, *args, **kwargs)
        if (_force_device_clean.get() and not session.clean_session
                and never_resumes(session.client_id or "")):
            session.clean_session = True
        return handler, session

    BrokerProtocolHandler.init_from_connect = classmethod(init_from_connect)  # type: ignore[method-assign]
    _clean_patch_installed = True


class DeviceBroker(Broker):
    """amqtt's broker, with a take-over that leaves the NEW connection standing.

    A take-over is a CONNECT for a client id whose connection is still open.
    A patched ESP32 makes one on every reconnect: its SDK (`_v=sdk-c-3.2.0`)
    sends a FIXED `timestamp=2524608000000` in the client id, so a box that
    comes back before the broker noticed it leave is the same client. (A T5
    puts the real time there, so its reconnect is a new client and none of
    this applies to it.) amqtt handles the two CONNECT flavours differently,
    and gets both wrong for us:

    * `clean_session=0` reuses the live `Session` object for the new
      connection and then closes the old one. The old connection's teardown
      (`Broker._handle_disconnect`) runs on that SHARED session: it marks it
      `disconnected` under the new connection — so broadcasts skip it and every
      command to the device is dropped — and fires `CLIENT_DISCONNECTED` with
      the very session the new connection is using, so `mqtt/auth.py` cannot
      tell it from a real loss and takes the device off MQTT. If the session's
      `clean_session` was set by an earlier, clean CONNECT (amqtt never updates
      it on resume), the session monitor then reaps the "disconnected" session
      a second later — and stops the NEW connection's handler with it. That is
      issue #106's log line for line.
    * `clean_session=1` gives the new connection a fresh session but never
      closes the old connection: it lingers, deaf, until its keep-alive runs
      out, holding a connection slot and holding up `shutdown`.

    So: once the new CONNECT has authenticated, the connection it replaces is
    marked superseded, closed (amqtt already does that for a resumed session),
    and its teardown stops only its own handler — the session state, the will
    message and `CLIENT_DISCONNECTED` all belong to the connection that is
    still open. A CONNECT that FAILS authentication marks nothing superseded:
    the open connection is closed with an ordinary teardown, which takes the
    device off MQTT, as amqtt itself does for a resumed session.

    On top of that, a device on the T4's SDK (`sdk-c-3.x`, `never_resumes`)
    never resumes a session (issue #150): its CONNECT is read as
    clean_session=1 whatever it sent, so every connection, take-over included,
    gets a fresh `Session` and a CONNACK with session-present 0. Not every
    device: a D4SH on `sdk-c-4.1.0` treated the same way reconnected every
    fifteen minutes, so newer SDKs keep amqtt's handling of the flag. The
    T4 sends clean_session=0 on its reconnects (amqtt's own "performing
    take-over" warning in #106's log only fires for a resumed session), and
    after such a take-over it stopped receiving anything we published while
    its own frames kept arriving — not reproducible broker-side, where the
    resumed session delivers fine. Session-present=1 is the one thing such a
    take-over shows the device that a working connection does not; the theory
    is that the firmware then skips its own SUBSCRIBEs and drops what arrives
    on topics it never registered. Unconfirmed on hardware. Nothing of value
    is lost either way: every device delivery is QoS 0 and `_server_subscribe`
    re-creates the subscriptions on each CONNECT. Our bridge and any other
    non-device client keep amqtt's handling of the flag.

    Overrides four private methods of amqtt (pinned `<1`) and wraps
    `BrokerProtocolHandler.init_from_connect`; the real-broker tests in
    `tests/test_mqtt_takeover.py` fail if their contract moves.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _install_device_clean_session()
        super().__init__(*args, **kwargs)
        #: client id -> (handler of the open connection a CONNECT in flight
        #: would replace, whether WE must close it). Lives from the CONNECT's
        #: session lookup to its authentication, never longer.
        self._replacing: dict[str, tuple[Any, bool]] = {}
        #: Handlers whose connection was replaced; their teardown is theirs alone.
        self._superseded: weakref.WeakSet = weakref.WeakSet()

    async def _initialize_client_session(self, *args: Any, **kwargs: Any) -> Any:
        """Read the CONNECT with a device's clean_session forced on (#150)."""
        token = _force_device_clean.set(True)
        try:
            return await super()._initialize_client_session(*args, **kwargs)
        finally:
            _force_device_clean.reset(token)

    async def _delete_session(self, client_id: str) -> None:
        """A clean CONNECT drops the stored session: note its open connection."""
        session, handler = self._sessions.get(client_id, (None, None))
        await super()._delete_session(client_id)
        if handler is not None and session is not None and session.transitions.is_connected():
            self._replacing[client_id] = (handler, True)

    async def _authenticate(self, session: Any, listener: Any) -> bool:
        """Authenticate, then retire the connection this CONNECT replaces."""
        client_id = session.client_id or ""
        held_session, held_handler = self._sessions.get(client_id, (None, None))
        if (held_session is session and held_handler is not None
                and session.transitions.is_connected()):
            # A resumed session that is still connected: amqtt is about to
            # take it over, and closes the old connection itself.
            self._replacing[client_id] = (held_handler, False)
        authenticated = await super()._authenticate(session, listener)
        replaced = self._replacing.pop(client_id, None)
        if replaced is not None and not authenticated and replaced[1]:
            # A refused clean CONNECT has already deleted the open connection's
            # session and subscriptions, leaving it deaf. Close it as amqtt
            # does for a resumed one, with an ordinary teardown, so
            # `CLIENT_DISCONNECTED` takes the device off MQTT rather than
            # leaving its commands routed to nobody.
            await replaced[0].handle_connection_closed()
        if replaced is not None and authenticated:
            old_handler, must_close = replaced
            self._superseded.add(old_handler)
            log.info("MQTT take-over: a new connection from %s:%s replaces the open one "
                     "of %s, which is closed", session.remote_address,
                     session.remote_port, client_id)
            if must_close:
                await old_handler.handle_connection_closed()
        return authenticated

    async def _handle_disconnect(self, client_session: Any, handler: Any,
                                 disconnect_waiter: Any) -> None:
        """Tear down a connection; a superseded one takes nothing else with it."""
        if handler in self._superseded:
            self._superseded.discard(handler)
            await self._stop_handler(handler)
            log.debug("Superseded MQTT connection of %s closed", client_session.client_id)
            return
        await super()._handle_disconnect(client_session, handler, disconnect_waiter)


async def start_broker(
    port: int = 1883,
    registry: DeviceRegistry | None = None,
    *,
    tls: bool = False,
    tls_port: int = 443,
    certfile: str = "",
    keyfile: str = "",
    strict_auth: bool = False,
    hub: EventHub | None = None,
    data_dir: str = "/data",
    presence: Any = None,
) -> Broker:
    """Start the embedded broker and hand the auth plugin its dependencies.

    amqtt instantiates plugins itself from the config, so the registry, strict
    flag and hub can only be injected afterwards by finding the live instance
    in `broker.plugins_manager` — hence the search below rather than a
    constructor argument.

    Args:
        port: Plain-TCP listener, used by the internal bridge.
        tls_port/certfile/keyfile: Only consulted when `tls` is set; a missing
            cert is generated. If it cannot be, the broker still starts without
            the TLS listener — a plain-HTTP device keeps working.
        strict_auth: Reject devices that fail signature validation. Off by
            default (see AliyunAuthPlugin).
        data_dir: Where the ESP32 firmware CA and its broker leaf live
            (`Esp32Certificate`).
        presence: `http/presence.py::CloudPresence`, handed to the auth plugin
            so the opt-in keep-online heartbeat follows each device's packets.

    Returns:
        The started Broker. The caller owns shutting it down.
    """
    listeners = {
        # Plain listener — used by the internal bridge (and plain devices).
        "default": ListenerConfig(type="tcp", bind=f"0.0.0.0:{port}", max_connections=50),
    }

    if tls:
        cert = certfile or "/data/certs/broker.crt"
        key = keyfile or "/data/certs/broker.key"
        # Re-issued here if a CA exists but its leaf is missing or stale, so
        # the SNI callback only ever has to read files. Never fatal: without
        # it a patched ESP32 falls back to HTTP, and nothing else is affected.
        try:
            ensure_broker_leaf(data_dir)
        except Exception:  # noqa: BLE001
            log.exception("Could not issue the ESP32 broker certificate")
        if ensure_self_signed(cert, key):
            listeners["tls"] = ListenerConfig(
                type="tcp", bind=f"0.0.0.0:{tls_port}", max_connections=50,
                ssl=True, certfile=cert, keyfile=key,
            )
            log.info("MQTT TLS listener enabled on port %d", tls_port)
        else:
            log.warning("TLS requested but no cert available - TLS listener not started")

    # A device on newer firmware opens with an MQTT 5.0 CONNECT. We still speak
    # only 3.1.1, but refusing it in a shape its client can read turns a
    # ten-second timeout into an immediate fallback. See protocol_compat.
    protocol_compat.install()

    config = BrokerConfig(
        listeners=listeners,
        timeout_disconnect_delay=2,
        plugins={"petkit_local.mqtt.auth.AliyunAuthPlugin": {}},
    )

    broker = DeviceBroker(config)

    # amqtt builds the listener's context itself; this is the one place to
    # reach it. `configure_device_tls` admits the ciphers a device's mbedTLS
    # offers and shows a patched ESP32 the leaf its firmware trusts.
    # Taken from the INSTANCE, so it comes back bound whatever amqtt made it:
    # a staticmethod up to 0.11, an instance method from 0.12 on. Reading it off
    # the class instead returned 0.12's plain function, which then wanted the
    # missing `self`, and the broker died on every start with the TLS listener
    # on. The replacement is a plain function set on the instance, so amqtt's
    # `self._create_ssl_context(listener)` calls it with the listener alone.
    _orig = broker._create_ssl_context

    def _device_friendly_ssl_context(listener):
        return configure_device_tls(_orig(listener), data_dir)

    broker._create_ssl_context = _device_friendly_ssl_context

    await broker.start()

    if registry:
        for plugin_wrapper in broker.plugins_manager._plugins:
            obj = plugin_wrapper.plugin_object if hasattr(plugin_wrapper, 'plugin_object') else plugin_wrapper
            if isinstance(obj, AliyunAuthPlugin):
                obj.set_registry(registry)
                obj.set_strict(strict_auth)
                obj.set_hub(hub)
                obj.set_broker(broker)
                obj.set_presence(presence)
                log.info("AliyunAuthPlugin registered (strict=%s)", strict_auth)
                break
        else:
            # Not fatal, but worth shouting about: without the plugin the
            # broker falls back to amqtt's own auth and no device connection
            # will ever be attributed to a registry entry.
            log.warning("AliyunAuthPlugin not found in broker plugins - "
                        "device MQTT sessions will not be recognised")

    log.info("MQTT broker started on port %d %s", port, "(+TLS)" if tls else "")
    return broker


def delivery_view(broker, product_key: str, device_name: str) -> dict:
    """What the BROKER will actually deliver to one device, right now.

    Everything else in this add-on reports intent: the auth plugin records the
    filters it asked for, and a publish reports that it was accepted. Neither
    is delivery. MQTT has no delivery report, so when a command changes nothing
    on the box the question "would a publish to that topic have reached it at
    all" had no answer anywhere — and it is the one question that separates a
    broker problem from a firmware one.

    Read straight off `Broker._subscriptions`, which is what `_run_broadcast`
    iterates, so this reports the same table delivery consults rather than a
    parallel copy that could drift from it. Sessions are matched by client id
    rather than by identity, so no session handle has to be plumbed here.

    Returns:
        `{"filters": [...], "sessions": [{"client_id", "state"}]}`. A session
        whose state is not "connected" is skipped by the broadcast loop, which
        is why the state is reported and not just the count.
    """
    subscriptions = getattr(broker, "_subscriptions", None) or {}

    def owns(session) -> bool:
        parsed = parse_client_id(getattr(session, "client_id", "") or "")
        return bool(parsed and parsed["product_key"] == product_key
                    and parsed["device_name"] == device_name)

    filters, sessions = [], {}
    for topic_filter, entries in subscriptions.items():
        for session, _qos in entries or ():
            if not owns(session):
                continue
            if topic_filter not in filters:
                filters.append(topic_filter)
            cid = getattr(session, "client_id", "")
            sessions[cid] = getattr(getattr(session, "transitions", None), "state", "?")
    return {
        "filters": sorted(filters),
        "sessions": [{"client_id": c, "state": s} for c, s in sessions.items()],
    }


async def drop_device_session(broker, product_key: str, device_name: str) -> bool:
    """Close one device's live session, the way amqtt's own take-over does.

    `handle_connection_closed` releases the session's message loop exactly as
    an abrupt drop would, so the socket is closed and `CLIENT_DISCONNECTED`
    fires, which clears `mqtt_connected` (`mqtt/auth.py`). Whether the device
    stays off is for `authenticate` to decide when it reconnects. Sessions are
    matched by client id, as in `delivery_view`.

    Returns:
        True if a connected session was closed.
    """
    dropped = False
    for client_id, (session, handler) in list((getattr(broker, "sessions", None) or {}).items()):
        parsed = parse_client_id(client_id or "")
        if not (parsed and parsed["product_key"] == product_key
                and parsed["device_name"] == device_name):
            continue
        if getattr(getattr(session, "transitions", None), "state", None) != "connected":
            continue
        await handler.handle_connection_closed()
        log.info("Closed the MQTT session of %s", client_id)
        dropped = True
    return dropped
