#!/usr/bin/env bash
# Start the add-on image exactly as docker-compose.yml does (minus the Home
# Assistant broker: --no-ha), then run smoke.py against it. See smoke.py for
# what is checked from inside; this checks what only the outside can see.
#
#   addon/tests/smoke/run.sh <image>
#
# Needs only docker. Exits non-zero on any failure, after printing the add-on's
# log, and removes the container either way.
set -euo pipefail

# Git Bash on Windows rewrites `--data-dir=/data` into a Windows path.
export MSYS_NO_PATHCONV=1

image="${1:?usage: $0 <image>}"
here="$(cd "$(dirname "$0")" && pwd)"
name="petkit-smoke-$$"

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# The production command (docker-compose.yml) and restart policy: a process
# that dies at start shows up as a restart loop, which is how 2.3.0 failed.
# api-url is the address the fake device dials, inside the shared namespace.
docker run -d --name "$name" --restart unless-stopped -e TZ=UTC "$image" \
  python3 -m petkit_local.main \
  --data-dir=/data \
  --api-url=http://127.0.0.1:8080 \
  --port=8080 \
  --mqtt-tls \
  --mqtt-tls-port=443 \
  --no-ha >/dev/null

status=0
# Same image, so the probe uses the paho-mqtt the lock installed and needs no
# Python on the host. It joins the add-on's network namespace: 127.0.0.1 there
# is the add-on, and no port is published.
docker run --rm -i --network "container:$name" --entrypoint python3 "$image" - \
  < "$here/smoke.py" || status=$?

log="$(docker logs "$name" 2>&1 || true)"
state="$(docker inspect -f '{{.State.Running}} {{.RestartCount}}' "$name" 2>/dev/null || echo 'gone ?')"

problem() { echo "FAIL $*"; status=1; }

[ "$state" = "true 0" ] || problem "container not up without restarts (running, restarts: $state)"
if grep -q "Traceback" <<<"$log"; then problem "the add-on log has a traceback"; fi
# Each of these is an amqtt internal the add-on reaches into, and each fails
# soft -- logged, not raised -- so only the log says it went wrong.
for want in "AliyunAuthPlugin registered" "MQTT TLS listener enabled" \
            "MQTT auth OK" "on its behalf"; do
  grep -q "$want" <<<"$log" || problem "the add-on log never says: $want"
done

if [ "$status" -ne 0 ]; then
  echo "----- add-on log ($name) -----"
  echo "$log" | tail -n 150
  exit "$status"
fi
echo "ok  container up with no restarts, no traceback in its log"
