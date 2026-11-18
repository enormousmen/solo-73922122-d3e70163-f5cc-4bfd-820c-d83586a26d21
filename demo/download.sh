#!/bin/sh
# Demonstration: a long download on the old generation survives a switch until
# its 5s drain window expires, while a new download immediately uses the new
# target. Requires the compose stack (see README).
set -eu

RELAY=${RELAY:-http://localhost:8080}

echo "== current target =="
curl -sS "$RELAY/api/status" | python3 -c 'import json,sys; s=json.load(sys.stdin); print(s["current"])'

echo "== starting slow 6 MiB download through the relay =="
# 6 MiB in 32 KiB chunks, 0.4s apart => roughly 75s.
curl -sS -D /tmp/relay-old.hdr -o /tmp/relay-old.bin \
  "$RELAY/artifact?bytes=6291456&chunk=32768&delay=0.4" &
OLD_PID=$!
sleep 1

echo "== switching to the other target (drain window 5s) =="
OTHER=$([ "$(curl -sS "$RELAY/api/targets" | python3 -c 'import json,sys; print(json.load(sys.stdin)["current"])')" = blue ] && echo green || echo blue)
curl -sS -X POST "$RELAY/api/switch" -H 'content-type: application/json' \
  -d "{\"target\":\"$OTHER\",\"wait\":true,\"wait_timeout\":6}" | python3 -m json.tool

echo "== new download uses the new target immediately =="
curl -sS -D /tmp/relay-new.hdr -o /tmp/relay-new.bin \
  "$RELAY/artifact?bytes=262144&chunk=65536&delay=0"
grep -i '^x-relay-target\|^x-upstream' /tmp/relay-new.hdr || true

echo "== waiting for the old download (expect truncation ~5s after the switch) =="
START=$(date +%s)
if wait "$OLD_PID"; then
  echo "old download completed (unexpected for a ~75s body)"
else
  echo "old download interrupted as designed, $(( $(date +%s) - START ))s after the switch"
fi
ls -l /tmp/relay-old.bin /tmp/relay-new.bin

echo "== final status =="
curl -sS "$RELAY/api/status" | python3 -m json.tool
