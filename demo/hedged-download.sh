#!/bin/sh
# Demonstration of POST /api/hedged-download.
#
# 1. Arms the CURRENT upstream so its *first* GET /artifact stalls one second
#    before response headers (the next identical GET answers immediately).
# 2. Asks the relay for a hedged download. The first attempt does not return
#    headers within 300 ms, so a second identical GET is sent to the same
#    target. It wins: headers arrive after ~300 ms with x-relay-hedged: true.
# 3. Verifies the delivered file is complete and byte-identical in length.
#
# Requires the compose stack (see README). Override the relay base with RELAY.
set -eu

RELAY=${RELAY:-http://localhost:8080}
ARTIFACT='/artifact?bytes=1048576&chunk=65536'
OUT=${OUT:-/tmp/relay-hedged.bin}

current=$(curl -sS "$RELAY/api/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["current"]["target"])')
case "$current" in
  blue) UPSTREAM=${UPSTREAM:-http://localhost:9001} ;;
  green) UPSTREAM=${UPSTREAM:-http://localhost:9002} ;;
  *) echo "unknown current target: $current" >&2; exit 1 ;;
esac

echo "== current target: $current ($UPSTREAM) =="
echo "== arming a one-shot 1s response-header stall on the first GET =="
# --data-urlencode keeps the '?'/'&' inside path as one query parameter, so the
# upstream's arming key matches the artifact URL the relay will request verbatim.
curl -sS -G -X POST "$UPSTREAM/_control/first-hit" \
  --data-urlencode "path=$ARTIFACT" --data-urlencode "delay=1" | python3 -m json.tool

echo "== hedged download through the relay =="
START=$(date +%s%3N)
curl -sS -D /tmp/relay-hedged.hdr -o "$OUT" \
  -H 'content-type: application/json' \
  -d "{\"path\":\"$ARTIFACT\"}" \
  "$RELAY/api/hedged-download"
END=$(date +%s%3N)
echo "elapsed: $((END - START)) ms (expect ~300, not ~1000)"
grep -i '^x-relay-' /tmp/relay-hedged.hdr || true

echo "== delivered file =="
ls -l "$OUT"
# 1 MiB exactly: a failed race must never deliver a partial/success file.
size=$(wc -c < "$OUT")
if [ "$size" -eq 1048576 ]; then
  echo "OK: complete 1 MiB artifact delivered from the winning attempt"
else
  echo "FAIL: expected 1048576 bytes, got $size" >&2
  exit 1
fi

echo "== final status (in-flight must be zero) =="
curl -sS "$RELAY/api/status" | python3 -m json.tool
