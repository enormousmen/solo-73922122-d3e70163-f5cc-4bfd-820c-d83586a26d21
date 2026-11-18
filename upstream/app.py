"""Demo artifact upstream with controllable readiness latency and slow downloads.

Run two copies (blue/green).  Every response identifies itself via
``X-Upstream-Name`` so the relay demo makes routing visible.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import AsyncIterator
from urllib.parse import parse_qsl, urlencode, urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
PROBE_DEFAULT_DELAY = float(os.environ.get("UPSTREAM_PROBE_DELAY", "0"))
PROBE_DEFAULT_STATUS = int(os.environ.get("UPSTREAM_PROBE_STATUS", "200"))
MAX_PROBE_DELAY = 120.0
MAX_HEADER_DELAY = 120.0
# Arming entries expire after this long, so a demo can be rerun without
# restarting the upstream.
FIRST_HIT_TTL_SECONDS = 120.0

app = FastAPI(title=f"Artifact upstream {NAME}")
state = {"probe_delay": PROBE_DEFAULT_DELAY, "probe_status": PROBE_DEFAULT_STATUS}
# Out-of-band arming: key -> (monotonic expiry, (delay, status, remaining_hits)).
first_hit_armed: dict[str, tuple[float, tuple[float, int, int]]] = {}


def _artifact_key(raw_url: str) -> str:
    parts = urlsplit(raw_url)
    params = parse_qsl(parts.query, keep_blank_values=True)
    return parts.path + ("?" + urlencode(params) if params else "")


def _prune_expired(now: float) -> None:
    for key in list(first_hit_armed):
        if first_hit_armed[key][0] <= now:
            first_hit_armed.pop(key, None)


@app.get("/ready")
async def ready():
    """Readiness endpoint (the relay's default probe path)."""
    return await _readiness()


@app.get("/healthz")
async def healthz():
    """Alias kept for probes/healthchecks that expect /healthz."""
    return await _readiness()


async def _readiness():
    """Readiness endpoint. A long delay mimics a slow control plane."""
    delay = state["probe_delay"]
    started = time.monotonic()
    if delay:
        await asyncio.sleep(delay)
    status = state["probe_status"]
    return JSONResponse(
        {"upstream": NAME, "status": "ok" if status == 200 else "not_ready"},
        status_code=status,
        headers={"x-upstream": NAME, "x-probe-delay": str(round(time.monotonic() - started, 3))},
    )


@app.post("/_control/probe")
async def control_probe(delay: float = 0.0, status: int = 200):
    """Set readiness latency/status, e.g. POST /_control/probe?delay=4&status=503."""
    if delay < 0 or delay > MAX_PROBE_DELAY:
        return JSONResponse({"error": "delay out of range"}, status_code=400)
    state["probe_delay"] = delay
    state["probe_status"] = status
    return {"upstream": NAME, "probe_delay": delay, "probe_status": status}


@app.get("/_control/probe")
async def control_probe_get():
    return {"upstream": NAME, **state}


@app.post("/_control/first-hit")
async def control_first_hit(request: Request, path: str, delay: float = 1.0,
                            status: int = 200, count: int = 1):
    """Arm slow/failed response headers for the next ``count`` requests to ``path``.

    Example: the next GET /artifact?bytes=4096 stalls one second on response
    headers, while the request after that answers immediately -- the exact shape
    needed to demonstrate a hedged relay retry winning the race::

        curl -X POST 'http://localhost:9001/_control/first-hit?path=/artifact?bytes=4096&delay=1'
    """
    if not path.startswith("/"):
        return JSONResponse({"error": "path must start with '/'"}, status_code=400)
    if delay < 0 or delay > MAX_HEADER_DELAY:
        return JSONResponse({"error": "delay out of range"}, status_code=400)
    count = max(1, count)
    base = str(request.base_url).rstrip("/")
    key = _artifact_key(base + path)
    first_hit_armed[key] = (
        time.monotonic() + FIRST_HIT_TTL_SECONDS,
        (delay, status, count),
    )
    return {"upstream": NAME, "armed": True, "key": key,
            "delay": delay, "status": status, "count": count}


@app.get("/artifact")
async def artifact(
    request: Request,
    bytes: int = 1024 * 1024,
    chunk: int = 65536,
    delay: float = 0.0,
    header_delay: float = 0.0,
):
    """Stream ``bytes`` bytes with ``delay`` seconds between chunks.

    Header latency for the hedge demo comes from two knobs:

    * ``header_delay`` query param — wait this long before response headers on
      *every* request.
    * ``POST /_control/first-hit`` — arm a one-shot (or ``count``-shot) slow or
      failed response header for an artifact path, so a hedged relay's second
      identical GET can win while the first one stalls.
    """
    bytes = max(0, min(bytes, 512 * 1024 * 1024))
    chunk = max(1, min(chunk, 1024 * 1024))
    delay = max(0.0, min(delay, 10.0))
    header_delay = max(0.0, min(header_delay, MAX_HEADER_DELAY))
    header_status = 200

    now = time.monotonic()
    _prune_expired(now)
    key = _artifact_key(str(request.url))
    entry = first_hit_armed.get(key)
    if entry is not None:
        _expiry, (slow_delay, slow_status, remaining) = entry
        header_delay = max(header_delay, slow_delay)
        header_status = slow_status
        if remaining <= 1:
            first_hit_armed.pop(key, None)
        else:
            first_hit_armed[key] = (
                time.monotonic() + FIRST_HIT_TTL_SECONDS,
                (slow_delay, slow_status, remaining - 1),
            )

    started = time.monotonic()
    if header_delay:
        await asyncio.sleep(header_delay)
    if header_status != 200:
        return Response(
            f'{{"upstream":"{NAME}","error":"armed first-hit failure"}}'.encode(),
            status_code=header_status,
            media_type="application/json",
            headers={"x-upstream": NAME, "x-first-hit": "true"},
        )

    def generate() -> AsyncIterator[bytes]:
        async def gen() -> AsyncIterator[bytes]:
            remaining = bytes
            seq = 0
            while remaining > 0:
                size = min(chunk, remaining)
                seed = f"{NAME}:{seq}:".encode()
                block = (seed * (size // len(seed) + 1))[:size]
                yield block
                remaining -= size
                seq += 1
                if delay:
                    await asyncio.sleep(delay)

        return gen()

    headers = {
        "x-upstream": NAME,
        "x-artifact-bytes": str(bytes),
        "x-artifact-delay": str(delay),
        "x-header-delay": str(round(time.monotonic() - started, 3)),
        # No Content-Length: exercises chunk-by-chunk relaying.
    }
    return StreamingResponse(generate(), media_type="application/octet-stream", headers=headers)


@app.get("/")
async def index():
    return {
        "upstream": NAME,
        "probe_delay": state["probe_delay"],
        "probe_status": state["probe_status"],
        "first_hit_armed": list(first_hit_armed),
    }
