"""Demo artifact upstream with controllable readiness latency and slow downloads.

Run two copies (blue/green).  Every response identifies itself via
``X-Upstream-Name`` so the relay demo makes routing visible.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
PROBE_DEFAULT_DELAY = float(os.environ.get("UPSTREAM_PROBE_DELAY", "0"))
PROBE_DEFAULT_STATUS = int(os.environ.get("UPSTREAM_PROBE_STATUS", "200"))
MAX_PROBE_DELAY = 120.0

app = FastAPI(title=f"Artifact upstream {NAME}")
state = {"probe_delay": PROBE_DEFAULT_DELAY, "probe_status": PROBE_DEFAULT_STATUS}


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


@app.get("/artifact")
async def artifact(
    request: Request,
    bytes: int = 1024 * 1024,
    chunk: int = 65536,
    delay: float = 0.0,
):
    """Stream ``bytes`` bytes with ``delay`` seconds between chunks."""
    bytes = max(0, min(bytes, 512 * 1024 * 1024))
    chunk = max(1, min(chunk, 1024 * 1024))
    delay = max(0.0, min(delay, 10.0))

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
        # No Content-Length: exercises chunk-by-chunk relaying.
    }
    return StreamingResponse(generate(), media_type="application/octet-stream", headers=headers)


@app.get("/")
async def index():
    return {"upstream": NAME, "probe_delay": state["probe_delay"], "probe_status": state["probe_status"]}
