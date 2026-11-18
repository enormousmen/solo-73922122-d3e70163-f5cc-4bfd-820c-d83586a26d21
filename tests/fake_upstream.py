"""Controllable upstream used by the integration tests."""

from __future__ import annotations

import asyncio
import time
from urllib.parse import parse_qsl, urlencode, urlsplit

from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

PATTERN = b"0123456789abcdef"
FIRST_HIT_TTL_SECONDS = 120.0


def _artifact_key_from_url(raw_url: str) -> str:
    parts = urlsplit(raw_url)
    params = parse_qsl(parts.query, keep_blank_values=True)
    return parts.path + ("?" + urlencode(params) if params else "")


def _prune(app) -> None:
    now = time.monotonic()
    for key in list(app.state.first_hit):
        if app.state.first_hit[key][0] <= now:
            app.state.first_hit.pop(key, None)


async def _artifact(request):
    params = request.query_params
    size = int(params.get("bytes", 1024))
    chunk = max(1, int(params.get("chunk", 64)))
    delay = float(params.get("delay", 0))
    header_delay = float(params.get("header_delay", 0))

    # Out-of-band one-shot arming (POST /_control/first-hit): only the next N
    # requests matching the key are slowed/failed.  This is what lets a hedged
    # relay's second identical GET win instead of being slowed the same way.
    header_status = 200
    armed = request.app.state.first_hit
    _prune(request.app)
    key = entry_key(request)
    entry = armed.get(key)
    if entry is not None:
        _expiry, payload = entry
        slow_delay, slow_status, remaining = payload
        header_delay = max(header_delay, slow_delay)
        header_status = slow_status
        if remaining <= 1:
            armed.pop(key, None)
        else:
            armed[key] = (
                time.monotonic() + FIRST_HIT_TTL_SECONDS,
                (slow_delay, slow_status, remaining - 1),
            )

    stats = request.app.state.stats
    stats["started"] += 1
    state = {"finished": False}

    if header_delay:
        await asyncio.sleep(header_delay)
    if header_status != 200:
        stats["non_200"] += 1
        return Response(
            b'{"error":"armed first-hit failure"}',
            status_code=header_status,
            media_type="application/json",
            headers={"x-first-hit": "true"},
        )

    async def gen():
        try:
            sent = 0
            while sent < size:
                n = min(chunk, size - sent)
                block = (PATTERN * (n // len(PATTERN) + 1))[:n]
                yield block
                sent += n
                if delay:
                    await asyncio.sleep(delay)
            state["finished"] = True
            stats["completed"] += 1
        except asyncio.CancelledError:
            stats["cancelled"] += 1
            raise
        finally:
            if not state["finished"]:
                stats["aborted"] += 1

    return StreamingResponse(gen(), media_type="application/octet-stream")


def entry_key(request) -> str:
    return _artifact_key_from_url(str(request.url))


async def _first_hit_control(request):
    """Arm a one-shot slow/failed response header for an artifact key.

    POST /_control/first-hit?path=/artifact?bytes=1&delay=2&status=200&count=1
    """
    q = request.query_params
    path = q.get("path")
    if not path or not path.startswith("/"):
        return JSONResponse({"error": "path must start with '/'"}, status_code=400)
    delay = float(q.get("delay", 1.0))
    status_code = int(q.get("status", 200))
    count = max(1, int(q.get("count", 1)))
    base = str(request.base_url).rstrip("/")
    key = _artifact_key_from_url(base + path)
    request.app.state.first_hit[key] = (
        time.monotonic() + FIRST_HIT_TTL_SECONDS,
        (delay, status_code, count),
    )
    return JSONResponse({"armed": True, "key": key, "delay": delay,
                         "status": status_code, "count": count})


async def _fixed(request):
    """Fixed-length response: verifies content-length pass-through."""
    request.app.state.stats["started"] += 1
    body = b"fixed-length-body" * 128
    headers = {"content-length": str(len(body)), "x-upstream-fixed": "1"}
    request.app.state.stats["completed"] += 1
    return Response(body, media_type="application/octet-stream", headers=headers)


async def _healthz(request):
    app = request.app
    app.state.probes += 1
    started = time.monotonic()
    if app.state.probe_delay:
        await asyncio.sleep(app.state.probe_delay)
    status = app.state.probe_status
    return JSONResponse(
        {"status": "ok" if status == 200 else "not_ready"},
        status_code=status,
        headers={"x-probe-delay": f"{time.monotonic() - started:.3f}"},
    )


async def _control(request):
    app = request.app
    if "delay" in request.query_params:
        app.state.probe_delay = float(request.query_params["delay"])
    if "status" in request.query_params:
        app.state.probe_status = int(request.query_params["status"])
    return JSONResponse({"probe_delay": app.state.probe_delay, "probe_status": app.state.probe_status})


async def _state(request):
    app = request.app
    return JSONResponse(
        {
            "name": app.state.name,
            "probe_delay": app.state.probe_delay,
            "probe_status": app.state.probe_status,
            "stats": app.state.stats,
            "probes": app.state.probes,
            "first_hit_armed": list(app.state.first_hit),
        }
    )


def make_app(name: str) -> Starlette:
    app = Starlette(
        routes=[
            Route("/ready", _healthz),
            Route("/healthz", _healthz),
            Route("/artifact", _artifact),
            Route("/fixed", _fixed),
            Route("/_control/probe", _control, methods=["POST", "GET"]),
            Route("/_control/first-hit", _first_hit_control, methods=["POST"]),
            Route("/_state", _state),
        ]
    )
    app.state.name = name
    app.state.probe_delay = 0.0
    app.state.probe_status = 200
    app.state.stats = {
        "started": 0,
        "completed": 0,
        "cancelled": 0,
        "aborted": 0,
        "non_200": 0,
    }
    app.state.probes = 0
    app.state.first_hit = {}
    return app
