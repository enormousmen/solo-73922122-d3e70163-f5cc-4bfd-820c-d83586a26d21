"""Controllable upstream used by the integration tests."""

from __future__ import annotations

import asyncio
import time

from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

PATTERN = b"0123456789abcdef"


async def _artifact(request):
    params = request.query_params
    size = int(params.get("bytes", 1024))
    chunk = max(1, int(params.get("chunk", 64)))
    delay = float(params.get("delay", 0))
    header_delay = float(params.get("header_delay", 0))
    stats = request.app.state.stats
    stats["started"] += 1
    state = {"finished": False}

    if header_delay:
        await asyncio.sleep(header_delay)

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
            Route("/_state", _state),
        ]
    )
    app.state.name = name
    app.state.probe_delay = 0.0
    app.state.probe_status = 200
    app.state.stats = {"started": 0, "completed": 0, "cancelled": 0, "aborted": 0}
    app.state.probes = 0
    return app
