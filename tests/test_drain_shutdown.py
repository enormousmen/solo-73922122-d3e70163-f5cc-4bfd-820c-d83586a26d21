"""Lifecycle edges: relay shutdown aborts everything, and pending probes are
cancelled.  Uses the state object directly plus a real relay server."""

import asyncio
import contextlib
import time

import aiohttp

from .test_proxy_integration import stack, status


async def test_shutdown_interrupts_all_in_flight_requests():
    async with stack(drain_seconds=60.0) as s:
        async with aiohttp.ClientSession() as session:
            response = await session.get(f"{s['base']}/artifact?bytes=2000000&chunk=2000&delay=0.02")
            await asyncio.sleep(0.1)
            assert (await status(s["base"]))["generations"][0]["in_flight"] == 1

            started = time.monotonic()
            await s["relay"].stop()  # bounded by timeout_graceful_shutdown=1
            assert time.monotonic() - started < 4.0
            with contextlib.suppress(aiohttp.ClientError, asyncio.TimeoutError):
                await asyncio.wait_for(response.read(), timeout=5)
            snap = s["state"].snapshot()
            assert snap["generations"][0]["in_flight"] == 0
