"""Integration tests for POST /api/hedged-download.

Covers the hedge race (slow first headers -> second attempt wins), the
"any status before the hedge delay behaves like an ordinary GET" rule, total
failure, drain/shutdown and client-disconnect interruption, in-flight cleanup,
and OpenAPI visibility.
"""

from __future__ import annotations

import asyncio
import time

import aiohttp
import pytest

from .test_proxy_integration import stack, status

pytestmark = pytest.mark.asyncio

HEDGE_PATH = "/api/hedged-download"


def hedge_payload(path: str) -> dict:
    return {"path": path}


async def post_hedge(session: aiohttp.ClientSession, base: str, path: str):
    return await session.post(base + HEDGE_PATH, json=hedge_payload(path))


async def arm_first_hit(upstream_base: str, path: str, *, delay: float,
                        status_code: int = 200, count: int = 1) -> dict:
    """Tell the demo upstream: the next ``count`` GETs matching ``path`` stall
    (or fail) on response headers; later identical requests answer at once."""
    async with aiohttp.ClientSession() as session:
        async with session.post(
            upstream_base + "/_control/first-hit",
            params={"path": path, "delay": delay, "status": status_code, "count": count},
        ) as response:
            assert response.status == 200
            return await response.json()


async def upstream_state(base: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(base + "/_state") as r:
            return await r.json()


async def test_fast_first_response_skips_second_attempt_and_streams_body():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with await post_hedge(
                session, s["base"], "/artifact?bytes=50000&chunk=1024&t=fast"
            ) as response:
                assert response.status == 200
                assert response.headers["x-relay-hedged"] == "false"
                assert response.headers["x-relay-hedge-attempt"] == "1"
                body = await response.read()
        assert len(body) == 50000
        assert body[:16] == b"0123456789abcdef"
        # Only one upstream GET was ever started: the 300ms hedge never fired.
        stats = await upstream_state(s["blue"])
        assert stats["stats"]["started"] == 1


async def test_slow_first_headers_trigger_second_attempt_which_wins():
    # First GET stalls 2s on headers; the hedged retry answers immediately.
    artifact = "/artifact?bytes=4096&chunk=1024&t=hedge-win"
    async with stack() as s:
        await arm_first_hit(s["blue"], artifact, delay=2.0)
        async with aiohttp.ClientSession() as session:
            started = time.monotonic()
            async with await post_hedge(session, s["base"], artifact) as response:
                elapsed = time.monotonic() - started
                assert response.status == 200
                assert response.headers["x-relay-hedged"] == "true"
                assert response.headers["x-relay-hedge-attempt"] == "2"
                body = await response.read()
        # Must return well before the stalled first attempt would have answered.
        assert elapsed < 1.5
        assert len(body) == 4096

        stats = await upstream_state(s["blue"])
        assert stats["stats"]["started"] == 2
        # The losing first attempt was released before its (delayed) response
        # was ever produced: exactly one full body was completed, so a success
        # body can never be spliced from two upstream responses.
        assert stats["stats"]["completed"] == 1
        # The slow generator never ran to completion on the cancelled attempt.
        assert stats["stats"]["aborted"] + stats["stats"]["cancelled"] <= 1


async def test_first_attempt_wins_when_headers_arrive_just_inside_hedge_window():
    # 100 ms header delay is inside the default 300 ms window: no second attempt.
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with await post_hedge(
                session,
                s["base"],
                "/artifact?bytes=1024&chunk=256&header_delay=0.1&t=inside-window",
            ) as response:
                assert response.status == 200
                assert response.headers["x-relay-hedged"] == "false"
                assert response.headers["x-relay-hedge-attempt"] == "1"
                assert len(await response.read()) == 1024
        stats = await upstream_state(s["blue"])
        assert stats["stats"]["started"] == 1


async def test_any_status_before_hedge_delay_is_forwarded_like_ordinary_get():
    # An immediate 404 settles the request before the hedge timer fires; the
    # relay must not fire a pointless second GET.
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with await post_hedge(session, s["base"], "/no-such-path?t=early-404") as r:
                assert r.status == 404
                assert r.headers["x-relay-hedged"] == "false"
                assert r.headers["x-relay-hedge-attempt"] == "1"
                await r.read()
        stats = await upstream_state(s["blue"])
        assert stats["stats"]["started"] == 0  # 404 route does not count artifact stats


async def test_non_200_after_hedge_window_loses_to_a_later_200():
    # First attempt: slow (past 300ms) 503. Second attempt: immediate 200.
    artifact = "/artifact?bytes=2048&chunk=512&t=slow-503"
    async with stack() as s:
        await arm_first_hit(s["blue"], artifact, delay=0.8, status_code=503)
        async with aiohttp.ClientSession() as session:
            async with await post_hedge(session, s["base"], artifact) as response:
                assert response.status == 200
                assert response.headers["x-relay-hedged"] == "true"
                assert response.headers["x-relay-hedge-attempt"] == "2"
                body = await response.read()
        assert len(body) == 2048
        stats = await upstream_state(s["blue"])
        assert stats["stats"]["started"] == 2
        # The 503 attempt was cancelled at the socket before its delayed
        # response was produced, so only the winning 200 completed a body.
        assert stats["stats"]["completed"] == 1


async def test_both_attempts_non_200_returns_failure_never_success_file():
    artifact = "/artifact?bytes=1024&t=both-503"
    async with stack() as s:
        # Both attempts arrive while the two-shot arming is active.
        await arm_first_hit(s["blue"], artifact, delay=0.5, status_code=503, count=2)
        async with aiohttp.ClientSession() as session:
            async with await post_hedge(session, s["base"], artifact) as response:
                assert response.status == 503
                assert response.headers["x-relay-hedged"] == "true"
                payload = await response.json()
                # A JSON error document is returned, never a streamed artifact.
                assert isinstance(payload, dict)
        stats = await upstream_state(s["blue"])
        assert stats["stats"]["started"] == 2
        assert stats["stats"]["completed"] == 0


async def test_both_attempts_unreachable_returns_502_and_clears_in_flight():
    async with stack() as s:
        await s["upstreams"]["blue"].stop()
        async with aiohttp.ClientSession() as session:
            async with await post_hedge(
                session, s["base"], "/artifact?bytes=1024&t=dead-upstream"
            ) as response:
                assert response.status == 502
                payload = await response.json()
                assert payload["error"] == "upstream_error"
        snap = await status(s["base"])
        assert all(g["in_flight"] == 0 for g in snap["generations"])


async def test_first_attempt_failure_fires_hedge_immediately_without_waiting_delay():
    # A refused connection fails the first attempt at once: the second attempt
    # must go out immediately rather than after the full 300ms hedge window, and
    # its failure must come back as a 502 (never as a success file).
    from .conftest import free_port
    from relay.config import load_config
    from relay.proxy import RelayApp
    from .test_proxy_integration import Server

    dead_port = free_port()  # nothing listens here -> ECONNREFUSED
    relay_port = free_port()
    config = load_config(
        {"RELAY_TARGETS": f"dead=http://127.0.0.1:{dead_port}",
         "RELAY_INITIAL_TARGET": "dead"}
    )
    relay_app = RelayApp(config)
    relay = await Server(relay_app.app, relay_port).start()
    base = f"http://127.0.0.1:{relay_port}"
    try:
        async with aiohttp.ClientSession() as session:
            started = time.monotonic()
            async with await post_hedge(
                session, base, "/artifact?bytes=1024&t=fast-fail"
            ) as response:
                elapsed = time.monotonic() - started
                assert response.status == 502
                assert (await response.json())["error"] == "upstream_error"
        # Two immediate refusals: well under one hedge delay (300ms) in total.
        assert elapsed < 0.25
        snap = relay_app.state.snapshot()
        assert all(g["in_flight"] == 0 for g in snap["generations"])
    finally:
        await relay.stop()


async def test_query_string_is_forwarded_verbatim_to_both_attempts():
    artifact = "/artifact?bytes=777&chunk=333&t=query-check"
    async with stack() as s:
        await arm_first_hit(s["blue"], artifact, delay=1.0)
        async with aiohttp.ClientSession() as session:
            async with await post_hedge(session, s["base"], artifact) as response:
                assert response.status == 200
                body = await response.read()
        # The 777-byte artifact described by the query string arrives intact.
        assert len(body) == 777
        stats = await upstream_state(s["blue"])
        assert stats["stats"]["started"] == 2


async def test_path_validation_rejects_absolute_and_relative_non_slash_paths():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            for bad in ("artifact?bytes=1", "http://evil.example/x", "//evil.example/x"):
                async with session.post(s["base"] + HEDGE_PATH, json={"path": bad}) as r:
                    assert r.status == 422, bad
            async with session.post(s["base"] + HEDGE_PATH, json={}) as r:
                assert r.status == 422


async def test_drain_expiry_aborts_both_attempts_and_clears_registration():
    async with stack(drain_seconds=0.5) as s:
        async with aiohttp.ClientSession() as session:
            pending = asyncio.create_task(
                post_hedge(
                    session,
                    s["base"],
                    "/artifact?bytes=1024&header_delay=3&t=drain-before-headers",
                )
            )
            await asyncio.sleep(0.15)
            snap = await status(s["base"])
            assert snap["generations"][0]["in_flight"] == 1

            code, _ = await _switch(session, s["base"], "green")
            assert code == 200
            response = await asyncio.wait_for(pending, timeout=5)
            try:
                assert response.status == 503
                assert (await response.json())["error"] == "generation_draining"
            finally:
                response.close()
        # Both attempts were cancelled; the slot is released after connections.
        for _ in range(50):
            snap = await status(s["base"])
            if snap["generations"][0]["in_flight"] == 0:
                break
            await asyncio.sleep(0.05)
        assert snap["generations"][0]["in_flight"] == 0


async def test_client_disconnect_aborts_both_attempts_and_releases_slot():
    # Long slow body: the winning stream stays in flight long enough to observe
    # the slot, while the stalled first attempt is cancelled when the winner is
    # chosen.  Dropping the client must abort everything and clear the slot.
    artifact = "/artifact?bytes=2000000&chunk=2000&delay=0.02&t=client-gone"
    async with stack(drain_seconds=30.0) as s:
        await arm_first_hit(s["blue"], artifact, delay=3.0)
        async with aiohttp.ClientSession() as aio_session:
            response = await post_hedge(aio_session, s["base"], artifact)
            assert response.status == 200
            assert response.headers["x-relay-hedged"] == "true"
            await asyncio.sleep(0.1)
            assert (await status(s["base"]))["generations"][0]["in_flight"] == 1
            response.close()
            # Let the disconnect propagate and the relay cancel the live stream.
            for _ in range(60):
                snap = await status(s["base"])
                if snap["generations"][0]["in_flight"] == 0:
                    break
                await asyncio.sleep(0.1)
            assert snap["generations"][0]["in_flight"] == 0
        stats = await upstream_state(s["blue"])
        assert stats["stats"]["started"] == 2
        # Only the winner streamed; the client left before it finished.
        assert stats["stats"]["completed"] == 0


async def test_drain_during_streaming_body_truncates_without_switching_source():
    # The winner's headers are already on the wire; a later drain must stop the
    # one chosen stream, never splice in the (already cancelled) sibling.
    artifact = "/artifact?bytes=2000000&chunk=2000&delay=0.02&t=drain-mid-body"
    async with stack(drain_seconds=1.0) as s:
        await arm_first_hit(s["blue"], artifact, delay=1.0)
        async with aiohttp.ClientSession() as session:
            response = await post_hedge(session, s["base"], artifact)
            assert response.status == 200
            assert response.headers["x-relay-hedged"] == "true"
            code, _ = await _switch(session, s["base"], "green")
            assert code == 200
            with pytest.raises(aiohttp.ClientError):
                await response.read()
            response.close()
        for _ in range(50):
            snap = await status(s["base"])
            if snap["generations"][0]["in_flight"] == 0:
                break
            await asyncio.sleep(0.05)
        assert snap["generations"][0]["in_flight"] == 0


async def test_slow_hedged_download_does_not_block_status_or_switch():
    artifact = "/artifact?bytes=2000000&chunk=2000&delay=0.02&t=not-blocking"
    async with stack(drain_seconds=30.0) as s:
        await arm_first_hit(s["blue"], artifact, delay=1.0)
        async with aiohttp.ClientSession() as session:
            download = asyncio.create_task(post_hedge(session, s["base"], artifact))
            await asyncio.sleep(0.1)
            t0 = time.monotonic()
            snap_task = asyncio.create_task(status(s["base"]))
            switch_task = asyncio.create_task(_switch(session, s["base"], "green"))
            snap, (code, _) = await asyncio.gather(snap_task, switch_task)
            assert time.monotonic() - t0 < 1.0
            assert code == 200
            assert snap["generations"][0]["in_flight"] == 1
            response = await download
            response.close()


async def test_hedged_endpoint_is_in_openapi_and_docs():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(s["base"] + "/openapi.json") as r:
                spec = await r.json()
            async with session.get(s["base"] + "/docs") as r:
                assert r.status == 200
        assert "/api/hedged-download" in spec["paths"]
        assert "post" in spec["paths"]["/api/hedged-download"]


async def _switch(session: aiohttp.ClientSession, base: str, target: str):
    async with session.post(
        base + "/api/switch",
        json={"target": target, "wait": True, "wait_timeout": 5},
    ) as response:
        return response.status, await response.json()
