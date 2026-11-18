"""End-to-end tests: real relay process handling + real upstream servers.

Each test starts two in-process upstreams (blue/green) and the relay
application over real TCP sockets, and then drives it with an HTTP client.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import aiohttp
import pytest
import uvicorn

from relay.config import load_config
from relay.proxy import RelayApp

from .conftest import free_port
from .fake_upstream import make_app

pytestmark = pytest.mark.asyncio


class Server:
    def __init__(self, app, port: int, **uvicorn_kwargs):
        self.port = port
        self.config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
            **uvicorn_kwargs,
        )
        self.server = uvicorn.Server(self.config)
        self.task: asyncio.Task | None = None

    async def start(self):
        self.task = asyncio.create_task(self.server.serve())
        deadline = time.monotonic() + 10
        while not self.server.started:
            if self.task.done():
                self.task.result()
            if time.monotonic() > deadline:
                raise RuntimeError("server did not start")
            await asyncio.sleep(0.01)
        return self

    async def stop(self):
        self.server.should_exit = True
        if self.task is not None:
            with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(self.task, timeout=5)


async def start_stack(drain_seconds: float = 1.0, probe_timeout: float = 0.5):
    ports = {name: free_port() for name in ("blue", "green", "relay")}
    upstreams = {}
    for name in ("blue", "green"):
        upstreams[name] = await Server(make_app(name), ports[name]).start()
    config = load_config(
        {
            "RELAY_TARGETS": f"blue=http://127.0.0.1:{ports['blue']},green=http://127.0.0.1:{ports['green']}",
            "RELAY_INITIAL_TARGET": "blue",
            "RELAY_DRAIN_SECONDS": str(drain_seconds),
            "RELAY_PROBE_TIMEOUT": str(probe_timeout),
        }
    )
    relay_app = RelayApp(config)
    relay = await Server(relay_app.app, ports["relay"], timeout_graceful_shutdown=1).start()
    return {
        "ports": ports,
        "upstreams": upstreams,
        "relay": relay,
        "state": relay_app.state,
        "config": config,
        "base": f"http://127.0.0.1:{ports['relay']}",
        "blue": f"http://127.0.0.1:{ports['blue']}",
        "green": f"http://127.0.0.1:{ports['green']}",
    }


async def stop_stack(stack):
    await stack["relay"].stop()
    for server in stack["upstreams"].values():
        await server.stop()


@contextlib.asynccontextmanager
async def stack(**kwargs):
    s = await start_stack(**kwargs)
    try:
        yield s
    finally:
        await stop_stack(s)


async def status(base: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(base + "/api/status") as response:
            return await response.json()


async def switch(base: str, target: str, wait: bool = True) -> tuple[int, dict]:
    async with aiohttp.ClientSession() as session:
        async with session.post(
            base + "/api/switch", json={"target": target, "wait": wait, "wait_timeout": 5}
        ) as response:
            return response.status, await response.json()


async def control(url: str, **params):
    async with aiohttp.ClientSession() as session:
        async with session.post(url + "/_control/probe", params=params) as response:
            return await response.json()


async def test_get_streams_from_current_target_and_preserves_bytes():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{s['base']}/artifact?bytes=200000&chunk=8192") as response:
                assert response.status == 200
                body = await response.read()
                assert response.headers["x-relay-target"] == "blue"
                assert response.headers["x-relay-generation"] == "1"
                assert response.headers["content-type"] == "application/octet-stream"
                assert "connection" not in {k.lower() for k in response.headers}
        assert len(body) == 200000
        assert body[:16] == b"0123456789abcdef"
        assert body == (b"0123456789abcdef" * (200000 // 16 + 1))[:200000]


async def test_switch_commits_and_status_reports_generations():
    async with stack() as s:
        code, payload = await switch(s["base"], "green")
        assert code == 200
        assert payload["operation"]["status"] == "committed"
        assert payload["operation"]["probe_status"] == 200
        assert payload["current"] == "green"

        snap = await status(s["base"])
        assert snap["current_target"] == "green"
        assert snap["current_generation"] == 2
        assert [g["state"] for g in snap["generations"]] == ["active", "draining"]
        assert snap["recent_operations"][0]["status"] == "committed"

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{s['base']}/artifact?bytes=32&chunk=16") as response:
                assert response.headers["x-relay-target"] == "green"


async def test_failed_probe_keeps_current_target():
    async with stack() as s:
        await control(s["green"], status=503)
        code, payload = await switch(s["base"], "green")
        assert code == 200
        assert payload["operation"]["status"] == "failed"
        assert payload["current"] == "blue"
        assert "HTTP 503" in payload["operation"]["reason"]
        snap = await status(s["base"])
        assert snap["current_generation"] == 1


async def test_slow_probe_does_not_block_status_or_switch_api():
    async with stack(probe_timeout=5.0) as s:
        await control(s["green"], delay=1.5)
        started = time.monotonic()
        code, payload = await switch(s["base"], "green", wait=False)
        assert code == 202
        assert payload["operation"]["status"] == "pending"
        # The API returned instantly even though the probe takes 1.5s.
        assert time.monotonic() - started < 0.5

        # Status queries stay responsive while the probe is in flight.
        t0 = time.monotonic()
        snap = await status(s["base"])
        assert time.monotonic() - t0 < 0.3
        assert snap["pending"]["target"] == "green"

        # The per-operation endpoint reports the same pending state.
        async with aiohttp.ClientSession() as session:
            async with session.get(s["base"] + f"/api/operations/{payload['operation']['id']}") as r:
                assert r.status == 200
                assert (await r.json())["operation"]["status"] == "pending"

        # Wait for the probe to finish and confirm it committed.
        for _ in range(100):
            snap = await status(s["base"])
            if snap["pending"] is None:
                break
            await asyncio.sleep(0.05)
        assert snap["current_target"] == "green"


async def test_newer_switch_supersedes_pending_candidate_and_late_probe_cannot_revert():
    async with stack(probe_timeout=5.0) as s:
        await control(s["green"], delay=0.8)  # green will succeed, but late
        await control(s["blue"], delay=0)  # blue (current target) is instant
        code, first = await switch(s["base"], "green", wait=False)
        assert code == 202 and first["operation"]["status"] == "pending"
        await asyncio.sleep(0.15)
        # Supersede with a switch to the *current* target: it must commit as a
        # no-op and must not be reverted when the stale green probe resolves.
        code, second = await switch(s["base"], "blue")
        assert code == 200 and second["operation"]["status"] == "noop"
        assert second["current"] == "blue"

        # Let the superseded green probe resolve (it would have returned 200).
        await asyncio.sleep(1.2)
        snap = await status(s["base"])
        assert snap["current_target"] == "blue"
        assert snap["current_generation"] == second["current_generation"]
        ops = {op["id"]: op for op in snap["recent_operations"]}
        assert ops[first["operation"]["id"]]["status"] == "superseded"
        assert ops[first["operation"]["id"]]["superseded_by"] == second["operation"]["id"]


async def test_in_flight_download_uses_pinned_generation_and_is_drained_after_window():
    async with stack(drain_seconds=1.0) as s:
        # ~6s download (300 chunks * 0.02s)
        async with aiohttp.ClientSession() as session:
            slow_task = asyncio.create_task(
                session.get(f"{s['base']}/artifact?bytes=1200000&chunk=4000&delay=0.02")
            )
            response = await slow_task
            assert response.status == 200
            assert response.headers["x-relay-target"] == "blue"
            await asyncio.sleep(0.1)

            snap = await status(s["base"])
            assert snap["generations"][0]["in_flight"] == 1

            switch_started = time.monotonic()
            code, payload = await switch(s["base"], "green")
            assert code == 200 and payload["current"] == "green"

            # New requests go to green immediately.
            async with session.get(f"{s['base']}/artifact?bytes=1000&chunk=100") as fresh:
                assert fresh.headers["x-relay-target"] == "green"
                await fresh.read()

            # The blue response keeps streaming during the 1s drain window.
            chunk = await asyncio.wait_for(response.content.readany(), timeout=0.6)
            assert chunk

            # ...and is cut off when the window expires.
            with pytest.raises(aiohttp.ClientError):
                async for _ in response.content.iter_any():
                    pass
            elapsed = time.monotonic() - switch_started
            assert 0.6 < elapsed < 3.0

            snap = await status(s["base"])
            old = [g for g in snap["generations"] if g["id"] == 1][0]
            assert old["state"] == "closed"
            assert old["in_flight"] == 0
            response.close()


async def test_drain_does_not_break_new_generation_after_switch_back_to_same_url():
    async with stack(drain_seconds=2.0) as s:
        async with aiohttp.ClientSession() as session:
            # Start a green download in generation 2 (after switching to green).
            code, _ = await switch(s["base"], "green")
            assert code == 200
            gen2_download = await session.get(
                f"{s['base']}/artifact?bytes=2000000&chunk=4000&delay=0.02"
            )
            assert gen2_download.headers["x-relay-generation"] == "2"
            assert gen2_download.headers["x-relay-target"] == "green"

            # Switch to blue, then back to green: generation 4 is a *new* epoch
            # on the same address as generation 2. Generation 2 starts draining,
            # but the download on generation 4 must survive its drain expiry.
            code, _ = await switch(s["base"], "blue")
            assert code == 200
            code, _ = await switch(s["base"], "green")
            assert code == 200
            gen4_download = await session.get(
                f"{s['base']}/artifact?bytes=1200000&chunk=4000&delay=0.02"
            )
            assert gen4_download.headers["x-relay-generation"] == "4"
            assert gen4_download.headers["x-relay-target"] == "green"

            # Generation 2's drain expires during generation 4's body.
            with pytest.raises(aiohttp.ClientError):
                await gen2_download.read()
            data = await asyncio.wait_for(gen4_download.read(), timeout=20)
            assert len(data) == 1200000
            snap = await status(s["base"])
            current = [g for g in snap["generations"] if g["id"] == 4][0]
            assert current["state"] == "active" and current["in_flight"] == 0


async def test_pipelined_keepalive_requests_use_target_at_accept_time():
    async with stack(drain_seconds=5.0) as s:
        host, port = "127.0.0.1", s["ports"]["relay"]

        async def read_response(reader):
            status_line = await reader.readline()
            headers = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                key, _, value = line.decode().partition(":")
                headers[key.strip().lower()] = value.strip()
            if headers.get("transfer-encoding", "").lower() == "chunked":
                body = b""
                while True:
                    size_line = await reader.readline()
                    size = int(size_line.split(b";")[0].strip(), 16)
                    if size == 0:
                        await reader.readline()  # trailing CRLF
                        break
                    body += await reader.readexactly(size)
                    await reader.readexactly(2)
            else:
                length = int(headers.get("content-length", "0"))
                body = await reader.readexactly(length) if length else b""
            return status_line, headers, body

        reader, writer = await asyncio.open_connection(host, port)
        try:
            # First request on generation 1 (blue).
            writer.write(b"GET /artifact?bytes=200&chunk=100 HTTP/1.1\r\nHost: relay\r\n\r\n")
            await writer.drain()
            line, headers, body = await read_response(reader)
            assert b"200" in line and headers["x-relay-target"] == "blue"
            assert headers["x-relay-generation"] == "1"

            code, _ = await switch(s["base"], "green")
            assert code == 200

            # Second request on the same connection is routed by the target at
            # its own accept time: green / generation 2.
            writer.write(b"GET /artifact?bytes=200&chunk=100 HTTP/1.1\r\nHost: relay\r\n\r\n")
            await writer.drain()
            line, headers, body = await read_response(reader)
            assert b"200" in line and headers["x-relay-target"] == "green"
            assert headers["x-relay-generation"] == "2"

            # Third request: switch back to blue, same connection again.
            code, _ = await switch(s["base"], "blue")
            writer.write(b"GET /artifact?bytes=200&chunk=100 HTTP/1.1\r\nHost: relay\r\n\r\n")
            await writer.drain()
            line, headers, body = await read_response(reader)
            assert headers["x-relay-target"] == "blue"
            assert headers["x-relay-generation"] == "3"
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


async def test_slow_download_does_not_block_switch_or_status():
    async with stack(drain_seconds=5.0) as s:
        async with aiohttp.ClientSession() as session:
            download = await session.get(f"{s['base']}/artifact?bytes=2000000&chunk=2000&delay=0.02")
            try:
                await asyncio.sleep(0.1)
                t0 = time.monotonic()
                p1 = asyncio.create_task(status(s["base"]))
                p2 = asyncio.create_task(switch(s["base"], "green"))
                snap, result = await asyncio.gather(p1, p2)
                assert time.monotonic() - t0 < 1.0
                assert snap["generations"][0]["in_flight"] == 1
                assert result[0] == 200
            finally:
                download.close()


async def test_unknown_target_and_validation_errors():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.post(s["base"] + "/api/switch", json={"target": "nope"}) as r:
                assert r.status == 404
            async with session.post(s["base"] + "/api/switch", json={}) as r:
                assert r.status == 422
        snap = await status(s["base"])
        assert snap["current_target"] == "blue"


async def test_docs_and_openapi_available():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(s["base"] + "/docs") as r:
                assert r.status == 200
                assert "swagger" in (await r.text()).lower()
            async with session.get(s["base"] + "/demo") as r:
                assert r.status == 200
                assert "Artifact Relay" in await r.text()
            async with session.get(s["base"] + "/openapi.json") as r:
                spec = await r.json()
        paths = spec["paths"]
        assert "/api/switch" in paths and "/api/status" in paths
        assert "post" in paths["/api/switch"]


async def test_dead_upstream_fails_probe_and_releases_slots():
    async with stack() as s:
        # green is initially healthy; move traffic there first.
        code, _ = await switch(s["base"], "green")
        assert code == 200
        # Kill blue, then switch to it -> failed probe keeps green.
        await s["upstreams"]["blue"].stop()
        code, payload = await switch(s["base"], "blue")
        assert payload["operation"]["status"] == "failed"
        assert payload["current"] == "green"
        snap = await status(s["base"])
        assert snap["current_target"] == "green"
        assert all(g["in_flight"] == 0 for g in snap["generations"])


async def test_probe_timeout_keeps_current_target():
    async with stack(probe_timeout=0.3) as s:
        await control(s["green"], delay=1.5)
        started = time.monotonic()
        code, payload = await switch(s["base"], "green")
        elapsed = time.monotonic() - started
        assert code == 200
        assert payload["operation"]["status"] == "failed"
        assert payload["current"] == "blue"
        assert elapsed < 1.4
        snap = await status(s["base"])
        assert snap["current_generation"] == 1
        # Green never receives proxied traffic.
        assert snap["generations"][0]["target"] == "blue"


async def test_request_that_cannot_reach_upstream_before_drain_expiry_gets_503():
    """A GET accepted on the old generation, still waiting for upstream response
    headers when the drain window ends, is rejected rather than left hanging."""
    async with stack(drain_seconds=0.5) as s:
        async with aiohttp.ClientSession() as session:
            pending = asyncio.create_task(
                session.get(f"{s['base']}/artifact?bytes=1000&chunk=100&header_delay=3")
            )
            await asyncio.sleep(0.15)
            assert (await status(s["base"]))["generations"][0]["in_flight"] == 1
            code, _ = await switch(s["base"], "green")
            assert code == 200
            response = await asyncio.wait_for(pending, timeout=5)
            assert response.status == 503
            assert (await response.json())["error"] == "generation_draining"
            response.close()
        for _ in range(40):
            snap = await status(s["base"])
            if snap["generations"][0]["state"] == "closed":
                break
            await asyncio.sleep(0.05)
        assert snap["generations"][0]["in_flight"] == 0


async def test_query_string_and_escaped_path_are_forwarded_verbatim():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(s["base"] + "/artifact?bytes=64&chunk=32&delay=0") as response:
                assert response.status == 200
                await response.read()
            # Percent-encoded path segment reaches the upstream unchanged
            # (404 from the upstream, not a relay error).
            async with session.get(s["base"] + "/fixtures/a%20b.bin") as response:
                assert response.status == 404
                assert response.headers["x-relay-target"] == "blue"
                await response.read()


async def test_head_request_is_forwarded_without_body():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.head(f"{s['base']}/artifact?bytes=4096&chunk=1024") as response:
                assert response.status == 200
                assert response.headers["x-relay-target"] == "blue"
                assert await response.read() == b""
        snap = await status(s["base"])
        assert snap["generations"][0]["in_flight"] == 0


async def test_upstream_non_200_status_and_headers_are_forwarded():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{s['base']}/no-such-path") as response:
                assert response.status == 404
                assert response.headers["x-relay-target"] == "blue"
                await response.read()


async def test_client_disconnect_releases_in_flight_slot():
    async with stack(drain_seconds=30.0) as s:
        async with aiohttp.ClientSession() as session:
            response = await session.get(f"{s['base']}/artifact?bytes=2000000&chunk=2000&delay=0.02")
            await asyncio.sleep(0.1)
            assert (await status(s["base"]))["generations"][0]["in_flight"] == 1
            response.close()
            async with session.get(s["base"] + "/api/status"):
                pass
        for _ in range(60):
            snap = await status(s["base"])
            if snap["generations"][0]["in_flight"] == 0:
                break
            await asyncio.sleep(0.1)
        assert snap["generations"][0]["in_flight"] == 0


async def test_proxy_does_not_buffer_whole_body():
    """The first chunk must reach the client long before the upstream finishes."""
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            # 100 chunks * 0.05s => ~5s total.
            start = time.monotonic()
            async with session.get(f"{s['base']}/artifact?bytes=200000&chunk=2000&delay=0.05") as response:
                first = await asyncio.wait_for(response.content.read(1024), timeout=2.0)
                first_at = time.monotonic() - start
                body = first + await response.content.read()
        assert first
        assert first_at < 1.0, f"first byte after {first_at:.2f}s => response was buffered"
        assert len(body) == 200000


async def test_content_length_is_passed_through_and_body_is_intact():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{s['base']}/fixed") as response:
                assert response.status == 200
                assert response.headers["x-upstream-fixed"] == "1"
                assert response.headers["content-length"] == str(17 * 128)
                body = await response.read()
        assert len(body) == 17 * 128


async def test_head_on_fixed_length_resource_keeps_length():
    async with stack() as s:
        async with aiohttp.ClientSession() as session:
            async with session.head(f"{s['base']}/fixed") as response:
                assert response.status == 200
                assert response.headers["content-length"] == str(17 * 128)
                assert await response.read() == b""


async def test_hop_by_hop_request_headers_are_not_forwarded():
    """Connection-scoped headers must not leak to the upstream."""
    async with stack() as s:
        # A bogus `Connection: x-remove-me` plus that header must be dropped;
        # an ordinary custom header must survive.
        reader, writer = await asyncio.open_connection("127.0.0.1", s["ports"]["relay"])
        try:
            writer.write(
                b"GET /fixed HTTP/1.1\r\n"
                b"Host: relay\r\n"
                b"Connection: keep-alive, x-remove-me\r\n"
                b"x-remove-me: secret\r\n"
                b"x-keep-me: yes\r\n\r\n"
            )
            await writer.drain()
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
            assert b"200" in head
            assert b"x-upstream-fixed" in head.lower()
            # The relay must not echo our internal routing markers as request
            # headers; nothing else to inspect from here.
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
