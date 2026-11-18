"""State-machine tests without any network I/O."""

import asyncio

import pytest

from relay.config import load_config
from relay.generations import RelayState


class FakeResponse:
    def __init__(self, status: int):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Programmable probe session: per-target status + delay, records calls."""

    def __init__(self):
        self.calls = []
        self.hook = None  # async callable(target_name) -> (status, delay)

    def get(self, url, *, timeout, allow_redirects):
        self.calls.append(url)

        class Ctx:
            async def __aenter__(inner):
                target = url.split("//")[1].split(".")[0]
                status, delay = await self.hook(target)
                if delay:
                    await asyncio.sleep(delay)
                return FakeResponse(status)

            async def __aexit__(inner, *exc):
                return False

        return Ctx()


def make_state(**env):
    base = {
        "RELAY_TARGETS": "a=http://a.local,b=http://b.local,c=http://c.local",
        "RELAY_DRAIN_SECONDS": "0.3",
    }
    base.update(env)
    cfg = load_config(base)
    state = RelayState(cfg)
    state.probe_session = FakeSession()
    return state


@pytest.mark.asyncio
async def test_failed_probe_keeps_current():
    state = make_state()
    state.probe_session.hook = lambda t: asyncio.sleep(0, result=(503, 0))
    op = await state.begin_switch("b")
    await asyncio.wait_for(op.changed.wait(), 2)
    assert op.status == "failed"
    assert op.probe_status == 503
    assert state.current.target_name == "a"
    assert state.current.id == 1  # no new generation on failure
    assert state.pending is None


@pytest.mark.asyncio
async def test_probe_exception_keeps_current():
    state = make_state()

    async def boom(target):
        raise TimeoutError("probe timed out")

    state.probe_session.hook = boom
    op = await state.begin_switch("b")
    await asyncio.wait_for(op.changed.wait(), 2)
    assert op.status == "failed"
    assert "TimeoutError" in op.reason
    assert state.current.target_name == "a"


@pytest.mark.asyncio
async def test_successful_probe_commits_new_generation():
    state = make_state()
    state.probe_session.hook = lambda t: asyncio.sleep(0, result=(200, 0))
    op = await state.begin_switch("b")
    await asyncio.wait_for(op.changed.wait(), 2)
    assert op.status == "committed"
    assert state.current.target_name == "b"
    assert state.current.id == 2
    assert state.generations[0].state == "draining"
    assert state.generations[0].drain_deadline is not None


@pytest.mark.asyncio
async def test_newer_switch_supersedes_unfinished_candidate():
    state = make_state()

    async def hook(target):
        return (200, 5 if target == "b" else 0)

    state.probe_session.hook = hook
    slow = await state.begin_switch("b")
    await asyncio.sleep(0)
    fast = await state.begin_switch("c")
    await asyncio.wait_for(fast.changed.wait(), 2)
    await asyncio.sleep(0.05)
    assert slow.status == "superseded"
    assert slow.superseded_by == fast.id
    assert fast.status == "committed"
    assert state.current.target_name == "c"
    # The superseded candidate's traffic switch belongs to the newer generation.
    assert state.current.id == fast.generation_id


@pytest.mark.asyncio
async def test_late_probe_after_supersede_cannot_flip_back():
    """A candidate whose probe already returned 200 but commits after a newer
    switch was accepted must be ignored."""
    state = make_state()
    gate = asyncio.Event()

    async def hook(target):
        if target == "b":
            await gate.wait()
        return (200, 0)

    state.probe_session.hook = hook
    first = await state.begin_switch("b")
    await asyncio.sleep(0.05)
    second = await state.begin_switch("c")
    await asyncio.wait_for(second.changed.wait(), 2)
    assert first.status == "superseded"
    gen_after_second = state.current.id
    gate.set()  # let the stale probe finish; it resolves 200
    await asyncio.sleep(0.2)
    assert state.current.target_name == "c"
    assert state.current.id == gen_after_second
    assert first.status == "superseded"


@pytest.mark.asyncio
async def test_drain_aborts_only_its_own_generation():
    state = make_state(RELAY_DRAIN_SECONDS="0.2")
    state.probe_session.hook = lambda t: asyncio.sleep(0, result=(200, 0))
    gen1 = state.current
    old_req = gen1.register("/old")
    op1 = await state.begin_switch("b")
    await asyncio.wait_for(op1.changed.wait(), 2)
    gen2 = state.current
    # A request that starts just after the commit is on generation 2.
    new_req = gen2.register("/new")
    await asyncio.sleep(0.45)
    assert old_req.abort_reason == "drain_expired"
    assert new_req.abort_reason is None
    assert gen2.state == "active"
    assert gen1.state == "closed"
    # Registrations after expiry are aborted immediately.
    late = gen1.register("/late")
    assert late.abort_reason == "drain_expired"


@pytest.mark.asyncio
async def test_switch_to_same_target_is_a_noop():
    state = make_state(RELAY_DRAIN_SECONDS="0.05")
    state.probe_session.hook = lambda t: asyncio.sleep(0, result=(200, 0))
    gen1 = state.current
    in_flight = gen1.register("/long-download")
    op = await state.begin_switch("a")
    await asyncio.wait_for(op.changed.wait(), 2)
    assert op.status == "noop"
    assert op.generation_id == gen1.id
    assert state.current is gen1 and gen1.state == "active"
    assert len(state.generations) == 1
    await asyncio.sleep(0.15)
    # A redundant switch must not drain (and therefore must not interrupt)
    # anything that is already running.
    assert in_flight.abort_reason is None


@pytest.mark.asyncio
async def test_drain_of_old_generation_does_not_touch_new_generation_on_same_url():
    state = make_state(RELAY_DRAIN_SECONDS="0.15")
    state.probe_session.hook = lambda t: asyncio.sleep(0, result=(200, 0))
    op = await state.begin_switch("b")
    await asyncio.wait_for(op.changed.wait(), 2)
    gen2 = state.current  # b
    op2 = await state.begin_switch("c")
    await asyncio.wait_for(op2.changed.wait(), 2)
    op3 = await state.begin_switch("b")  # same address as generation 2, new generation
    await asyncio.wait_for(op3.changed.wait(), 2)
    gen4 = state.current
    assert gen4.id == 4 and gen4 is not gen2
    req = gen4.register("/keep")
    await asyncio.sleep(0.3)  # generation 2's drain expires in here
    assert gen2.state == "closed"
    assert req.abort_reason is None
    assert gen4.state == "active"


@pytest.mark.asyncio
async def test_rapid_switch_chain_commits_only_latest_candidate():
    state = make_state(RELAY_DRAIN_SECONDS="0.05")
    state.probe_session.hook = lambda t: asyncio.sleep(0, result=(200, 0))
    ops = [await state.begin_switch("a" if i % 2 else "b") for i in range(6)]
    for op in ops:
        await asyncio.wait_for(op.changed.wait(), 2)
    await asyncio.sleep(0.3)
    # Only the last accepted candidate commits; the rest were superseded
    # before their probes completed. The last one targets the current target,
    # so it is reported as a no-op and no generation is created.
    assert [op.status for op in ops] == ["superseded"] * 5 + ["noop"]
    assert state.current.target_name == "a" and state.current.id == 1
    active = [g for g in state.generations if g.state == "active"]
    assert active == [state.current]


@pytest.mark.asyncio
async def test_sequential_switches_each_commit_a_fresh_generation():
    state = make_state(RELAY_DRAIN_SECONDS="0.05")
    state.probe_session.hook = lambda t: asyncio.sleep(0, result=(200, 0))
    for i in range(6):
        target = ("b", "a", "b", "c", "a", "b")[i]
        op = await state.begin_switch(target)
        await asyncio.wait_for(op.changed.wait(), 2)
        assert op.status == "committed"
    await asyncio.sleep(0.2)
    assert [g.id for g in state.generations] == list(range(1, 8))
    assert [g.state for g in state.generations] == ["closed"] * 6 + ["active"]


@pytest.mark.asyncio
async def test_status_snapshot_is_nonblocking_and_counts_requests():
    state = make_state()
    req = state.current.register("/artifact")
    snap = state.snapshot()
    assert snap["current_target"] == "a"
    assert snap["generations"][0]["in_flight"] == 1
    assert snap["generations"][0]["in_flight_requests"][0]["path"] == "/artifact"
    state.current.unregister(req)
    assert state.snapshot()["generations"][0]["in_flight"] == 0


@pytest.mark.asyncio
async def test_closed_generation_history_is_pruned_but_current_is_kept():
    state = make_state(RELAY_DRAIN_SECONDS="0.02", RELAY_MAX_CLOSED_GENERATIONS="2")
    assert state.config.max_closed_generations == 2
    state.probe_session.hook = lambda t: asyncio.sleep(0, result=(200, 0))
    targets = ["b", "c", "a", "b", "c", "a"]
    for target in targets:
        op = await state.begin_switch(target)
        await asyncio.wait_for(op.changed.wait(), 2)
    await asyncio.sleep(0.1)
    closed = [g for g in state.generations if g.state == "closed"]
    assert len(closed) == 2
    assert state.current in state.generations
    assert state.generations[-1] is state.current
    snap = state.snapshot()
    assert snap["current_generation"] == state.current.id
