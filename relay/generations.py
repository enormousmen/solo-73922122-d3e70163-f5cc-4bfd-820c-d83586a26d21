"""Generation tracking and switch state machine for the relay.

A *generation* is one concrete routing epoch.  Every successful switch creates a
new generation, even when the target name/URL is the same as an earlier epoch.
That is what makes "switch back to the same address" safe: a drain timer only
ever aborts the in-flight requests that were registered on its own generation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque

from .config import Config

log = logging.getLogger("relay.generations")

HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


@dataclass
class InFlightRequest:
    """One GET pinned to a generation.  ``event`` is its abort signal."""

    id: int
    path: str
    started_at: float
    event: asyncio.Event = field(default_factory=asyncio.Event)
    abort_reason: str | None = None

    def abort(self, reason: str) -> None:
        if self.abort_reason is None:
            self.abort_reason = reason
        self.event.set()

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "path": self.path,
            "started_at": self.started_at,
            "age_seconds": round(time.time() - self.started_at, 3),
            "abort_reason": self.abort_reason,
        }


@dataclass
class Generation:
    id: int
    target_name: str
    base_url: str
    created_at: float
    state: str = "active"  # active | draining | closed
    drain_deadline: float | None = None  # monotonic clock
    drain_deadline_wall: float | None = None
    closed_at: float | None = None
    requests: dict[int, InFlightRequest] = field(default_factory=dict)
    _request_seq: int = 0
    _aborted: bool = False
    on_idle: Callable[["Generation"], None] | None = None

    def register(self, path: str) -> InFlightRequest:
        self._request_seq += 1
        req = InFlightRequest(id=self._request_seq, path=path, started_at=time.time())
        self.requests[req.id] = req
        if self._aborted:
            req.abort("drain_expired")
        return req

    def unregister(self, req: InFlightRequest) -> None:
        self.requests.pop(req.id, None)
        if not self.requests and self.on_idle is not None:
            self.on_idle(self)

    def abort_all(self, reason: str) -> None:
        """Abort in-flight requests of *this* generation only."""
        self._aborted = True
        for req in list(self.requests.values()):
            req.abort(reason)

    @property
    def in_flight(self) -> int:
        return len(self.requests)


@dataclass
class SwitchOperation:
    id: int
    target_name: str
    status: str = "pending"  # pending | committed | noop | failed | superseded
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    probe_status: int | None = None
    reason: str | None = None
    generation_id: int | None = None
    superseded_by: int | None = None
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None

    def finish(self, status: str, *, reason: str | None = None, probe_status: int | None = None) -> None:
        self.status = status
        self.reason = reason
        if probe_status is not None:
            self.probe_status = probe_status
        self.finished_at = time.time()
        self.changed.set()

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "target": self.target_name,
            "status": self.status,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "probe_status": self.probe_status,
            "reason": self.reason,
            "generation_id": self.generation_id,
            "superseded_by": self.superseded_by,
        }


class RelayState:
    """All mutable relay state.  Mutations are serialised by a short lock.

    The lock is never held across network I/O, and status reads do not take it
    (all code runs on one event loop, so a synchronous snapshot is coherent).
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.upstream_session: Any = None  # aiohttp.ClientSession, bound in lifespan
        self.probe_session: Any = None
        self._lock = asyncio.Lock()
        self._generation_seq = 0
        self._operation_seq = 0
        self.generations: list[Generation] = []
        self.current = self._new_generation(config.initial)
        self.pending: SwitchOperation | None = None
        self.operations: Deque[SwitchOperation] = deque(maxlen=50)
        self._drain_tasks: set[asyncio.Task] = set()

    # -- generation helpers -------------------------------------------------
    def _new_generation(self, target_name: str) -> Generation:
        target = self.config.targets[target_name]
        self._generation_seq += 1
        generation = Generation(
            id=self._generation_seq,
            target_name=target.name,
            base_url=target.base_url,
            created_at=time.time(),
        )
        generation.on_idle = self._close_if_idle
        self.generations.append(generation)
        self._prune_closed_generations()
        return generation

    def _prune_closed_generations(self) -> None:
        """Forget the oldest finished generations once the history is long.

        Only generations that are ``closed`` *and* have nothing left in flight
        are pruned; ``current`` is always the newest entry, so an active or
        still-draining epoch is never dropped from the report.
        """
        room = self.config.max_closed_generations
        finished = [g for g in self.generations if g.state == "closed" and not g.in_flight]
        excess = len(finished) - room
        if excess <= 0:
            return
        drop = {id(g) for g in finished[:excess]}
        self.generations = [g for g in self.generations if id(g) not in drop]

    def _close_if_idle(self, generation: Generation) -> bool:
        """Release a non-current generation with nothing left in flight.

        Keeps status snapshots honest: a generation stays ``draining`` for at
        most its window and only while it still has requests to protect.  Safe
        without the lock because it is a synchronous, event-loop-only mutation
        of flags (single process, no awaits).
        """
        if generation.state != "draining" or generation.in_flight:
            return False
        generation.state = "closed"
        generation.closed_at = time.time()
        self._prune_closed_generations()
        return True

    def _probe_url(self, target_name: str) -> str:
        target = self.config.targets[target_name]
        return target.base_url + self.config.probe_path

    # -- switching ----------------------------------------------------------
    def target_names(self) -> list[str]:
        return list(self.config.targets)

    async def begin_switch(self, target_name: str) -> SwitchOperation:
        """Accept a switch and start its readiness probe in the background.

        Accepting a newer switch supersedes the unfinished candidate.  The
        superseded operation is marked before its probe task is cancelled; even
        if its HTTP request had already completed, its commit path checks that
        it is still the pending operation and therefore cannot flip traffic.
        """
        async with self._lock:
            self._operation_seq += 1
            op = SwitchOperation(id=self._operation_seq, target_name=target_name)
            previous = self.pending
            if previous is not None:
                previous.superseded_by = op.id
                previous.finish("superseded", reason=f"superseded by switch #{op.id}")
                if previous.task is not None:
                    previous.task.cancel()
            self.pending = op
            self.operations.append(op)
            op.task = asyncio.create_task(
                self._probe_and_maybe_commit(op), name=f"relay-probe-{op.id}"
            )
            log.info("accepted switch #%s to %s; probing", op.id, target_name)
            return op

    async def _probe_and_maybe_commit(self, op: SwitchOperation) -> None:
        probe_status: int | None = None
        reason: str | None = None
        try:
            timeout = self.config.probe_timeout
            async with self.probe_session.get(
                self._probe_url(op.target_name),
                timeout=timeout,
                allow_redirects=False,
            ) as response:
                probe_status = response.status
                # The body is irrelevant for readiness.  Do not read it: a slow
                # or endless body must not delay the switch decision.
        except asyncio.CancelledError:
            # Superseded before finishing; begin_switch already recorded it.
            raise
        except Exception as exc:  # noqa: BLE001 - timeout/connection/... all mean not ready
            reason = f"{type(exc).__name__}: {exc}"

        if probe_status == 200:
            committed = await self._commit_if_pending(op, probe_status)
            if not committed:
                log.info("probe for superseded switch #%s returned 200; ignored", op.id)
        else:
            if reason is None:
                reason = f"readiness returned HTTP {probe_status}"
            await self._fail_if_pending(op, reason, probe_status)

    async def _commit_if_pending(self, op: SwitchOperation, probe_status: int) -> bool:
        async with self._lock:
            if self.pending is not op or op.status != "pending":
                return False
            old = self.current
            self.pending = None
            if op.target_name == old.target_name:
                # The candidate is already the current target: traffic does not
                # move, so no new generation is created and nothing drains.  In
                # particular an in-flight download is not cut short by a
                # redundant switch.
                op.generation_id = old.id
                op.finish("noop", probe_status=probe_status, reason="target already current")
                log.info("switch #%s is a no-op: %s is already current", op.id, op.target_name)
                return True
            new = self._new_generation(op.target_name)
            self.current = new
            op.generation_id = new.id
            op.finish("committed", probe_status=probe_status)
            self._begin_drain(old)
            log.info(
                "switch #%s committed: generation %s (%s) -> %s (%s); draining %s for %ss",
                op.id,
                old.id,
                old.target_name,
                new.id,
                new.target_name,
                old.id,
                self.config.drain_seconds,
            )
            return True

    async def _fail_if_pending(self, op: SwitchOperation, reason: str, probe_status: int | None) -> None:
        async with self._lock:
            if self.pending is not op or op.status != "pending":
                return
            self.pending = None
            op.finish("failed", reason=reason, probe_status=probe_status)
            log.warning(
                "switch #%s to %s failed (%s); keeping generation %s (%s)",
                op.id,
                op.target_name,
                reason,
                self.current.id,
                self.current.target_name,
            )

    def _begin_drain(self, generation: Generation) -> None:
        """Called under the state lock when ``generation`` stops being current."""
        delay = self.config.drain_seconds
        loop = asyncio.get_running_loop()
        generation.state = "draining"
        generation.drain_deadline = loop.time() + delay
        generation.drain_deadline_wall = time.time() + delay
        task = asyncio.create_task(
            self._drain_after(generation), name=f"relay-drain-{generation.id}"
        )
        self._drain_tasks.add(task)
        task.add_done_callback(self._drain_tasks.discard)

    async def _drain_after(self, generation: Generation) -> None:
        await asyncio.sleep(self.config.drain_seconds)
        generation.state = "closed"
        generation.closed_at = time.time()
        generation.abort_all("drain_expired")
        log.info(
            "drain expired for generation %s (%s); aborted %s unfinished request(s)",
            generation.id,
            generation.target_name,
            generation.in_flight,
        )
        self._prune_closed_generations()

    # -- observability ------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        now_mono = loop.time()
        now_wall = time.time()

        def gen_dict(generation: Generation) -> dict[str, Any]:
            remaining = None
            if generation.drain_deadline is not None:
                remaining = max(0.0, round(generation.drain_deadline - now_mono, 3))
            return {
                "id": generation.id,
                "target": generation.target_name,
                "base_url": generation.base_url,
                "state": generation.state,
                "in_flight": generation.in_flight,
                "created_at": generation.created_at,
                "age_seconds": round(now_wall - generation.created_at, 3),
                "drain_remaining_seconds": remaining,
                "drain_deadline_at": generation.drain_deadline_wall,
                "closed_at": generation.closed_at,
                "in_flight_requests": [r.to_dict() for r in generation.requests.values()],
            }

        current = self.current
        return {
            "current": {
                "target": current.target_name,
                "base_url": current.base_url,
                "generation": current.id,
                "since": current.created_at,
            },
            "current_target": current.target_name,
            "current_generation": current.id,
            "pending": self.pending.to_dict() if self.pending is not None else None,
            "generations": [gen_dict(g) for g in reversed(self.generations)],
            "targets": {name: t.base_url for name, t in self.config.targets.items()},
            "drain_seconds": self.config.drain_seconds,
            "probe_path": self.config.probe_path,
            "probe_timeout_seconds": self.config.probe_timeout,
            "recent_operations": [op.to_dict() for op in reversed(self.operations)],
        }

    # -- shutdown -----------------------------------------------------------
    async def aclose(self) -> None:
        if self.pending is not None and self.pending.task is not None:
            self.pending.task.cancel()
        for task in list(self._drain_tasks):
            task.cancel()
        for generation in self.generations:
            generation.abort_all("relay_shutdown")
