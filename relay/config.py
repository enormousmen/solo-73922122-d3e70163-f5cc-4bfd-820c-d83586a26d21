"""Configuration for the switchable artifact proxy."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Target:
    """An immutable upstream generation ("generation" == one target base URL)."""

    name: str
    base_url: str


@dataclass(frozen=True)
class Config:
    targets: dict[str, Target]
    initial: str
    probe_path: str
    probe_timeout: float
    drain_seconds: float
    connect_timeout: float
    sock_read_timeout: float
    chunk_size: int
    host: str
    port: int
    max_closed_generations: int = 100


def _parse_targets(raw: str) -> dict[str, Target]:
    """Accept ``name=url,name=url`` or a JSON object ``{"name": "url"}``."""
    raw = raw.strip()
    if not raw:
        raise ValueError("RELAY_TARGETS must not be empty")
    if raw.startswith("{"):
        try:
            mapping = json.loads(raw)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise ValueError(f"RELAY_TARGETS is not valid JSON: {exc}") from exc
        if not isinstance(mapping, dict):
            raise ValueError("RELAY_TARGETS JSON must be an object of name->url")
        items = [(str(k), str(v)) for k, v in mapping.items()]
    else:
        items = []
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            if "=" not in part:
                raise ValueError(f"invalid RELAY_TARGETS entry {part!r}; expected name=url")
            name, url = part.split("=", 1)
            items.append((name.strip(), url.strip()))
    targets: dict[str, Target] = {}
    for name, url in items:
        if not name or not url:
            raise ValueError(f"invalid RELAY_TARGETS entry {name!r}={url!r}")
        if name in targets:
            raise ValueError(f"duplicate target name {name!r}")
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError(f"target {name!r} URL must start with http:// or https://")
        targets[name] = Target(name=name, base_url=url.rstrip("/"))
    if not targets:
        raise ValueError("RELAY_TARGETS resolved to no targets")
    return targets


def _env_float(env: dict[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    value = float(raw)
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return value


def load_config(env: dict[str, str] | None = None) -> Config:
    e: dict[str, str] = dict(os.environ) if env is None else env
    targets = _parse_targets(e.get("RELAY_TARGETS", "blue=http://127.0.0.1:9001,green=http://127.0.0.1:9002"))
    initial = e.get("RELAY_INITIAL_TARGET") or next(iter(targets))
    if initial not in targets:
        raise ValueError(f"RELAY_INITIAL_TARGET {initial!r} is not one of {sorted(targets)}")
    drain = _env_float(e, "RELAY_DRAIN_SECONDS", 5.0)
    probe_timeout = _env_float(e, "RELAY_PROBE_TIMEOUT", 2.0)
    if probe_timeout <= 0:
        raise ValueError("RELAY_PROBE_TIMEOUT must be > 0")
    connect_timeout = _env_float(e, "RELAY_CONNECT_TIMEOUT", 5.0)
    sock_read_timeout = _env_float(e, "RELAY_SOCK_READ_TIMEOUT", 0.0)
    chunk_size = int(e.get("RELAY_CHUNK_SIZE", "65536"))
    if chunk_size <= 0:
        raise ValueError("RELAY_CHUNK_SIZE must be > 0")
    max_closed = int(e.get("RELAY_MAX_CLOSED_GENERATIONS", "100"))
    if max_closed < 1:
        raise ValueError("RELAY_MAX_CLOSED_GENERATIONS must be >= 1")
    probe_path = e.get("RELAY_PROBE_PATH", "/ready")
    if not probe_path.startswith("/"):
        probe_path = "/" + probe_path
    return Config(
        targets=targets,
        initial=initial,
        probe_path=probe_path,
        probe_timeout=probe_timeout,
        drain_seconds=drain,
        connect_timeout=connect_timeout,
        sock_read_timeout=sock_read_timeout,
        chunk_size=chunk_size,
        host=e.get("RELAY_HOST", "0.0.0.0"),
        port=int(e.get("RELAY_PORT", "8080")),
        max_closed_generations=max_closed,
    )
