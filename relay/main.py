"""Entrypoint: ``python -m relay.main`` (uvicorn, one process)."""

from __future__ import annotations

import logging
import os

import uvicorn

from .config import load_config
from .proxy import RelayApp


def build_app():
    config = load_config()
    return RelayApp(config).app


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("RELAY_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = load_config()
    logging.getLogger("relay").info(
        "starting relay on %s:%s; targets=%s initial=%s drain=%ss probe=%s%s",
        config.host,
        config.port,
        {n: t.base_url for n, t in config.targets.items()},
        config.initial,
        config.drain_seconds,
        config.probe_timeout,
        config.probe_path,
    )
    # Pass the app object (not an import string) so uvicorn never treats it as
    # an app factory and we never spawn worker processes.
    uvicorn.run(
        RelayApp(config).app,
        host=config.host,
        port=config.port,
        log_level=os.environ.get("UVICORN_LOG_LEVEL", "info"),
        access_log=os.environ.get("RELAY_ACCESS_LOG", "1") == "1",
        # Single process on purpose: in-memory routing state must be authoritative.
        workers=1,
        limit_concurrency=int(os.environ.get("RELAY_LIMIT_CONCURRENCY", "0")) or None,
        # Long downloads must not hold shutdown open forever: after this grace
        # period uvicorn closes the remaining connections and the lifespan
        # handler aborts every generation's in-flight requests.
        timeout_graceful_shutdown=int(os.environ.get("RELAY_SHUTDOWN_GRACE_SECONDS", "5")),
    )


if __name__ == "__main__":
    main()
