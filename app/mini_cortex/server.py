"""`uv run mini-cortex`: the whole app in one process.

    hub        ws://127.0.0.1:8765/connectors (token "dev-token")   connectors dial in here
    pipeline   items -> packets -> memory (SQLite + LanceDB in the data dir)
    API + UI   http://127.0.0.1:8787                                prebuilt UI from mini_cortex/static
    connectors child processes, e.g. --connector replay --connector slack (or MINI_CORTEX_CONNECTORS=replay,slack)

    uv run mini-cortex --backfill-days 30 --connector slack
    uv run mini-cortex snapshot save|load|list [NAME]

HTTP API (consumed by web/src/api/client.ts in live mode):
    GET  /api/health      GET /api/connectors   GET /api/frames   GET /api/items
    POST /api/fetch       {"connector", "stream", "since", "until"}  -> runs a fetch over the protocol
    GET  /api/events      server-sent events: frame | intake | connectors | stats | ...
    /api/llm/*            model settings + "Test connection" (llm_routes)
    /api/stats, /api/packets, /api/memory-log, /api/chat, ...   (pipeline_routes)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import launcher, snapshot
from .hub import Hub
from .hub_base import parse_iso
from .llm_config import REPO_ROOT, load_settings
from .llm_routes import make_router as llm_router
from .pipeline import Pipeline
from .pipeline_routes import make_router as pipeline_router
from .store import data_dir

log = logging.getLogger("mini-cortex")
STATIC = Path(__file__).resolve().parent / "static"
WEB_DIST = REPO_ROOT / "web" / "dist"


def static_dir() -> Path | None:
    """The committed build in mini_cortex/static; a fresher local `npm run build` (web/dist) wins."""
    shipped, local = STATIC / "index.html", WEB_DIST / "index.html"
    if local.exists() and (not shipped.exists() or local.stat().st_mtime > shipped.stat().st_mtime):
        return WEB_DIST
    return STATIC if shipped.exists() else None


class FetchRequest(BaseModel):
    connector: str
    stream: str
    since: str
    until: str


def make_app(hub: Hub, hub_url: str, static: Path | None) -> FastAPI:
    app = FastAPI(title="mini-cortex")
    pipe = hub.pipeline
    # New model settings take effect without a restart: the pipeline gets a fresh client.
    app.include_router(llm_router(on_saved=pipe.set_llm if pipe else None))
    if pipe:
        app.include_router(pipeline_router(pipe))     # stats, packets, memory-log, process, settings, chat

    @app.get("/api/health")
    async def health() -> dict:
        return {"mode": "live", "hub": hub_url}

    @app.get("/api/connectors")
    async def connectors() -> dict:
        return {"hub": hub_url, "connectors": hub.connector_list()}

    @app.get("/api/frames")
    async def frames() -> list[dict]:
        return list(hub.frames)

    @app.get("/api/items")
    async def items(limit: int = 200) -> list[dict]:
        return hub.recent_items(limit)

    if not pipe:
        @app.get("/api/memory-log")
        async def memory_log() -> list[dict]:
            return list(reversed(hub.memory_log))

    @app.post("/api/fetch")
    async def fetch(req: FetchRequest) -> dict:
        try:
            got = await hub.fetch_window(req.connector, req.stream, parse_iso(req.since), parse_iso(req.until),
                                         purpose="manual")
        except LookupError as e:
            raise HTTPException(404, str(e)) from e
        except Exception as e:
            raise HTTPException(502, str(e)) from e
        return {"items": len(got)}

    @app.get("/api/events")
    async def events() -> StreamingResponse:
        q: asyncio.Queue = asyncio.Queue()
        hub.subscribers.add(q)

        async def stream() -> AsyncIterator[str]:
            try:
                yield "retry: 2000\n\n"
                while True:
                    try:
                        event, data = await asyncio.wait_for(q.get(), 15)
                        yield f"event: {event}\ndata: {json.dumps(data)}\n\n"
                    except TimeoutError:
                        yield ": keep-alive\n\n"
            finally:
                hub.subscribers.discard(q)

        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    if static and static.is_dir():
        app.mount("/", StaticFiles(directory=static, html=True), name="web")
    return app


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s", datefmt="%H:%M:%S")
    for noisy in ("websockets", "httpx", "openai"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def stop_on_signals() -> None:
    """Closing the terminal (SIGHUP), a plain `kill` (SIGTERM) or Ctrl+Break on Windows (SIGBREAK) shuts down like
    Ctrl+C. Connectors run in their own session, so without this they would outlive the app and dial into the next
    one with their old code. uvicorn handles SIGTERM itself while serving and re-raises it afterwards: that lands
    here too."""
    def stop(signum: int, frame: object) -> None:
        raise KeyboardInterrupt
    for name in ("SIGTERM", "SIGHUP", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), stop)


def serve(args: argparse.Namespace) -> None:
    data = data_dir()
    if url := snapshot.running(data):
        raise SystemExit(f"mini-cortex already runs on {data} ({url}). Stop it, or use --data for a second one.")
    hub = Hub(args.token, args.backfill_days or None, str(data / "items.jsonl"))
    if not args.no_pipeline:
        hub.pipeline = Pipeline(load_settings(), publish=hub.publish, data=data)
    hub_url = f"ws://{args.host}:{args.hub_port}/connectors"
    static = static_dir()
    app = make_app(hub, hub_url, static)
    names = launcher.enabled(args.connector, os.environ.get("MINI_CORTEX_CONNECTORS"))
    children = launcher.Launcher(names, hub_url, args.token)

    async def run() -> None:
        async with hub.serving(args.host, args.hub_port):
            log.info("hub on %s (token %r)", hub_url, args.token)
            if hub.pipeline:
                hub.pipeline.start()
                s = hub.pipeline.stats()
                log.info("memory in %s · %d packets · %d items pending", data, s["packets"], s["pending"])
            if static is None:
                log.warning("no prebuilt UI found: run `uv run python scripts/build_ui.py` (needs Node)")
            server = uvicorn.Server(uvicorn.Config(app, host=args.host, port=args.port, log_level="warning",
                                                   timeout_graceful_shutdown=2))
            serving_task = asyncio.create_task(server.serve())
            while not server.started and not serving_task.done():
                await asyncio.sleep(0.05)
            if server.started:
                snapshot.write_lock(data, args.host, args.port)
                log.info("open http://%s:%d", args.host, args.port)
                if names:
                    children.start()
                else:
                    log.info("no connectors enabled: add --connector NAME (available: %s)",
                             ", ".join(launcher.available()) or "none")
            await serving_task

    stop_on_signals()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    except OSError as e:          # a port is taken, typically by another mini-cortex
        raise SystemExit(f"cannot start: {e}. Try --port / --hub-port.") from None
    finally:
        children.stop()
        (data / snapshot.LOCK).unlink(missing_ok=True)


def snapshot_cmd(args: argparse.Namespace) -> None:
    data, settings = data_dir(), load_settings()
    try:
        if args.action == "list":
            names = snapshot.listing()
            for n in names:
                m = snapshot.read_manifest(snapshot.SNAPSHOTS_DIR / f"{n}.tar.gz")
                print(f"{n:<24} {m.get('packets', 0):>5} packets {m.get('items', 0):>6} items   "
                      f"{m.get('embed_model') or '?'} ({m.get('vector_dim') or m.get('embed_dim') or '?'}d)   "
                      f"{m.get('created', '')}")
            if not names:
                print(f"no snapshots in {snapshot.SNAPSHOTS_DIR}")
        elif not args.name:
            raise snapshot.SnapshotError(f"snapshot {args.action} needs a NAME")
        elif args.action == "save":
            path, m = snapshot.save(args.name, data, settings)
            print(f"saved {m['packets']} packets, {m['items']} items to {path} "
                  f"({path.stat().st_size / 1e6:.1f} MB)")
        else:
            m, warning = snapshot.load(args.name, data, settings)
            print(f"loaded {m['name']}: {m.get('packets', 0)} packets, {m.get('items', 0)} items into {data}"
                  f" (previous data in {data.name}.prev)")
            if warning:
                print(f"WARNING: {warning}", file=sys.stderr)
    except snapshot.SnapshotError as e:
        raise SystemExit(str(e)) from None


def main(argv: list[str] | None = None) -> None:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data", default=argparse.SUPPRESS,
                        help="memory directory (default: MINI_CORTEX_DATA or <repo>/.data)")
    ap = argparse.ArgumentParser(prog="mini-cortex", description=__doc__, parents=[common],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8787, help="HTTP port for the UI and API")
    ap.add_argument("--hub-port", type=int, default=8765, help="WebSocket port connectors dial into")
    ap.add_argument("--token", default=os.environ.get("CONNECTOR_TOKEN", "dev-token"))
    ap.add_argument("--backfill-days", type=int, default=60, help="fetch this many days on a connector's first hello"
                                                                   " (0 = none)")
    ap.add_argument("--no-pipeline", action="store_true", help="intake only: no packets, memory or chat")
    ap.add_argument("--connector", action="append", metavar="NAME",
                    help="start connectors/NAME_adapter.py (repeatable; 'none' = no connectors)")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd")
    sp = sub.add_parser("snapshot", parents=[common], help="save, load or list memory snapshots")
    sp.add_argument("action", choices=["save", "load", "list"])
    sp.add_argument("name", nargs="?")
    args = ap.parse_args(argv)

    if getattr(args, "data", None):
        os.environ["MINI_CORTEX_DATA"] = str(Path(args.data).resolve())
    if args.cmd == "snapshot":
        snapshot_cmd(args)
    else:
        setup_logging(args.verbose)
        serve(args)


if __name__ == "__main__":
    main()
