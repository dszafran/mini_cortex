"""The universal adapter interface: one class per data source, and nothing about the wire protocol.

PROTOCOL.md §10 gives every connector four duties. A source implements them as methods of `SourceAdapter`:

    1. authenticate   async authenticate()                         log in to the source, discover what to read
    2. map            async to_item(stream, native) -> dict|None   native object -> IngestItem (stable source_id!)
    3. answer fetch   async fetch_window(stream, since, until)     every item with since <= timestamp < until
                      (or override fetch(...) for native paging)
    4. push           async watch(emit)                            await emit(stream, items) for new/changed things

Everything else (WebSocket, hello, paging with cursors, acked pushes, reconnects, the push-cursor file) lives in
`connector_sdk.py` and `run_adapter()` below. Hub and adapter never share code: a new source only adds a file.

    class Notes(SourceAdapter):
        name = "notes"
        streams = [StreamSpec("notes", kind="note")]
        async def to_item(self, stream, path): ...
        async def fetch_window(self, stream, since, until): ...

    if __name__ == "__main__":
        main(Notes)          # `serve` (talk to the hub) or `fetch --days 7` (print items, no hub needed)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from connector_sdk import Connector, Page, Stream, iso, paginate, parse_iso, setup_logging

Emit = Callable[[str, list[dict]], Awaitable[dict]]
WINDOW_CACHE_SECONDS = 60


@dataclass
class StreamSpec:
    """What a stream can do. Mirrors the `hello` stream entry (PROTOCOL.md §3.1)."""

    name: str
    kind: str = "other"             # chat | email | note | issue | event | other: the hub's buffering hint
    description: str = ""
    fetch: bool = True              # False = push-only (live-only source: the hub answers from what it received)
    push: bool = True
    time_filter: str = "emulated"   # "native" if the source itself filters by time
    max_limit: int = 200


class SourceAdapter(ABC):
    name: str = ""                  # connector name, unique per hub: [a-z0-9-]{1,40}
    display_name: str = ""
    version: str = "0.1.0"
    streams: list[StreamSpec] = []

    def __init__(self) -> None:
        self.connector: Connector | None = None      # set by run_adapter(); None in `fetch` dry runs
        self._window_cache: dict[tuple, tuple[float, list[dict]]] = {}

    # --- the four duties ------------------------------------------------------------------------------------

    async def authenticate(self) -> None:
        """Duty 1. Log in and discover what to read. Raise to abort start-up with a clear message."""

    @abstractmethod
    async def to_item(self, stream: str, native: Any) -> dict | None:
        """Duty 2. Map one native object to an IngestItem dict (PROTOCOL.md §5), or None to skip it.

        Required: source_id (stable: "<stream>:<native id>"), content (LLM-ready text), timestamp (UTC event time).
        """

    async def fetch_window(self, stream: str, since: datetime, until: datetime) -> list[dict]:
        """Duty 3, simple form. Return ALL items with since <= timestamp < until, in any order.

        Sorting, paging and cursors are done for you; the window is cached briefly so later pages are cheap.
        Only needed if the stream has fetch=True.
        """
        raise NotImplementedError(f"{type(self).__name__} declares fetch but implements no fetch_window()")

    async def fetch(self, stream: str, since: datetime, until: datetime, cursor: str | None, limit: int) -> Page:
        """Duty 3, full form. Override for sources with native paging; the default pages over fetch_window()."""
        key = (stream, since, until)
        now = asyncio.get_running_loop().time()
        cached = self._window_cache.get(key)
        if cached and now - cached[0] < WINDOW_CACHE_SECONDS:
            items = cached[1]
        else:
            items = await self.fetch_window(stream, since, until)
            self._window_cache = {key: (now, items)}
        page = paginate(items, cursor, limit)
        coverage = self.coverage(stream, since)
        # A configured limit is only an estimate: if the source did serve older items, don't claim it can't.
        if coverage and not any(parse_iso(i["timestamp"]) < parse_iso(coverage["since"]) for i in items):
            page.coverage = coverage
        return page

    async def watch(self, emit: Emit) -> None:
        """Duty 4. Run forever; call `await emit(stream, items)` for new or changed items.

        emit() returns once the hub has stored the items (at-least-once), so advance your own position
        (self.save_cursor) only after it returns. Not overriding this = no live push.
        """
        await asyncio.Event().wait()

    # --- optional hooks -------------------------------------------------------------------------------------

    def coverage(self, stream: str, since: datetime) -> dict | None:
        """Return {"since": iso} when the source cannot serve data as old as `since` (partial history).

        Lets the hub tell "nothing happened" apart from "I can't see that far back" (PROTOCOL.md, `coverage`).
        """
        return None

    # --- helpers for watch() --------------------------------------------------------------------------------

    def load_cursor(self, key: str = "push_cursor") -> datetime:
        """Where live push should resume: own state file, else the hub's newest stored item (§7.3), else now."""
        assert self.connector, "load_cursor() needs a running connector"
        saved = self.connector.state.get(key)
        if saved:
            return parse_iso(saved)
        hello = self.connector.hello_result or {}
        newest = [s.get("last_timestamp") for s in (hello.get("streams") or {}).values() if s.get("last_timestamp")]
        start = max(parse_iso(t) for t in newest) if newest else datetime.now(UTC)
        self.save_cursor(start, key)
        return start

    def save_cursor(self, value: datetime, key: str = "push_cursor") -> None:
        assert self.connector, "save_cursor() needs a running connector"
        self.connector.state.set(key, iso(value))


# --- runner -------------------------------------------------------------------------------------------------

async def run_adapter(adapter: SourceAdapter, hub_url: str, token: str, state_path: str | Path | None) -> None:
    """Wire an adapter to the hub and run until cancelled."""
    connector = Connector(adapter.name, hub_url=hub_url, token=token, version=adapter.version,
                          display_name=adapter.display_name or adapter.name, state_path=state_path)
    for spec in adapter.streams:
        fetch = None
        if spec.fetch:
            async def fetch(since, until, cursor, limit, _stream=spec.name):
                return await adapter.fetch(_stream, since, until, cursor, limit)
        connector.add_stream(Stream(spec.name, kind=spec.kind, description=spec.description, fetch=fetch,
                                    push=spec.push, time_filter=spec.time_filter, max_limit=spec.max_limit))
    adapter.connector = connector
    await adapter.authenticate()

    async def watch() -> None:
        await connector.connected.wait()            # hello_result (for load_cursor) is known from here on
        await adapter.watch(connector.push)

    await asyncio.gather(connector.run(), watch())


async def dry_fetch(adapter: SourceAdapter, stream: str, since: datetime, until: datetime, limit: int) -> list[dict]:
    """Page through a window exactly like the hub would, without a hub. For testing an adapter against a source."""
    await adapter.authenticate()
    items: list[dict] = []
    cursor = None
    while True:
        page = await adapter.fetch(stream, since, until, cursor, limit)
        items += page.items
        if page.coverage:
            print(f"# coverage: {json.dumps(page.coverage)}", file=sys.stderr)
        if not page.has_more:
            return items
        cursor = page.next_cursor


def load_env_file(path: str | Path = ".env") -> None:
    """KEY=value lines into os.environ; variables that are already set win."""
    p = Path(path)
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.split(" #")[0].strip().strip("'\""))


def main(adapter_cls: Callable[[], SourceAdapter], argv: list[str] | None = None) -> None:
    """Command line shared by every adapter: `serve` (default) or `fetch` (dry run, prints JSONL)."""
    load_env_file()
    env = os.environ.get
    ap = argparse.ArgumentParser(description=sys.modules[adapter_cls.__module__].__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd")
    s = sub.add_parser("serve", help="connect to the hub: answer fetch, push live items (default)")
    s.add_argument("--hub-url", default=env("HUB_URL", "ws://127.0.0.1:8765/connectors"))
    s.add_argument("--hub-token", default=env("CONNECTOR_TOKEN", "dev-token"))
    s.add_argument("--state", default=env("ADAPTER_STATE_FILE"), help="push-cursor file (default .<name>_state.json)")
    f = sub.add_parser("fetch", help="dry run: fetch a window straight from the source and print IngestItems")
    f.add_argument("--stream", help="default: the first stream")
    f.add_argument("--days", type=float, default=7, help="window = the last N days (default 7)")
    f.add_argument("--since")
    f.add_argument("--until")
    f.add_argument("--limit", type=int, default=50, help="page size")
    args = ap.parse_args(argv)
    setup_logging(args.verbose)

    adapter = adapter_cls()
    try:
        if args.cmd == "fetch":
            until = parse_iso(args.until) if args.until else datetime.now(UTC)
            since = parse_iso(args.since) if args.since else until - timedelta(days=args.days)
            stream = args.stream or adapter.streams[0].name
            items = asyncio.run(dry_fetch(adapter, stream, since, until, args.limit))
            for it in items:
                print(json.dumps(it, ensure_ascii=False))
            print(f"# {len(items)} item(s) in {stream} [{iso(since)}, {iso(until)})", file=sys.stderr)
        else:
            hub_url = getattr(args, "hub_url", env("HUB_URL", "ws://127.0.0.1:8765/connectors"))
            token = getattr(args, "hub_token", env("CONNECTOR_TOKEN", "dev-token"))
            state = getattr(args, "state", None) or env("ADAPTER_STATE_FILE") or f".{adapter.name}_state.json"
            asyncio.run(run_adapter(adapter, hub_url, token, state))
    except KeyboardInterrupt:
        pass
