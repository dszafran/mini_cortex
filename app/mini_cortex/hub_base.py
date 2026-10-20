"""Hub side of the connector protocol (PROTOCOL.md), vendored from connectors/dev_hub.py. Keep in sync.

Vendored rather than imported from ../connectors so the app is one self-contained package: no sys.path tricks, one
virtualenv, and the connectors project stays free to pin its own dependencies. tests/test_hub.py checks the protocol
version and limits still match the connector SDK.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

PROTOCOL_VERSION = "1.1"    # connector_sdk.PROTOCOL_VERSION


def iso(dt: datetime) -> str:
    """UTC RFC 3339 with a Z suffix; milliseconds only when present."""
    dt = dt.astimezone(UTC)
    spec = "milliseconds" if dt.microsecond else "seconds"
    return dt.isoformat(timespec=spec).replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"timestamp without timezone: {value!r}")
    return dt.astimezone(UTC)


log = logging.getLogger("dev-hub")
LIMITS = {"max_frame_bytes": 1_048_576, "max_items_per_message": 50, "max_content_chars": 200_000}
REQUIRED_ITEM_FIELDS = ("source_id", "content", "timestamp")


@dataclass
class Session:
    name: str
    ws: ServerConnection
    streams: dict[str, dict]
    pending: dict[str, asyncio.Future] = field(default_factory=dict)


class DevHub:
    def __init__(self, token: str = "dev-token", backfill_days: int | None = None,
                 store_path: str | Path | None = None, fetch_limit: int = 50):
        self.token = token
        self.backfill_days = backfill_days
        self.fetch_limit = fetch_limit
        self.store_path = Path(store_path) if store_path else None
        self.items: dict[tuple[str, str], dict] = {}        # (connector, source_id) -> {"stream", "item"}
        self.sessions: dict[str, Session] = {}
        self.pushes: asyncio.Queue[dict] = asyncio.Queue()  # every push batch, for tests
        self.rate_limit_next_pushes = 0                     # >0: answer that many pushes with -32005 (tests)
        self._ids = (f"h-{n}" for n in itertools.count(1))

    # --- server -----------------------------------------------------------------------------------------

    @asynccontextmanager
    async def serving(self, host: str = "127.0.0.1", port: int = 8765) -> AsyncIterator[Server]:
        async with serve(self._handle, host, port, max_size=LIMITS["max_frame_bytes"]) as server:
            yield server

    async def _handle(self, ws: ServerConnection) -> None:
        url = urlsplit(ws.request.path)
        if url.path == "/control":
            return await self._control(ws)
        if url.path != "/connectors":
            return await ws.close(1008, "unknown path")
        auth = ws.request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip() or parse_qs(url.query).get("token", [""])[0]
        if token != self.token:
            log.warning("rejected connector: bad token")
            return await ws.close(4001, "auth failed")
        try:
            raw = await asyncio.wait_for(ws.recv(), 10)
        except TimeoutError:
            return await ws.close(4008, "no hello within 10 s")
        msg = json.loads(raw)
        if msg.get("method") != "hello":
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": msg.get("id"),
                                      "error": {"code": -32008, "message": "not_ready"}}))
            return await ws.close(4008, "hello must come first")
        session = await self._hello(ws, msg)
        if session is None:
            return
        try:
            async for raw in ws:
                await self._on_message(session, json.loads(raw))
        except ConnectionClosed:
            pass
        finally:
            for fut in session.pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("connector disconnected"))
            if self.sessions.get(session.name) is session:
                del self.sessions[session.name]
            log.info("connector %r disconnected", session.name)

    async def _hello(self, ws: ServerConnection, msg: dict) -> Session | None:
        p = msg.get("params") or {}
        if str(p.get("protocol", "")).split(".")[0] != PROTOCOL_VERSION.split(".")[0]:
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "error": {
                "code": -32002, "message": "unsupported_protocol", "data": {"supported": [PROTOCOL_VERSION]}}}))
            await ws.close(4002, "unsupported protocol")
            return None
        name = p.get("connector", "")
        if name in self.sessions:
            await ws.close(4003, "duplicate connector name")
            return None
        streams = {s["name"]: s for s in p.get("streams", [])}
        session = Session(name, ws, streams)
        self.sessions[name] = session
        result = {"protocol": PROTOCOL_VERSION, "session_id": f"s-{os.urandom(2).hex()}", "hub": "dev-hub/0.1.0",
                  "limits": LIMITS,
                  "streams": {s: {"accepted": True, "last_timestamp": self.last_timestamp(name, s)} for s in streams}}
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": result}))
        log.info("hello from %r: streams %s", name,
                 ", ".join(f"{s} ({v.get('kind')}, {v['capabilities'].get('time_filter')})" for s, v in streams.items()))
        asyncio.create_task(self._after_hello(session))
        return session

    async def _after_hello(self, session: Session) -> None:
        """Backfill (first time) or catch-up (reconnect) for every fetchable stream (PROTOCOL.md §7.2)."""
        for stream, info in session.streams.items():
            if not info["capabilities"].get("fetch"):
                continue
            now = datetime.now(UTC)
            last = self.last_timestamp(session.name, stream)
            if last:
                since, purpose = parse_iso(last) - timedelta(minutes=5), "catchup"
            elif self.backfill_days:
                since, purpose = now - timedelta(days=self.backfill_days), "backfill"
            else:
                continue
            try:
                items = await self.fetch_window(session.name, stream, since, now, purpose=purpose)
                log.info("%s %s/%s: %d item(s) in [%s, %s)", purpose, session.name, stream, len(items),
                         iso(since), iso(now))
            except Exception as e:
                log.warning("%s %s/%s failed: %s", purpose, session.name, stream, e)

    async def _on_message(self, session: Session, msg: dict) -> None:
        if "method" not in msg:                     # a response to one of our fetches
            fut = session.pending.pop(msg.get("id"), None)
            if fut and not fut.done():
                fut.set_result(msg)
            return
        method, rid = msg["method"], msg.get("id")
        if method == "log":
            log.info("[%s log] %s: %s", session.name, msg["params"].get("level"), msg["params"].get("message"))
            return
        if method == "ping":
            reply: dict[str, Any] = {"result": {"pong": True}}
        elif method == "push":
            reply = self._push(session, msg.get("params") or {})
        else:
            reply = {"error": {"code": -32601, "message": "method_not_found", "data": {"method": method}}}
        if rid is not None:
            await session.ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, **reply}))

    def _push(self, session: Session, params: dict) -> dict:
        stream, items = params.get("stream"), params.get("items") or []
        if stream not in session.streams:
            return {"error": {"code": -32003, "message": "unknown_stream", "data": {"stream": stream}}}
        if self.rate_limit_next_pushes > 0:
            self.rate_limit_next_pushes -= 1
            return {"error": {"code": -32005, "message": "rate_limited", "data": {"retry_after_ms": 200}}}
        counts = self.store(session.name, stream, items)
        log.info("push %s/%s: %s", session.name, stream, counts)
        for it in items:
            log.info("   %s  %-28s %s", it.get("timestamp"), it.get("source_id"), _preview(it))
        self.pushes.put_nowait({"connector": session.name, "stream": stream, "items": items, "result": counts})
        return {"result": counts}

    # --- storage ----------------------------------------------------------------------------------------

    def store(self, connector: str, stream: str, items: list[dict]) -> dict:
        counts: dict[str, Any] = {"accepted": 0, "duplicates": 0, "updated": 0}
        rejected = []
        for it in items:
            missing = [f for f in REQUIRED_ITEM_FIELDS if not it.get(f)]
            if missing:
                rejected.append({"source_id": it.get("source_id", "?"), "reason": f"missing {', '.join(missing)}"})
                continue
            key = (connector, it["source_id"])
            old = self.items.get(key)
            if old and old["item"] == it:
                counts["duplicates"] += 1
                continue
            counts["updated" if old else "accepted"] += 1
            self.items[key] = {"stream": stream, "item": it}
            if self.store_path:
                with self.store_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"connector": connector, "stream": stream, "item": it}) + "\n")
        if rejected:
            counts["rejected"] = rejected
        return counts

    def last_timestamp(self, connector: str, stream: str) -> str | None:
        ts = [parse_iso(v["item"]["timestamp"]) for (c, _), v in self.items.items() if c == connector and v["stream"] == stream]
        return iso(max(ts)) if ts else None

    # --- fetch ------------------------------------------------------------------------------------------

    async def call(self, connector: str, method: str, params: dict, timeout: float = 30) -> dict:
        session = self.sessions.get(connector)
        if session is None:
            raise LookupError(f"connector {connector!r} is not connected (connected: {sorted(self.sessions)})")
        rid = next(self._ids)
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        session.pending[rid] = fut
        await session.ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}))
        msg = await asyncio.wait_for(fut, timeout)
        if "error" in msg:
            raise RuntimeError(f"{method} failed: {msg['error']}")
        return msg["result"]

    async def fetch_window(self, connector: str, stream: str, since: datetime, until: datetime,
                           limit: int | None = None, purpose: str = "backfill") -> list[dict]:
        """Page through a whole window, store the items, and return them (checking order across pages)."""
        out: list[dict] = []
        cursor = None
        while True:
            res = await self.call(connector, "fetch", {"stream": stream, "since": iso(since), "until": iso(until),
                                                       "cursor": cursor, "limit": limit or self.fetch_limit,
                                                       "purpose": purpose})
            out += res["items"]
            if not res["has_more"]:
                break
            cursor = res["next_cursor"]
        keys = [(parse_iso(i["timestamp"]), i["source_id"]) for i in out]
        if keys != sorted(keys):
            log.warning("fetch %s/%s: items are NOT in ascending (timestamp, source_id) order", connector, stream)
        outside = [i["source_id"] for i in out if not since <= parse_iso(i["timestamp"]) < until]
        if outside:
            log.warning("fetch %s/%s: %d item(s) outside the window: %s", connector, stream, len(outside), outside[:5])
        self.store(connector, stream, out)
        return out

    # --- control channel (used by the `fetch` and `status` CLI commands) ---------------------------------

    async def _control(self, ws: ServerConnection) -> None:
        async for raw in ws:
            req = json.loads(raw)
            try:
                if req["cmd"] == "status":
                    res: Any = {"connectors": {n: list(s.streams) for n, s in self.sessions.items()},
                                "items": len(self.items)}
                elif req["cmd"] == "fetch":
                    items = await self.fetch_window(req["connector"], req["stream"], parse_iso(req["since"]),
                                                    parse_iso(req["until"]), req.get("limit"), purpose="agent")
                    res = {"items": items}
                else:
                    res = {"error": f"unknown command {req['cmd']!r}"}
            except Exception as e:
                res = {"error": str(e)}
            await ws.send(json.dumps(res))


def _preview(item: dict, n: int = 70) -> str:
    author = (item.get("author") or {}).get("name", "")
    text = " ".join(item.get("content", "").split())
    text = text if len(text) <= n else text[: n - 1] + "…"
    return f"{author}: {text}" if author else text
