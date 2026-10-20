"""The connector hub of the app: connectors dial in over WebSocket, every frame is recorded for the UI's wire log, and
stored items flow into the memory pipeline.
"""

from __future__ import annotations

import asyncio
import copy
import itertools
import json
from collections import Counter, deque
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

from .hub_base import REQUIRED_ITEM_FIELDS, DevHub, Session, iso
from .pipeline import Pipeline

MAX_FRAMES = 500
MAX_LOG = 500
PREVIEW_ITEMS = 3
PREVIEW_CHARS = 400


def now() -> str:
    return iso(datetime.now(UTC))


def snippet(item: dict, n: int) -> str:
    text = " ".join(str(item.get("content", "")).split())
    return text if len(text) <= n else text[: n - 1] + "…"


def preview(item: dict, n: int = 110) -> str:
    author = (item.get("author") or {}).get("name", "")
    return f"{author}: {snippet(item, n)}" if author else snippet(item, n)


def slim(msg: dict) -> dict:
    """Keep frames small for the browser: first few items only, long strings cut."""
    out = copy.deepcopy(msg)

    def cut_items(holder: dict | None) -> None:
        if not isinstance(holder, dict) or not isinstance(holder.get("items"), list):
            return
        items = holder["items"]
        for it in items[:PREVIEW_ITEMS]:
            if isinstance(it, dict) and isinstance(it.get("content"), str) and len(it["content"]) > PREVIEW_CHARS:
                it["content"] = it["content"][:PREVIEW_CHARS] + "…"
        if len(items) > PREVIEW_ITEMS:
            holder["items"] = items[:PREVIEW_ITEMS] + [f"… {len(items) - PREVIEW_ITEMS} more items"]

    cut_items(out.get("params"))
    cut_items(out.get("result"))
    return out


class RecordingWS:
    """Wraps a server connection so every frame in and out is recorded. DevHub sees no difference."""

    def __init__(self, ws: Any, hub: Hub):
        self._ws = ws
        self._hub = hub
        self.connector: str | None = None
        self.requests_in: dict[Any, str] = {}    # id -> method, for requests the connector sent us
        self.requests_out: dict[Any, str] = {}   # id -> method, for requests we sent the connector

    @property
    def request(self) -> Any:
        return self._ws.request

    async def recv(self) -> Any:
        raw = await self._ws.recv()
        self._hub.record("in", raw, self)
        return raw

    async def send(self, raw: Any) -> None:
        self._hub.record("out", raw, self)
        await self._ws.send(raw)

    async def close(self, *args: Any, **kwargs: Any) -> None:
        await self._ws.close(*args, **kwargs)

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[Any]:
        async for raw in self._ws:
            self._hub.record("in", raw, self)
            yield raw


class Hub(DevHub):
    """The connector hub plus what the UI needs: recorded frames, connector state, intake log, the pipeline feed."""

    def __init__(self, *args: Any, pipeline: Pipeline | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline          # mini-cortex memory pipeline; None = intake only
        self._live = False                # True while storing a push (live) rather than a fetch (backfill)
        self.frames: deque[dict] = deque(maxlen=MAX_FRAMES)
        self.memory_log: deque[dict] = deque(maxlen=MAX_LOG)
        self.connectors: dict[str, dict] = {}                 # name -> hello info + status
        self.backfill: dict[tuple[str, str], dict] = {}       # (connector, stream) -> state
        self.subscribers: set[asyncio.Queue] = set()
        self._frame_ids = (f"f{n}" for n in itertools.count(1))
        self._log_ids = (f"ml{n}" for n in itertools.count(1))

    # --- events -----------------------------------------------------------------------------------------

    def publish(self, event: str, data: Any) -> None:
        for q in list(self.subscribers):
            if q.qsize() < 1000:
                q.put_nowait((event, data))

    def publish_connectors(self) -> None:
        self.publish("connectors", self.connector_list())

    # --- frame recording --------------------------------------------------------------------------------

    def record(self, direction: str, raw: Any, ws: RecordingWS) -> None:
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            return
        rid = msg.get("id")
        if "method" in msg:
            method = msg["method"]
            if method == "hello":
                ws.connector = (msg.get("params") or {}).get("connector")
            if rid is not None:
                (ws.requests_in if direction == "in" else ws.requests_out)[rid] = method
        else:
            req = (ws.requests_in if direction == "out" else ws.requests_out).pop(rid, None)
            if "error" in msg:
                method = "error"
            elif req == "push":
                method = "ack"
            elif req == "hello":
                method = "welcome"
            else:
                method = "result"
        frame = {"id": next(self._frame_ids), "at": now(), "dir": direction, "connector": ws.connector or "?",
                 "method": method, "summary": self.summarize(method, msg, ws.connector), "payload": slim(msg)}
        self.frames.append(frame)
        self.publish("frame", frame)

    @staticmethod
    def summarize(method: str, msg: dict, connector: str | None) -> str:
        p, r = msg.get("params") or {}, msg.get("result") or {}
        if method == "hello":
            return f"{p.get('connector')} v{p.get('version', '?')} · {len(p.get('streams', []))} streams"
        if method == "welcome":
            return f"session {r.get('session_id', '?')} · protocol {r.get('protocol', '?')}"
        if method == "fetch":
            return (f"{p.get('stream')} · {str(p.get('since', ''))[:10]} → {str(p.get('until', ''))[:10]}"
                    f" · limit {p.get('limit')}{' · ' + p['purpose'] if p.get('purpose') else ''}")
        if method == "result":
            if "items" in r:
                more = f" · has_more · cursor {r.get('next_cursor')!r}" if r.get("has_more") else ""
                return f"{len(r['items'])} items{more}"
            return json.dumps(r)[:80]
        if method == "push":
            return f"{p.get('stream')} · {len(p.get('items') or [])} items · id {msg.get('id')}"
        if method == "ack":
            return ", ".join(f"{k} {v}" for k, v in r.items() if not isinstance(v, list)) or "ok"
        if method == "error":
            e = msg.get("error") or {}
            return f"{e.get('code')} {e.get('message')}"
        if method == "log":
            return f"{p.get('level')}: {p.get('message')}"
        return json.dumps(p)[:80] if p else ""

    # --- hub hooks --------------------------------------------------------------------------------------

    async def _handle(self, ws: Any) -> None:
        if ws.request.path.startswith("/control"):
            return await super()._handle(ws)
        rec = RecordingWS(ws, self)
        try:
            await super()._handle(rec)
        finally:
            name = rec.connector
            if name and name in self.connectors and name not in self.sessions:
                self.connectors[name]["status"] = "disconnected"
                self.publish_connectors()

    async def _hello(self, ws: Any, msg: dict) -> Session | None:
        session = await super()._hello(ws, msg)
        if session is not None:
            p = msg.get("params") or {}
            self.connectors[session.name] = {
                "name": session.name, "display_name": p.get("display_name") or session.name,
                "version": p.get("version", ""), "protocol": p.get("protocol", ""),
                "streams": list(session.streams.values()), "connected_since": now(), "status": "live"}
            self.publish_connectors()
        return session

    def store(self, connector: str, stream: str, items: list[dict]) -> dict:
        counts = super().store(connector, stream, items)
        if self.pipeline:
            valid = [it for it in items if all(it.get(f) for f in REQUIRED_ITEM_FIELDS)]
            self.pipeline.ingest(connector, stream, valid, live=self._live)
        return counts

    def _push(self, session: Session, params: dict) -> dict:
        self._live = True
        try:
            reply = super()._push(session, params)
        finally:
            self._live = False
        items = params.get("items") or []
        if "result" in reply and items:
            self.log_intake("PUSH", session.name, params.get("stream", "?"), items, reply["result"])
        return reply

    async def fetch_window(self, connector: str, stream: str, since: datetime, until: datetime,
                           limit: int | None = None, purpose: str = "backfill") -> list[dict]:
        key = (connector, stream)
        self.backfill[key] = {"state": "running", "purpose": purpose, "since": iso(since), "until": iso(until)}
        self.publish_connectors()
        try:
            items = await super().fetch_window(connector, stream, since, until, limit, purpose)
        except Exception:
            self.backfill[key]["state"] = "failed"
            self.publish_connectors()
            raise
        self.backfill[key].update(state="done", fetched=len(items))
        self.log_intake("FETCH", connector, stream, items, None,
                        page=f"{purpose} · {iso(since)[:10]} → {iso(until)[:10]}")
        self.publish_connectors()
        return items

    def log_intake(self, action: str, connector: str, stream: str, items: list[dict],
                   counts: dict | None, page: str | None = None) -> None:
        first = items[0] if items else {}
        new = counts.get("accepted", 0) + counts.get("updated", 0) if counts else None
        entry = {
            "id": next(self._log_ids), "at": now(), "stage": "intake", "action": action,
            "title": f"{len(items)} {'item' if len(items) == 1 else 'items'} from {connector} {stream}",
            "reason": preview(first) if action == "PUSH" and first else None,
            "packet_ids": [],
            "source": {"source_id": first.get("source_id", ""), "connector": connector, "stream": stream,
                       "timestamp": first.get("timestamp", now()), "author": (first.get("author") or {}).get("name")},
            "detail": {"items": len(items), "page": page, "new": new,
                       "preview": [{"author": (it.get("author") or {}).get("name"),
                                    "channel": (it.get("metadata") or {}).get("channel") or it.get("thread_id"),
                                    "timestamp": it.get("timestamp"), "text": snippet(it, 160)}
                                   for it in items[:8]]},
        }
        self.memory_log.append(entry)
        if self.pipeline:
            self.pipeline.state.log(entry)     # intake rows next to packetize/dedup, kept across restarts
        self.publish("intake", entry)

    # --- views for the UI -------------------------------------------------------------------------------

    def connector_list(self) -> list[dict]:
        per_stream: Counter = Counter()
        per_channel: dict[tuple[str, str], Counter] = {}
        for (connector, _), v in self.items.items():
            key = (connector, v["stream"])
            per_stream[key] += 1
            it = v["item"]
            channel = (it.get("metadata") or {}).get("channel") or it.get("thread_id")
            if channel:
                per_channel.setdefault(key, Counter())[channel] += 1
        out = []
        for name, c in self.connectors.items():
            streams = []
            for s in c["streams"]:
                key = (name, s["name"])
                caps = s.get("capabilities") or {}
                bf = self.backfill.get(key)
                streams.append({
                    "name": s["name"], "kind": s.get("kind"), "description": s.get("description", ""),
                    "supports": [cap for cap in ("fetch", "push") if caps.get(cap)],
                    "items": per_stream[key],
                    "state": bf["state"] if bf else "idle",
                    "last_fetch": bf,
                    "backfill": None,
                    "channels": [{"name": ch, "items": n} for ch, n in sorted(per_channel.get(key, Counter()).items())],
                })
            out.append({"name": name, "label": c["display_name"], "version": c["version"], "status": c["status"],
                        "detail": f"protocol {c['protocol']}", "connected_since": c["connected_since"],
                        "streams": streams})
        return out

    def recent_items(self, limit: int) -> list[dict]:
        rows = [{"connector": c, "stream": v["stream"], "item": v["item"]} for (c, _), v in self.items.items()]
        rows.sort(key=lambda r: r["item"].get("timestamp", ""), reverse=True)
        return rows[:limit]
