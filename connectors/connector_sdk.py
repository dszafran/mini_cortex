"""Minimal SDK for the mini-cortex connector protocol v1 (see PROTOCOL.md).

A connector author describes their streams and writes two things:
  * a fetch handler:  async (since, until, cursor, limit) -> Page   (answers the hub's `fetch`)
  * calls to `await connector.push(stream, items)` whenever the source produces something new.

The SDK handles the rest: dial-in WebSocket with the token, `hello` first, JSON-RPC request/response matching,
acknowledged pushes (one in flight per stream, retry until acked, honour rate limits), reconnect with backoff,
and a small JSON state file for the connector's own push cursor.

    connector = Connector("notes", hub_url="ws://127.0.0.1:8765/connectors", token="dev-token")
    connector.add_stream(Stream("notes", kind="note", fetch=my_fetch, time_filter="emulated"))
    await connector.run()      # runs forever; reconnects on its own
"""

from __future__ import annotations

import asyncio
import base64
import itertools
import json
import logging
import os
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

PROTOCOL_VERSION = "1.1"
DEFAULT_LIMITS = {"max_frame_bytes": 1_048_576, "max_items_per_message": 50, "max_content_chars": 200_000}
FATAL_CLOSE_CODES = {4001: "auth failed", 4002: "unsupported protocol version", 4003: "duplicate connector name",
                     4008: "no hello within 10 s"}
FATAL_HELLO_ERRORS = {-32001, -32002}
REQUEST_TIMEOUT_S = 30.0

log = logging.getLogger("connector")


# --- errors -------------------------------------------------------------------------------------------------

class RpcError(Exception):
    """A JSON-RPC error. Raise a subclass from a fetch handler to send that error to the hub."""

    code = -32603
    name = "internal_error"

    def __init__(self, message: str | None = None, data: dict | None = None, code: int | None = None):
        super().__init__(message or self.name)
        self.message = message or self.name
        self.data = data
        if code is not None:
            self.code = code

    def to_json(self) -> dict:
        err: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.data:
            err["data"] = self.data
        return err


class InvalidParams(RpcError):
    code, name = -32602, "invalid_params"


class UnknownStream(RpcError):
    code, name = -32003, "unknown_stream"


class InvalidRange(RpcError):
    code, name = -32004, "invalid_range"


class RateLimited(RpcError):
    code, name = -32005, "rate_limited"


class SourceUnavailable(RpcError):
    code, name = -32006, "source_unavailable"


class InvalidCursor(RpcError):
    code, name = -32007, "invalid_cursor"


class CapabilityUnsupported(RpcError):
    code, name = -32009, "capability_unsupported"


class FatalConnectorError(RuntimeError):
    """The hub refused us for a reason retrying will not fix (bad token, wrong protocol, duplicate name)."""


class ConnectionLost(RuntimeError):
    pass


# --- time and cursor helpers --------------------------------------------------------------------------------

def iso(dt: datetime) -> str:
    """UTC RFC 3339 with a Z suffix; milliseconds only when present."""
    dt = dt.astimezone(UTC)
    spec = "milliseconds" if dt.microsecond else "seconds"
    return dt.isoformat(timespec=spec).replace("+00:00", "Z")


def iso_from_millis(ms: int) -> str:
    return iso(datetime.fromtimestamp(ms / 1000, UTC))


def parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"timestamp without timezone: {value!r}")
    return dt.astimezone(UTC)


def encode_cursor(item: dict) -> str:
    raw = json.dumps({"t": item["timestamp"], "id": item["source_id"]}).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return parse_iso(data["t"]), data["id"]
    except Exception as e:
        raise InvalidCursor() from e


def sort_key(item: dict) -> tuple[datetime, str]:
    return parse_iso(item["timestamp"]), item["source_id"]


@dataclass
class Page:
    items: list[dict]
    next_cursor: str | None = None
    coverage: dict | None = None    # [optional, 1.1] {"since": iso} = oldest data the source could serve (§4.1.4)

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None


def paginate(items: list[dict], cursor: str | None, limit: int) -> Page:
    """Page through ALL items of a window (for sources with time_filter="emulated").

    Sorts by (timestamp, source_id) as the protocol requires; the cursor is the last returned item's key.
    """
    ordered = sorted(items, key=sort_key)
    start = 0
    if cursor:
        after = decode_cursor(cursor)
        start = next((i for i, it in enumerate(ordered) if sort_key(it) > after), len(ordered))
    page = ordered[start:start + limit]
    has_more = start + limit < len(ordered)
    return Page(page, encode_cursor(page[-1]) if has_more and page else None)


def split_item(item: dict, max_chars: int) -> list[dict]:
    """Split an oversized item into parts (PROTOCOL.md §5.3), on paragraph boundaries where possible."""
    content = item["content"]
    if len(content) <= max_chars:
        return [item]
    chunks: list[str] = []
    while content:
        if len(content) <= max_chars:
            chunks.append(content)
            break
        cut = content.rfind("\n\n", 0, max_chars)
        cut = cut if cut > max_chars // 2 else max_chars
        chunks.append(content[:cut])
        content = content[cut:].lstrip("\n")
    total = len(chunks)
    return [{**item, "source_id": f"{item['source_id']}#p{i}", "content": c, "part": {"index": i, "total": total}}
            for i, c in enumerate(chunks, start=1)]


# --- connector state (the connector's own push cursor) ------------------------------------------------------

class State:
    """A small JSON file. Writes are atomic (temp file + rename), so a crash never leaves it half-written."""

    def __init__(self, path: str | Path | None):
        self.path = Path(path) if path else None
        self.data: dict[str, Any] = {}
        if self.path and self.path.is_file():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value
        if self.path:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
            os.replace(tmp, self.path)


# --- streams and the connector ------------------------------------------------------------------------------

FetchHandler = Callable[[datetime, datetime, str | None, int], Awaitable[Page]]


@dataclass
class Stream:
    name: str
    kind: str = "other"                     # chat | email | note | issue | event | other
    description: str = ""
    fetch: FetchHandler | None = None       # None = push-only stream
    push: bool = True
    time_filter: str = "native"             # native | emulated
    max_limit: int = 200
    earliest: datetime | None = None

    def hello_entry(self) -> dict:
        caps: dict[str, Any] = {"fetch": self.fetch is not None, "push": self.push,
                                "time_filter": self.time_filter, "max_limit": self.max_limit}
        if self.earliest:
            caps["earliest"] = iso(self.earliest)
        return {"name": self.name, "description": self.description, "kind": self.kind, "capabilities": caps}


@dataclass
class Connector:
    name: str
    hub_url: str
    token: str
    display_name: str = ""
    version: str = "0.1.0"
    state_path: str | Path | None = None
    streams: dict[str, Stream] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.state = State(self.state_path)
        self.hello_result: dict | None = None
        self.limits = dict(DEFAULT_LIMITS)
        self.connected = asyncio.Event()     # set after a successful hello, cleared on disconnect
        self._ws: ClientConnection | None = None
        self._ids = (f"c-{n}" for n in itertools.count(1))
        self._pending: dict[str, asyncio.Future] = {}
        self._stream_locks: dict[str, asyncio.Lock] = {}
        self._push_slots = asyncio.Semaphore(4)   # at most 4 unacknowledged pushes across all streams
        self._tasks: set[asyncio.Task] = set()
        self._on_connect: list[Callable[[dict], Awaitable[None]]] = []

    def add_stream(self, stream: Stream) -> None:
        self.streams[stream.name] = stream
        self._stream_locks[stream.name] = asyncio.Lock()

    def on_connect(self, callback: Callable[[dict], Awaitable[None]]) -> None:
        """Run `callback(hello_result)` after every successful hello (e.g. to start or resume a watcher)."""
        self._on_connect.append(callback)

    # --- public API ------------------------------------------------------------------------------------

    async def run(self) -> None:
        """Connect, say hello, serve fetches; reconnect with exponential backoff (1 s, 2 s, 4 s ... 30 s)."""
        attempt = 0
        while True:
            try:
                async with connect(self.hub_url, additional_headers={"Authorization": f"Bearer {self.token}"},
                                   max_size=DEFAULT_LIMITS["max_frame_bytes"]) as ws:
                    await self._session(ws)
                    attempt = 0
                    if ws.close_code in FATAL_CLOSE_CODES:
                        raise FatalConnectorError(f"hub closed the connection: {ws.close_code} "
                                                  f"{FATAL_CLOSE_CODES[ws.close_code]}")
            except ConnectionClosed as e:
                code = e.rcvd.code if e.rcvd else None
                if code in FATAL_CLOSE_CODES:
                    raise FatalConnectorError(f"hub closed the connection: {code} {FATAL_CLOSE_CODES[code]}") from e
                log.warning("connection to hub lost (%s)", code or "no close frame")
            except InvalidStatus as e:
                if e.response.status_code in (401, 403):
                    raise FatalConnectorError(f"hub rejected the token (HTTP {e.response.status_code})") from e
                log.warning("hub refused the WebSocket handshake: HTTP %s", e.response.status_code)
            except ConnectionLost as e:
                log.warning("connection to hub lost: %s", e)
            except (OSError, InvalidHandshake, TimeoutError) as e:
                log.warning("cannot reach hub at %s: %s", self.hub_url, e)
            finally:
                self._disconnected()
            delay = min(30.0, 2.0 ** attempt) * random.uniform(0.8, 1.2)
            attempt += 1
            log.info("reconnecting in %.1f s", delay)
            await asyncio.sleep(delay)

    async def push(self, stream: str, items: list[dict]) -> dict:
        """Deliver items to the hub and return once they are acknowledged (stored).

        Retries across disconnects and rate limits, so a caller can safely advance its own cursor afterwards.
        Returns the summed ack counters: {"accepted", "duplicates", "updated", "rejected": [...]}.
        """
        if stream not in self.streams:
            raise UnknownStream(data={"stream": stream})
        parts = [p for it in items for p in split_item(it, self.limits["max_content_chars"])]
        size = self.limits["max_items_per_message"]
        total = {"accepted": 0, "duplicates": 0, "updated": 0, "rejected": []}
        for i in range(0, len(parts), size):
            result = await self._push_chunk(stream, parts[i:i + size])
            for key in ("accepted", "duplicates", "updated"):
                total[key] += result.get(key, 0)
            total["rejected"] += result.get("rejected", [])
        return total

    async def log(self, level: str, message: str) -> None:
        """Optional `log` notification: shows up in the hub's Sources tab. Dropped silently when offline."""
        if self._ws and self.connected.is_set():
            await self._send({"jsonrpc": "2.0", "method": "log", "params": {"level": level, "message": message}})

    # --- internals -------------------------------------------------------------------------------------

    async def _push_chunk(self, stream: str, items: list[dict]) -> dict:
        async with self._stream_locks[stream], self._push_slots:   # one unacked push per stream
            while True:
                await self.connected.wait()
                try:
                    return await self._call("push", {"stream": stream, "items": items})
                except RateLimited as e:
                    wait_ms = (e.data or {}).get("retry_after_ms", 1000)
                    log.warning("hub is busy, retrying push in %d ms", wait_ms)
                    await asyncio.sleep(wait_ms / 1000)
                except (ConnectionLost, TimeoutError):
                    log.warning("push not acknowledged, will resend after reconnect")
                    await asyncio.sleep(0.5)

    async def _session(self, ws: ClientConnection) -> None:
        self._ws = ws
        reader = asyncio.create_task(self._reader(ws))
        try:
            hello = await self._call("hello", {
                "protocol": PROTOCOL_VERSION, "connector": self.name,
                "display_name": self.display_name or self.name, "version": self.version,
                "streams": [s.hello_entry() for s in self.streams.values()],
            })
        except RpcError as e:
            reader.cancel()
            if e.code in FATAL_HELLO_ERRORS:
                raise FatalConnectorError(f"hello refused: {e.code} {e.message}") from e
            raise ConnectionLost(f"hello failed: {e.message}") from e
        except ConnectionLost:
            await reader          # surfaces the ConnectionClosed (and its close code) to run()
            raise
        self.hello_result = hello
        self.limits.update(hello.get("limits") or {})
        for name, info in (hello.get("streams") or {}).items():
            if not info.get("accepted", True):
                log.warning("hub ignores stream %r: %s", name, info.get("reason", "no reason given"))
        log.info("connected to %s (session %s)", hello.get("hub", "hub"), hello.get("session_id"))
        self.connected.set()
        for cb in self._on_connect:
            self._spawn(cb(hello))
        await reader              # returns/raises when the socket closes

    def _disconnected(self) -> None:
        self.connected.clear()
        self._ws = None
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionLost())
        self._pending.clear()

    async def _send(self, msg: dict) -> None:
        if self._ws is None:
            raise ConnectionLost()
        try:
            await self._ws.send(json.dumps(msg, ensure_ascii=False))
        except ConnectionClosed as e:
            raise ConnectionLost() from e

    async def _call(self, method: str, params: dict) -> Any:
        rid = next(self._ids)
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            await self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
            msg = await asyncio.wait_for(fut, REQUEST_TIMEOUT_S)
        finally:
            self._pending.pop(rid, None)
        if "error" in msg:
            err = msg["error"]
            cls = next((c for c in RpcError.__subclasses__() if c.code == err.get("code")), RpcError)
            raise cls(err.get("message"), err.get("data"), code=err.get("code"))
        return msg.get("result")

    async def _reader(self, ws: ClientConnection) -> None:
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    await self._send({"jsonrpc": "2.0", "id": None,
                                      "error": {"code": -32700, "message": "parse_error"}})
                    continue
                if "method" in msg and "id" in msg:            # a request from the hub
                    self._spawn(self._answer(msg))
                elif "id" in msg:                              # a response to one of our requests
                    fut = self._pending.get(msg["id"])
                    if fut and not fut.done():
                        fut.set_result(msg)
                # notifications from the hub: none defined in v1, ignore
        finally:
            self._disconnected()   # fail in-flight requests now instead of waiting for their timeout

    async def _answer(self, msg: dict) -> None:
        try:
            result = await self._dispatch(msg["method"], msg.get("params") or {})
            reply = {"jsonrpc": "2.0", "id": msg["id"], "result": result}
        except RpcError as e:
            reply = {"jsonrpc": "2.0", "id": msg["id"], "error": e.to_json()}
        except Exception as e:  # a bug in the handler: report it, keep the connection alive
            log.exception("handler for %s failed", msg.get("method"))
            reply = {"jsonrpc": "2.0", "id": msg["id"], "error": {"code": -32603, "message": f"internal_error: {e}"}}
        try:
            await self._send(reply)
        except ConnectionLost:
            pass  # the hub re-asks after reconnecting

    async def _dispatch(self, method: str, params: dict) -> Any:
        if method == "ping":
            return {"pong": True}
        if method != "fetch":
            raise RpcError("method_not_found", {"method": method}, code=-32601)
        stream = self.streams.get(params.get("stream", ""))
        if stream is None:
            raise UnknownStream(data={"stream": params.get("stream")})
        if stream.fetch is None:
            raise CapabilityUnsupported(data={"capability": "fetch"})
        try:
            since, until = parse_iso(params["since"]), parse_iso(params["until"])
        except (KeyError, ValueError) as e:
            raise InvalidParams(data={"field": "since/until", "reason": str(e)}) from e
        if since >= until:
            raise InvalidRange("since must be before until")
        limit = max(1, min(int(params.get("limit") or 50), stream.max_limit))
        cursor = params.get("cursor")
        # Keep the response frame under the size limit by asking the handler for smaller pages.
        while True:
            page = await stream.fetch(since, until, cursor, limit)
            result = {"items": page.items, "next_cursor": page.next_cursor, "has_more": page.has_more}
            if page.coverage:
                result["coverage"] = page.coverage
            size = len(json.dumps(result, ensure_ascii=False).encode()) + 200
            if size <= self.limits["max_frame_bytes"]:
                return result
            if limit == 1:
                raise RpcError("a single item exceeds the frame size; split it (PROTOCOL.md §5.3)")
            limit = max(1, len(page.items) // 2)

    def _spawn(self, coro: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
