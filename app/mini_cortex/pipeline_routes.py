"""HTTP routes for memory and chat (consumed by web/src/api/client.ts in live mode).

    GET  /api/stats                    pipeline counts + progress
    GET  /api/packets                  all packets (newest first)
    GET  /api/packets/{id}/sources     the packet + its original messages (text, author, time, link)
    GET  /api/raw?connector=&source_id=  one original item
    GET  /api/memory-log               packetize / dedup / intake decisions (newest first)
    POST /api/pipeline/process         process every pending item (the Process button)
    POST /api/pipeline/rebuild         forget packets, mark everything pending
    GET  /api/settings, PUT            θ, buffer, enabled packet types (memory_types.yaml)
    GET  /api/threads                  chat threads
    GET  /api/threads/{id}/messages
    POST /api/chat                     {"thread_id"?, "message"} → server-sent events: thread | step | token | citations | done
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .chat import Chat
from .doctor import explain, short
from .pipeline import Pipeline
from .store import DimensionMismatch


class ChatRequest(BaseModel):
    message: str
    thread_id: str | None = None


def make_router(pipeline: Pipeline) -> APIRouter:
    router = APIRouter(prefix="/api")

    @router.get("/stats")
    async def stats() -> dict:
        return pipeline.stats()

    @router.get("/packets")
    async def packets() -> list[dict]:
        try:
            return pipeline.packets.all()
        except DimensionMismatch as e:
            raise HTTPException(409, str(e)) from e

    @router.get("/packets/{packet_id}/sources")
    async def packet_sources(packet_id: str) -> dict:
        """The packet and the original messages it came from, so a person can check the model's work."""
        p = pipeline.packets.get(packet_id)
        if not p:
            raise HTTPException(404, f"no packet {packet_id}")
        sources = []
        for s in p["sources"]:
            raw = pipeline.state.raw_item(s["connector"], s["source_id"])
            it = raw["item"] if raw else {}
            sources.append({**s, "text": it.get("content"), "url": it.get("url") or s.get("url"),
                            "reply_to": it.get("reply_to"), "edited": bool(it.get("updated_at"))})
        packet = {k: p[k] for k in ("id", "type", "title", "content", "entities", "source_timestamp", "deprecated")}
        return {"packet": packet, "sources": sorted(sources, key=lambda s: s["timestamp"])}

    @router.get("/raw")
    async def raw(connector: str, source_id: str) -> dict:
        """One original item as the connector delivered it (any connector: only protocol fields are used)."""
        row = pipeline.state.raw_item(connector, source_id)
        if not row:
            raise HTTPException(404, f"no item {source_id} from {connector}")
        it = row["item"]
        return {"connector": connector, "stream": row["stream"], "source_id": source_id, "text": it.get("content"),
                "url": it.get("url"), "author": (it.get("author") or {}).get("name"), "timestamp": it.get("timestamp"),
                "reply_to": it.get("reply_to"), "edited": bool(it.get("updated_at")), "status": row["status"]}

    @router.get("/memory-log")
    async def memory_log(limit: int = 500) -> list[dict]:
        return pipeline.state.memory_log(limit)

    @router.post("/pipeline/process")
    async def process() -> dict:
        if pipeline.lock.locked():
            return {"busy": True, **pipeline.progress}
        asyncio.create_task(pipeline.process())   # progress arrives as events
        return {"started": True, "pending": pipeline.stats()["pending"]}

    @router.post("/pipeline/rebuild")
    async def rebuild() -> dict:
        if pipeline.lock.locked():
            raise HTTPException(409, "A run is in progress; wait for it to finish.")
        await pipeline.rebuild()
        return pipeline.stats()

    @router.get("/settings")
    async def get_settings() -> dict:
        return pipeline.settings_view()

    @router.put("/settings")
    async def put_settings(body: dict) -> dict:
        return pipeline.update_settings(body)

    @router.get("/threads")
    async def threads() -> list[dict]:
        return pipeline.state.threads()

    @router.get("/threads/{thread_id}/messages")
    async def messages(thread_id: str) -> list[dict]:
        return pipeline.state.messages(thread_id)

    @router.post("/chat")
    async def chat(req: ChatRequest) -> StreamingResponse:
        bot = Chat(pipeline.llm, pipeline.packets, pipeline.state, pipeline.cfg)

        async def events() -> AsyncIterator[str]:
            try:
                async for ev in bot.answer(req.thread_id, req.message):
                    yield f"event: {ev['type']}\ndata: {json.dumps(ev, ensure_ascii=False)}\n\n"
            except Exception as e:  # noqa: BLE001 — show the failure in the chat instead of a dropped stream
                fix = explain(e, pipeline.settings, pipeline.settings.chat_model)
                err = {"type": "error", "message": f"{short(e)[:300]}" + (f"\n\n{fix}" if fix else "")}
                yield f"event: error\ndata: {json.dumps(err)}\n\n"

        return StreamingResponse(events(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return router
