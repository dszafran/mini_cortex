"""The memory pipeline: intake → packetize → dedup → packets (PLAN §6).

Two ways in:
- Process (button): every pending item, e.g. after a backfill.
- Live buffer: pushed items wait per conversation until N messages or T seconds, then that conversation is processed.

Extraction runs for several windows at once; dedup then writes their packets one at a time, so packets from the same
run can merge with each other.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import openai

from .connector_items import source_ref
from .dedup import dedup
from .doctor import FATAL, explain
from .llm import LLM
from .llm_config import LLMSettings
from .memory_config import MemoryConfig, load_memory_config, save_memory_config
from .packetize import Window, build_windows, chunk_long_items, extract, window_title
from .store import DimensionMismatch, PacketStore, StateDB, conversation_key, data_dir

log = logging.getLogger("pipeline")
Publish = Callable[[str, Any], None]


class ModelUnreachable(RuntimeError):
    """The model endpoint refuses or can't be reached; the message says how to fix it."""


def is_fatal(result: object) -> bool:
    """Errors that hit every call alike (not a single stalled or bad reply)."""
    return isinstance(result, FATAL) and not isinstance(result, openai.APITimeoutError)


class Pipeline:
    def __init__(self, settings: LLMSettings, publish: Publish | None = None, data: Path | None = None,
                 llm: LLM | None = None, cfg: MemoryConfig | None = None):
        self.dir = data or data_dir()
        self.state = StateDB(self.dir / "state.sqlite")
        self.cfg = cfg or load_memory_config()
        self.publish = publish or (lambda event, payload: None)
        self.lock = asyncio.Lock()
        self.buffer: dict[str, dict] = {}          # conversation -> {"first": loop time, "n": new items}
        self.progress: dict[str, Any] = {"state": "idle", "done": 0, "total": 0, "error": None, "trigger": None}
        self._task: asyncio.Task | None = None
        self.set_llm(settings, llm)

    def set_llm(self, settings: LLMSettings, llm: LLM | None = None) -> None:
        """Called again when the Models settings change."""
        self.settings = settings
        # Gateways sometimes keep a request open and never answer: give up on a call after 90 s and retry it.
        self.llm = llm or LLM(settings, timeout=60, max_retries=2, deadline=90, stall_retries=2)
        self._packets: PacketStore | None = None

    @property
    def packets(self) -> PacketStore:
        if self._packets is None:
            self._packets = PacketStore(self.dir / "lance", self.settings.embed_dim)
        return self._packets

    # --- intake -----------------------------------------------------------------------------------------

    def ingest(self, connector: str, stream: str, items: list[dict], live: bool) -> int:
        """Store items as pending. Live items also start their conversation's buffer clock."""
        changed = self.state.upsert_raw(connector, stream, items)
        if live:
            t = asyncio.get_running_loop().time()
            for it in changed:
                b = self.buffer.setdefault(conversation_key(connector, it), {"first": t, "n": 0})
                b["n"] += 1
        if changed:
            self.publish("stats", self.stats())
        return len(changed)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._buffer_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    async def _buffer_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            k, t = self.cfg.pipeline, asyncio.get_running_loop().time()
            due = [c for c, b in self.buffer.items() if b["n"] >= k.buffer_messages or t - b["first"] >= k.buffer_seconds]
            for conv in due:
                self.buffer.pop(conv, None)
                try:
                    await self.process(conv=conv, trigger="live")
                except Exception:  # noqa: BLE001 — a failed flush must not stop the buffer
                    log.exception("live flush of %s failed", conv)

    # --- processing -------------------------------------------------------------------------------------

    async def process(self, conv: str | None = None, trigger: str = "manual") -> dict:
        if trigger == "manual" and self.lock.locked():
            return {"busy": True, **self.progress}
        async with self.lock:
            rows = self.state.pending(conv)
            if not rows:
                return {"windows": 0}
            windows = chunk_long_items(build_windows(rows, self.cfg), self.cfg)
            self._attach_context(windows)
            windows.sort(key=lambda w: w.rows[0]["item"]["timestamp"])
            self._set_progress(state="running", done=0, total=len(windows), error=None, trigger=trigger)

            sem = asyncio.Semaphore(self.cfg.pipeline.parallel_windows)

            async def extract_one(w: Window) -> list[dict] | Exception:
                async with sem:
                    try:
                        return await extract(self.llm, w, self.cfg)
                    except Exception as e:  # noqa: BLE001 — one bad window must not stop the run
                        return e

            async def tagged(w: Window) -> tuple[Window, list[dict] | Exception]:
                return w, await extract_one(w)

            # Written as soon as each extraction finishes: one slow call must not hold up the rest. The merge
            # prompt carries dates, so "newer wins" does not depend on the order packets arrive in.
            tasks = [asyncio.create_task(tagged(w)) for w in windows]
            try:
                for next_done in asyncio.as_completed(tasks):
                    w, drafts = await next_done
                    if is_fatal(drafts):
                        # Wrong key / no network: every other window would fail the same way. Stop, keep the items
                        # pending, and say how to fix it instead of marking the whole intake as failed.
                        raise ModelUnreachable(explain(drafts, self.settings, self.settings.extract_model)
                                               or str(drafts))
                    try:
                        await self._write(w, drafts)
                    except DimensionMismatch:
                        raise
                    except Exception as e:  # noqa: BLE001 — a stalled dedup call fails its window, not the run
                        log.warning("writing %s failed: %s", window_title(w), e)
                        await self._write(w, e)
                    self._set_progress(done=self.progress["done"] + 1)
            except (DimensionMismatch, ModelUnreachable) as e:
                self._set_progress(state="failed", error=str(e))
                return {"error": str(e)}
            except BaseException as e:
                self._set_progress(state="failed", error=f"run stopped: {e!r}"[:300])
                raise
            finally:
                for t in tasks:
                    t.cancel()
            self._set_progress(state="done")
            return {"windows": len(windows)}

    async def _write(self, w: Window, drafts: list[dict] | Exception) -> None:
        keys = [(r["connector"], r["item"]["source_id"]) for r in w.rows]
        first = source_ref(w.rows[0], w.label)
        preview = [{"author": (r["item"].get("author") or {}).get("name"), "channel": w.label,
                    "timestamp": r["item"]["timestamp"], "text": r["item"].get("content", "")[:300]}
                   for r in w.rows[:8]]
        if isinstance(drafts, Exception):
            # Errors stay retryable: Process picks up `error` items again (see StateDB.pending).
            self.state.mark(keys, "error", str(drafts)[:500])
            self._log({"stage": "packetize", "action": "SKIP", "title": window_title(w), "packet_ids": [],
                       "reason": f"Failed: {str(drafts)[:200]}", "source": first,
                       "detail": {"items": len(w.rows), "preview": preview}})
            return
        produced = []
        for d in drafts:
            entry = await dedup(self.llm, self.packets, d, self.cfg)
            self._log(entry)
            produced += entry["packet_ids"] if entry["action"] != "SKIP" else []
        self.state.mark(keys, "packetized")
        self._log({"stage": "packetize", "action": "PACKETIZE", "title": window_title(w),
                   "packet_ids": list(dict.fromkeys(produced)),
                   "reason": None if drafts else "Nothing durable in these messages.", "source": first,
                   "detail": {"items": len(w.rows), "produced": list(dict.fromkeys(produced)), "preview": preview}})

    def _attach_context(self, windows: list[Window]) -> None:
        n = self.cfg.pipeline.context_items
        prev: dict[str, Window] = {}
        for w in windows:
            if w.conv in prev:
                w.context = (prev[w.conv].context + prev[w.conv].rows)[-n:]
            else:
                w.context = self.state.context_before(w.conv, w.rows[0]["item"]["timestamp"], n)
            prev[w.conv] = w

    async def rebuild(self) -> None:
        """Forget all packets and decisions; every item goes back to pending. For θ / type / prompt experiments."""
        async with self.lock:
            self.packets.clear()
            self.state.clear_pipeline_log()
            self.state.reset_all_pending()
            self.buffer.clear()
            self._set_progress(state="idle", done=0, total=0, error=None)
            self.publish("stats", self.stats())

    def _log(self, entry: dict) -> None:
        self.publish("memory", self.state.log(entry))

    def _set_progress(self, **kw: Any) -> None:
        self.progress.update(kw)
        self.publish("progress", self.progress)
        self.publish("stats", self.stats())

    # --- views ------------------------------------------------------------------------------------------

    def stats(self) -> dict:
        raw = self.state.raw_counts()
        counts = self.state.log_counts()
        empty = self.state.db.execute("select count(*) from memory_log where action='PACKETIZE' "
                                      "and packet_ids='[]'").fetchone()[0]
        streams = self.state.db.execute("select count(distinct connector || stream) from raw_items").fetchone()[0]
        try:
            live = self.packets.all(include_deprecated=False)
        except DimensionMismatch:
            live = []
        return {"intake": sum(raw.values()), "streams": streams, "pending": raw.get("pending", 0),
                "errors": raw.get("error", 0), "packetized": raw.get("packetized", 0), "nothing_durable": empty,
                "merged": counts.get(("dedup", "MERGE"), 0), "skipped_known": counts.get(("dedup", "SKIP"), 0),
                "packets": len(live), "types": len({p["type"] for p in live}), "progress": self.progress}

    def settings_view(self) -> dict:
        k = self.cfg.pipeline
        return {"lens": "work", "theta": k.theta, "buffer": {"messages": k.buffer_messages, "seconds": k.buffer_seconds},
                "types": [{"name": t.name, "definition": t.definition, "enabled": t.enabled} for t in self.cfg.types]}

    def update_settings(self, s: dict) -> dict:
        k = self.cfg.pipeline
        k.theta = float(s.get("theta", k.theta))
        if "buffer" in s:
            k.buffer_messages = int(s["buffer"].get("messages", k.buffer_messages))
            k.buffer_seconds = float(s["buffer"].get("seconds", k.buffer_seconds))
        enabled = {t["name"]: t["enabled"] for t in s.get("types", [])}
        for t in self.cfg.types:
            t.enabled = enabled.get(t.name, t.enabled)
        save_memory_config(self.cfg)
        return self.settings_view()
