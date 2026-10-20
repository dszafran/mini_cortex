"""Replay connector for mini-cortex: serves the Herdlink story from JSONL fixtures. Offline, zero setup.

    uv run python replay_adapter.py fetch --days 60                  # dry run: print IngestItems, no hub needed
    uv run python replay_adapter.py fetch --stream mail --days 60    # the mailbox
    uv run python replay_adapter.py serve                            # talk to the hub (dev_hub.py or mini-cortex)

Streams (all from fixtures/herdlink/herdlink.jsonl):
  channels  chat    the team's public channels (#general, #product, #firmware, #noc-incidents, #farmers, #random, #live)
  dms       chat    Tess's direct messages
  mail      email   Tess's work and personal mail, threaded by in_reply_to
  notes     note    Tess's Markdown notes and journal

Time: the story is written relative to D0 = workshop day. D0 is mapped to REPLAY_D0 (times of day kept, in the
dataset's timezone), and `{D+5}` / `{D-3 10:00}` in the text become real dates, so "what's due this week" works.
Only items whose timestamp has passed are visible.

Live: the cues (fixtures/herdlink/cues.jsonl) are pushed at REPLAY_START + minute / REPLAY_SPEED. While serving,
press Enter (or `kill -USR1 <pid>`) to push the next cue right away. REPLAY_CUES picks the mode:
  timed    on schedule; Enter / SIGUSR1 pulls the next one forward (default)
  manual   only on Enter / SIGUSR1
  all-now  everything at start-up
  off      no live pushes

The four duties (source_adapter.py):
  1. authenticate  load and time-shift the fixtures
  2. map           record -> IngestItem; source_id = "<stream>:<conversation>:<record id>"; metadata.channel_id
                   is set for channels and DMs (one conversation each), mail threads by thread_id, notes by path
  3. fetch         native time filter over the shifted timestamps; paging by the base class
  4. push          the cues, on their schedule

Settings (env or .env): REPLAY_D0 (ISO date, default today in Europe/Vienna), REPLAY_START (ISO datetime, or HH:MM
on D0 in Europe/Vienna; default = when the adapter starts. Set it for the workshop so a restart keeps the
schedule), REPLAY_SPEED (1.0; 10 = ten times faster), REPLAY_CUES (timed), REPLAY_STREAMS (comma-separated, default
all), REPLAY_CHANNELS (comma-separated channel names, default all), REPLAY_LIMIT (serve only the newest N story
items, default all), REPLAY_DATASET / REPLAY_CUE_FILE (other fixture files), REPLAY_NOW (a fixed clock, ISO
datetime in Europe/Vienna; with REPLAY_D0 it builds a workshop-day memory snapshot before the day).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import sys
import threading
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from connector_sdk import iso
from source_adapter import Emit, SourceAdapter, StreamSpec, main

log = logging.getLogger("replay")
FIXTURES = Path(__file__).with_name("fixtures") / "herdlink"
DATE_TOKEN = re.compile(r"\{D([+-]\d+)(?: (\d{1,2}:\d{2}))?\}")
SUBJECT_PREFIX = re.compile(r"^((re|fwd?|aw|wg)\s*:\s*)+", re.IGNORECASE)
CUE_MODES = ("timed", "manual", "all-now", "off")
STREAMS = {
    "channels": StreamSpec("channels", kind="chat", description="Herdlink's team channels", time_filter="native"),
    "dms": StreamSpec("dms", kind="chat", description="Tess's direct messages", time_filter="native"),
    "mail": StreamSpec("mail", kind="email", description="Tess's work and personal mail", time_filter="native"),
    "notes": StreamSpec("notes", kind="note", description="Tess's notes and journal", time_filter="native"),
}
SOURCE_STREAM = {"chat": "channels", "chat_dm": "dms", "email": "mail", "note": "notes"}


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def csv_env(name: str) -> set[str]:
    return {v.strip().lstrip("#") for v in os.environ.get(name, "").split(",") if v.strip()}


class ReplayAdapter(SourceAdapter):
    name = "replay"
    display_name = "Replay (Herdlink story)"
    version = "0.1.0"

    def __init__(self, now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        super().__init__()
        env = os.environ.get
        if env("REPLAY_NOW"):
            # A fixed clock: build workshop-day snapshots before the day (with REPLAY_D0 set to that day).
            fixed = datetime.fromisoformat(env("REPLAY_NOW"))
            fixed = (fixed if fixed.tzinfo else fixed.replace(tzinfo=ZoneInfo("Europe/Vienna"))).astimezone(UTC)
            now = lambda: fixed  # noqa: E731
        self.now = now
        rows = load_jsonl(Path(env("REPLAY_DATASET") or FIXTURES / "herdlink.jsonl"))
        self.meta = next(r for r in rows if r["kind"] == "meta")
        self.tz = ZoneInfo(self.meta["timezone"])
        self.personas = {r["id"]: r for r in rows if r["kind"] == "persona"}
        self.channels = {r["id"]: r for r in rows if r["kind"] == "channel"}
        self.d0 = date.fromisoformat(env("REPLAY_D0") or now().astimezone(self.tz).date().isoformat())
        self.speed = float(env("REPLAY_SPEED") or 1.0)
        self.start = self._parse_start(env("REPLAY_START")) if env("REPLAY_START") else now()
        self.cue_mode = (env("REPLAY_CUES") or "timed").lower()
        if self.cue_mode not in CUE_MODES:
            raise SystemExit(f"REPLAY_CUES={self.cue_mode!r}; use one of {', '.join(CUE_MODES)}")

        wanted = csv_env("REPLAY_STREAMS") or set(STREAMS)
        if unknown := wanted - set(STREAMS):
            raise SystemExit(f"REPLAY_STREAMS: unknown stream(s) {', '.join(sorted(unknown))}")
        self.streams = [spec for name, spec in STREAMS.items() if name in wanted]
        self.channel_filter = csv_env("REPLAY_CHANNELS")

        items = [r for r in rows if r["kind"] == "item"]
        self.by_id = {r["id"]: r for r in items}
        self.times = self._anchor(items)
        items = [r for r in items if self._selected(SOURCE_STREAM[r["source"]], r.get("channel"))]
        if limit := int(env("REPLAY_LIMIT") or 0):
            items = sorted(items, key=lambda r: self.times[r["id"]])[-limit:]
        self.items = items

        cues = load_jsonl(Path(env("REPLAY_CUE_FILE") or FIXTURES / "cues.jsonl"))
        # l-13 stands for the participants' own posts; it is a note for the presenter, not a message.
        self.cues = sorted((c for c in cues if not c["content"].startswith("[Placeholder]")
                            and self._selected(self._cue_stream(c), c["channel"])),
                           key=lambda c: (c["minute"], c["order"]))
        self.released: dict[str, datetime] = {}    # cue id -> when it was pushed (its timestamp)
        self._next = asyncio.Event()                # set = push the next cue now

    def _parse_start(self, value: str) -> datetime:
        if re.fullmatch(r"\d{1,2}:\d{2}", value):
            return datetime.combine(self.d0, time.fromisoformat(value.zfill(5)), self.tz).astimezone(UTC)
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=self.tz)).astimezone(UTC)

    def _anchor(self, items: list[dict]) -> dict[str, datetime]:
        """Item id -> UTC time. Same day+time gets +1 s, +2 s ... in file order (as fixtures/dataset.py does)."""
        seen: Counter[tuple[int, str]] = Counter()
        out = {}
        for r in items:
            key = (r["day"], r["time"])
            local = datetime.combine(self.d0 + timedelta(days=r["day"]), time.fromisoformat(r["time"].zfill(5)),
                                     self.tz)
            out[r["id"]] = local.astimezone(UTC) + timedelta(seconds=seen[key])
            seen[key] += 1
        return out

    @staticmethod
    def _cue_stream(cue: dict) -> str:
        return "mail" if "email" in cue else "channels"

    def _selected(self, stream: str, channel: str | None) -> bool:
        if stream not in {s.name for s in self.streams}:
            return False
        return stream != "channels" or not self.channel_filter or channel in self.channel_filter

    def due(self, cue: dict) -> datetime:
        return self.start + timedelta(minutes=cue["minute"] / self.speed)

    # --- duty 1 -----------------------------------------------------------------------------------------------

    async def authenticate(self) -> None:
        counts = Counter(SOURCE_STREAM[r["source"]] for r in self.items)
        log.info("%s: D0 = %s, %s; cues %s from %s at %gx", self.meta["dataset"], self.d0,
                 ", ".join(f"{s.name} {counts[s.name]}" for s in self.streams), self.cue_mode,
                 self.start.astimezone(self.tz).strftime("%Y-%m-%d %H:%M"), self.speed)

    # --- duty 2 -----------------------------------------------------------------------------------------------

    async def to_item(self, stream: str, native: tuple[dict, datetime]) -> dict | None:
        r, ts = native
        if stream == "channels":
            return self._chat_item(r, ts)
        if stream == "dms":
            return self._dm_item(r, ts)
        if stream == "mail":
            return self._mail_item(r, ts)
        return self._note_item(r, ts)

    def render(self, text: str) -> str:
        """`{D+5}` -> "Sat 25 Oct", `{D-3 10:00}` -> "Fri 17 Oct, 10:00" (relative to REPLAY_D0)."""
        def sub(m: re.Match) -> str:
            day = self.d0 + timedelta(days=int(m.group(1)))
            s = f"{day:%a} {day.day} {day:%b}"
            return f"{s}, {m.group(2)}" if m.group(2) else s
        return DATE_TOKEN.sub(sub, text)

    def author(self, persona_id: str) -> dict:
        p = self.personas.get(persona_id)
        return {"name": p["name"], "id": p["username"]} if p else {"name": persona_id, "id": persona_id}

    def address(self, value: str) -> str:
        """A persona id in from/to/cc -> "Name <email>"; real addresses stay as they are."""
        p = self.personas.get(value)
        if not p:
            return value
        return f"{p['name']} <{p.get('email') or p['username'] + '@' + self.meta['email_domain']}>"

    def _chat_item(self, r: dict, ts: datetime) -> dict:
        channel = r["channel"]
        sid = f"channels:{channel}:{r['id']}"
        metadata = {"channel": channel, "channel_id": channel,
                    "channel_type": "private" if self.channels.get(channel, {}).get("private") else "public",
                    "team": self.meta["team"]["display_name"]}
        if r.get("post_as") == "bot" or r["author"].endswith("-bot"):
            metadata["posted_via"] = "bot"
        item = {"source_id": sid, "timestamp": iso(ts), "content": self.render(r["content"]),
                "author": self.author(r["author"]), "thread_id": channel, "metadata": metadata}
        if r.get("reply_to"):
            item["reply_to"] = f"channels:{channel}:{r['reply_to']}"
        return item

    def _dm_item(self, r: dict, ts: datetime) -> dict:
        me = self.meta["protagonist"]
        members = sorted(set(r["participants"]))
        others = [p for p in members if p != me] or members
        conv = "dm-" + "-".join(others)
        name = "dm-" + ", ".join(self.author(p)["name"] for p in others)
        return {"source_id": f"dms:{conv}:{r['id']}", "timestamp": iso(ts), "content": self.render(r["content"]),
                "author": self.author(r["author"]), "thread_id": name,
                "participants": [self.author(p)["name"] for p in members],
                "metadata": {"channel": name, "channel_id": conv, "channel_type": "dm",
                             "team": self.meta["team"]["display_name"]}}

    def _mail_item(self, r: dict, ts: datetime) -> dict:
        mail = r.get("email") or r     # a cue carries its headers in "email"
        sender = self.address(mail["from"])
        to = [self.address(a) for a in mail.get("to", [])]
        cc = [self.address(a) for a in mail.get("cc", [])]
        subject = self.render(mail.get("subject", ""))
        root = r
        while root.get("in_reply_to") in self.by_id:
            root = self.by_id[root["in_reply_to"]]
        thread = SUBJECT_PREFIX.sub("", self.render((root.get("email") or root).get("subject", ""))) or root["id"]
        header = [f"From: {sender}", f"To: {', '.join(to)}"] + ([f"Cc: {', '.join(cc)}"] if cc else [])
        content = "\n".join(header + [f"Subject: {subject}", "", self.render(r["content"])])
        name, _, addr = sender.rpartition("<")
        author = {"name": name.strip() or sender, "email": addr.rstrip(">")} if addr else {"name": sender}
        item = {"source_id": f"mail:inbox:{r['id']}", "timestamp": iso(ts), "content": content, "title": subject,
                "author": author, "participants": [sender, *to, *cc], "thread_id": thread,
                "metadata": {"from": sender, "to": to, "cc": cc, "subject": subject}}
        if r.get("in_reply_to"):
            item["reply_to"] = f"mail:inbox:{r['in_reply_to']}"
        return item

    def _note_item(self, r: dict, ts: datetime) -> dict:
        return {"source_id": f"notes:{r['path']}:{r['id']}", "timestamp": iso(ts), "content": self.render(r["content"]),
                "title": self.render(r.get("title") or Path(r["path"]).stem),
                "author": self.author(self.meta["protagonist"]), "thread_id": r["path"],
                "metadata": {"path": r["path"]}}

    # --- duty 3 -----------------------------------------------------------------------------------------------

    def records(self, stream: str) -> list[tuple[dict, datetime]]:
        """Every story item and released cue of a stream, with its shifted time."""
        out = [(r, self.times[r["id"]]) for r in self.items if SOURCE_STREAM[r["source"]] == stream]
        out += [(c, self.released[c["id"]]) for c in self.cues
                if c["id"] in self.released and self._cue_stream(c) == stream]
        return out

    async def fetch_window(self, stream: str, since: datetime, until: datetime) -> list[dict]:
        now = self.now()
        return [await self.to_item(stream, (r, ts)) for r, ts in self.records(stream)
                if since <= ts < until and ts <= now]

    # --- duty 4 -----------------------------------------------------------------------------------------------

    def next_cue(self) -> None:
        """Push the next cue now (Enter on stdin, SIGUSR1, or tests)."""
        self._next.set()

    async def watch(self, emit: Emit) -> None:
        if self.cue_mode != "off":
            remove = self._install_triggers()
            try:
                await self._play(emit)
            finally:
                remove()
        await asyncio.Event().wait()

    async def _play(self, emit: Emit) -> None:
        pending = [c for c in self.cues if c["id"] not in self.released]
        if pending and self.cue_mode != "all-now":
            first = pending[0]
            log.info("%d cue(s); next %s at %s (Enter or SIGUSR1 = now)", len(pending), first["id"],
                     "your signal" if self.cue_mode == "manual" else
                     self.due(first).astimezone(self.tz).strftime("%H:%M:%S"))
        for cue in pending:
            if self.cue_mode == "manual":
                await self._next.wait()
            elif self.cue_mode == "timed":
                delay = (self.due(cue) - self.now()).total_seconds()
                if delay > 0:
                    try:
                        await asyncio.wait_for(self._next.wait(), delay)
                    except TimeoutError:
                        pass
            self._next.clear()
            await self.release(cue, emit)
        log.info("all cues pushed")

    async def release(self, cue: dict, emit: Emit) -> None:
        last = max(self.released.values(), default=None)
        ts = self.now()
        if last and ts <= last:      # cues released together keep their order
            ts = last + timedelta(milliseconds=1)
        self.released[cue["id"]] = ts
        stream = self._cue_stream(cue)
        result = await emit(stream, [await self.to_item(stream, (cue, ts))])
        log.info("cue %s -> %s/%s: %s", cue["id"], stream, cue["channel"], {k: v for k, v in result.items() if v})

    def _install_triggers(self) -> Callable[[], None]:
        loop = asyncio.get_running_loop()
        removers = []
        if hasattr(signal, "SIGUSR1"):
            try:
                loop.add_signal_handler(signal.SIGUSR1, self.next_cue)
                removers.append(lambda: loop.remove_signal_handler(signal.SIGUSR1))
            except (NotImplementedError, RuntimeError, ValueError):
                pass
        if sys.stdin and sys.stdin.isatty():
            def read() -> None:     # a daemon thread: a blocked readline must not keep the process alive
                for _ in sys.stdin:
                    loop.call_soon_threadsafe(self.next_cue)
            threading.Thread(target=read, daemon=True).start()
        return lambda: [r() for r in removers]


if __name__ == "__main__":
    main(ReplayAdapter)
