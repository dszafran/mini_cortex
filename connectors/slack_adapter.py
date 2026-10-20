"""Slack connector for mini-cortex. Works with a bot token or a user token; the token prefix decides which.

    uv run python slack_adapter.py fetch --days 30               # dry run: print IngestItems, no hub needed
    uv run python slack_adapter.py fetch --stream dms --days 30  # your DMs (user token only)
    uv run python slack_adapter.py serve                         # talk to the hub (dev_hub.py or mini-cortex)

Token types (SLACK_TOKEN; SLACK_TOKEN_TYPE=auto|bot|user overrides the prefix detection):
  bot   xoxb-…  stream `channels`: public channels, joined automatically (SLACK_JOIN_CHANNELS=1)
  user  xoxp-…  stream `channels`: public and private channels YOU are in (never joins anything in your name)
                stream `dms`:      your direct messages and group DMs. A bot can never read these: Slack only lets
                                   the people in a conversation read it.

The four duties (source_adapter.py):
  1. authenticate  auth.test, list conversations, cache users. The list is refreshed while running, so new
                   channels and DMs are picked up without a restart
  2. map           message -> IngestItem; source_id = "<stream>:<conversation id>:<ts>" (ts is Slack's message id,
                   so it survives edits); <@U…>/<#C…>/<url|label> are rendered into readable text
  3. fetch         conversations.history filters by time natively; thread replies come from conversations.replies
                   for every thread whose parent is at most SLACK_THREAD_LOOKBACK_DAYS older than the window
  4. push          Socket Mode when SLACK_APP_TOKEN (xapp-…) is set: real time, incl. replies and edits.
                   Otherwise polling every SLACK_POLL_SECONDS (no app event config needed).

Scopes and events: see slack_manifest.yaml. Socket Mode also needs an app-level token with connections:write.

Settings (env or .env): SLACK_TOKEN (or SLACK_BOT_TOKEN), SLACK_TOKEN_TYPE (auto), SLACK_APP_TOKEN,
SLACK_CHANNELS (comma-separated channel names, default all), SLACK_JOIN_CHANNELS (1, bot only),
SLACK_POLL_SECONDS (20), SLACK_THREAD_LOOKBACK_DAYS (14), SLACK_HISTORY_DAYS (90 = Slack Free's visible
history; 0 = unlimited), SLACK_API_URL (for tests).
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import random
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from connector_sdk import RateLimited, SourceUnavailable, iso
from source_adapter import Emit, SourceAdapter, StreamSpec, main

log = logging.getLogger("slack")
STREAM = "channels"
DM_STREAM = "dms"
# Message subtypes that carry real content; joins, topic changes, pins … are skipped.
CONTENT_SUBTYPES = {None, "bot_message", "thread_broadcast", "file_share", "me_message"}
AUTH_ERRORS = {"invalid_auth", "not_authed", "token_revoked", "token_expired", "account_inactive"}
POLL_OVERLAP = timedelta(minutes=2)
SYSTEM_USERS = {"USLACKBOT", "USLACK"}   # DMs from Slack itself: trial notices, tips. Not worth remembering
REFRESH_SECONDS = 60         # re-read the conversation list at most this often while polling
EVENT_REFRESH_SECONDS = 5     # ... and at most this often when an event arrives for an unknown conversation


class SlackError(RuntimeError):
    def __init__(self, method: str, error: str):
        super().__init__(f"{method}: {error}")
        self.error = error


def ts_to_dt(ts: str) -> datetime:
    return datetime.fromtimestamp(float(ts), UTC)


def dt_to_ts(dt: datetime) -> str:
    return f"{dt.timestamp():.6f}"


def token_type(token: str, override: str = "auto") -> str:
    if override in ("bot", "user"):
        return override
    if token.startswith("xoxp-"):
        return "user"
    if token.startswith("xoxb-"):
        return "bot"
    raise SystemExit(f"cannot tell the Slack token type from its prefix {token[:5]!r}; "
                     "set SLACK_TOKEN_TYPE=bot or SLACK_TOKEN_TYPE=user")


# --- Slack Web API --------------------------------------------------------------------------------------------

class SlackClient:
    def __init__(self, token: str, base_url: str = "https://slack.com/api/"):
        self.token = token
        self.http = httpx.AsyncClient(base_url=base_url.rstrip("/") + "/", timeout=30)

    async def call(self, method: str, token: str | None = None, **params: Any) -> dict:
        data = {k: v for k, v in params.items() if v is not None}
        headers = {"Authorization": f"Bearer {token or self.token}"}
        for _ in range(3):
            try:
                r = await self.http.post(method, data=data, headers=headers)
            except httpx.HTTPError as e:
                raise SourceUnavailable(data={"detail": f"{method}: {e}"}) from e
            if r.status_code == 429:       # Slack tells us how long to wait; short waits are absorbed here
                wait = int(r.headers.get("Retry-After", "5"))
                if wait > 30:
                    raise RateLimited(data={"retry_after_ms": wait * 1000})
                log.warning("Slack rate limit on %s, waiting %d s", method, wait)
                await asyncio.sleep(wait)
                continue
            if r.status_code >= 500:
                raise SourceUnavailable(data={"detail": f"{method}: HTTP {r.status_code}"})
            body = r.json()
            if not body.get("ok"):
                raise SlackError(method, body.get("error", "unknown_error"))
            return body
        raise RateLimited(data={"retry_after_ms": 30_000})

    async def paginate(self, method: str, key: str, **params: Any) -> list[dict]:
        out, cursor = [], None
        while True:
            body = await self.call(method, cursor=cursor, limit=200, **params)
            out += body.get(key, [])
            cursor = (body.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                return out


# --- the adapter ----------------------------------------------------------------------------------------------

class SlackAdapter(SourceAdapter):
    name = "slack"
    version = "0.2.0"

    def __init__(self) -> None:
        super().__init__()
        env = os.environ.get
        token = env("SLACK_TOKEN") or env("SLACK_BOT_TOKEN")
        if not token:
            raise SystemExit("SLACK_TOKEN is not set (bot token xoxb-… or user token xoxp-…; see the module docstring)")
        self.token_type = token_type(token, env("SLACK_TOKEN_TYPE", "auto").lower())
        self.slack = SlackClient(token, env("SLACK_API_URL", "https://slack.com/api/"))
        self.app_token = env("SLACK_APP_TOKEN") or None
        self.channel_filter = {c.strip().lstrip("#") for c in env("SLACK_CHANNELS", "").split(",") if c.strip()}
        # Joining is fine for a bot; a user token must never join channels in the person's name.
        self.join = self.token_type == "bot" and env("SLACK_JOIN_CHANNELS", "1") not in ("0", "false", "no")
        self.poll_seconds = float(env("SLACK_POLL_SECONDS", "20"))
        self.thread_lookback = timedelta(days=float(env("SLACK_THREAD_LOOKBACK_DAYS", "14")))
        self.history_days = float(env("SLACK_HISTORY_DAYS", "90"))

        if self.token_type == "user":
            self.streams = [StreamSpec(STREAM, kind="chat", description="Public and private channels you are in",
                                       time_filter="native", max_limit=200),
                            StreamSpec(DM_STREAM, kind="chat", description="Your direct messages and group DMs",
                                       time_filter="native", max_limit=200)]
        else:
            self.streams = [StreamSpec(STREAM, kind="chat", description="Messages from public Slack channels",
                                       time_filter="native", max_limit=200)]
        self.team: dict = {}
        self.convs: dict[str, dict] = {}          # conversation id -> {"stream", "name", "type", "participants"}
        self.all_channels: dict[str, str] = {}    # channel id -> name (for rendering <#C…>)
        self.users: dict[str, str] = {}           # user id -> display name
        self._pushed: dict[str, tuple] = {}       # source_id -> (content, updated_at) last pushed, for diffs
        self._refreshed_at = float("-inf")

    # --- duty 1: authenticate and discover --------------------------------------------------------------------

    async def authenticate(self) -> None:
        try:
            self.team = await self.slack.call("auth.test")
        except SlackError as e:
            raise SystemExit(f"Slack rejected SLACK_TOKEN: {e.error}") from e
        self.display_name = self.display_name or f"Slack ({self.team.get('team')})"
        for u in await self.slack.paginate("users.list", "members"):
            self.users[u["id"]] = self._user_name(u)
        await self.refresh_conversations()
        missing = self.channel_filter - {c["name"] for c in self.convs.values() if c["stream"] == STREAM}
        if missing:
            log.warning("SLACK_CHANNELS not found or not readable: %s", ", ".join(sorted(missing)))
        for spec in self.streams:
            names = sorted(c["name"] for c in self.convs.values() if c["stream"] == spec.name)
            log.info("%s token for %s as %s; %s: %d (%s)", self.token_type, self.team.get("team"),
                     self.team.get("user"), spec.name, len(names), ", ".join(names))

    async def refresh_conversations(self) -> list[str]:
        """(Re)list readable conversations; returns the names of ones that are new since the last call."""
        types = "public_channel" if self.token_type == "bot" else "public_channel,private_channel,im,mpim"
        added = []
        for ch in await self.slack.paginate("conversations.list", "channels", types=types, exclude_archived="true"):
            if ch.get("is_im") and ch.get("user") in SYSTEM_USERS:
                continue
            if ch.get("is_im") or ch.get("is_mpim"):
                conv = await self._dm(ch)
            else:
                self.all_channels[ch["id"]] = ch["name"]
                if self.channel_filter and ch["name"] not in self.channel_filter:
                    continue
                if not ch.get("is_member"):
                    if not self.join:
                        continue
                    await self.slack.call("conversations.join", channel=ch["id"])
                conv = {"stream": STREAM, "name": ch["name"], "type": "private" if ch.get("is_private") else "public"}
            if ch["id"] not in self.convs:
                added.append(conv["name"])
            self.convs[ch["id"]] = conv
        self._refreshed_at = asyncio.get_running_loop().time()
        return added

    async def maybe_refresh(self, min_age: float = REFRESH_SECONDS) -> None:
        if asyncio.get_running_loop().time() - self._refreshed_at < min_age:
            return
        try:
            added = await self.refresh_conversations()
        except (SlackError, RateLimited, SourceUnavailable) as e:
            log.warning("could not refresh the conversation list: %s", e)
            return
        if added and self.connector:      # don't log on the first (start-up) listing in dry runs
            log.info("now also reading: %s", ", ".join(added))

    async def _dm(self, ch: dict) -> dict:
        me = self.team.get("user_id")
        if ch.get("is_im"):
            other = await self.user_name(ch["user"])
            return {"stream": DM_STREAM, "name": f"dm-{other}", "type": "im",
                    "participants": [await self.user_name(me), other] if me != ch["user"] else [other]}
        try:
            members = await self.slack.paginate("conversations.members", "members", channel=ch["id"])
        except SlackError:
            members = []
        names = [await self.user_name(u) for u in members]
        others = [n for u, n in zip(members, names) if u != me]
        return {"stream": DM_STREAM, "name": "dm-" + ", ".join(others) if others else ch.get("name", ch["id"]),
                "type": "mpim", "participants": names}

    @staticmethod
    def _user_name(u: dict) -> str:
        p = u.get("profile") or {}
        name = p.get("real_name") or p.get("display_name") or u.get("real_name") or u.get("name") or u["id"]
        return " ".join(name.split())          # "Tess  Kerrigan" -> "Tess Kerrigan": one spelling per person

    async def user_name(self, user_id: str) -> str:
        if user_id not in self.users:          # someone who joined after start-up
            try:
                self.users[user_id] = self._user_name((await self.slack.call("users.info", user=user_id))["user"])
            except SlackError:
                self.users[user_id] = user_id
        return self.users[user_id]

    # --- duty 2: map a message to an IngestItem ---------------------------------------------------------------

    async def to_item(self, stream: str, native: tuple[str, dict]) -> dict | None:
        conv_id, m = native
        if m.get("subtype") not in CONTENT_SUBTYPES:
            return None
        content = await self.render(m.get("text") or "")
        files = [f.get("name") or f.get("title") for f in m.get("files") or []]
        if files:
            content = (content + "\n" if content else "") + "\n".join(f"[attached file: {n}]" for n in files)
        if not content.strip():
            return None

        conv = self.convs.get(conv_id) or {"name": conv_id, "type": "unknown"}
        # Only stable fields: volatile ones (reactions, reply counts) would make every re-fetch look like an edit.
        metadata: dict[str, Any] = {"channel": conv["name"], "channel_id": conv_id, "channel_type": conv["type"],
                                    "team": self.team.get("team")}
        if m.get("user"):
            author = {"name": await self.user_name(m["user"]), "id": m["user"]}
        else:   # bot message; with chat:write.customize the persona name is in `username`
            author = {"name": m.get("username") or (m.get("bot_profile") or {}).get("name") or "bot"}
            if m.get("bot_id"):
                author["id"] = m["bot_id"]
                metadata["posted_via"] = "bot"

        ts, thread_ts = m["ts"], m.get("thread_ts")
        url = f"{self.team.get('url', '')}archives/{conv_id}/p{ts.replace('.', '')}"
        item = {"source_id": f"{stream}:{conv_id}:{ts}", "timestamp": iso(ts_to_dt(ts)), "content": content,
                "author": author, "thread_id": conv["name"], "url": url, "metadata": metadata}
        if conv.get("participants"):
            item["participants"] = conv["participants"]
        if thread_ts and thread_ts != ts:
            item["reply_to"] = f"{stream}:{conv_id}:{thread_ts}"
            item["url"] = f"{url}?thread_ts={thread_ts}&cid={conv_id}"
        if (m.get("edited") or {}).get("ts"):
            item["updated_at"] = iso(ts_to_dt(m["edited"]["ts"]))
        return item

    async def render(self, text: str) -> str:
        """Slack mrkdwn -> plain text: resolve mentions, channel links and <url|label>; unescape &amp; &lt; &gt;."""
        out, pos = [], 0
        for match in re.finditer(r"<([^<>]+)>", text):
            out.append(text[pos:match.start()])
            target, _, label = match.group(1).partition("|")
            if target.startswith("@"):
                out.append("@" + (label or await self.user_name(target[1:])))
            elif target.startswith("#"):
                out.append("#" + (label or self.all_channels.get(target[1:], target[1:])))
            elif target.startswith("!"):           # <!here>, <!channel>, <!subteam^S…|@team>, <!date^…|fallback>
                cmd = target[1:].split("^")[0]
                out.append(label if label else f"@{cmd}")
            else:
                url = target.removeprefix("mailto:")
                out.append(f"{label} ({url})" if label and label != url else url)
            pos = match.end()
        out.append(text[pos:])
        return html.unescape("".join(out))

    # --- duty 3: fetch a window -------------------------------------------------------------------------------

    async def fetch_window(self, stream: str, since: datetime, until: datetime) -> list[dict]:
        await self.maybe_refresh()
        items: dict[str, dict] = {}
        for conv_id in [c for c, conv in self.convs.items() if conv["stream"] == stream]:
            for m in await self.channel_window(conv_id, since, until):
                item = await self.to_item(stream, (conv_id, m))
                if item:
                    items[item["source_id"]] = item   # thread_broadcast shows up twice: dedupe
        return list(items.values())

    async def channel_window(self, channel_id: str, since: datetime, until: datetime) -> list[dict]:
        """Top-level messages in [since, until) plus replies in that window, also in threads started earlier."""
        lo, hi = since.timestamp(), until.timestamp()
        parents = await self.slack.paginate("conversations.history", "messages", channel=channel_id,
                                            oldest=dt_to_ts(since - self.thread_lookback), latest=dt_to_ts(until),
                                            inclusive="true")
        out = [m for m in parents if lo <= float(m["ts"]) < hi]
        for p in parents:
            if p.get("reply_count") and float(p.get("latest_reply", p["ts"])) >= lo:
                replies = await self.slack.paginate("conversations.replies", "messages", channel=channel_id,
                                                    ts=p["ts"], oldest=dt_to_ts(since), latest=dt_to_ts(until),
                                                    inclusive="true")
                out += [r for r in replies if r["ts"] != p["ts"] and lo <= float(r["ts"]) < hi]
        return out

    def coverage(self, stream: str, since: datetime) -> dict | None:
        if self.history_days <= 0:
            return None
        visible_from = datetime.now(UTC) - timedelta(days=self.history_days)
        return {"since": iso(visible_from)} if since < visible_from else None

    # --- duty 4: push new and edited messages -----------------------------------------------------------------

    async def watch(self, emit: Emit) -> None:
        if self.app_token:
            await self._socket_mode(emit)
        else:
            log.info("no SLACK_APP_TOKEN: polling every %.0f s", self.poll_seconds)
            while True:
                await self._poll_once(emit)
                await asyncio.sleep(self.poll_seconds)

    async def _poll_once(self, emit: Emit) -> None:
        """Push everything new or changed since the push cursor (minus a small overlap). Also used as catch-up."""
        cursor = self.load_cursor()
        now = datetime.now(UTC)
        for spec in self.streams:
            try:
                items = await self.fetch_window(spec.name, cursor - POLL_OVERLAP, now + timedelta(seconds=1))
            except (SlackError, RateLimited, SourceUnavailable) as e:
                log.warning("poll of %s failed: %s", spec.name, e)
                continue
            await self._emit(emit, spec.name, sorted(items, key=lambda i: (i["timestamp"], i["source_id"])))

    async def _emit(self, emit: Emit, stream: str, items: list[dict]) -> None:
        """Push items that are new or changed since we last pushed them; advance the cursor after the ack."""
        items = [i for i in items if self._pushed.get(i["source_id"]) != (i["content"], i.get("updated_at"))]
        if not items:
            return
        result = await emit(stream, items)                   # returns once the hub has stored them
        log.info("pushed %d item(s) to %s: %s", len(items), stream, {k: v for k, v in result.items() if v})
        for i in items:
            self._pushed[i["source_id"]] = (i["content"], i.get("updated_at"))
        newest = max(datetime.fromisoformat(i["timestamp"].replace("Z", "+00:00")) for i in items)
        if newest > self.load_cursor():
            self.save_cursor(newest)                         # advance only after the ack

    async def _socket_mode(self, emit: Emit) -> None:
        queue: asyncio.Queue[dict | None] = asyncio.Queue()   # Slack events; None = run a catch-up poll

        async def worker() -> None:   # one at a time, in arrival order, so the cursor never skips ahead
            while True:
                event = await queue.get()
                if event is None:
                    await self._poll_once(emit)
                elif routed := await self._message_event(event):
                    stream, native = routed
                    if item := await self.to_item(stream, native):
                        await self._emit(emit, stream, [item])

        worker_task = asyncio.create_task(worker())
        attempt = 0
        try:
            while True:
                try:
                    url = (await self.slack.call("apps.connections.open", token=self.app_token))["url"]
                    async with connect(url) as ws:
                        async for raw in ws:
                            env = json.loads(raw)
                            if env.get("envelope_id"):          # ack first, or Slack re-delivers
                                await ws.send(json.dumps({"envelope_id": env["envelope_id"]}))
                            kind = env.get("type")
                            if kind == "hello":
                                attempt = 0
                                log.info("Socket Mode connected; catching up on what we missed")
                                queue.put_nowait(None)
                            elif kind == "disconnect":
                                break
                            elif kind == "events_api":
                                queue.put_nowait((env.get("payload") or {}).get("event") or {})
                except SlackError as e:
                    if e.error in AUTH_ERRORS or e.error == "not_allowed_token_type":
                        raise SystemExit(f"SLACK_APP_TOKEN rejected: {e.error}") from e
                    log.warning("Socket Mode: %s", e)
                except (ConnectionClosed, OSError, InvalidHandshake, TimeoutError, SourceUnavailable,
                        RateLimited) as e:
                    log.warning("Socket Mode connection lost: %s", e)
                delay = min(30.0, 2.0 ** attempt) * random.uniform(0.8, 1.2)
                attempt += 1
                await asyncio.sleep(delay)
        finally:
            worker_task.cancel()

    async def _message_event(self, event: dict) -> tuple[str, tuple[str, dict]] | None:
        """A Slack `message` event -> (stream, (conversation id, message)), or None to ignore it."""
        conv_id = event.get("channel")
        if event.get("type") != "message" or not conv_id:
            return None
        if conv_id not in self.convs:         # a DM or channel that appeared after start-up
            # Wait out the throttle rather than skip it: nothing re-delivers this event if we drop it.
            wait = self._refreshed_at + EVENT_REFRESH_SECONDS - asyncio.get_running_loop().time()
            if wait > 0:
                await asyncio.sleep(wait)
            await self.maybe_refresh(EVENT_REFRESH_SECONDS)
            if conv_id not in self.convs:
                return None
        stream = self.convs[conv_id]["stream"]
        subtype = event.get("subtype")
        if subtype == "message_changed":            # an edit: same ts -> same source_id, hub updates it
            return stream, (conv_id, event["message"])
        if subtype == "message_deleted":            # protocol v1 does not propagate deletes (PROTOCOL.md §11)
            log.info("message %s deleted in Slack; not propagated", event.get("deleted_ts"))
            return None
        return stream, (conv_id, event)


if __name__ == "__main__":
    main(SlackAdapter)
