"""An in-memory fake of the Slack Web API methods the adapter uses, as an httpx transport (no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from urllib.parse import parse_qs

import httpx

TEAM_URL = "https://mock-co.slack.com/"


def ts(dt: datetime, seq: int = 0) -> str:
    return f"{int(dt.timestamp())}.{seq:06d}"


class FakeSlack:
    def __init__(self) -> None:
        self.users = {"U1": "Ania Kowalska", "U2": "Marek Nowak"}
        self.channels = {"C1": {"id": "C1", "name": "general", "is_member": True},
                         "C2": {"id": "C2", "name": "proj-atlas", "is_member": False},
                         "C3": {"id": "C3", "name": "random", "is_member": False}}
        self.convs_for_user = {     # what a user token (U1) additionally sees
            "G1": {"id": "G1", "name": "leads", "is_private": True, "is_member": True},
            "D1": {"id": "D1", "is_im": True, "user": "U2"},
            "D9": {"id": "D9", "is_im": True, "user": "USLACKBOT"},
        }
        self.messages: dict[str, list[dict]] = {"C1": [], "C2": [], "C3": [], "G1": [], "D1": [], "D2": [],
                                                "M1": []}   # flat: parents and replies
        self.joined: list[str] = []
        self.calls: list[str] = []
        self.socket_url = ""

    def post(self, channel: str, dt: datetime, text: str, seq: int = 0, **extra) -> dict:
        m = {"type": "message", "ts": ts(dt, seq), "text": text, **extra}
        if "user" not in m and "bot_id" not in m:
            m["user"] = "U1"
        thread = m.get("thread_ts")
        if thread and thread != m["ts"]:
            parent = next(p for p in self.messages[channel] if p["ts"] == thread)
            parent["thread_ts"] = thread
            parent["reply_count"] = parent.get("reply_count", 0) + 1
            parent["latest_reply"] = max(parent.get("latest_reply", "0"), m["ts"], key=float)
        self.messages[channel].append(m)
        return m

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        method = request.url.path.rsplit("/", 1)[-1]
        p = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
        self.calls.append(method)
        self._token = request.headers["Authorization"].removeprefix("Bearer ")
        if self._token.endswith("bad"):
            return self._ok({"ok": False, "error": "invalid_auth"})
        handler = getattr(self, "api_" + method.replace(".", "_"), None)
        if handler is None:
            return self._ok({"ok": False, "error": "unknown_method"})
        return self._ok({"ok": True, **handler(p)})

    @staticmethod
    def _ok(body: dict) -> httpx.Response:
        return httpx.Response(200, content=json.dumps(body))

    def _is_user(self) -> bool:
        return self._token.startswith("xoxp-")

    def api_auth_test(self, p):
        if self._is_user():
            return {"team": "Mock Co", "user": "ania", "url": TEAM_URL, "user_id": "U1"}
        return {"team": "Mock Co", "user": "cortex-bot", "url": TEAM_URL, "user_id": "UBOT"}

    def api_conversations_members(self, p):
        return {"members": ["U1", "U2", "U3"]}

    def api_users_list(self, p):
        return {"members": [{"id": k, "profile": {"real_name": v}} for k, v in self.users.items()]}

    def api_users_info(self, p):
        return {"user": {"id": p["user"], "name": p["user"].lower()}}

    def api_conversations_list(self, p):
        types = p["types"].split(",")
        convs = list(self.channels.values()) + (list(self.convs_for_user.values()) if self._is_user() else [])
        kind = lambda c: ("im" if c.get("is_im") else "mpim" if c.get("is_mpim")   # noqa: E731
                          else "private_channel" if c.get("is_private") else "public_channel")
        return {"channels": [c for c in convs if kind(c) in types]}

    def api_conversations_join(self, p):
        self.joined.append(p["channel"])
        self.channels[p["channel"]]["is_member"] = True
        return {}

    def _range(self, msgs, p):
        lo, hi = float(p.get("oldest", 0)), float(p.get("latest", 9e12))
        return [m for m in msgs if lo <= float(m["ts"]) <= hi]   # we always send inclusive=true

    def api_conversations_history(self, p):
        conv = self.channels.get(p["channel"]) or self.convs_for_user[p["channel"]]
        assert conv.get("is_member", True), "not_in_channel"
        top = [m for m in self.messages[p["channel"]]
               if m.get("thread_ts") in (None, m["ts"]) or m.get("subtype") == "thread_broadcast"]
        return {"messages": sorted(self._range(top, p), key=lambda m: -float(m["ts"])), "has_more": False}

    def api_conversations_replies(self, p):
        msgs = [m for m in self.messages[p["channel"]] if m.get("thread_ts") == p["ts"]]
        parent = [m for m in msgs if m["ts"] == p["ts"]]
        return {"messages": parent + sorted(self._range([m for m in msgs if m["ts"] != p["ts"]], p),
                                            key=lambda m: float(m["ts"]))}

    def api_apps_connections_open(self, p):
        return {"url": self.socket_url}


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=UTC)
