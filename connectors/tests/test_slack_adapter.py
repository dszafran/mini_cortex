import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from websockets.asyncio.server import serve

import slack_adapter
from dev_hub import DevHub
from slack_adapter import SlackAdapter
from source_adapter import SourceAdapter, StreamSpec, run_adapter
from tests.fake_slack import TEAM_URL, FakeSlack, at, ts


@pytest.fixture
def fake(monkeypatch):
    for k in ("SLACK_APP_TOKEN", "SLACK_CHANNELS", "SLACK_JOIN_CHANNELS", "SLACK_TOKEN", "SLACK_TOKEN_TYPE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setenv("SLACK_POLL_SECONDS", "0.1")
    f = FakeSlack()
    f.post("C1", at(1, 9), "<@U2> has joined the channel", subtype="channel_join", user="U2")
    f.post("C1", at(1, 10), "Welcome <@U2>! Spec: <https://example.com/spec|spec doc> &amp; <#C2>")
    parent = f.post("C2", at(5, 10), "Launch date proposal: Oct 28", user="U1")
    f.post("C2", at(5, 11), "QA can make it", user="U2", thread_ts=parent["ts"])
    f.post("C2", at(12, 9), "Late reply in an old thread", user="U2", thread_ts=parent["ts"])
    f.post("C2", at(12, 10), "Decision: launch moves to Nov 4", username="Ania Kowalska", bot_id="B1",
           subtype="bot_message", edited={"user": "B1", "ts": ts(at(12, 11))})
    f.post("C2", at(12, 12), "", user="U1", subtype="file_share", files=[{"name": "plan.pdf"}])
    return f


def make_adapter(fake: FakeSlack) -> SlackAdapter:
    a = SlackAdapter()
    a.slack.http = httpx.AsyncClient(base_url="https://slack.test/api/", transport=fake.transport())
    return a


# --- duties 1-3 without a hub --------------------------------------------------------------------------------

async def test_authenticate_joins_public_channels(fake, monkeypatch):
    monkeypatch.setenv("SLACK_CHANNELS", "general,proj-atlas")
    a = make_adapter(fake)
    await a.authenticate()
    assert set(a.convs) == {"C1", "C2"} and fake.joined == ["C2"]       # C1 already member, C3 filtered out


async def test_fetch_window_maps_messages(fake):
    a = make_adapter(fake)
    await a.authenticate()
    items = {i["content"]: i for i in await a.fetch_window("channels", at(1, 0), at(13, 0))}
    assert "<@U2> has joined the channel" not in items and len(items) == 6

    welcome = items["Welcome @Marek Nowak! Spec: spec doc (https://example.com/spec) & #proj-atlas"]
    assert welcome["source_id"] == f"channels:C1:{ts(at(1, 10))}"
    assert welcome["timestamp"] == "2026-10-01T10:00:00Z" and welcome["author"] == {"name": "Ania Kowalska",
                                                                                     "id": "U1"}
    assert welcome["url"] == f"{TEAM_URL}archives/C1/p{ts(at(1, 10)).replace('.', '')}"

    reply = items["QA can make it"]
    assert reply["reply_to"] == f"channels:C2:{ts(at(5, 10))}" and reply["thread_id"] == "proj-atlas"
    assert "?thread_ts=" in reply["url"]

    persona = items["Decision: launch moves to Nov 4"]
    assert persona["author"] == {"name": "Ania Kowalska", "id": "B1"}
    assert persona["updated_at"] == "2026-10-12T11:00:00Z"
    assert "[attached file: plan.pdf]" in items


async def test_window_is_half_open_and_finds_replies_in_older_threads(fake):
    a = make_adapter(fake)
    await a.authenticate()
    items = await a.fetch_window("channels", at(12, 0), at(12, 12))
    assert sorted(i["content"] for i in items) == ["Decision: launch moves to Nov 4", "Late reply in an old thread"]


async def test_paging_and_coverage(fake, monkeypatch):
    monkeypatch.setenv("SLACK_HISTORY_DAYS", "90")
    a = make_adapter(fake)
    await a.authenticate()
    p1 = await a.fetch("channels", at(1, 0), at(13, 0), None, 4)
    p2 = await a.fetch("channels", at(1, 0), at(13, 0), p1.next_cursor, 4)
    assert len(p1.items) == 4 and len(p2.items) == 2 and not p2.has_more
    keys = [(i["timestamp"], i["source_id"]) for i in p1.items + p2.items]
    assert keys == sorted(keys)
    assert p1.coverage is None
    old = await a.fetch("channels", datetime.now(UTC) - timedelta(days=200), datetime.now(UTC), None, 50)
    assert old.coverage and old.coverage["since"] > (datetime.now(UTC) - timedelta(days=91)).isoformat()
    # An item older than the configured limit was served (seen live: a 95-day-old imported message) -> no claim.
    fake.post("C1", datetime.now(UTC) - timedelta(days=95), "older than 90 days, still visible")
    a._window_cache.clear()
    old = await a.fetch("channels", datetime.now(UTC) - timedelta(days=200), datetime.now(UTC), None, 50)
    assert old.coverage is None and any("still visible" in i["content"] for i in old.items)


async def test_bad_token_exits(fake, monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-bad")
    with pytest.raises(SystemExit, match="invalid_auth"):
        await make_adapter(fake).authenticate()


# --- against the dev hub ------------------------------------------------------------------------------------

async def start(adapter: SourceAdapter, port: int, tmp_path) -> asyncio.Task:
    task = asyncio.create_task(run_adapter(adapter, f"ws://127.0.0.1:{port}/connectors", "dev-token",
                                           tmp_path / "state.json"))
    for _ in range(100):
        if adapter.connector and adapter.connector.connected.is_set():
            return task
        await asyncio.sleep(0.05)
    raise TimeoutError("adapter did not connect")


async def wait_for_push(hub: DevHub) -> dict:
    return await asyncio.wait_for(hub.pushes.get(), 5)


async def test_backfill_then_poll_push(fake, unused_tcp_port, tmp_path):
    hub = DevHub()
    a = make_adapter(fake)
    async with hub.serving(port=unused_tcp_port):
        task = await start(a, unused_tcp_port, tmp_path)
        got = await hub.fetch_window("slack", "channels", at(1, 0), at(13, 0), limit=2)
        assert len(got) == 6 and len(hub.items) == 6

        # The first poll pushes the overlap window only (cursor starts at "now"); then new posts arrive.
        now = datetime.now(UTC)
        fake.post("C1", now, "Heads-up: payment sandbox is down until 3pm", user="U2")
        push = await wait_for_push(hub)
        assert [i["content"] for i in push["items"]] == ["Heads-up: payment sandbox is down until 3pm"]
        assert push["result"]["accepted"] == 1

        # An edit: same ts -> same source_id -> the hub reports an update.
        fake.messages["C1"][-1].update(text="Heads-up: sandbox is back", edited={"user": "U2", "ts": ts(now, 1)})
        push = await wait_for_push(hub)
        assert push["result"]["updated"] == 1
        assert json.loads((tmp_path / "state.json").read_text())["push_cursor"].startswith(now.strftime("%Y-%m-%d"))
        task.cancel()


async def test_socket_mode_push(fake, unused_tcp_port, unused_tcp_port_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-test")
    acks: list[str] = []
    now_ts = ts(datetime.now(UTC))
    events = [
        {"type": "hello"},
        {"type": "events_api", "envelope_id": "e1", "payload": {"event": {
            "type": "message", "channel": "C2", "user": "U1", "ts": now_ts, "text": "Live from Socket Mode"}}},
        {"type": "events_api", "envelope_id": "e2", "payload": {"event": {
            "type": "message", "channel": "C2", "subtype": "message_changed",
            "message": {"type": "message", "user": "U1", "ts": now_ts, "text": "Live (edited)",
                        "edited": {"user": "U1", "ts": now_ts}}}}},
        {"type": "events_api", "envelope_id": "e3", "payload": {"event": {
            "type": "message", "channel": "C2", "subtype": "message_deleted", "deleted_ts": now_ts}}},
    ]

    async def socket(ws):
        for e in events:
            await ws.send(json.dumps(e))
        async for raw in ws:
            acks.append(json.loads(raw)["envelope_id"])
        await asyncio.Future()

    sock_port = unused_tcp_port_factory()
    fake.socket_url = f"ws://127.0.0.1:{sock_port}/link"
    hub = DevHub()
    async with serve(socket, "127.0.0.1", sock_port), hub.serving(port=unused_tcp_port):
        task = await start(make_adapter(fake), unused_tcp_port, tmp_path)
        contents = []
        while len(contents) < 2:
            contents += [i["content"] for i in (await wait_for_push(hub))["items"]]
        assert contents == ["Live from Socket Mode", "Live (edited)"]
        await asyncio.sleep(0.1)
        assert acks == ["e1", "e2", "e3"]
        assert len([k for k in hub.items if k[1] == f"channels:C2:{now_ts}"]) == 1
        task.cancel()


# --- user token: own channels + DMs, never joins ----------------------------------------------------------

@pytest.fixture
def user_fake(fake, monkeypatch):
    monkeypatch.setenv("SLACK_TOKEN", "xoxp-test")      # wins over SLACK_BOT_TOKEN
    fake.post("G1", at(12, 9), "Private: budget is approved", user="U2")
    fake.post("D1", at(12, 10), "hey, can you review my PR?", user="U2")
    fake.post("D1", at(12, 11), "sure, after lunch", user="U1")
    return fake


def test_token_type_detection():
    assert slack_adapter.token_type("xoxb-1") == "bot" and slack_adapter.token_type("xoxp-1") == "user"
    assert slack_adapter.token_type("xoxe-1", "user") == "user"
    with pytest.raises(SystemExit, match="SLACK_TOKEN_TYPE"):
        slack_adapter.token_type("weird")


async def test_bot_token_has_no_dm_stream(fake):
    a = make_adapter(fake)
    await a.authenticate()
    assert [s.name for s in a.streams] == ["channels"]
    assert "D1" not in a.convs and "G1" not in a.convs


async def test_user_token_reads_own_channels_and_dms_without_joining(user_fake):
    a = make_adapter(user_fake)
    await a.authenticate()
    assert [s.name for s in a.streams] == ["channels", "dms"]
    assert user_fake.joined == []                                  # never joins in the person's name
    assert {c for c, v in a.convs.items() if v["stream"] == "channels"} == {"C1", "G1"}   # C2/C3: not a member
    assert {c for c, v in a.convs.items() if v["stream"] == "dms"} == {"D1"}              # D9: Slackbot, skipped

    dms = await a.fetch_window("dms", at(12, 0), at(13, 0))
    assert [i["content"] for i in sorted(dms, key=lambda i: i["timestamp"])] == [
        "hey, can you review my PR?", "sure, after lunch"]
    dm = dms[0]
    assert dm["source_id"].startswith("dms:D1:") and dm["thread_id"] == "dm-Marek Nowak"
    assert dm["participants"] == ["Ania Kowalska", "Marek Nowak"] and dm["metadata"]["channel_type"] == "im"
    private = await a.fetch_window("channels", at(12, 0), at(13, 0))
    assert [i["metadata"]["channel_type"] for i in private] == ["private"]


async def test_new_dm_is_picked_up_from_an_event(user_fake, monkeypatch):
    monkeypatch.setattr(slack_adapter, "EVENT_REFRESH_SECONDS", 0.2)   # the event arrives right after start-up
    a = make_adapter(user_fake)
    await a.authenticate()
    user_fake.convs_for_user["M1"] = {"id": "M1", "is_mpim": True, "name": "mpdm-ania--marek--u3-1"}
    routed = await a._message_event({"type": "message", "channel": "M1", "user": "U2", "ts": ts(at(12, 12)),
                                     "text": "group chat!"})
    assert routed and routed[0] == "dms"
    item = await a.to_item(*routed)
    assert item["thread_id"] == "dm-Marek Nowak, u3"
    assert item["participants"] == ["Ania Kowalska", "Marek Nowak", "u3"]
    assert await a._message_event({"type": "message", "channel": "X9", "ts": "1.0", "text": "?"}) is None


# --- the interface itself, with a minimal adapter ------------------------------------------------------------

class ListAdapter(SourceAdapter):
    name = "list"
    streams = [StreamSpec("notes", kind="note", push=False)]

    def __init__(self, n: int):
        super().__init__()
        self.notes = [(f"n{i}", datetime(2026, 10, 1, 10, i, tzinfo=UTC)) for i in range(n)]
        self.window_calls = 0

    async def to_item(self, stream, native):
        name, when = native
        return {"source_id": f"notes:{name}", "timestamp": when.isoformat().replace("+00:00", "Z"),
                "content": f"note {name}"}

    async def fetch_window(self, stream, since, until):
        self.window_calls += 1
        return [await self.to_item(stream, n) for n in reversed(self.notes) if since <= n[1] < until]


async def test_generic_adapter_pages_from_one_cached_window(unused_tcp_port, tmp_path):
    hub = DevHub()
    a = ListAdapter(7)
    async with hub.serving(port=unused_tcp_port):
        task = await start(a, unused_tcp_port, tmp_path)
        got = await hub.fetch_window("list", "notes", datetime(2026, 10, 1, 10, 1, tzinfo=UTC),
                                     datetime(2026, 10, 1, 10, 6, tzinfo=UTC), limit=2)
        assert [i["source_id"] for i in got] == ["notes:n1", "notes:n2", "notes:n3", "notes:n4", "notes:n5"]
        assert a.window_calls == 1          # 3 pages, one source query
        task.cancel()


def test_module_has_entry_point():
    assert callable(slack_adapter.main)
