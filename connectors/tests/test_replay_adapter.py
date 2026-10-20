import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from connector_sdk import parse_iso
from dev_hub import DevHub
from replay_adapter import ReplayAdapter
from source_adapter import dry_fetch, run_adapter

NOW = datetime(2026, 11, 20, 9, 0, tzinfo=UTC)          # D0 = 2026-11-20, 10:00 in Vienna
D0_START = datetime(2026, 11, 20, 8, 0, tzinfo=UTC)     # workshop starts 09:00 Vienna
OLD = NOW - timedelta(days=60)


@pytest.fixture(autouse=True)
def env(monkeypatch):
    for k in ("REPLAY_SPEED", "REPLAY_CUES", "REPLAY_STREAMS", "REPLAY_CHANNELS", "REPLAY_LIMIT", "REPLAY_DATASET",
              "REPLAY_CUE_FILE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("REPLAY_D0", "2026-11-20")
    monkeypatch.setenv("REPLAY_START", "09:00")


def adapter(now: datetime = NOW) -> ReplayAdapter:
    return ReplayAdapter(now=lambda: now)


async def fetch_all(a: ReplayAdapter, stream: str, since=OLD, until=NOW + timedelta(days=60)) -> dict[str, dict]:
    return {i["source_id"]: i for i in await a.fetch_window(stream, since, until)}


# --- mapping and time shift -----------------------------------------------------------------------------------

async def test_channel_messages_and_threads():
    items = await fetch_all(adapter(), "channels")
    reply = items["channels:product:ab-f01"]
    # D-40 11:05 Vienna (CEST in October) -> 09:05Z
    assert reply["timestamp"] == "2026-10-11T09:05:00Z"
    assert reply["author"] == {"name": "Tess Kerrigan", "id": "tess.kerrigan"}
    assert reply["reply_to"] == "channels:product:a-01" and reply["thread_id"] == "product"
    assert reply["metadata"] == {"channel": "product", "channel_id": "product", "channel_type": "public",
                                 "team": "Herdlink"}
    assert "url" not in reply


async def test_dms_mail_notes():
    a = adapter()
    dm = (await fetch_all(a, "dms"))["dms:dm-freya:ab-f03"]
    assert dm["metadata"]["channel_id"] == "dm-freya" and dm["metadata"]["channel_type"] == "dm"
    assert dm["participants"] == ["Freya Lund", "Tess Kerrigan"] and dm["thread_id"] == "dm-Freya Lund"

    mail = await fetch_all(a, "mail")
    reply = mail["mail:inbox:f-03"]
    assert reply["reply_to"] == "mail:inbox:f-01" and reply["thread_id"] == mail["mail:inbox:f-01"]["thread_id"]
    assert reply["metadata"]["to"] == ["Petra Ambros <petra.ambros@talbank.example>"]   # persona id resolved
    assert reply["content"].startswith("From: Tess Kerrigan <tess.kerrigan@postbox.example>\n")
    assert "channel_id" not in reply["metadata"]       # one conversation per mail thread, not one inbox

    note = next(iter((await fetch_all(a, "notes")).values()))
    assert note["source_id"] == f"notes:{note['metadata']['path']}:{note['source_id'].rsplit(':', 1)[1]}"
    assert note["title"] and note["author"]["name"] == "Tess Kerrigan"


async def test_date_tokens_are_rendered_relative_to_d0():
    a = adapter()
    assert a.render("by {D+2}, call at {D-1 10:00}") == "by Sun 22 Nov, call at Thu 19 Nov, 10:00"
    for stream in ("channels", "dms", "mail", "notes"):
        assert not any("{D" in i["content"] + i.get("title", "") for i in (await fetch_all(a, stream)).values())


async def test_all_story_items_are_served_and_ids_are_stable():
    a = adapter()
    counts = {s: len(await fetch_all(a, s)) for s in ("channels", "dms", "mail", "notes")}
    assert counts == {"channels": 105, "dms": 27, "mail": 53, "notes": 47}
    first = await fetch_all(a, "channels")
    assert first == await fetch_all(adapter(), "channels")


# --- fetch window, paging, visibility -------------------------------------------------------------------------

async def test_window_is_half_open_and_pages():
    a = adapter()
    items = sorted((await fetch_all(a, "channels")).values(), key=lambda i: (i["timestamp"], i["source_id"]))
    lo, hi = parse_iso(items[10]["timestamp"]), parse_iso(items[20]["timestamp"])
    window = await fetch_all(a, "channels", lo, hi)
    assert set(window) == {i["source_id"] for i in items[10:20]}

    paged = await dry_fetch(a, "channels", OLD, NOW, limit=7)
    assert [i["source_id"] for i in paged] == [i["source_id"] for i in items]


async def test_future_items_stay_hidden():
    a = adapter(now=datetime(2026, 11, 15, 12, 0, tzinfo=UTC))      # D-5
    items = (await fetch_all(a, "channels")).values()
    assert items and all(parse_iso(i["timestamp"]) <= datetime(2026, 11, 15, 12, 0, tzinfo=UTC) for i in items)
    assert len(items) < 105


async def test_slice_env(monkeypatch):
    monkeypatch.setenv("REPLAY_STREAMS", "channels,mail")
    monkeypatch.setenv("REPLAY_CHANNELS", "#noc-incidents,farmers")
    a = adapter()
    assert [s.name for s in a.streams] == ["channels", "mail"]
    channels = await fetch_all(a, "channels")
    assert {i["metadata"]["channel"] for i in channels.values()} == {"noc-incidents", "farmers"}
    assert len(channels) == 26 + 6
    assert {c["channel"] for c in a.cues} == {"noc-incidents", "farmers", "inbox"}

    unlimited = sorted([i["timestamp"] for s in ("channels", "mail") for i in (await fetch_all(a, s)).values()])
    monkeypatch.setenv("REPLAY_LIMIT", "10")
    a = adapter()
    items = sorted([i["timestamp"] for s in ("channels", "mail") for i in (await fetch_all(a, s)).values()])
    assert items == unlimited[-10:]                     # the newest 10 of the selected streams


# --- cues -----------------------------------------------------------------------------------------------------

def test_cue_schedule_and_speed(monkeypatch):
    a = adapter()
    l01 = next(c for c in a.cues if c["id"] == "l-01")
    assert a.start == D0_START and a.due(l01) == D0_START + timedelta(minutes=18)
    assert "l-13" not in {c["id"] for c in a.cues}       # presenter placeholder, not a message
    monkeypatch.setenv("REPLAY_SPEED", "10")
    assert adapter().due(l01) == D0_START + timedelta(minutes=1.8)


async def test_cues_push_on_schedule_and_on_demand(monkeypatch):
    monkeypatch.setenv("REPLAY_SPEED", "6000")          # minute 18 -> 0.18 s
    monkeypatch.delenv("REPLAY_START")
    a = ReplayAdapter()
    pushed: list[tuple[str, dict]] = []

    async def emit(stream, items):
        pushed.extend((stream, i) for i in items)
        return {"accepted": len(items)}

    task = asyncio.create_task(a._play(emit))
    await asyncio.sleep(0.25)
    assert [i["source_id"] for _, i in pushed] == ["channels:noc-incidents:l-01"]
    a.next_cue()                                        # pulls l-02 (due at 0.28 s) forward
    await asyncio.sleep(0.01)
    assert len(pushed) == 2
    await asyncio.wait_for(task, 3)
    assert len(pushed) == len(a.cues) == 12

    mail = next(i for s, i in pushed if s == "mail")
    assert mail["source_id"] == "mail:inbox:l-10" and mail["title"] == "Biscuit vs. the loaf"
    assert mail["author"] == {"name": "Kit Moreau", "email": "kit.moreau@postbox.example"}
    times = [i["timestamp"] for _, i in pushed]
    assert times == sorted(times)
    # released cues are part of later fetches too
    assert "channels:noc-incidents:l-01" in await fetch_all(a, "channels", OLD, datetime.now(UTC) + timedelta(1))


async def test_cue_modes(monkeypatch):
    monkeypatch.setenv("REPLAY_CUES", "all-now")
    a = adapter()
    pushed = []

    async def emit(stream, items):
        pushed.extend(items)
        return {}

    await asyncio.wait_for(a._play(emit), 1)
    assert len(pushed) == 12 and len({i["timestamp"] for i in pushed}) == 12

    monkeypatch.setenv("REPLAY_CUES", "manual")
    a, pushed = adapter(), []
    task = asyncio.create_task(a._play(emit))
    await asyncio.sleep(0.05)
    assert pushed == []
    a.next_cue()
    await asyncio.sleep(0.01)
    assert len(pushed) == 1
    task.cancel()

    monkeypatch.setenv("REPLAY_CUES", "nope")
    with pytest.raises(SystemExit):
        adapter()


# --- against the dev hub --------------------------------------------------------------------------------------

async def test_against_dev_hub(unused_tcp_port, monkeypatch, tmp_path):
    monkeypatch.setenv("REPLAY_SPEED", "6000")
    monkeypatch.delenv("REPLAY_START")
    monkeypatch.setenv("REPLAY_STREAMS", "channels")
    monkeypatch.delenv("REPLAY_D0")                     # today: the whole story is in the past
    hub = DevHub()
    async with hub.serving(port=unused_tcp_port):
        a = ReplayAdapter()
        task = asyncio.create_task(run_adapter(a, f"ws://127.0.0.1:{unused_tcp_port}/connectors", "dev-token",
                                               tmp_path / "state.json"))
        while "replay" not in hub.sessions:
            await asyncio.sleep(0.01)
        now = datetime.now(UTC)
        got = await hub.fetch_window("replay", "channels", now - timedelta(days=60), now, limit=20)
        assert len(got) >= 105
        push = await asyncio.wait_for(hub.pushes.get(), 5)
        assert push["stream"] == "channels" and push["items"][0]["source_id"].endswith(":l-01")
        task.cancel()
